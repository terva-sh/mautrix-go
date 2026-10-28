// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package crypto

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type localMegolmFixture struct {
	sender, receiver *OlmMachine
	event            *event.Event
	requests         atomic.Int32
}

func newLocalMegolmFixture(t *testing.T) *localMegolmFixture {
	t.Helper()
	f := &localMegolmFixture{sender: newMachine(t, "@sender:example.invalid"), receiver: newMachine(t, "@receiver:example.invalid")}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_matrix/client/v3/keys/query" {
			t.Errorf("unexpected request: %s", r.URL.Path)
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		f.requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mautrix.RespQueryKeys{DeviceKeys: map[id.UserID]map[id.DeviceID]mautrix.DeviceKeys{
			f.sender.Client.UserID: {f.sender.Client.DeviceID: *f.sender.account.getInitialKeys(f.sender.Client.UserID, f.sender.Client.DeviceID)},
		}})
	}))
	t.Cleanup(server.Close)
	var err error
	f.receiver.Client.HomeserverURL, err = url.Parse(server.URL)
	require.NoError(t, err)
	out, err := f.sender.newOutboundGroupSession(t.Context(), "!local:example.invalid")
	require.NoError(t, err)
	in, err := NewInboundGroupSession(f.sender.account.IdentityKey(), f.sender.account.SigningKey(), out.RoomID, out.Internal.Key(), 0, 0, nil, false)
	require.NoError(t, err)
	require.NoError(t, f.receiver.CryptoStore.PutGroupSession(t.Context(), in))
	ciphertext, err := out.Internal.Encrypt([]byte(`{"room_id":"!local:example.invalid","type":"m.room.message","content":{"msgtype":"m.text","body":"synthetic"}}`))
	require.NoError(t, err)
	f.event = &event.Event{ID: "$local", RoomID: out.RoomID, Sender: f.sender.Client.UserID, Type: event.EventEncrypted,
		Content: event.Content{Parsed: &event.EncryptedEventContent{Algorithm: id.AlgorithmMegolmV1, SessionID: out.ID(), SenderKey: f.sender.account.IdentityKey(), MegolmCiphertext: ciphertext}}}
	return f
}

func TestDecryptMegolmEventLocalPreservesDefaultFetch(t *testing.T) {
	for _, next := range []string{"megolm", "olm"} {
		t.Run(next, func(t *testing.T) {
			f := newLocalMegolmFixture(t)
			plain, err := f.receiver.DecryptMegolmEventLocal(t.Context(), f.event)
			require.NoError(t, err)
			require.Equal(t, "synthetic", plain.Content.AsMessage().Body)
			require.Equal(t, id.TrustStateUnknownDevice, plain.Mautrix.TrustState)
			require.Nil(t, plain.Mautrix.TrustSource)
			require.Zero(t, f.requests.Load())
			require.False(t, f.receiver.DisableDecryptKeyFetching)
			if next == "megolm" {
				plain, err = f.receiver.DecryptMegolmEvent(t.Context(), f.event)
				require.NoError(t, err)
				require.NotNil(t, plain.Mautrix.TrustSource)
				require.Equal(t, f.sender.account.SigningKey(), plain.Mautrix.TrustSource.SigningKey)
			} else {
				// A real legacy Olm message has no embedded sender_device_keys,
				// so sender verification depends on the shared fetch-attempt set.
				otks := f.receiver.account.getOneTimeKeys(f.receiver.Client.UserID, f.receiver.Client.DeviceID, 0)
				var otk mautrix.OneTimeKey
				for _, value := range otks {
					otk = value
					break
				}
				f.receiver.account.Internal.MarkKeysAsPublished()
				session, err := f.sender.account.Internal.NewOutboundSession(f.receiver.account.IdentityKey(), otk.Key)
				require.NoError(t, err)
				payload, err := json.Marshal(DecryptedOlmEvent{
					Sender: f.sender.Client.UserID, Keys: OlmEventKeys{Ed25519: f.sender.account.SigningKey()},
					Recipient: f.receiver.Client.UserID, RecipientKeys: OlmEventKeys{Ed25519: f.receiver.account.SigningKey()},
					Type: event.ToDeviceDummy, Content: event.Content{Raw: map[string]any{}},
				})
				require.NoError(t, err)
				kind, ciphertext, err := session.Encrypt(payload)
				require.NoError(t, err)
				decrypted, err := f.receiver.decryptAndParseOlmCiphertext(t.Context(), &event.Event{Sender: f.sender.Client.UserID, Type: event.ToDeviceEncrypted}, f.sender.account.IdentityKey(), kind, string(ciphertext))
				require.NoError(t, err)
				require.NotNil(t, decrypted.SenderDevice)
				require.Equal(t, f.sender.account.SigningKey(), decrypted.SenderDevice.SigningKey)
			}
			require.EqualValues(t, 1, f.requests.Load(), "local decryption consumed the ordinary device-fetch attempt")
		})
	}
}

func TestDecryptMegolmEventLocalCachedTrustAndValidation(t *testing.T) {
	f := newLocalMegolmFixture(t)
	device := &id.Device{UserID: f.sender.Client.UserID, DeviceID: f.sender.Client.DeviceID, IdentityKey: f.sender.account.IdentityKey(), SigningKey: f.sender.account.SigningKey(), Trust: id.TrustStateVerified}
	require.NoError(t, f.receiver.CryptoStore.PutDevices(t.Context(), device.UserID, map[id.DeviceID]*id.Device{device.DeviceID: device}))
	plain, err := f.receiver.DecryptMegolmEventLocal(t.Context(), f.event)
	require.NoError(t, err)
	require.Equal(t, id.TrustStateVerified, plain.Mautrix.TrustState)
	device.SigningKey = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	plain, err = f.receiver.DecryptMegolmEventLocal(t.Context(), f.event)
	require.NoError(t, err)
	require.Equal(t, id.TrustStateDeviceKeyMismatch, plain.Mautrix.TrustState)
	replay := *f.event
	replay.ID = "$replayed"
	_, err = f.receiver.DecryptMegolmEventLocal(t.Context(), &replay)
	require.ErrorIs(t, err, ErrDuplicateMessageIndex)
	missing := *f.event
	content := *f.event.Content.AsEncrypted()
	content.SessionID = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	missing.Content = event.Content{Parsed: &content}
	_, err = f.receiver.DecryptMegolmEventLocal(t.Context(), &missing)
	require.ErrorIs(t, err, ErrNoSessionFound)
	// Store the same key under a different room to exercise plaintext room
	// validation rather than merely the absence of a session for that room.
	in, err := f.receiver.CryptoStore.GetGroupSession(t.Context(), f.event.RoomID, f.event.Content.AsEncrypted().SessionID)
	require.NoError(t, err)
	wrongSession := *in
	wrongSession.RoomID = "!wrong:example.invalid"
	require.NoError(t, f.receiver.CryptoStore.PutGroupSession(t.Context(), &wrongSession))
	wrongRoom := *f.event
	wrongRoom.RoomID = wrongSession.RoomID
	_, err = f.receiver.DecryptMegolmEventLocal(t.Context(), &wrongRoom)
	require.True(t, errors.Is(err, ErrWrongRoom), "room check lost: %v", err)
	require.Zero(t, f.requests.Load())
}
