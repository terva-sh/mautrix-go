# Minimal mautrix-go lifecycle fork

This MPL-2.0 fork of [mautrix-go](https://github.com/mautrix/go) adds opt-in
cancellation and joining for crypto background workers. It is based on
upstream v0.31.0. Upstream copyright notices, MPL source licensing and the
original Apache attribution remain intact. Kuiske uses it while seeking
upstream adoption; the patch does not change ordinary background behavior.

The module keeps its upstream import path. Select this fork explicitly in
your application's root `go.mod`:

```go
require maunium.net/go/mautrix v0.31.0

replace maunium.net/go/mautrix => github.com/terva-sh/mautrix-go v0.31.0-terva.2
```

A library's replacement directives do not select the fork for its consumers.
Release tags and source remain available at this repository. The source API
version constant retains the upstream baseline; module replacement metadata
identifies the fork version.

Before processing events or issuing crypto commands, call
`OlmMachine.EnableManagedBackground(ctx)`. Stop all foreground producers,
then call `StopManagedBackground()` before closing stores. The lifetime is
single-use. It admits at most 128 active tasks and refuses later work after
cancellation/stop; rejected best-effort work may need application retry.
Callbacks that ignore cancellation can delay joining. No cgo is required for
the supported client/crypto graph when built with the `goolm` tag.

The pure-Go gate covers worker/default/concurrency tests, client/event/id/format,
helper/Olm/goolm tests, vet, and client/crypto builds on Linux amd64/arm64,
macOS arm64 and Windows amd64. This is build coverage, not native platform
runtime coverage. Upstream bridge binaries and full crypto/verificationhelper
SQLite fixtures still require cgo and are outside that gate. Applications
must separately own foreground commands, verification helpers and persistent
message recovery; the worker API does not join arbitrary application work.

For cached message reads, `OlmMachine.DecryptMegolmEventLocal(ctx, event)` uses
local room keys and sender-device information without fetching unknown sender
devices. Unknown devices keep conservative trust. The method preserves replay,
room and cryptographic validation, and can update local session/replay state.
It does not consume the device-fetch attempt used by normal Megolm/Olm sync
or alter `DisableDecryptKeyFetching`. A missing room key returns the normal
missing-session error. Store implementations must themselves be local to avoid
networking; ordinary `DecryptMegolmEvent` behavior is unchanged.
