// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package crypto

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestManagedBackgroundJoinsAndRefusesLateNestedWork(t *testing.T) {
	mach := &OlmMachine{}
	if err := mach.EnableManagedBackground(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered, release, cancelled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	if !mach.GoBackground(context.Background(), func(ctx context.Context) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		if mach.GoBackground(context.Background(), func(context.Context) { t.Error("late nested work admitted") }) {
			t.Error("late work admitted")
		}
	}) {
		t.Fatal("worker not admitted")
	}
	<-entered
	stopped := make(chan struct{})
	go func() { mach.StopManagedBackground(); close(stopped) }()
	<-cancelled
	select {
	case <-stopped:
		t.Fatal("stop passed active worker")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("worker not joined")
	}
	if mach.GoBackground(context.Background(), func(context.Context) { t.Error("post-stop worker ran") }) {
		t.Fatal("post-stop worker admitted")
	}
	mach.StopManagedBackground()
}

func TestManagedBackgroundOwnsNestedWorkersAndCapacity(t *testing.T) {
	mach := &OlmMachine{}
	if err := mach.EnableManagedBackground(context.Background()); err != nil {
		t.Fatal(err)
	}
	var entered sync.WaitGroup
	entered.Add(128)
	for i := 0; i < 128; i++ {
		if !mach.GoBackground(context.Background(), func(ctx context.Context) { entered.Done(); <-ctx.Done() }) {
			t.Fatal("bounded worker refused prematurely")
		}
	}
	entered.Wait()
	if mach.GoBackground(context.Background(), func(context.Context) { t.Error("excess worker ran") }) {
		t.Fatal("capacity exceeded")
	}
	mach.StopManagedBackground()
	mach = &OlmMachine{}
	if err := mach.EnableManagedBackground(context.Background()); err != nil {
		t.Fatal(err)
	}
	parentDone, childEntered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	mach.GoBackground(context.Background(), func(ctx context.Context) {
		if !mach.GoBackground(ctx, func(ctx context.Context) { close(childEntered); <-ctx.Done(); <-release }) {
			t.Error("nested child refused")
		}
		close(parentDone)
	})
	<-parentDone
	<-childEntered
	stopped := make(chan struct{})
	go func() { mach.StopManagedBackground(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("child outlived joined parent")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-stopped
}

func TestManagedBackgroundHonorsBothCancellationSources(t *testing.T) {
	for _, source := range []string{"caller", "lifetime"} {
		t.Run(source, func(t *testing.T) {
			life, stopLife := context.WithCancel(context.Background())
			defer stopLife()
			caller, stopCaller := context.WithCancel(context.Background())
			defer stopCaller()
			mach := &OlmMachine{}
			if err := mach.EnableManagedBackground(life); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			mach.GoBackground(caller, func(ctx context.Context) { <-ctx.Done(); close(done) })
			if source == "caller" {
				stopCaller()
			} else {
				stopLife()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("cancellation did not reach worker")
			}
			mach.StopManagedBackground()
		})
	}
}

func TestBackgroundDefaultRetainsCallerContext(t *testing.T) {
	mach := &OlmMachine{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan context.Context, 1)
	// Default mode continues admitting background work even for a cancelled caller.
	if !mach.GoBackground(ctx, func(got context.Context) { done <- got }) {
		t.Fatal("default work refused")
	}
	select {
	case got := <-done:
		if got != ctx {
			t.Fatal("default caller context replaced")
		}
	case <-time.After(time.Second):
		t.Fatal("default worker did not run")
	}
	mach.StopManagedBackground()
}

func TestManagedBackgroundRejectsCancelledAdmissionAndReenable(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	mach := &OlmMachine{}
	if err := mach.EnableManagedBackground(cancelled); err != context.Canceled {
		t.Fatal("cancelled lifetime admitted")
	}
	life, stop := context.WithCancel(context.Background())
	if err := mach.EnableManagedBackground(life); err != nil {
		t.Fatal(err)
	}
	if err := mach.EnableManagedBackground(context.Background()); err == nil {
		t.Fatal("lifetime replaced")
	}
	if mach.GoBackground(cancelled, func(context.Context) { t.Error("cancelled caller ran") }) {
		t.Fatal("cancelled caller admitted")
	}
	stop()
	if mach.GoBackground(context.Background(), func(context.Context) { t.Error("cancelled lifetime ran") }) {
		t.Fatal("cancelled lifetime admitted")
	}
	mach.StopManagedBackground()
	if err := mach.EnableManagedBackground(context.Background()); err == nil {
		t.Fatal("stopped lifetime reused")
	}
}

func TestManagedBackgroundPreservesCallerValues(t *testing.T) {
	type callerKey struct{}
	mach := &OlmMachine{}
	if err := mach.EnableManagedBackground(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mach.StopManagedBackground()
	ctx := context.WithValue(context.Background(), callerKey{}, "caller")
	done := make(chan any, 1)
	if !mach.GoBackground(ctx, func(got context.Context) { done <- got.Value(callerKey{}) }) {
		t.Fatal("worker refused")
	}
	select {
	case got := <-done:
		if got != "caller" {
			t.Fatal("caller context value lost")
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not run")
	}
}

func TestManagedBackgroundConcurrentAdmissionAndStop(t *testing.T) {
	mach := &OlmMachine{}
	if err := mach.EnableManagedBackground(context.Background()); err != nil {
		t.Fatal(err)
	}
	var callers sync.WaitGroup
	gate := make(chan struct{})
	for i := 0; i < 64; i++ {
		callers.Go(func() {
			<-gate
			mach.GoBackground(context.Background(), func(ctx context.Context) { <-ctx.Done() })
		})
	}
	stopped := make(chan struct{})
	go func() { <-gate; mach.StopManagedBackground(); close(stopped) }()
	close(gate)
	callers.Wait()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("concurrent admissions prevented joining")
	}
	group := mach.managedBackground
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.active != 0 || !group.closed {
		t.Fatal("shutdown left admitted work")
	}
}

func TestManagedBackgroundNestedChildSurvivesNormalParentCompletion(t *testing.T) {
	caller, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()
	mach := &OlmMachine{}
	if err := mach.EnableManagedBackground(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mach.StopManagedBackground()
	parentDone, childReady, childCancelled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	if !mach.GoBackground(caller, func(ctx context.Context) {
		if !mach.GoBackground(ctx, func(ctx context.Context) { close(childReady); <-ctx.Done(); close(childCancelled) }) {
			t.Error("nested child refused")
		}
		close(parentDone)
	}) {
		t.Fatal("parent refused")
	}
	<-parentDone
	<-childReady
	select {
	case <-childCancelled:
		t.Fatal("normal parent return cancelled its child")
	case <-time.After(20 * time.Millisecond):
	}
	cancelCaller()
	select {
	case <-childCancelled:
	case <-time.After(time.Second):
		t.Fatal("child lost original caller cancellation")
	}
}

func TestManagedBackgroundNestedWithoutCancelStillStops(t *testing.T) {
	mach := &OlmMachine{}
	if err := mach.EnableManagedBackground(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	mach.GoBackground(context.Background(), func(ctx context.Context) {
		if !mach.GoBackground(context.WithoutCancel(ctx), func(ctx context.Context) {
			close(ready)
			select {
			case <-ctx.Done():
				close(cancelled)
			case <-release:
			}
		}) {
			t.Error("nested worker refused")
		}
	})
	<-ready
	stopped := make(chan struct{})
	go func() { mach.StopManagedBackground(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("nested WithoutCancel bypassed managed shutdown")
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("nested worker lost lifetime cancellation")
	}
}

func TestManagedUnwedgeContextKeepsCallerCancellation(t *testing.T) {
	background := context.Background()
	caller, cancel := context.WithCancel(background)
	defer cancel()
	mach := &OlmMachine{backgroundCtx: background}
	if mach.unwedgeContext(caller) != background {
		t.Fatal("default unwedge context changed")
	}
	if err := mach.EnableManagedBackground(background); err != nil {
		t.Fatal(err)
	}
	defer mach.StopManagedBackground()
	cancelled := make(chan struct{})
	if !mach.GoBackground(mach.unwedgeContext(caller), func(ctx context.Context) { <-ctx.Done(); close(cancelled) }) {
		t.Fatal("worker refused")
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("unwedge worker lost caller cancellation")
	}
}
