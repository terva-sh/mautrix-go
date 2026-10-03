// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package crypto

import (
	"context"
	"errors"
	"sync"
)

type backgroundScopeKey struct{}

// Each admitted context has its own lifetime link. Children retain their
// ancestor links until completion, so normal parent return does not cancel
// descendants and cancellation-stripping wrappers cannot bypass shutdown.
type backgroundScope struct {
	group  *backgroundTasks
	parent *backgroundScope
	cancel context.CancelFunc
	detach func() bool
	refs   int
}

type backgroundTasks struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
	active int
}

// EnableManagedBackground opts into a bounded, single-use background lifetime.
// Call it before processing events or making crypto commands, and stop those
// foreground producers before StopManagedBackground. A fresh machine is needed
// to restart. Without this opt-in, background behavior remains unchanged.
func (mach *OlmMachine) EnableManagedBackground(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if mach.managedBackground != nil {
		return errors.New("managed crypto background already enabled")
	}
	lifetime, cancel := context.WithCancel(ctx)
	mach.managedBackground = &backgroundTasks{ctx: lifetime, cancel: cancel}
	return nil
}

// ManagedBackgroundEnabled reports whether the opt-in lifetime was installed.
// Initialization must finish before foreground work starts.
func (mach *OlmMachine) ManagedBackgroundEnabled() bool { return mach.managedBackground != nil }

// GoBackground schedules dependency-owned work. Managed mode refuses work
// after shutdown/cancellation and when 128 workers are already active. The
// caller's context and the machine lifetime both cancel each admitted worker.
// Nested workers retain ancestor links after normal parent return; those links
// are released when their last admitted descendant finishes. Every admitted
// worker is linked to the machine lifetime, even if its caller strips cancellation.
func (mach *OlmMachine) GoBackground(ctx context.Context, run func(context.Context)) bool {
	group := mach.managedBackground
	if group == nil {
		go run(ctx)
		return true
	}
	group.mu.Lock()
	if group.closed || group.ctx.Err() != nil || ctx.Err() != nil || group.active >= 128 {
		group.mu.Unlock()
		return false
	}
	parent, _ := ctx.Value(backgroundScopeKey{}).(*backgroundScope)
	if parent != nil && parent.group == group && parent.refs > 0 {
		parent.refs++
	} else {
		parent = nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	scope := &backgroundScope{group: group, parent: parent, cancel: cancel, refs: 1}
	scope.detach = context.AfterFunc(group.ctx, cancel)
	if group.ctx.Err() != nil {
		cancel()
	}
	workerCtx = context.WithValue(workerCtx, backgroundScopeKey{}, scope)
	group.active++
	group.wg.Add(1)
	group.mu.Unlock()
	go func() {
		defer group.wg.Done()
		defer func() {
			group.mu.Lock()
			group.active--
			for current := scope; current != nil; current = current.parent {
				current.refs--
				if current.refs != 0 {
					break
				}
				current.detach()
				current.cancel()
			}
			group.mu.Unlock()
		}()
		run(workerCtx)
	}()
	return true
}

// StopManagedBackground cancels, refuses later admission and joins all admitted
// workers, including nested helper waits and key requests, before stores close.
// It cannot interrupt a caller callback that ignores cancellation.
func (mach *OlmMachine) StopManagedBackground() {
	group := mach.managedBackground
	if group == nil {
		return
	}
	group.mu.Lock()
	group.closed = true
	group.cancel()
	group.mu.Unlock()
	group.wg.Wait()
}

// Unmanaged unwedge work retains its original independent background context.
func (mach *OlmMachine) unwedgeContext(ctx context.Context) context.Context {
	if mach.ManagedBackgroundEnabled() {
		return ctx
	}
	return mach.backgroundCtx
}
