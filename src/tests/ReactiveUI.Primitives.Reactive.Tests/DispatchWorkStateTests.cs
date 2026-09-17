// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Tests the run/cancel handshake in <see cref="DispatchWorkState{TState}"/>, which tears down work a cancelled action returned.</summary>
public sealed class DispatchWorkStateTests
{
    /// <summary>Verifies a cancel that races the running action disposes the work the action returned.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RunDisposesTheStartedWorkWhenCancellationClaimsTheItemDuringItsAction()
    {
        BooleanDisposable started = new();
        ProbeWorkItem item = new(self =>
        {
            // Cancellation claims disposal before Run publishes its started work.
            _ = self.ClaimCancellation();
            return started;
        });

        item.Run();

        await Assert.That(started.IsDisposed).IsTrue();
    }

    /// <summary>Verifies an action that returns no disposable still runs, and a later cancel has nothing to release.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RunAcceptsAnActionThatReturnsNoDisposable()
    {
        var ran = false;
        ProbeWorkItem item = new(_ =>
        {
            ran = true;
            return null!;
        });

        item.Run();
        var claimed = item.ClaimCancellation();

        await Assert.That(ran).IsTrue();
        await Assert.That(claimed).IsTrue();
    }

    /// <summary>A work item that embeds the state and lets the test claim its cancellation directly.</summary>
    private sealed class ProbeWorkItem
    {
        /// <summary>The scheduled state value the probe work item carries.</summary>
        private const int ProbeState = 0;

        /// <summary>The run and cancel state.</summary>
        private DispatchWorkState<int> _work;

        /// <summary>Initializes a new instance of the <see cref="ProbeWorkItem"/> class.</summary>
        /// <param name="action">The scheduled action, given this item.</param>
        public ProbeWorkItem(Func<ProbeWorkItem, IDisposable> action) =>
            _work = new(Scheduler.Immediate, ProbeState, (_, _) => action(this));

        /// <summary>Runs the scheduled action.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Run() => _work.Run();

        /// <summary>Claims the single cancellation transition, as a concurrent dispose would.</summary>
        /// <returns><see langword="true"/> for the first caller.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ClaimCancellation() => _work.TryClaimDispose();
    }
}
