// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>
/// Tests for the run/cancel handshake in <see cref="DispatchWorkItemBase{TState}"/>: when a cancel claims the
/// item after its action has started but before the run finishes, the disposable the action produced must still
/// be torn down.
/// </summary>
public sealed class DispatchWorkItemBaseTests
{
    /// <summary>The scheduled state value the probe work item carries.</summary>
    private const int ProbeState = 0;

    /// <summary>Verifies a cancel that races the running action still disposes the work the action returned.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RunDisposesTheStartedWorkWhenCancellationClaimsTheItemDuringItsAction()
    {
        BooleanDisposable started = new();
        ProbeWorkItem? item = null;
        item = new(Scheduler.Default, ProbeState, (_, _) =>
        {
            // Reproduce the interleaving the guard exists for: a concurrent cancel has claimed the single
            // dispose transition, but the run's compare-exchange has already published the started work, so
            // the run itself is what tears the work down.
            _ = item!.ClaimCancellation();
            return started;
        });

        item.Run();

        await Assert.That(started.IsDisposed).IsTrue();
    }

    /// <summary>A dispatch work item whose cancellation transition the test can claim directly.</summary>
    /// <param name="scheduler">The scheduler passed back to the scheduled action.</param>
    /// <param name="state">The scheduled state.</param>
    /// <param name="action">The scheduled action.</param>
    private sealed class ProbeWorkItem(IScheduler scheduler, int state, Func<IScheduler, int, IDisposable> action)
        : DispatchWorkItemBase<int>(scheduler, state, action)
    {
        /// <summary>Claims the single cancellation transition, as a concurrent dispose would.</summary>
        /// <returns><see langword="true"/> for the first caller.</returns>
        public bool ClaimCancellation() => TryClaimDispose();
    }
}
