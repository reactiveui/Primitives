// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests replay observer removal during cancellation and teardown.</summary>
public sealed class ReplayLatestSignalAsyncStateObserverLeaseTests
{
    /// <summary>The gate is held by another flow and the signal is cancelled.</summary>
    private const int HeldAndCancelled = 1;

    /// <summary>The whole signal state has been disposed.</summary>
    private const int StateDisposed = 3;

    /// <summary>Verifies replay lease removal tolerates cancellation while the gate is held, and prior state disposal.</summary>
    /// <param name="stateKind">The state present when lease disposal starts.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(HeldAndCancelled)]
    [Arguments(StateDisposed)]
    public async Task WhenReplayLeaseDisposed_ThenRemovalFollowsState(int stateKind)
    {
        ReplayLatestSignalAsyncState<int> state = new(default);
        TaskCompletionSource releaseGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            CallbackWitnessAsync<int> observer = new(static (_, _) => default);
            state.Observers = [observer];
            state.IsDisposed = stateKind == StateDisposed;
            ReplayLatestSignalAsyncStateObserverLease<int> lease = new(state, observer);
            if (stateKind == HeldAndCancelled)
            {
                _ = AsyncFlowGateHolder.HoldAsync(state.Gate, releaseGate.Task);
                await state.DisposedCts.CancelAsync();
            }
            else if (stateKind == StateDisposed)
            {
                state.Dispose();
            }

            await lease.DisposeAsync();
            await lease.DisposeAsync();
            await Assert.That(state.Observers.Contains(observer)).IsEqualTo(stateKind is HeldAndCancelled or StateDisposed);
        }
        finally
        {
            _ = releaseGate.TrySetResult();
            state.Dispose();
        }
    }
}
