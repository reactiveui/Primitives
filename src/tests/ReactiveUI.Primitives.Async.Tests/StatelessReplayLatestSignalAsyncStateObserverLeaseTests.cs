// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests stateless replay observer removal and initial-value restoration.</summary>
public sealed class StatelessReplayLatestSignalAsyncStateObserverLeaseTests
{
    /// <summary>The gate is held by another flow and the signal is cancelled.</summary>
    private const int HeldAndCancelled = 1;

    /// <summary>The whole signal state has been disposed.</summary>
    private const int StateDisposed = 3;

    /// <summary>A second observer remains after removal.</summary>
    private const int AnotherObserver = 4;

    /// <summary>Verifies the final stateless replay lease restores its initial value.</summary>
    /// <param name="stateKind">The state present when lease disposal starts.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(HeldAndCancelled)]
    [Arguments(StateDisposed)]
    [Arguments(AnotherObserver)]
    public async Task WhenStatelessReplayLeaseDisposed_ThenLastRemovalResetsValue(int stateKind)
    {
        const int InitialValue = 1;
        const int PublishedValue = 2;
        StatelessReplayLatestSignalAsyncState<int> state = new(new(InitialValue));
        TaskCompletionSource releaseGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            CallbackWitnessAsync<int> observer = new(static (_, _) => default);
            state.Observers = stateKind == AnotherObserver
                ? [observer, new CallbackWitnessAsync<int>(static (_, _) => default)]
                : [observer];
            state.Value = new(PublishedValue);
            state.IsDisposed = stateKind == StateDisposed;
            StatelessReplayLatestSignalAsyncStateObserverLease<int> lease = new(state, observer);
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
            if (stateKind == StateDisposed)
            {
                return;
            }

            await Assert.That(state.Value.Value).IsEqualTo(stateKind == 0 ? InitialValue : PublishedValue);
        }
        finally
        {
            _ = releaseGate.TrySetResult();
            state.Dispose();
        }
    }
}
