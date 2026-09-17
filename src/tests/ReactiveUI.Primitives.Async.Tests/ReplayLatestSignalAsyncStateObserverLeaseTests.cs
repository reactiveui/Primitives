// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests replay observer removal during cancellation and teardown.</summary>
public sealed class ReplayLatestSignalAsyncStateObserverLeaseTests
{
    /// <summary>The gate has been disposed independently of its signal state.</summary>
    private const int GateDisposed = 2;

    /// <summary>The whole signal state has been disposed.</summary>
    private const int StateDisposed = 3;

    /// <summary>Verifies replay lease removal tolerates cancellation and prior state or gate disposal.</summary>
    /// <param name="stateKind">The state present when lease disposal starts.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(GateDisposed)]
    [Arguments(StateDisposed)]
    public async Task WhenReplayLeaseDisposed_ThenRemovalFollowsState(int stateKind)
    {
        ReplayLatestSignalAsyncState<int> state = new(default);
        try
        {
            CallbackWitnessAsync<int> observer = new(static (_, _) => default);
            state.Observers = [observer];
            state.IsDisposed = stateKind == StateDisposed;
            ReplayLatestSignalAsyncStateObserverLease<int> lease = new(state, observer);
            using AsyncSerialGate.Lease? held = stateKind is 1 or GateDisposed
                ? await state.Gate.EnterForThreadAsync(-1, CancellationToken.None)
                : null;
            switch (stateKind)
            {
                case 1:
                {
                    await state.DisposedCts.CancelAsync();
                    break;
                }

                case GateDisposed:
                {
                    state.Gate.Dispose();
                    break;
                }

                case StateDisposed:
                {
                    state.Dispose();
                    break;
                }
            }

            await lease.DisposeAsync();
            await lease.DisposeAsync();
            await Assert.That(state.Observers.Contains(observer)).IsEqualTo(stateKind is 1 or GateDisposed or StateDisposed);
        }
        finally
        {
            state.Dispose();
        }
    }
}
