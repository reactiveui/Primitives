// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Internal;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="ConcurrencyRaceHelpers"/>.</summary>
public sealed class ConcurrencyRaceHelpersTests
{
    /// <summary>Verifies atomic state claiming succeeds once and rejects competing callers.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryClaim_TransitionsOnceAndRejectsSecondClaim()
    {
        var state = 0;
        await Assert.That(ConcurrencyRaceHelpers.TryClaim(ref state, 0, 1)).IsTrue();
        await Assert.That(state).IsEqualTo(1);
        await Assert.That(ConcurrencyRaceHelpers.TryClaim(ref state, 0, 1)).IsFalse();
    }

    /// <summary>Verifies <see cref="ConcurrencyRaceHelpers.TryCancelAsync"/> cancels active sources and swallows disposed ones.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryCancelAsync_ActiveAndDisposedSources_ReturnsExpectedStatus()
    {
        using CancellationTokenSource active = new();
        await Assert.That(await ConcurrencyRaceHelpers.TryCancelAsync(active)).IsTrue();
        await Assert.That(active.IsCancellationRequested).IsTrue();

        CancellationTokenSource disposed = new();
        disposed.Dispose();
        await Assert.That(await ConcurrencyRaceHelpers.TryCancelAsync(disposed)).IsFalse();
    }

    /// <summary>Verifies <see cref="ConcurrencyRaceHelpers.IgnoreCanceledOrDisposedAsync{TState}"/> swallows cancellation and disposal while propagating unexpected failures.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IgnoreCanceledOrDisposedAsync_HandlesSuccessCancellationDisposalAndOtherErrors()
    {
        StrongBox<int> calls = new();
        var completed = await ConcurrencyRaceHelpers.IgnoreCanceledOrDisposedAsync(
            calls,
            static box =>
            {
                box.Value++;
                return default;
            });
        await Assert.That(completed).IsTrue();
        await Assert.That(calls.Value).IsEqualTo(1);

        var canceled = await ConcurrencyRaceHelpers.IgnoreCanceledOrDisposedAsync(
            0,
            static _ => ValueTask.FromException(new OperationCanceledException()));
        await Assert.That(canceled).IsFalse();

        var taskCanceled = await ConcurrencyRaceHelpers.IgnoreCanceledOrDisposedAsync(
            0,
            static _ => ValueTask.FromException(new TaskCanceledException()));
        await Assert.That(taskCanceled).IsFalse();

        var disposed = await ConcurrencyRaceHelpers.IgnoreCanceledOrDisposedAsync(
            0,
            static _ => ValueTask.FromException(new ObjectDisposedException("disposed")));
        await Assert.That(disposed).IsFalse();

        await Assert.That(static async () => await ConcurrencyRaceHelpers.IgnoreCanceledOrDisposedAsync(
                0,
                static _ => ValueTask.FromException(new InvalidOperationException("unexpected"))))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies <see cref="ConcurrencyRaceHelpers.RunUnderGateIgnoringTeardownAsync{TState}"/> runs under an open gate and tolerates cancelled waits.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RunUnderGateIgnoringTeardownAsync_RunsWhenOpenAndSwallowsCancelledWait()
    {
        AsyncFlowGate gate = new();
        StrongBox<int> counter = new();
        await ConcurrencyRaceHelpers.RunUnderGateIgnoringTeardownAsync(
            gate,
            counter,
            static box => box.Value++,
            CancellationToken.None);
        await Assert.That(counter.Value).IsEqualTo(1);

        TaskCompletionSource releaseGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenSource cts = new();
        try
        {
            _ = AsyncFlowGateHolder.HoldAsync(gate, releaseGate.Task);
            await cts.CancelAsync();
            await ConcurrencyRaceHelpers.RunUnderGateIgnoringTeardownAsync(
                gate,
                counter,
                static box => box.Value++,
                cts.Token);
            await Assert.That(counter.Value).IsEqualTo(1);
        }
        finally
        {
            _ = releaseGate.TrySetResult();
        }
    }
}
