// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Signal"/> awaiter completion and cancellation contracts.</summary>
public class SignalGetAwaiterTests
{
    /// <summary>The first expected value.</summary>
    private const int First = 1;

    /// <summary>The second expected value.</summary>
    private const int Second = 2;

    /// <summary>Awaiter source values.</summary>
    private static readonly int[] AwaiterSource = [First, Second];

    /// <summary>Covers signal awaiter completion, pre-cancellation, and registered cancellation paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetAwaiterCoversCompletionAndCancellationPaths()
    {
        var completed = Signal.FromEnumerable(AwaiterSource).GetAwaiter();
        await Assert.That(completed.IsCompleted).IsTrue();
        await Assert.That(completed.GetResult()).IsEqualTo(Second);
        using CancellationTokenSource canceledBeforeSubscribe = new();
        await canceledBeforeSubscribe.CancelAsync().ConfigureAwait(false);
        var alreadyCanceled = Signal.Silent<int>().GetAwaiter(canceledBeforeSubscribe.Token);
        await Assert.That(alreadyCanceled.IsCompleted).IsTrue();
        _ = Assert.Throws<OperationCanceledException>(() => alreadyCanceled.GetResult());
        using CancellationTokenSource canceledAfterSubscribe = new();
        Signal<int> source = new();
        var awaiter = source.GetAwaiter(canceledAfterSubscribe.Token);
        await canceledAfterSubscribe.CancelAsync().ConfigureAwait(false);
        await Assert.That(awaiter.IsCompleted).IsTrue();
        _ = Assert.Throws<OperationCanceledException>(() => awaiter.GetResult());
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).GetAwaiter());
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).GetAwaiter(CancellationToken.None));
    }
}
