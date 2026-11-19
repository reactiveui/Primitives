// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests immediate upstream disposal on early stop conditions across async TakeUntil coordinators.</summary>
public sealed class TaskStopCoordinatorTests
{
    /// <summary>TakeUntil with an already-completed Task disposes the source subscription before returning.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SubscribeAsync_AlreadyCompletedTask_DisposesUpstreamImmediately()
    {
        var disposals = 0;
        var source = CreateTrackingSource(() => disposals++);

        await using var sub = await source.TakeUntil(Task.CompletedTask)
            .SubscribeAsync(static (_, _) => default, CancellationToken.None);

        await Assert.That(disposals).IsEqualTo(1);
    }

    /// <summary>TakeUntil with an already-cancelled CancellationToken disposes or skips the source subscription immediately.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SubscribeAsync_AlreadyCancelledToken_DoesNotRetainUpstreamSubscription()
    {
        int[] counters = [0, 0];
        var source = SignalAsync.Create<int>((_, _) =>
        {
            _ = Interlocked.Increment(ref counters[0]);
            return new(DisposableAsync.Create(counters, static state =>
            {
                _ = Interlocked.Increment(ref state[1]);
                return default;
            }));
        });

        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        await using var sub = await source.TakeUntil(cts.Token)
            .SubscribeAsync(static (_, _) => default, CancellationToken.None);

        var subscribed = Volatile.Read(ref counters[0]);
        var disposals = Volatile.Read(ref counters[1]);
        await Assert.That(subscribed == 0 || disposals == 1).IsTrue();
    }

    /// <summary>TakeUntil predicate overload with an already-cancelled stopToken disposes the source subscription immediately.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SubscribeAsync_PredicateWithAlreadyCancelledStopToken_DisposesUpstreamImmediately()
    {
        var disposals = 0;
        var source = CreateTrackingSource(() => disposals++);
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        await using var sub = await source.TakeUntil(static _ => false, cts.Token)
            .SubscribeAsync(static (_, _) => default, CancellationToken.None);

        await Assert.That(disposals).IsEqualTo(1);
    }

    /// <summary>TakeUntil with a synchronous CompletionSignalDelegate disposes the source subscription immediately.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SubscribeAsync_SynchronousCompletionDelegate_DisposesUpstreamImmediately()
    {
        var disposals = 0;
        var source = CreateTrackingSource(() => disposals++);
        CompletionSignalDelegate immediateStop = static handler =>
        {
            handler(Result.Success);
            return DisposableAsync.Empty;
        };

        await using var sub = await source.TakeUntil(immediateStop)
            .SubscribeAsync(static (_, _) => default, CancellationToken.None);

        await Assert.That(disposals).IsEqualTo(1);
    }

    /// <summary>Creates a cold async source incrementing <paramref name="onDispose"/> on subscription disposal.</summary>
    /// <param name="onDispose">Callback invoked when the returned subscription is disposed.</param>
    /// <returns>The tracking async observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservableAsync<int> CreateTrackingSource(Action onDispose) =>
        SignalAsync.Create<int>((_, _) =>
            new(DisposableAsync.Create(onDispose, static cb =>
            {
                cb();
                return default;
            })));
}
