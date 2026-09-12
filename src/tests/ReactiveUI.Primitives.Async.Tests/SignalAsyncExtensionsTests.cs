// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading.Channels;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests asynchronous signal extension overloads and enumeration disposal.</summary>
public sealed class SignalAsyncExtensionsTests
{
    /// <summary>The second distinct value in an ordered sequence.</summary>
    private const int SecondValue = 2;

    /// <summary>Chaining an enumerable preserves source order.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Chain_Enumerable_EmitsSourcesInOrder()
    {
        IEnumerable<IObservableAsync<int>> sources = [SignalAsync.Return(1), SignalAsync.Return(SecondValue)];

        await Assert.That(await sources.Chain().ToListAsync()).IsCollectionEqualTo([1, SecondValue]);
    }

    /// <summary>Zero-duration throttling on the system clock still removes consecutive duplicates.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ThrottleDistinct_ZeroDuration_EmitsDistinctValues()
    {
        var source = Signal.Create<int>();
        List<int> values = [];
        await using var subscription = await source.Values.ThrottleDistinct(TimeSpan.Zero)
            .SubscribeAsync(values.Add);

        await source.OnNextAsync(1, CancellationToken.None);
        await source.OnNextAsync(1, CancellationToken.None);
        await source.OnNextAsync(SecondValue, CancellationToken.None);
        await source.OnCompletedAsync(Result.Success);

        await Assert.That(values).IsCollectionEqualTo([1, SecondValue]);
    }

    /// <summary>Timeout without an explicit clock selects the system-clock signal.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Timeout_DefaultClock_CreatesTimeoutSignal()
    {
        var source = SignalAsync.Never<int>().Timeout(TimeSpan.FromSeconds(1));

        await Assert.That(source).IsTypeOf<SignalAsyncExtensions.TimeoutSignal<int>>();
    }

    /// <summary>Timeout with a fallback selects the system-clock fallback signal.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Timeout_DefaultClockAndFallback_CreatesFallbackSignal()
    {
        var source = SignalAsync.Never<int>().Timeout(TimeSpan.FromSeconds(1), SignalAsync.Return(1));

        await Assert.That(source).IsTypeOf<SignalAsyncExtensions.TimeoutWithFallbackSignal<int>>();
    }

    /// <summary>Stopping enumeration awaits disposal of the upstream subscription.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ToAsyncEnumerable_DisposeWhileSourceIsActive_AwaitsUpstreamDisposal()
    {
        TaskCompletionSource disposing = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(async (observer, token) =>
        {
            await observer.OnNextAsync(1, token);
            return DisposableAsync.Create((disposing, release), static async state =>
            {
                state.disposing.SetResult();
                await state.release.Task;
            });
        });
        var enumerator = source.ToAsyncEnumerable(Channel.CreateUnbounded<int>).GetAsyncEnumerator();
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        await Assert.That(enumerator.Current).IsEqualTo(1);

        var pending = enumerator.DisposeAsync().AsTask();
        await disposing.Task;
        await Assert.That(pending.IsCompleted).IsFalse();
        release.SetResult();
        await pending;
    }

    /// <summary>Stopping enumeration propagates an upstream disposal failure.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ToAsyncEnumerable_UpstreamDisposalFails_PropagatesFailure()
    {
        InvalidOperationException expected = new("upstream disposal failed");
        var source = SignalAsync.Create<int>(async (observer, token) =>
        {
            await observer.OnNextAsync(1, token);
            return DisposableAsync.Create(expected, ValueTask.FromException);
        });
        var enumerator = source.ToAsyncEnumerable(Channel.CreateUnbounded<int>).GetAsyncEnumerator();
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();

        var error = await Assert.That(async () => await enumerator.DisposeAsync())
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(error).IsSameReferenceAs(expected);
    }
}
