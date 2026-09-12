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

    /// <summary>Minimum and maximum reductions include values beyond vector boundaries.</summary>
    /// <param name="sourceCount">The number of latest values to reduce.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(3)]
    [Arguments(8)]
    [Arguments(17)]
    [Arguments(65)]
    public async Task GetMinMax_IntegerInputs_FindExtremesAcrossVectorBoundaries(int sourceCount)
    {
        var sources = new IObservableAsync<int>[sourceCount - 1];
        for (var i = 0; i < sources.Length; i++)
        {
            sources[i] = SignalAsync.Return(i);
        }

        sources[^1] = SignalAsync.Return(int.MaxValue);
        var first = SignalAsync.Return(int.MinValue);

        await Assert.That(await first.GetMin(sources).FirstAsync()).IsEqualTo(int.MinValue);
        await Assert.That(await first.GetMax(sources).FirstAsync()).IsEqualTo(int.MaxValue);
    }

    /// <summary>Each update reduces the current values after all sources have emitted.</summary>
    /// <param name="cancellationToken">Cancels the test's signal notifications.</param>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task GetMinMax_Updates_UseCurrentValues(CancellationToken cancellationToken)
    {
        const int InitialMinimum = 3;
        const int MiddleValue = 6;
        const int InitialMaximum = 9;
        const int UpdatedMaximum = 12;
        await using var first = Signal.Create<int>();
        await using var second = Signal.Create<int>();
        await using var third = Signal.Create<int>();
        List<int> minima = [];
        List<int> maxima = [];
        await using var minimum = await first.Values.GetMin(second.Values, third.Values).SubscribeAsync(minima.Add, cancellationToken);
        await using var maximum = await first.Values.GetMax(second.Values, third.Values).SubscribeAsync(maxima.Add, cancellationToken);

        await first.OnNextAsync(InitialMinimum, cancellationToken);
        await second.OnNextAsync(MiddleValue, cancellationToken);
        await Assert.That(minima).IsEmpty();
        await Assert.That(maxima).IsEmpty();

        await third.OnNextAsync(InitialMaximum, cancellationToken);
        await first.OnNextAsync(UpdatedMaximum, cancellationToken);
        await third.OnNextAsync(1, cancellationToken);

        await Assert.That(minima).IsCollectionEqualTo([InitialMinimum, MiddleValue, 1]);
        await Assert.That(maxima).IsCollectionEqualTo([InitialMaximum, UpdatedMaximum, UpdatedMaximum]);
    }

    /// <summary>Floating-point reductions retain the default comparer's NaN ordering.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task GetMinMax_FloatingPointInputs_PreserveNaNOrdering()
    {
        var source = SignalAsync.Return(double.NaN);
        IObservableAsync<double>[] others = [SignalAsync.Return((double)SecondValue), SignalAsync.Return(1D)];

        await Assert.That(double.IsNaN(await source.GetMin(others).FirstAsync())).IsTrue();
        await Assert.That(await source.GetMax(others).FirstAsync()).IsEqualTo((double)SecondValue);
    }

    /// <summary>Equal floating-point values retain the first value's zero sign.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task GetMinMax_EqualZeroValues_PreserveFirstSign()
    {
        const double NegativeZero = -0D;
        var source = SignalAsync.Return(NegativeZero);
        var second = SignalAsync.Return(0D);

        var minimum = await source.GetMin(second).FirstAsync();
        var maximum = await source.GetMax(second).FirstAsync();

        await Assert.That(BitConverter.DoubleToInt64Bits(minimum)).IsEqualTo(long.MinValue);
        await Assert.That(BitConverter.DoubleToInt64Bits(maximum)).IsEqualTo(long.MinValue);
    }

    /// <summary>Non-numeric values continue to use their default comparison contract.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task GetMinMax_DateInputs_UseDefaultComparer()
    {
        var earliest = DateTime.UnixEpoch;
        var latest = earliest.AddDays(1);
        var source = SignalAsync.Return(latest);
        var second = SignalAsync.Return(earliest);

        await Assert.That(await source.GetMin(second).FirstAsync()).IsEqualTo(earliest);
        await Assert.That(await source.GetMax(second).FirstAsync()).IsEqualTo(latest);
    }

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
