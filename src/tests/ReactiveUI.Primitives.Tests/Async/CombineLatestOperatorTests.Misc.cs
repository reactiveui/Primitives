// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>Second value emitted by the selector-throws test.</summary>
    private const int SelectorThrowSecondValue = 2;

    /// <summary>Tests CombineLatest error-resume after disposal is ignored.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestDisposedThenErrorIgnored()
    {
        const int PostDisposeValue = 10;
        var source1 = new DirectSource<int>();
        var source2 = new DirectSource<int>();
        var items = new List<int>();

        var sub = await source1.CombineLatest(source2, (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        await source1.EmitNext(1);
        await source2.EmitNext(Source1Value);
        await sub.DisposeAsync();

        // Emissions after disposal should be ignored
        await source1.EmitNext(PostDisposeValue);
        await Assert.That(items).Count().IsEqualTo(1);
    }

    /// <summary>Tests CombineLatest error from source propagates.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSourceEmitsError_ThenErrorPropagated()
    {
        var error = new InvalidOperationException("src-error");
        var source1 = new DirectSource<int>();
        var source2 = new DirectSource<int>();
        Exception? caughtError = null;

        await using var sub = await source1.CombineLatest(source2, (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caughtError = ex;
                    return default;
                });

        await source1.EmitNext(1);
        await source2.EmitNext(Source1Value);
        await source1.EmitError(error);
        await source1.Complete(Result.Success);
        await source2.Complete(Result.Success);

        await Assert.That(caughtError).IsNotNull();
    }

    /// <summary>Tests CombineLatestEnumerable where sources is already an IReadOnlyList.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestWithReadOnlyListSources_ThenWorks()
    {
        const int SecondSourceValue = 2;
        const int ExpectedCount = 2;
        IReadOnlyList<IObservableAsync<int>> sources =
        [
            SignalAsync.Return(1),
            SignalAsync.Return(SecondSourceValue)
        ];

        var result = await sources.CombineLatest().FirstAsync();
        await Assert.That(result).Count().IsEqualTo(ExpectedCount);
    }

    /// <summary>Verifies that when the <c>resultSelector</c> throws synchronously while combining
    /// the latest snapshot, the failure is forwarded as a terminal completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSelectorThrows_ThenCompletesWithFailure()
    {
        var a = Signal.Create<int>();
        var b = Signal.Create<int>();
        IReadOnlyList<IObservableAsync<int>> sources = [a.Values, b.Values];
        var expected = new InvalidOperationException("selector-failed");
        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await sources.CombineLatest<int, int>(
                snapshot => throw expected)
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completed.TrySetResult(result);
                    return default;
                });

        await a.OnNextAsync(1, CancellationToken.None);
        await b.OnNextAsync(SelectorThrowSecondValue, CancellationToken.None);

        var terminal = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(terminal.IsFailure).IsTrue();
        await Assert.That(terminal.Exception).IsSameReferenceAs(expected);
    }
}
