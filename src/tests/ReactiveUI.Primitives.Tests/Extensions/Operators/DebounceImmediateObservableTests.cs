// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.IO;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <c>DebounceImmediateObservable</c> covering the after-terminal guards
/// on the sink that fire only when an upstream pushes events past its own completion.</summary>
public class DebounceImmediateObservableTests
{
    /// <summary>Tick window for the debounce.</summary>
    private const int DebounceTicks = 10;

    /// <summary>Ticks to advance past the debounce window in settle assertions.</summary>
    private const int SettleTicks = 100;

    /// <summary>Verifies that <c>OnNext</c> after the source has completed is silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAfterCompleted_ThenDropped()
    {
        var scheduler = new VirtualClock();
        var source = new SyncDirectSource<int>();
        var values = new List<int>();
        var completed = false;

        using var sub = source.DebounceImmediate(TimeSpan.FromTicks(DebounceTicks), scheduler)
            .Subscribe(values.Add, () => completed = true);

        source.Observer.OnCompleted();
        scheduler.AdvanceBy(SettleTicks);
        source.Observer.OnNext(1);
        scheduler.AdvanceBy(SettleTicks);

        await Assert.That(completed).IsTrue();
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies that <c>OnError</c> after the source has completed is silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorAfterCompleted_ThenDropped()
    {
        var scheduler = new VirtualClock();
        var source = new SyncDirectSource<int>();
        Exception? caught = null;
        var completed = false;

        using var sub = source.DebounceImmediate(TimeSpan.FromTicks(DebounceTicks), scheduler)
            .Subscribe(static _ => { }, ex => caught = ex, () => completed = true);

        source.Observer.OnCompleted();
        source.Observer.OnError(new InvalidOperationException("late"));

        await Assert.That(completed).IsTrue();
        await Assert.That(caught).IsNull();
    }

    /// <summary>Verifies that a duplicate <c>OnCompleted</c> after an error is silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAfterError_ThenDropped()
    {
        var scheduler = new VirtualClock();
        var source = new SyncDirectSource<int>();
        Exception? caught = null;
        var completed = false;
        var expected = new InvalidOperationException("first");

        using var sub = source.DebounceImmediate(TimeSpan.FromTicks(DebounceTicks), scheduler)
            .Subscribe(static _ => { }, ex => caught = ex, () => completed = true);

        source.Observer.OnError(expected);
        source.Observer.OnCompleted();

        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
    }
}
