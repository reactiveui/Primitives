// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case tests for <c>Using</c> (action / func overloads) and
/// <c>SwitchIfEmpty</c> that target paths not exercised by the existing happy-path tests.</summary>
public class UsingAndSwitchIfEmptyEdgeTests
{
    /// <summary>Sentinel value the fallback observable would emit if it were subscribed.</summary>
    private const int FallbackSentinel = 99;

    /// <summary>How long the scheduler test spins for the resource dispose to land, in milliseconds.</summary>
    private const int DisposeWaitMilliseconds = 5000;

    /// <summary>Longest a test waits for an asynchronous signal before failing.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies that <c>Using</c> with a null action still emits RxVoid, completes, and disposes the resource.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingActionNull_ThenEmitsUnitAndDisposesResource()
    {
        TrackedDisposable resource = new();
        List<RxVoid> results = [];
        var completed = false;
        using var sub = resource.Using(null).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(completed).IsTrue();
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that when the action throws the error is forwarded and the resource is still disposed exactly once.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingActionThrows_ThenForwardsErrorAndDisposes()
    {
        TrackedDisposable resource = new();
        Exception? caught = null;
        InvalidOperationException expected = new("action failed");
        using var sub = resource.Using(_ => throw expected).Subscribe(
            static _ => { },
            ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that the scheduler overload defers execution to the scheduler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingActionWithScheduler_ThenRunsOnScheduler()
    {
        TrackedDisposable resource = new();
        var ran = false;
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = resource.Using(_ => ran = true, Sequencer.Default).Subscribe(
            static _ => { },
            () => completed.TrySetResult());
        await completed.Task.WaitAsync(GuardTimeout);
        await Assert.That(ran).IsTrue();

        // OnCompleted is signalled before the resource is disposed on the scheduler
        // thread, so spin briefly for the dispose to land.
        var deadline = Environment.TickCount64 + DisposeWaitMilliseconds;
        while (resource.DisposeCount == 0 && Environment.TickCount64 < deadline)
        {
            await Task.Yield();
        }

        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that the Func overload returns the function's result and disposes the resource.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingFunc_ThenEmitsFunctionResultAndDisposes()
    {
        const int Expected = 42;
        TrackedDisposable resource = new();
        List<int> results = [];
        using var sub = resource.Using(static _ => Expected).Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo([Expected]);
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that when the func throws the error is forwarded and the resource is still disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingFuncThrows_ThenForwardsErrorAndDisposes()
    {
        TrackedDisposable resource = new();
        Exception? caught = null;
        InvalidOperationException expected = new("func failed");
        using var sub = resource.Using<TrackedDisposable, int>(_ => throw expected).Subscribe(
            static _ => { },
            ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that SwitchIfEmpty forwards source errors without subscribing to the fallback.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchIfEmptySourceErrors_ThenForwardsErrorAndIgnoresFallback()
    {
        Subject<int> source = new();
        var fallbackSubscribed = false;
        var fallback = Observable.Defer(() =>
        {
            fallbackSubscribed = true;
            return Observable.Return(FallbackSentinel);
        });
        Exception? caught = null;
        InvalidOperationException expected = new("source error");
        using var sub = source.SwitchIfEmpty(fallback).Subscribe(
            static _ => { },
            ex => caught = ex);
        source.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(fallbackSubscribed).IsFalse();
    }

    /// <summary>Verifies that SwitchIfEmpty completes immediately when the source emits at least one value
    /// without subscribing to the fallback.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchIfEmptySourceEmits_ThenCompletesWithoutFallback()
    {
        const int Value = 5;
        Subject<int> source = new();
        var fallbackSubscribed = false;
        var fallback = Observable.Defer(() =>
        {
            fallbackSubscribed = true;
            return Observable.Return(FallbackSentinel);
        });
        List<int> results = [];
        var completed = false;
        using var sub = source.SwitchIfEmpty(fallback).Subscribe(results.Add, () => completed = true);
        source.OnNext(Value);
        source.OnCompleted();
        await Assert.That(results).IsCollectionEqualTo([Value]);
        await Assert.That(completed).IsTrue();
        await Assert.That(fallbackSubscribed).IsFalse();
    }

    /// <summary>Verifies that disposing a SwitchIfEmpty subscription before the source completes stops further emissions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchIfEmptyDisposed_ThenNoFurtherEmissions()
    {
        Subject<int> source = new();
        Subject<int> fallback = new();
        List<int> results = [];
        var sub = source.SwitchIfEmpty(fallback).Subscribe(results.Add);
        sub.Dispose();
        source.OnCompleted();
        fallback.OnNext(FallbackSentinel);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Tracks dispose invocations to verify resource lifecycle.</summary>
    private sealed class TrackedDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref = "Dispose"/> has been invoked.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
