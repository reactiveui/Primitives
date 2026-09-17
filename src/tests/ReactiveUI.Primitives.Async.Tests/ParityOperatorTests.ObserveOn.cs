// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests optional context and scheduler selection.</summary>
public partial class ParityOperatorTests
{
    /// <summary>Tests that ObserveOnSafe with a null AsyncContext returns the source unchanged.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeAsyncContext_WithNull_ThenReturnsSourceUnchanged()
    {
        var source = AsyncObs.Return(1);

        var observed = source.ObserveOnSafe((AsyncContext?)null);

        var result = await observed.FirstAsync();
        await Assert.That(result).IsEqualTo(1);
    }

    /// <summary>Tests that ObserveOnSafe with a non-null AsyncContext applies ObserveOn.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeAsyncContext_WithValue_ThenAppliesObserveOn()
    {
        var context = AsyncContext.Default;

        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnSafe(context)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnSafe with a null TaskScheduler returns the source unchanged.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeTaskScheduler_WithNull_ThenReturnsSourceUnchanged()
    {
        var source = AsyncObs.Return(1);

        var observed = source.ObserveOnSafe((TaskScheduler?)null);

        var result = await observed.FirstAsync();
        await Assert.That(result).IsEqualTo(1);
    }

    /// <summary>Tests that ObserveOnSafe with a non-null TaskScheduler applies ObserveOn.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeTaskScheduler_WithValue_ThenAppliesObserveOn()
    {
        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnSafe(TaskScheduler.Default)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnIf with true condition applies ObserveOn with AsyncContext.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfAsyncContext_WithTrueCondition_ThenAppliesObserveOn()
    {
        var context = AsyncContext.Default;

        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnIf(true, context)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnIf with false condition returns the source unchanged for AsyncContext.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfAsyncContext_WithFalseCondition_ThenReturnsSourceUnchanged()
    {
        var context = AsyncContext.Default;

        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnIf(false, context)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnIf with true condition applies ObserveOn with TaskScheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfTaskScheduler_WithTrueCondition_ThenAppliesObserveOn()
    {
        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnIf(true, TaskScheduler.Default)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnIf with false condition returns the source unchanged for TaskScheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfTaskScheduler_WithFalseCondition_ThenReturnsSourceUnchanged()
    {
        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnIf(false, TaskScheduler.Default)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }
}
