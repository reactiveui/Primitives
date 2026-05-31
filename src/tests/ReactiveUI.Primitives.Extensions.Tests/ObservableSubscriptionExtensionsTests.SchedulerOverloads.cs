// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using System.Reactive.Linq;
using System.Reactive.Subjects;

using System.IO;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using ReactiveUI.Primitives.Extensions.Tests;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Threading.Tasks;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Coverage for the scheduler-routed overloads of the WaitFor* helpers on
/// <see cref="ObservableSubscriptionExtensions"/>. The non-scheduler overloads are
/// already covered in the companion file; these dispatch the subscribe call via a
/// scheduler before blocking.</summary>
public partial class ObservableSubscriptionExtensionsTests
{
    /// <summary>Sentinel value emitted by single-value scheduler tests.</summary>
    private const int SchedulerSentinelValue = 13;

    /// <summary>Default test timeout for the scheduler-routed overloads.</summary>
    private static readonly TimeSpan SchedulerWaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies that <c>WaitForValue</c> with a scheduler returns the emitted value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValueWithSchedulerOnly_ThenReturnsEmittedValue()
    {
        var result = Observable.Return(SchedulerSentinelValue)
            .WaitForValue(TaskPoolSequencer.Default);

        await Assert.That(result).IsEqualTo(SchedulerSentinelValue);
    }

    /// <summary>Verifies that <c>WaitForValue</c> with a scheduler and explicit timeout returns the emitted value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValueWithSchedulerAndTimeout_ThenReturnsEmittedValue()
    {
        var result = Observable.Return(SchedulerSentinelValue)
            .WaitForValue(TaskPoolSequencer.Default, SchedulerWaitTimeout);

        await Assert.That(result).IsEqualTo(SchedulerSentinelValue);
    }

    /// <summary>Verifies that the scheduler+timeout form of <c>WaitForValue</c> times out for a never-terminating source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValueWithSchedulerTimesOut_ThenTimeoutException()
    {
        Action call = () => Observable.Never<int>()
            .WaitForValue(TaskPoolSequencer.Default, TimeSpan.FromMilliseconds(50));
        var ex = Assert.Throws<TimeoutException>(call);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies that <c>WaitForCompletion</c> with a scheduler returns after terminal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletionWithSchedulerOnly_ThenReturnsAfterTerminal()
    {
        Observable.Return(RxVoid.Default)
            .WaitForCompletion(TaskPoolSequencer.Default);

        // Sentinel follow-up to give TUnit a real assertion.
        var sentinel = Observable.Return(SchedulerSentinelValue).SubscribeGetValue();
        await Assert.That(sentinel).IsEqualTo(SchedulerSentinelValue);
    }

    /// <summary>Verifies that <c>WaitForCompletion</c> with a scheduler and explicit timeout returns after terminal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletionWithSchedulerAndTimeout_ThenReturnsAfterTerminal()
    {
        Observable.Return(RxVoid.Default)
            .WaitForCompletion(TaskPoolSequencer.Default, SchedulerWaitTimeout);

        var sentinel = Observable.Return(SchedulerSentinelValue).SubscribeGetValue();
        await Assert.That(sentinel).IsEqualTo(SchedulerSentinelValue);
    }

    /// <summary>Verifies that <c>WaitForError</c> with a scheduler returns null for a normal completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorWithSchedulerOnly_ThenReturnsNullOnNormalCompletion()
    {
        var error = Observable.Return(SchedulerSentinelValue)
            .WaitForError(TaskPoolSequencer.Default);

        await Assert.That(error).IsNull();
    }

    /// <summary>Verifies that <c>WaitForError</c> with a scheduler and timeout captures the error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorWithSchedulerAndTimeout_ThenCapturesError()
    {
        var expected = new InvalidOperationException("scheduler-captured");
        var error = Observable.Throw<int>(expected)
            .WaitForError(TaskPoolSequencer.Default, SchedulerWaitTimeout);

        await Assert.That(error).IsEqualTo(expected);
    }
}
