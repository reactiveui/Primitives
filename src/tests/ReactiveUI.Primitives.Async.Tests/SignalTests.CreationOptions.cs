// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for the option-driven <see cref="Signal"/> factories on their serial, stateful setting — the
/// default combination each factory maps to its serial signal type — plus the disposal contract of the
/// concurrent replay-latest signal those options can select.
/// </summary>
public partial class SignalTests
{
    /// <summary>The value the option-driven factories start from or publish.</summary>
    private const int OptionsStartValue = 7;

    /// <summary>A second value published after the first, used to prove the latest one is replayed.</summary>
    private const int OptionsLatestValue = 9;

    /// <summary>Verifies <c>Signal.Create(options)</c> with serial, stateful options returns a working signal
    /// that broadcasts published values to its subscribers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreateWithSerialStatefulOptions_ThenPublishesToSubscribers()
    {
        var signal = Signal.Create<int>(SignalCreationOptions.Default);
        TaskCompletionSource<int> received = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync((x, _) =>
        {
            IgnoredResult.Of(received.TrySetResult(x));
            return default;
        });

        await signal.OnNextAsync(OptionsStartValue, CancellationToken.None);

        var value = await received.Task.WaitAsync(WaitTimeout);
        await Assert.That(value).IsEqualTo(OptionsStartValue);
    }

    /// <summary>Verifies <c>Signal.CreateBehavior(startValue, options)</c> with serial, stateful options replays
    /// the start value to a subscriber that arrives before anything else is published.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreateBehaviorWithSerialStatefulOptions_ThenReplaysStartValue()
    {
        var signal = Signal.CreateBehavior(OptionsStartValue, BehaviorSignalCreationOptions.Default);
        TaskCompletionSource<int> received = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync((x, _) =>
        {
            IgnoredResult.Of(received.TrySetResult(x));
            return default;
        });

        var value = await received.Task.WaitAsync(WaitTimeout);
        await Assert.That(value).IsEqualTo(OptionsStartValue);
    }

    /// <summary>Verifies <c>Signal.CreateReplayLatest(options)</c> with serial, stateful options holds no value
    /// until one is published, then replays the most recent one to a late subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreateReplayLatestWithSerialStatefulOptions_ThenReplaysLatestValue()
    {
        var signal = Signal.CreateReplayLatest<int>(ReplayLatestSignalCreationOptions.Default);
        await signal.OnNextAsync(OptionsStartValue, CancellationToken.None);
        await signal.OnNextAsync(OptionsLatestValue, CancellationToken.None);

        TaskCompletionSource<int> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync((x, _) =>
        {
            IgnoredResult.Of(received.TrySetResult(x));
            return default;
        });

        var value = await received.Task.WaitAsync(WaitTimeout);
        await Assert.That(value).IsEqualTo(OptionsLatestValue);
    }

    /// <summary>Verifies that disposing the concurrent replay-latest signal cancels its lifetime, so a
    /// subsequent subscribe is rejected rather than silently attaching to a dead signal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentReplayLatestSignalDisposed_ThenLaterSubscribeIsCancelled()
    {
        BehaviorSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };
        var signal = Signal.CreateBehavior(OptionsStartValue, options);

        await signal.DisposeAsync();

        OperationCanceledException? caught = null;
        try
        {
            await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default);
        }
        catch (OperationCanceledException ex)
        {
            caught = ex;
        }

        await Assert.That(caught).IsNotNull();
    }

    /// <summary>Verifies the shared signal-state helper's <c>Values</c> projection hands back the signal itself —
    /// a signal is its own observable sequence, so no wrapper is allocated for the <c>Values</c> view.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSignalStateHelperValuesRequested_ThenReturnsTheSignalItself()
    {
        var signal = Signal.Create<int>();

        var values = SignalAsyncStateHelper.Values(signal);

        await Assert.That(ReferenceEquals(values, signal)).IsTrue();
        await Assert.That(values).IsSameReferenceAs(signal.Values);
    }
}
