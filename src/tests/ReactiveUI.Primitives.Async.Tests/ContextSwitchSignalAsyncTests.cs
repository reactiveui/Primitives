// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests notification context switches through explicitly delivered callbacks.</summary>
public class ContextSwitchSignalAsyncTests
{
    /// <summary>The value delivered by the source.</summary>
    private const int Sentinel = 7;

    /// <summary>Default context overloads construct context-switching signals without dispatching.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task DefaultContextOverloadsWrapTheSource()
    {
        var source = SignalAsync.Return(Sentinel);
        await Assert.That(source.WitnessOn(AsyncContext.Default, true)).IsTypeOf<WitnessOnSignal<int>>();
        await Assert.That(source.WitnessOn(AsyncContext.Default)).IsTypeOf<WitnessOnSignal<int>>();
        await Assert.That(source.WitnessOn(TaskScheduler.Default)).IsTypeOf<WitnessOnSignal<int>>();
        await Assert.That(source.WitnessOn(new SynchronizationContext())).IsTypeOf<WitnessOnSignal<int>>();
    }

    /// <summary>The async context overload waits for a callback before delivering a value.</summary>
    /// <param name="forceYielding">Whether to force a context switch.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AsyncContextOverloadWaitsForDelivery(bool forceYielding)
    {
        ManualContext context = new();
        var source = Signal.Create<int>();
        RecordingWitness observer = new();
        await using var subscription = await source.Values.WitnessOn(AsyncContext.From(context), forceYielding)
            .SubscribeAsync(observer, CancellationToken.None);
        var pending = source.OnNextAsync(Sentinel, CancellationToken.None);
        await Assert.That(observer.Value).IsNull();
        await Assert.That(context.PendingCount).IsEqualTo(1);
        context.RunNext();
        await pending;
        await Assert.That(observer.Value).IsEqualTo(Sentinel);
    }

    /// <summary>Synchronization context overloads retain notifications until their callback runs.</summary>
    /// <param name="forceYielding">Whether to use the forced-yield overload.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SynchronizationContextOverloadsWaitForDelivery(bool forceYielding)
    {
        ManualContext context = new();
        var source = Signal.Create<int>();
        RecordingWitness observer = new();
        var observed = forceYielding ? source.Values.WitnessOn(context, true) : source.Values.WitnessOn(context);
        await using var subscription = await observed.SubscribeAsync(observer, CancellationToken.None);
        var pending = source.OnNextAsync(Sentinel, CancellationToken.None);
        await Assert.That(observer.Value).IsNull();
        context.RunNext();
        await pending;
        await Assert.That(observer.Value).IsEqualTo(Sentinel);
    }

    /// <summary>Task scheduler overloads wait for explicit scheduler execution.</summary>
    /// <param name="forceYielding">Whether to use the forced-yield overload.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TaskSchedulerOverloadsWaitForDelivery(bool forceYielding)
    {
        ManualTaskScheduler scheduler = new();
        var source = Signal.Create<int>();
        RecordingWitness observer = new();
        var observed = forceYielding ? source.Values.WitnessOn(scheduler, true) : source.Values.WitnessOn(scheduler);
        await using var subscription = await observed.SubscribeAsync(observer, CancellationToken.None);
        var pending = source.OnNextAsync(Sentinel, CancellationToken.None);
        await Assert.That(observer.Value).IsNull();
        await Assert.That(scheduler.PendingCount).IsEqualTo(1);
        scheduler.RunNext();
        await pending;
        await Assert.That(observer.Value).IsEqualTo(Sentinel);
    }

    /// <summary>Failure completion is delivered only after the context callback runs.</summary>
    /// <param name="forceYielding">Whether to force a context switch.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FailureCompletionWaitsForDelivery(bool forceYielding)
    {
        ManualContext context = new();
        var source = Signal.Create<int>();
        RecordingWitness observer = new();
        InvalidOperationException expected = new();
        await using var subscription = await source.Values.WitnessOn(context, forceYielding)
            .SubscribeAsync(observer, CancellationToken.None);
        var pending = source.OnCompletedAsync(Result.Failure(expected));
        await Assert.That(observer.Completion).IsNull();
        context.RunNext();
        await pending;
        await Assert.That(observer.Completion!.Value.Exception).IsSameReferenceAs(expected);
    }

    /// <summary>Successful completion waits for the context callback.</summary>
    /// <param name="forceYielding">Whether to force a context switch.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SuccessfulCompletionWaitsForDelivery(bool forceYielding)
    {
        ManualContext context = new();
        var source = Signal.Create<int>();
        RecordingWitness observer = new();
        await using var subscription = await source.Values.WitnessOn(context, forceYielding)
            .SubscribeAsync(observer, CancellationToken.None);
        var pending = source.OnCompletedAsync(Result.Success);
        await Assert.That(observer.Completion).IsNull();
        context.RunNext();
        await pending;
        await Assert.That(observer.Completion!.Value.IsSuccess).IsTrue();
    }

    /// <summary>A resumable error waits for the context callback and preserves its identity.</summary>
    /// <param name="forceYielding">Whether to force a context switch.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ResumableErrorWaitsForDelivery(bool forceYielding)
    {
        ManualContext context = new();
        var source = Signal.Create<int>();
        RecordingWitness observer = new();
        InvalidOperationException expected = new();
        await using var subscription = await source.Values.WitnessOn(context, forceYielding)
            .SubscribeAsync(observer, CancellationToken.None);
        var pending = source.OnErrorResumeAsync(expected, CancellationToken.None);
        await Assert.That(observer.Error).IsNull();
        context.RunNext();
        await pending;
        await Assert.That(observer.Error).IsSameReferenceAs(expected);
    }

    /// <summary>Direct forwarding waits for each manually delivered continuation.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task DirectForwardingWaitsForEachDelivery()
    {
        ManualContext context = new();
        RecordingWitness observer = new();
        await using ContextSwitchSignalAsync<int>.ContextSwitchWitness witness = new(observer, AsyncContext.From(context), true);
        var value = witness.ForwardAfterContextSwitchAsync(Sentinel, CancellationToken.None);
        await Assert.That(observer.Value).IsNull();
        context.RunNext();
        await value;
        await Assert.That(observer.Value).IsEqualTo(Sentinel);
        InvalidOperationException expected = new();
        var error = witness.ForwardErrorAfterContextSwitchAsync(expected, CancellationToken.None);
        await Assert.That(observer.Error).IsNull();
        context.RunNext();
        await error;
        await Assert.That(observer.Error).IsSameReferenceAs(expected);
        var completion = witness.ForwardCompletionAfterContextSwitchAsync(Result.Success);
        await Assert.That(observer.Completion).IsNull();
        context.RunNext();
        await completion;
        await Assert.That(observer.Completion!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Subscribing directly wraps every notification kind in the selected context.</summary>
    /// <param name="forceYielding">Whether to force a context switch.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SubscribeWrapsAllNotificationKinds(bool forceYielding)
    {
        ManualContext context = new();
        var source = Signal.Create<int>();
        RecordingWitness observer = new();
        IObservableAsync<int> signal = new ContextSwitchSignalAsync<int>(source.Values, AsyncContext.From(context), forceYielding);
        await using var subscription = await signal.SubscribeAsync(observer, CancellationToken.None);
        var value = source.OnNextAsync(Sentinel, CancellationToken.None);
        context.RunNext();
        await value;
        await Assert.That(observer.Value).IsEqualTo(Sentinel);
        InvalidOperationException expected = new();
        var error = source.OnErrorResumeAsync(expected, CancellationToken.None);
        context.RunNext();
        await error;
        await Assert.That(observer.Error).IsSameReferenceAs(expected);
        var completion = source.OnCompletedAsync(Result.Success);
        context.RunNext();
        await completion;
        await Assert.That(observer.Completion!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Retains context continuations until explicitly invoked.</summary>
    private sealed class ManualContext : SynchronizationContext
    {
        /// <summary>Pending callbacks and their state.</summary>
        private readonly Queue<(SendOrPostCallback Callback, object? State)> _callbacks = new();

        /// <summary>Gets the number of pending callbacks.</summary>
        public int PendingCount => _callbacks.Count;

        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state) => _callbacks.Enqueue((d, state));

        /// <summary>Runs one retained continuation.</summary>
        public void RunNext()
        {
            var pending = _callbacks.Dequeue();
            pending.Callback(pending.State);
        }
    }

    /// <summary>Records notifications without scheduling continuations.</summary>
    private sealed class RecordingWitness : IObserverAsync<int>
    {
        /// <summary>Gets the last value.</summary>
        public int? Value { get; private set; }

        /// <summary>Gets the last resumable error.</summary>
        public Exception? Error { get; private set; }

        /// <summary>Gets the completion result.</summary>
        public Result? Completion { get; private set; }

        /// <inheritdoc/>
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken)
        {
            Value = value;
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
        {
            Error = error;
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result)
        {
            Completion = result;
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
