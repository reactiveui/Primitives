// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="ContextSwitchSignalAsync{T}"/> — exercises the
/// <c>forceYielding: true</c> slow-path branches that switch context on every
/// <c>OnNext</c> / <c>OnErrorResume</c> / <c>OnCompleted</c> regardless of whether
/// the call site is already on the target context.</summary>
public class ObserveOnAsyncSignalTests
{
    /// <summary>Single sentinel emitted by the happy-path tests.</summary>
    private const int Sentinel = 7;

    /// <summary>Maximum time a test waits for a forwarded notification to arrive.</summary>
    private static readonly TimeSpan ForwardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies the <c>forceYielding: true</c> overload forwards values via the context-switching slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForceYielding_ThenValueForwarded()
    {
        var result = await SignalAsync.Return(Sentinel)
            .WitnessOn(AsyncContext.Default, true)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(Sentinel);
    }

    /// <summary>Verifies the <c>forceYielding: true</c> overload routes <c>OnErrorResume</c> through the context-switching slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForceYieldingSourceErrors_ThenErrorForwarded()
    {
        InvalidOperationException expected = new("forced");
        InvalidOperationException? caught = null;

        try
        {
            await SignalAsync.Throw<int>(expected)
                .WitnessOn(AsyncContext.Default, true)
                .ToListAsync();
        }
        catch (InvalidOperationException ex)
        {
            caught = ex;
        }

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies the <c>forceYielding: true</c> overload routes the completion notification through the context-switching slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForceYieldingSourceEmpty_ThenCompletesSuccessfully()
    {
        var result = await SignalAsync.Empty<int>()
            .WitnessOn(AsyncContext.Default, true)
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Verifies the <c>SynchronizationContext</c> + <c>forceYielding: true</c> overload also forwards values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncContextForceYielding_ThenEmits()
    {
        var ctx = SynchronizationContext.Current ?? new SynchronizationContext();

        var result = await SignalAsync.Return(Sentinel)
            .WitnessOn(ctx, true)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(Sentinel);
    }

    /// <summary>Verifies the default <c>SynchronizationContext</c> overload forwards through the non-forced wrapper.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncContextDefaultOverload_ThenEmits()
    {
        var ctx = SynchronizationContext.Current ?? new SynchronizationContext();

        var result = await SignalAsync.Return(Sentinel)
            .WitnessOn(ctx)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(Sentinel);
    }

    /// <summary>Verifies the default <see cref="TaskScheduler"/> overload forwards through the non-forced wrapper.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskSchedulerDefaultOverload_ThenEmits()
    {
        var result = await SignalAsync.Return(Sentinel)
            .WitnessOn(TaskScheduler.Default)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(Sentinel);
    }

    /// <summary>Verifies that <c>ObserveOn</c> with a different SynchronizationContext routes
    /// the error through the slow-path context-switch even when <c>forceYielding</c> is false.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnDifferentContextSourceErrors_ThenForwardedViaSlowPath()
    {
        InvalidOperationException expected = new("differing-context-error");
        InvalidOperationException? caught = null;
        SynchronizationContext customCtx = new();

        try
        {
            await SignalAsync.Throw<int>(expected)
                .WitnessOn(customCtx, false)
                .ToListAsync();
        }
        catch (InvalidOperationException ex)
        {
            caught = ex;
        }

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>ObserveOn</c> with a different SynchronizationContext routes
    /// the completion through the slow-path context-switch even when <c>forceYielding</c> is false.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnDifferentContextSourceEmpty_ThenCompletesViaSlowPath()
    {
        SynchronizationContext customCtx = new();

        var result = await SignalAsync.Empty<int>()
            .WitnessOn(customCtx, false)
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Exercises <c>ContextSwitchObserver.OnErrorResumeAsyncCore</c>'s slow-path branch —
    /// when <c>forceYielding == true</c>, the resumable-error path returns
    /// <c>ForwardErrorAfterContextSwitchAsync(...)</c> rather than the fast-path direct forward.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForceYieldingSourceEmitsResumableError_ThenSlowPathForwards()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        TaskCompletionSource errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .WitnessOn(AsyncContext.Default, true)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    IgnoredResult.Of(errorTcs.TrySetResult());
                    return default;
                });

        InvalidOperationException expected = new("observeon-resume");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(ForwardTimeout);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>ContextSwitchObserver.ForwardAfterContextSwitchAsync</c> by calling it directly
    /// — the slow path performs the context switch and then forwards the value downstream,
    /// independent of the fast/slow choice in <c>OnNextAsyncCore</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardAfterContextSwitchAsyncInvokedDirectly_ThenValueForwarded()
    {
        TaskCompletionSource<int> captured = new(TaskCreationOptions.RunContinuationsAsynchronously);
        CapturingAsyncWitness<int> downstream = new(captured);
        ContextSwitchSignalAsync<int>.ContextSwitchWitness sut = new(downstream, AsyncContext.Default, true);

        await sut.ForwardAfterContextSwitchAsync(Sentinel, CancellationToken.None);

        var received = await captured.Task.WaitAsync(ForwardTimeout);
        await Assert.That(received).IsEqualTo(Sentinel);
    }

    /// <summary>Verifies <c>ContextSwitchObserver.ForwardErrorAfterContextSwitchAsync</c> by calling it directly.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardErrorAfterContextSwitchAsyncInvokedDirectly_ThenErrorForwarded()
    {
        TaskCompletionSource<Exception> captured = new(TaskCreationOptions.RunContinuationsAsynchronously);
        CapturingAsyncWitness<int> downstream = new(captured);
        ContextSwitchSignalAsync<int>.ContextSwitchWitness sut = new(downstream, AsyncContext.Default, true);
        InvalidOperationException expected = new("slow-path-error");

        await sut.ForwardErrorAfterContextSwitchAsync(expected, CancellationToken.None);

        var received = await captured.Task.WaitAsync(ForwardTimeout);
        await Assert.That(received).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>ContextSwitchObserver.ForwardCompletionAfterContextSwitchAsync</c> by calling it directly.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardCompletionAfterContextSwitchAsyncInvokedDirectly_ThenCompletionForwarded()
    {
        TaskCompletionSource<Result> captured = new(TaskCreationOptions.RunContinuationsAsynchronously);
        CapturingAsyncWitness<int> downstream = new(captured);
        ContextSwitchSignalAsync<int>.ContextSwitchWitness sut = new(downstream, AsyncContext.Default, true);

        await sut.ForwardCompletionAfterContextSwitchAsync(Result.Success);

        var result = await captured.Task.WaitAsync(ForwardTimeout);
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that subscribing to a <see cref="ContextSwitchSignalAsync{T}"/> directly — rather than
    /// through <c>WitnessOn</c>, which builds a <c>WitnessOnSignal</c> instead — wraps the downstream observer in
    /// the context-switching witness and forwards source values through it.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContextSwitchSignalSubscribed_ThenValueForwarded()
    {
        var signal = Signal.Create<int>();
        ContextSwitchSignalAsync<int> sut = new(signal.Values, AsyncContext.Default, true);
        TaskCompletionSource<int> received = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await sut.SubscribeAsync((x, _) =>
        {
            IgnoredResult.Of(received.TrySetResult(x));
            return default;
        });

        await signal.OnNextAsync(Sentinel, CancellationToken.None);

        var value = await received.Task.WaitAsync(ForwardTimeout);
        await Assert.That(value).IsEqualTo(Sentinel);
    }

    /// <summary>Verifies that a resumable error raised by the source of a <see cref="ContextSwitchSignalAsync{T}"/>
    /// reaches the downstream observer through the witness's <c>OnErrorResumeAsyncCore</c> context-switch path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContextSwitchSignalSourceEmitsResumableError_ThenErrorForwarded()
    {
        var signal = Signal.Create<int>();
        ContextSwitchSignalAsync<int> sut = new(signal.Values, AsyncContext.Default, true);
        Exception? caught = null;
        TaskCompletionSource errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await sut.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                IgnoredResult.Of(errorTcs.TrySetResult());
                return default;
            });

        InvalidOperationException expected = new("context-switch-resume");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(ForwardTimeout);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Test observer that captures the first <c>OnNextAsync</c> value, the first
    /// <c>OnErrorResumeAsync</c> exception, and the <c>OnCompletedAsync</c> result via TCSes.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class CapturingAsyncWitness<T> : IObserverAsync<T>
    {
        /// <summary>Captures the first <c>OnNextAsync</c> value, if a TCS was supplied.</summary>
        private readonly TaskCompletionSource<T>? _onNext;

        /// <summary>Captures the first <c>OnErrorResumeAsync</c> exception, if a TCS was supplied.</summary>
        private readonly TaskCompletionSource<Exception>? _onError;

        /// <summary>Captures the <c>OnCompletedAsync</c> result, if a TCS was supplied.</summary>
        private readonly TaskCompletionSource<Result>? _onCompleted;

        /// <summary>Initializes a new instance of the <see cref="CapturingAsyncWitness{T}"/> class with an <c>OnNext</c> capture target.</summary>
        /// <param name="onNext">The TCS that receives the first <c>OnNextAsync</c> value.</param>
        public CapturingAsyncWitness(TaskCompletionSource<T> onNext) => _onNext = onNext;

        /// <summary>Initializes a new instance of the <see cref="CapturingAsyncWitness{T}"/> class with an <c>OnErrorResume</c> capture target.</summary>
        /// <param name="onError">The TCS that receives the first <c>OnErrorResumeAsync</c> exception.</param>
        public CapturingAsyncWitness(TaskCompletionSource<Exception> onError) => _onError = onError;

        /// <summary>Initializes a new instance of the <see cref="CapturingAsyncWitness{T}"/> class with an <c>OnCompleted</c> capture target.</summary>
        /// <param name="onCompleted">The TCS that receives the <c>OnCompletedAsync</c> result.</param>
        public CapturingAsyncWitness(TaskCompletionSource<Result> onCompleted) => _onCompleted = onCompleted;

        /// <inheritdoc/>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken)
        {
            _onNext?.TrySetResult(value);
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
        {
            _onError?.TrySetResult(error);
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result)
        {
            _onCompleted?.TrySetResult(result);
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
