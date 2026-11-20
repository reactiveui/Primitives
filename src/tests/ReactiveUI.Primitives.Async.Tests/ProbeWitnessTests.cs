// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="ProbeWitness{T}"/> terminal gating and concurrent disposal safety.</summary>
public sealed class ProbeWitnessTests
{
    /// <summary>Value held when timer fires concurrently with completion.</summary>
    private const int HeldValue = 10;

    /// <summary>Period used in manual-clock probe tests.</summary>
    private static readonly TimeSpan SamplePeriod = TimeSpan.FromSeconds(1);

    /// <summary>Once OnCompletedAsync enters the downstream gate, a racing timer tick does not emit OnNext after completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompletedAsync_WhenTimerTickWaitsOnDownstreamGate_SuppressesPostCompletionOnNext()
    {
        ManualTimeProvider time = new();
        TaskCompletionSource<bool> enterCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> releaseCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<string> events = [];

        var observer = new ScriptedAsyncObserver<int>(
            onNext: (value, _) =>
            {
                lock (events)
                {
                    events.Add($"next:{value}");
                }

                return default;
            },
            onCompleted: async result =>
            {
                _ = result;
                _ = enterCompletion.TrySetResult(true);
                _ = await releaseCompletion.Task.ConfigureAwait(false);
                lock (events)
                {
                    events.Add("completed");
                }
            });

        await using ProbeWitness<int> witness = new(observer, SamplePeriod, time);

        // First value starts the periodic timer; complete the sequence while blocking inside downstream OnCompletedAsync.
        await witness.OnNextAsync(HeldValue, CancellationToken.None);
        var completionTask = witness.OnCompletedAsync(Result.Success).AsTask();
        _ = await enterCompletion.Task;

        // Seed a late held value and fire the registered timer so it waits on _downstreamGate behind completion.
        _ = witness.HoldAsync(HeldValue + 1, CancellationToken.None);
        await time.FireNextAsync();

        _ = releaseCompletion.TrySetResult(true);
        await completionTask;
        await Task.Yield();

        string[] snapshot;
        lock (events)
        {
            snapshot = [.. events];
        }

        await Assert.That(snapshot[^1]).IsEqualTo("completed");
    }

    /// <summary>Disposing ProbeWitness while a timer tick holds the downstream gate does not raise ObjectDisposedException.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsync_WhileTimerHoldsDownstreamGate_ReleasesWithoutUnhandledException()
    {
        using UnhandledExceptionCapture unhandled = new();
        ManualTimeProvider time = new();
        TaskCompletionSource<bool> insideOnNext = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> releaseOnNext = new(TaskCreationOptions.RunContinuationsAsynchronously);

        var observer = new ScriptedAsyncObserver<int>(
            onNext: async (_, _) =>
            {
                _ = insideOnNext.TrySetResult(true);
                _ = await releaseOnNext.Task.ConfigureAwait(false);
            },
            onCompleted: static _ => default);

        ProbeWitness<int> witness = new(observer, SamplePeriod, time);
        var tickTask = witness.HoldAsync(HeldValue, CancellationToken.None);
        await time.FireNextAsync();
        _ = await insideOnNext.Task;

        var disposeTask = witness.DisposeAsync().AsTask();
        _ = releaseOnNext.TrySetResult(true);
        await Task.WhenAll(tickTask, disposeTask);

        await Assert.That(unhandled.Count).IsZero();
    }

    /// <summary>Disposing a Probe subscription tears down the upstream subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsync_SubscribedProbe_DisposesUpstreamSubscription()
    {
        int[] disposals = [0];
        var source = SignalAsync.Create<int>((_, _) =>
            new(DisposableAsync.Create(disposals, static state =>
            {
                _ = Interlocked.Increment(ref state[0]);
                return default;
            })));

        var sub = await source.Probe(SamplePeriod).SubscribeAsync(static (_, _) => default, CancellationToken.None);
        await sub.DisposeAsync();

        await Assert.That(Volatile.Read(ref disposals[0])).IsEqualTo(1);
    }

    /// <summary>Verifies <c>Probe(period, timeProvider)</c>, resumed-error suppression after terminal, duplicate terminal guard, and unhandled tick exceptions.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ProbeOverloadAndTerminalAndTickExceptionGuards_BehaveAsExpected()
    {
        ManualTimeProvider time = new();
        var probed = SignalAsync.Empty<int>().Probe(SamplePeriod, time);
        await Assert.That(await probed.ToListAsync()).IsEmpty();

        List<Exception> resumed = [];
        CallbackWitnessAsync<int> observer = new(
            static (_, _) => default,
            (ex, _) =>
            {
                resumed.Add(ex);
                return default;
            });
        await using ProbeWitness<int> witness = new(observer, SamplePeriod, time);
        await witness.OnErrorResumeAsync(new InvalidOperationException("live"), CancellationToken.None);
        await witness.DeliverTerminalAsync(default, false, Result.Success);
        await witness.DeliverTerminalAsync(default, false, Result.Success);
        await ((IWitnessAsync<int>)witness).OnErrorResumeAsyncCore(new InvalidOperationException("late"), CancellationToken.None);
        await Assert.That(resumed).Count().IsEqualTo(1);

        using UnhandledExceptionCapture unhandled = new();
        InvalidOperationException tickFailure = new("tick-boom");
        ScriptedAsyncObserver<int> throwingObserver = new(
            onNext: (_, _) => throw tickFailure,
            onCompleted: static _ => default);
        await using ProbeWitness<int> throwingWitness = new(throwingObserver, SamplePeriod, time);
        var holdTask = throwingWitness.HoldAsync(HeldValue, CancellationToken.None);
        await time.FireNextAsync();
        await holdTask;
        var captured = await unhandled.WaitForAsync(ex => ReferenceEquals(ex, tickFailure));
        await Assert.That(captured).IsSameReferenceAs(tickFailure);
    }

    /// <summary>Configurable async observer for gate-ordering tests.</summary>
    /// <typeparam name="T">The observed element type.</typeparam>
    /// <param name="onNext">Value handler.</param>
    /// <param name="onCompleted">Completion handler.</param>
    private sealed class ScriptedAsyncObserver<T>(
        Func<T, CancellationToken, ValueTask> onNext,
        Func<Result, ValueTask> onCompleted) : IObserverAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) => onNext(value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => onCompleted(result);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
