// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Collections.Concurrent;
using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Covers renamed async internal members and scheduler adapters that are part of the current PR diff.</summary>
public sealed class AsyncRenameCoverageTests
{
    /// <summary>Verifies renamed <see cref = "AsyncContext"/> default-context and sequencer scheduler members.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task AsyncContextRenamedMembersExposeDefaultAndSequencerSchedulerPaths()
    {
        var sequencer = new QueuedSequencer();
        var sequencerContext = AsyncContext.From(sequencer);
        var scheduler = new AsyncContext.SequencerTaskScheduler(sequencer);
        var syncSequencer = new SynchronizationSequencer();
        var syncSequencerContext = AsyncContext.From((ISequencer)syncSequencer);
        var sameInSequencer = false;
        var ran = false;
        await Assert.That(AsyncContext.Default.UsesDefaultSequencer).IsTrue();
        await Assert.That(sequencerContext.UsesDefaultSequencer).IsFalse();
        await Assert.That(AsyncContext.From(new SynchronizationContext()).UsesDefaultSequencer).IsFalse();
        await Assert.That(AsyncContext.From(NewThreadTaskScheduler.Instance).UsesDefaultSequencer).IsFalse();
        await Assert.That(syncSequencerContext.SynchronizationContext).IsSameReferenceAs(syncSequencer);
        await Assert.That(sequencerContext.IsSameAsCurrentAsyncContext()).IsFalse();
        await Assert.That(scheduler.Sequencer).IsSameReferenceAs(sequencer);
        await Assert.That(scheduler.GetScheduledTasksForTesting()).IsNull();
        var task = Task.Factory.StartNew(
            () =>
        {
            sameInSequencer = sequencerContext.IsSameAsCurrentAsyncContext();
            ran = true;
        },
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            scheduler);
        await Assert.That(task.IsCompleted).IsFalse();
        sequencer.DrainAll();
        await task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await Assert.That(ran).IsTrue();
        await Assert.That(sameInSequencer).IsTrue();
        await Assert.That(
            scheduler.TryExecuteTaskInlineForTesting(
                new Task(() =>
                {
                }),
                taskWasPreviouslyQueued: false)).IsFalse();
    }

    /// <summary>Verifies current-context capture and explicit awaiter scheduling branches.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task AsyncContextCurrentAndSwitcherBranchesCoverCustomSchedulersAndCancellation()
    {
        var previous = SynchronizationContext.Current;
        var currentContext = new SynchronizationContext();
        try
        {
            SynchronizationContext.SetSynchronizationContext(currentContext);
            var captured = AsyncContext.GetCurrent();
            await Assert.That(captured.SynchronizationContext).IsSameReferenceAs(currentContext);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }

        var cancellationCallbacks = 0;
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(false);
        var canceledAwaitable = AsyncContext.Default.SwitchContextAsync(forceYielding: true, cancellation.Token);
        canceledAwaitable.OnCompleted(() => cancellationCallbacks++);
        await Assert.That(cancellationCallbacks).IsEqualTo(1);
        var scheduled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var schedulerAwaitable = AsyncContext.From(NewThreadTaskScheduler.Instance).SwitchContextAsync(forceYielding: true, CancellationToken.None);
        schedulerAwaitable.OnCompleted(scheduled.SetResult);
        await scheduled.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
    }

    /// <summary>Verifies task-signal completion failures are routed through the unhandled exception hook.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskSignalSubscriptionCompleteWithFailureReportsThrownCompletion()
    {
        using var unhandled = new UnhandledExceptionCapture();
        var expected = new InvalidOperationException("task-signal-completion");
        var observer = new ThrowingCompletionWitness(expected);
        await TaskSignalSubscription<int>.CompleteWithFailureAsync(observer, new InvalidOperationException("source")).ConfigureAwait(false);
        var reported = await unhandled.WaitForAsync(expected.Message, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await Assert.That(reported).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies renamed <see cref = "WitnessAsync{T}"/> disposal members track and dispose an assigned source subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverAsyncRenamedDisposalMembersTrackAssignedSubscription()
    {
        var disposed = 0;
        var observer = new RenameCoverageWitness();
        await Assert.That(observer.HasDisposed).IsFalse();
        await observer.AssignSourceSubscriptionAsync(new CallbackAsyncDisposable(() => disposed++)).ConfigureAwait(false);
        await observer.DisposeAsync().ConfigureAwait(false);
        await Assert.That(observer.HasDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Verifies observer disposal reports failures thrown by the assigned source subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverAsyncDisposeReportsAssignedSubscriptionFailure()
    {
        using var unhandled = new UnhandledExceptionCapture();
        var expected = new InvalidOperationException("assigned-dispose");
        var observer = new RenameCoverageWitness();
        await observer.AssignSourceSubscriptionAsync(new ThrowingAsyncDisposable(expected)).ConfigureAwait(false);
        await observer.DisposeAsync().ConfigureAwait(false);
        var reported = await unhandled.WaitForAsync(expected.Message, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await Assert.That(reported).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies renamed <see cref = "WitnessAsync{T}.RouteObserverErrorAsync"/> routes canceled and thrown handlers through the unhandled exception hook.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RouteObserverErrorAsyncReportsCanceledAndThrownHandlerPaths()
    {
        using var unhandled = new UnhandledExceptionCapture();
        var canceledObserver = new RenameCoverageWitness();
        var canceledError = new InvalidOperationException("route-canceled");
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(false);
        await canceledObserver.RouteObserverErrorAsync(canceledError, cancellation.Token).ConfigureAwait(false);
        var canceledReported = await unhandled.WaitForAsync(canceledError.Message, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await Assert.That(canceledReported).IsSameReferenceAs(canceledError);
        var operationCanceledError = new InvalidOperationException("route-operation-canceled");
        var operationCanceledObserver = new RenameCoverageWitness((_, _) => throw new OperationCanceledException());
        await operationCanceledObserver.RouteObserverErrorAsync(operationCanceledError, CancellationToken.None).ConfigureAwait(false);
        var operationCanceledReported = await unhandled.WaitForAsync(operationCanceledError.Message, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await Assert.That(operationCanceledReported).IsSameReferenceAs(operationCanceledError);
        var handlerError = new InvalidOperationException("route-handler");
        var throwingObserver = new RenameCoverageWitness((_, _) => throw handlerError);
        await throwingObserver.RouteObserverErrorAsync(new InvalidOperationException("source"), CancellationToken.None).ConfigureAwait(false);
        var handlerReported = await unhandled.WaitForAsync(handlerError.Message, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await Assert.That(handlerReported).IsSameReferenceAs(handlerError);
    }

    /// <summary>Verifies completion slow-path failures are routed through the renamed unhandled exception hook.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverAsyncCompletionSlowPathReportsThrownCompletion()
    {
        using var unhandled = new UnhandledExceptionCapture();
        var expected = new InvalidOperationException("completion-slow");
        var observer = new RenameCoverageWitness(onCompleted: _ => new ValueTask(Task.FromException(expected)));
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        var reported = await unhandled.WaitForAsync(expected.Message, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await Assert.That(reported).IsSameReferenceAs(expected);
    }

    /// <summary>Test observer exposing the renamed internal observer members.</summary>
    /// <param name = "onError">Optional error handler used by <see cref = "OnErrorResumeAsyncCore"/>.</param>
    /// <param name = "onCompleted">Optional completion handler used by <see cref = "OnCompletedAsyncCore"/>.</param>
    private sealed class RenameCoverageWitness(Func<Exception, CancellationToken, ValueTask>? onError = null, Func<Result, ValueTask>? onCompleted = null) : WitnessAsync<int>
    {
        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) => onCompleted?.Invoke(result) ?? default;

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) => onError?.Invoke(error, cancellationToken) ?? default;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(int value, CancellationToken cancellationToken) => default;
    }

    /// <summary>Async disposable that invokes a callback when disposed.</summary>
    /// <param name = "onDispose">The callback invoked during disposal.</param>
    private sealed class CallbackAsyncDisposable(Action onDispose) : IAsyncDisposable
    {
        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask DisposeAsync()
        {
            onDispose();
            return default;
        }
    }

    /// <summary>Async disposable that throws the supplied exception when disposed.</summary>
    /// <param name = "error">The exception thrown during disposal.</param>
    private sealed class ThrowingAsyncDisposable(Exception error) : IAsyncDisposable
    {
        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask DisposeAsync() => throw error;
    }

    /// <summary>Observer that throws when completion is delivered.</summary>
    /// <param name = "error">The exception to throw from completion.</param>
    private sealed class ThrowingCompletionWitness(Exception error) : IObserverAsync<int>
    {
        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask DisposeAsync() => default;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnCompletedAsync(Result result) => throw error;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) => default;
    }

    /// <summary>Synchronization-context-backed sequencer used to exercise <see cref = "AsyncContext.From(ISequencer)"/>.</summary>
    private sealed class SynchronizationSequencer : SynchronizationContext, ISequencer
    {
        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <inheritdoc/>
        public long Timestamp => 0;

        /// <inheritdoc/>
        public void Schedule(IWorkItem item) => item.Execute();

        /// <inheritdoc/>
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);
    }

    /// <summary>Sequencer that queues scheduled work until the test drains it.</summary>
    private sealed class QueuedSequencer : ISequencer
    {
        /// <summary>Fixed deterministic timestamp.</summary>
        private static readonly DateTimeOffset FixedNow = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>Scheduled work items.</summary>
        private readonly ConcurrentQueue<IWorkItem> _items = new();

        /// <inheritdoc/>
        public DateTimeOffset Now => FixedNow;

        /// <inheritdoc/>
        public long Timestamp => FixedNow.Ticks;

        /// <inheritdoc/>
        public void Schedule(IWorkItem item) => _items.Enqueue(item);

        /// <inheritdoc/>
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);

        /// <summary>Executes all queued work items.</summary>
        public void DrainAll()
        {
            while (_items.TryDequeue(out var item))
            {
                item.Execute();
            }
        }
    }
}
