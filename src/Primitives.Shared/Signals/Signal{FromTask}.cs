// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Provides static factory and operator methods for signals.</summary>
public static partial class Signal
{
    /// <summary>Handles Asnyc Tasks with cancellation.</summary>
    /// <param name="execution">The function to execute.</param>
    /// <returns>
    /// An ITaskSignal of T.
    /// </returns>
    public static ITaskSignal<RxVoid> FromTask(Func<CancellationTokenSource, Task<RxVoid>> execution) =>
        FromTask(execution, null, null);

    /// <summary>Handles Asnyc Tasks with cancellation.</summary>
    /// <param name="execution">The function to execute.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>
    /// An ITaskSignal of T.
    /// </returns>
    public static ITaskSignal<RxVoid> FromTask(
        Func<CancellationTokenSource, Task<RxVoid>> execution,
        ISequencer? scheduler) =>
        FromTask(execution, scheduler, null);

    /// <summary>Handles Asnyc Tasks with cancellation.</summary>
    /// <param name="execution">The function to execute.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="cancellationTokenSource">The cancellation token source.</param>
    /// <returns>
    /// An ITaskSignal of T.
    /// </returns>
    public static ITaskSignal<RxVoid> FromTask(
        Func<CancellationTokenSource, Task<RxVoid>> execution,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        CreateTaskSignal(execution, scheduler, cancellationTokenSource);

    /// <summary>Froms the asynchronous.</summary>
    /// <typeparam name="TResult">The type of the return value.</typeparam>
    /// <param name="actionAsync">The action asynchronous.</param>
    /// <returns>
    /// An TaskSignal of T.
    /// </returns>
    public static ITaskSignal<TResult> FromTask<TResult>(Func<CancellationTokenSource, Task<TResult>> actionAsync) =>
        FromTask(actionAsync, null, null);

    /// <summary>Froms the asynchronous.</summary>
    /// <typeparam name="TResult">The type of the return value.</typeparam>
    /// <param name="actionAsync">The action asynchronous.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>
    /// An TaskSignal of T.
    /// </returns>
    public static ITaskSignal<TResult> FromTask<TResult>(
        Func<CancellationTokenSource, Task<TResult>> actionAsync,
        ISequencer? scheduler) =>
        FromTask(actionAsync, scheduler, null);

    /// <summary>Froms the asynchronous.</summary>
    /// <typeparam name="TResult">The type of the return value.</typeparam>
    /// <param name="actionAsync">The action asynchronous.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="cancellationTokenSource">The cancellation token source.</param>
    /// <returns>
    /// An TaskSignal of T.
    /// </returns>
    public static ITaskSignal<TResult> FromTask<TResult>(
        Func<CancellationTokenSource, Task<TResult>> actionAsync,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        CreateTaskSignal(actionAsync, scheduler, cancellationTokenSource);

    /// <summary>Executes the CreateTaskSignal operation.</summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="execution">The execution value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    /// <param name="cancellationTokenSource">The cancellationTokenSource value.</param>
    /// <returns>The result.</returns>
    private static ITaskSignal<TResult> CreateTaskSignal<TResult>(
        Func<CancellationTokenSource, Task<TResult>> execution,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        ReferenceEquals(scheduler, Sequencer.Immediate)
            ? new ImmediateTaskSignal<TResult>(execution, cancellationTokenSource)
            : TaskSignal.Create<TResult>(
                ao => Lazy(() => Create<TResult>(observer => SubscribeTask(ao, execution, observer))),
                scheduler,
                cancellationTokenSource);

    /// <summary>Executes the SubscribeTask operation.</summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="signal">The signal value.</param>
    /// <param name="execution">The execution value.</param>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    private static IDisposable SubscribeTask<TResult>(
        ITaskSignal<TResult> signal,
        Func<CancellationTokenSource, Task<TResult>> execution,
        IObserver<TResult> observer)
    {
        var source = signal.CancellationTokenSource!;
        var token = source.Token;
        token.ThrowIfCancellationRequested();
        Task<TResult> task;
        try
        {
            task = execution(source) ?? throw new InvalidOperationException("The task factory returned null.");
        }
        catch (Exception error)
        {
            observer.OnError(error);
            return EmptyDisposable.Instance;
        }

        if (TryEmitSynchronously(task, observer, token))
        {
            return EmptyDisposable.Instance;
        }

        TaskStopGate gate = new();
        _ = ObserveTask(task.WhenCancelled(token), observer, gate, token);

        return CancelOnDispose(gate, source);
    }

    /// <summary>Builds a disposer that cancels the source if it wins the terminal transition.</summary>
    /// <param name="gate">The terminal-notification gate shared with the continuation.</param>
    /// <param name="source">The cancellation source to cancel on disposal.</param>
    /// <returns>The disposer.</returns>
    private static ActionDisposable CancelOnDispose(TaskStopGate gate, CancellationTokenSource source) =>
        new(() =>
        {
            if (!gate.TryStop())
            {
                return;
            }

            Cancel(source);
        });

    /// <summary>Emits a synchronous terminal notification when the task has already finished.</summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="task">The task to inspect.</param>
    /// <param name="observer">The observer value.</param>
    /// <param name="token">The token value.</param>
    /// <returns><see langword="true"/> when a synchronous terminal notification was produced.</returns>
    /// <remarks>
    /// Runs before any disposer is handed out, so no dispose race is possible and the emission is ungated.
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4462:Calls to \"async\" methods should not be blocking",
        Justification =
            "Synchronous read of an already-completed (RanToCompletion) task for an allocation-free fast path; await is invalid in this synchronous factory.")]
    private static bool TryEmitSynchronously<TResult>(
        Task<TResult> task,
        IObserver<TResult> observer,
        CancellationToken token)
    {
        if (task.Status == TaskStatus.RanToCompletion)
        {
            observer.OnNext(task.Result);
            observer.OnCompleted();
            return true;
        }

        if (task.IsCanceled || token.IsCancellationRequested)
        {
            observer.OnError(new OperationCanceledException());
            return true;
        }

        if (!task.IsFaulted)
        {
            return false;
        }

        observer.OnError(task.Exception!.InnerException ?? task.Exception);
        return true;
    }

    /// <summary>Observes a pending task and forwards the terminal notification while honoring disposal.</summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="cancellableTask">The cancellableTask value.</param>
    /// <param name="observer">The observer value.</param>
    /// <param name="gate">The terminal-notification gate shared with the disposer.</param>
    /// <param name="token">The token value.</param>
    /// <returns>The result.</returns>
    /// <remarks>
    /// The terminal notification is gated on <see cref="TaskStopGate.TryStop"/>, the same atomic
    /// transition the disposer wins when the subscription is torn down. Only the winner emits, so a
    /// subscription disposed while the task continuation runs never observes a post-dispose notification.
    /// </remarks>
    private static async Task ObserveTask<TResult>(
        Task<(TResult Value, bool IsCanceled)> cancellableTask,
        IObserver<TResult> observer,
        TaskStopGate gate,
        CancellationToken token)
    {
        try
        {
            var (result, isCanceled) = await cancellableTask.ConfigureAwait(false);
            if (!gate.TryStop())
            {
                return;
            }

            if (!isCanceled && !token.IsCancellationRequested)
            {
                observer.OnNext(result);
                observer.OnCompleted();
            }
            else
            {
                observer.OnError(new OperationCanceledException());
            }
        }
        catch (Exception error)
        {
            if (gate.TryStop())
            {
                observer.OnError(error);
            }
        }
    }

    /// <summary>Executes the Cancel operation.</summary>
    /// <param name="source">The source value.</param>
    private static void Cancel(CancellationTokenSource source)
    {
        try
        {
            source.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Another completion path already released the token source.
        }
    }

    /// <summary>Immediate task signal that subscribes directly instead of building a nested observable pipeline.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    private sealed class ImmediateTaskSignal<TResult> : ITaskSignal<TResult>
    {
        /// <summary>Executes the task.</summary>
        private readonly Func<CancellationTokenSource, Task<TResult>> _execution;

        /// <summary>Non-zero after disposal.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="ImmediateTaskSignal{TResult}"/> class.</summary>
        /// <param name="execution">Task factory.</param>
        /// <param name="cancellationTokenSource">Optional cancellation source.</param>
        public ImmediateTaskSignal(
            Func<CancellationTokenSource, Task<TResult>> execution,
            CancellationTokenSource? cancellationTokenSource)
        {
            _execution = execution ?? throw new ArgumentNullException(nameof(execution));
            SourceCore = cancellationTokenSource ?? new CancellationTokenSource();
        }

        /// <inheritdoc/>
        CancellationTokenSource? ITaskSignal<TResult>.CancellationTokenSource => SourceCore;

        /// <inheritdoc/>
        public bool IsCancellationRequested => SourceCore.IsCancellationRequested;

        /// <inheritdoc/>
        public IObservable<TResult>? Source => this;

        /// <inheritdoc/>
        public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

        /// <summary>Gets the owned cancellation source.</summary>
        private CancellationTokenSource SourceCore { get; }

        /// <inheritdoc/>
        public void GetOperationCanceled(IObserver<Exception> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            _ = SourceCore.Token.Register(
                static state => ((IObserver<Exception>)state!).OnNext(new OperationCanceledException()),
                observer,
                false);
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            ThrowIfDisposed();
            var token = SourceCore.Token;
            token.ThrowIfCancellationRequested();

            Task<TResult> task;
            try
            {
                task = _execution(SourceCore) ?? throw new InvalidOperationException("The task factory returned null.");
            }
            catch (Exception error)
            {
                observer.OnError(error);
                return EmptyDisposable.Instance;
            }

            if (TryEmitSynchronously(task, observer, token))
            {
                return EmptyDisposable.Instance;
            }

            TaskStopGate gate = new();
            _ = ObserveTask(task.WhenCancelled(token), observer, gate, token);

            return CancelOnDispose(gate, SourceCore);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            Cancel(SourceCore);
            SourceCore.Dispose();
        }

        /// <summary>Throws when disposed.</summary>
        private void ThrowIfDisposed()
        {
            if (!IsDisposed)
            {
                return;
            }

            throw new ObjectDisposedException(nameof(ImmediateTaskSignal<>));
        }
    }

    /// <summary>Atomic gate that serializes the terminal notification against subscription disposal.</summary>
    /// <remarks>
    /// Mirrors the <c>TaskInstanceSubscription.TryStop()</c> pattern: both the task continuation and the
    /// disposer race on a single <see cref="Interlocked.Exchange(ref int, int)"/>; only the winner proceeds,
    /// so a notification can never reach a subscription that has already been disposed.
    /// </remarks>
    private sealed class TaskStopGate
    {
        /// <summary>Non-zero once the continuation has emitted or the subscription has been disposed.</summary>
        private int _stopped;

        /// <summary>Attempts to win the terminal transition.</summary>
        /// <returns><see langword="true"/> when this caller won the stop race.</returns>
        public bool TryStop() => Interlocked.Exchange(ref _stopped, 1) == 0;
    }
}
