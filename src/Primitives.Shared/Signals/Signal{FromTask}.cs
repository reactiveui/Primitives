// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Provides static factory and operator methods for signals.</summary>
public static partial class Signal
{
    /// <summary>Creates a task-backed signal that runs the function on subscription, handing it the cancellation source disposal cancels.</summary>
    /// <param name="execution">The function to execute.</param>
    /// <returns>A signal that emits the function's result and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ITaskSignal<RxVoid> FromTask(Func<CancellationTokenSource, Task<RxVoid>> execution) =>
        FromTask(execution, null, null);

    /// <summary>Creates a task-backed signal that runs the function on subscription and notifies on the supplied sequencer.</summary>
    /// <param name="execution">The function to execute.</param>
    /// <param name="scheduler">The sequencer notifications are delivered on, or <see langword="null"/> for the current thread.</param>
    /// <returns>A signal that emits the function's result and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ITaskSignal<RxVoid> FromTask(
        Func<CancellationTokenSource, Task<RxVoid>> execution,
        ISequencer? scheduler) =>
        FromTask(execution, scheduler, null);

    /// <summary>Creates a task-backed signal that runs the function on subscription, observing the supplied cancellation source.</summary>
    /// <param name="execution">The function to execute.</param>
    /// <param name="scheduler">The sequencer notifications are delivered on, or <see langword="null"/> for the current thread.</param>
    /// <param name="cancellationTokenSource">The cancellation source handed to the function, or <see langword="null"/> to own a new one.</param>
    /// <returns>A signal that emits the function's result and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ITaskSignal<RxVoid> FromTask(
        Func<CancellationTokenSource, Task<RxVoid>> execution,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        CreateTaskSignal(execution, scheduler, cancellationTokenSource);

    /// <summary>Creates a task-backed signal that runs the function on subscription, handing it the cancellation source disposal cancels.</summary>
    /// <typeparam name="TResult">The type of the return value.</typeparam>
    /// <param name="actionAsync">The function to execute.</param>
    /// <returns>A signal that emits the function's result and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ITaskSignal<TResult> FromTask<TResult>(Func<CancellationTokenSource, Task<TResult>> actionAsync) =>
        FromTask(actionAsync, null, null);

    /// <summary>Creates a task-backed signal that runs the function on subscription and notifies on the supplied sequencer.</summary>
    /// <typeparam name="TResult">The type of the return value.</typeparam>
    /// <param name="actionAsync">The function to execute.</param>
    /// <param name="scheduler">The sequencer notifications are delivered on, or <see langword="null"/> for the current thread.</param>
    /// <returns>A signal that emits the function's result and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ITaskSignal<TResult> FromTask<TResult>(
        Func<CancellationTokenSource, Task<TResult>> actionAsync,
        ISequencer? scheduler) =>
        FromTask(actionAsync, scheduler, null);

    /// <summary>Creates a task-backed signal that runs the function on subscription, observing the supplied cancellation source.</summary>
    /// <typeparam name="TResult">The type of the return value.</typeparam>
    /// <param name="actionAsync">The function to execute.</param>
    /// <param name="scheduler">The sequencer notifications are delivered on, or <see langword="null"/> for the current thread.</param>
    /// <param name="cancellationTokenSource">The cancellation source handed to the function, or <see langword="null"/> to own a new one.</param>
    /// <returns>A signal that emits the function's result and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ITaskSignal<TResult> FromTask<TResult>(
        Func<CancellationTokenSource, Task<TResult>> actionAsync,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        CreateTaskSignal(actionAsync, scheduler, cancellationTokenSource);

    /// <summary>Builds a disposer that cancels the source if it wins the terminal transition.</summary>
    /// <param name="gate">The terminal-notification gate shared with the continuation.</param>
    /// <param name="source">The cancellation source to cancel on disposal.</param>
    /// <returns>The disposer.</returns>
    internal static ActionDisposable CancelOnDispose(TaskStopGate gate, CancellationTokenSource source) =>
        new(() =>
        {
            if (!gate.TryStop())
            {
                return;
            }

            Cancel(source);
        });

    /// <summary>Delivers task completion only if the subscription has not claimed disposal.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="cancellableTask">The task raced against cancellation.</param>
    /// <param name="observer">The observer receiving the notification.</param>
    /// <param name="gate">The terminal-notification gate shared with the disposer.</param>
    /// <param name="token">The token checked for cancellation.</param>
    /// <returns>A task that completes once the notification is forwarded or suppressed.</returns>
    internal static async Task ObserveTask<TResult>(
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

    /// <summary>Builds the task-backed signal, taking a direct-subscription form for the immediate sequencer.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="execution">The function to execute.</param>
    /// <param name="scheduler">The sequencer notifications are delivered on.</param>
    /// <param name="cancellationTokenSource">The cancellation source handed to the function.</param>
    /// <returns>The task-backed signal.</returns>
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

    /// <summary>Starts the task for one subscription and forwards its terminal notification to the observer.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="signal">The signal whose cancellation source the task runs under.</param>
    /// <param name="execution">The function producing the task.</param>
    /// <param name="observer">The observer receiving the result.</param>
    /// <returns>A disposable that cancels the task when it wins the terminal transition.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="execution"/> returned <see langword="null"/>.</exception>
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

    /// <summary>Delivers synchronous completion before the subscription handle is returned.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="task">The task to inspect.</param>
    /// <param name="observer">The observer receiving the notification.</param>
    /// <param name="token">The token checked for cancellation.</param>
    /// <returns><see langword="true"/> when a synchronous terminal notification was produced.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Concurrency",
        "PSH1315:A blocking wait on an awaitable that may not be done",
        Justification = "The result is read only once the task status is RanToCompletion.")]
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

        observer.OnError(task.Exception!.InnerException!);
        return true;
    }

    /// <summary>Cancels the source, tolerating a source another completion path disposed.</summary>
    /// <param name="source">The cancellation source to cancel.</param>
    private static void Cancel(CancellationTokenSource source)
    {
        try
        {
            source.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Cancellation remains harmless after task completion.
        }
    }

    /// <summary>Claims completion before notifying the observer, excluding disposed subscriptions.</summary>
    internal sealed class TaskStopGate
    {
        /// <summary>Non-zero once the continuation has emitted or the subscription has been disposed.</summary>
        private int _stopped;

        /// <summary>Attempts to win the terminal transition.</summary>
        /// <returns><see langword="true"/> when this caller won the stop race.</returns>
        internal bool TryStop() => Interlocked.Exchange(ref _stopped, 1) == 0;
    }

    /// <summary>Task signal that starts the task in <c>Subscribe</c> rather than through a nested observable pipeline.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    private sealed class ImmediateTaskSignal<TResult> : ITaskSignal<TResult>
    {
        /// <summary>The factory that starts the task for each subscription.</summary>
        private readonly Func<CancellationTokenSource, Task<TResult>> _execution;

        /// <summary>Non-zero after disposal.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="ImmediateTaskSignal{TResult}"/> class.</summary>
        /// <param name="execution">Task factory.</param>
        /// <param name="cancellationTokenSource">Optional cancellation source.</param>
        /// <exception cref="ArgumentNullException"><paramref name="execution"/> is <see langword="null"/>.</exception>
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

        /// <summary>Throws when the signal has been disposed.</summary>
        /// <exception cref="ObjectDisposedException">The signal has been disposed.</exception>
        private void ThrowIfDisposed()
        {
            if (!IsDisposed)
            {
                return;
            }

            throw new ObjectDisposedException(nameof(ImmediateTaskSignal<>));
        }
    }
}
