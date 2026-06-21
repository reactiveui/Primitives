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
    /// <summary>Stores state for the signal implementation.</summary>
    private const int TaskCompleted = 1;

    /// <summary>Stores state for the signal implementation.</summary>
    private const int TaskFaulted = 2;

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
    public static ITaskSignal<RxVoid> FromTask(Func<CancellationTokenSource, Task<RxVoid>> execution, ISequencer? scheduler) =>
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
        CreateTaskSignal(execution, static _ => true, scheduler, cancellationTokenSource);

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
    public static ITaskSignal<TResult> FromTask<TResult>(Func<CancellationTokenSource, Task<TResult>> actionAsync, ISequencer? scheduler) =>
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
        CreateTaskSignal(actionAsync, static _ => true, scheduler, cancellationTokenSource);

    /// <summary>Executes the CreateTaskSignal operation.</summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="execution">The execution value.</param>
    /// <param name="shouldEmit">The shouldEmit value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    /// <param name="cancellationTokenSource">The cancellationTokenSource value.</param>
    /// <returns>The result.</returns>
    private static ITaskSignal<TResult> CreateTaskSignal<TResult>(
        Func<CancellationTokenSource, Task<TResult>> execution,
        Func<TResult, bool> shouldEmit,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        ReferenceEquals(scheduler, Sequencer.Immediate)
            ? new ImmediateTaskSignal<TResult>(execution, shouldEmit, cancellationTokenSource)
            : TaskSignal.Create<TResult>(
                ao => Lazy(() => Create<TResult>(observer => SubscribeTask(ao, execution, shouldEmit, observer))),
                scheduler,
                cancellationTokenSource);

    /// <summary>Executes the SubscribeTask operation.</summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="signal">The signal value.</param>
    /// <param name="execution">The execution value.</param>
    /// <param name="shouldEmit">The shouldEmit value.</param>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4462:Calls to \"async\" methods should not be blocking",
        Justification = "Synchronous read of an already-completed (RanToCompletion) task for an allocation-free fast path; await is invalid in this synchronous factory.")]
    private static IDisposable SubscribeTask<TResult>(
        ITaskSignal<TResult> signal,
        Func<CancellationTokenSource, Task<TResult>> execution,
        Func<TResult, bool> shouldEmit,
        IObserver<TResult> observer)
    {
        var source = signal.CancellationTokenSource!;
        var token = source.Token;
        token.ThrowIfCancellationRequested();
        var completionState = 0;
        Task<TResult> task;
        try
        {
            task = execution(source) ?? throw new InvalidOperationException("The task factory returned null.");
        }
        catch (Exception error)
        {
            Volatile.Write(ref completionState, TaskFaulted);
            observer.OnError(error);
            return EmptyDisposable.Instance;
        }

        if (task.Status == TaskStatus.RanToCompletion)
        {
            var result = task.Result;
            if (shouldEmit(result))
            {
                observer.OnNext(result);
                Volatile.Write(ref completionState, TaskCompleted);
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            Volatile.Write(ref completionState, TaskFaulted);
            observer.OnError(new OperationCanceledException());
            return EmptyDisposable.Instance;
        }

        if (task.IsCanceled || token.IsCancellationRequested)
        {
            Volatile.Write(ref completionState, TaskFaulted);
            observer.OnError(new OperationCanceledException());
            return EmptyDisposable.Instance;
        }

        if (task.IsFaulted)
        {
            Volatile.Write(ref completionState, TaskFaulted);
            observer.OnError(task.Exception!.InnerException ?? task.Exception);
            return EmptyDisposable.Instance;
        }

        var cancellableTask = task.WhenCancelled(token);

        _ = ObserveTask(
            cancellableTask,
            shouldEmit,
            observer,
            () => Volatile.Write(ref completionState, TaskCompleted),
            () => Volatile.Write(ref completionState, TaskFaulted),
            token);

        return new ActionDisposable(() =>
        {
            if (Volatile.Read(ref completionState) == TaskCompleted)
            {
                return;
            }

            Cancel(source);
        });
    }

    /// <summary>Executes the ObserveTask operation.</summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="cancellableTask">The cancellableTask value.</param>
    /// <param name="shouldEmit">The shouldEmit value.</param>
    /// <param name="observer">The observer value.</param>
    /// <param name="setCompleted">The setCompleted value.</param>
    /// <param name="setFaulted">The setFaulted value.</param>
    /// <param name="token">The token value.</param>
    /// <returns>The result.</returns>
    private static async Task ObserveTask<TResult>(
        Task<(TResult Value, bool IsCanceled)> cancellableTask,
        Func<TResult, bool> shouldEmit,
        IObserver<TResult> observer,
        Action setCompleted,
        Action setFaulted,
        CancellationToken token)
    {
        try
        {
            var (result, isCanceled) = await cancellableTask.ConfigureAwait(false);
            if (!isCanceled && !token.IsCancellationRequested && shouldEmit(result))
            {
                observer.OnNext(result);
                setCompleted();
                observer.OnCompleted();
                return;
            }

            setFaulted();
            observer.OnError(new OperationCanceledException());
        }
        catch (Exception error)
        {
            setFaulted();
            observer.OnError(error);
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

        /// <summary>Filters successful task results.</summary>
        private readonly Func<TResult, bool> _shouldEmit;

        /// <summary>Non-zero after disposal.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="ImmediateTaskSignal{TResult}"/> class.</summary>
        /// <param name="execution">Task factory.</param>
        /// <param name="shouldEmit">Result filter.</param>
        /// <param name="cancellationTokenSource">Optional cancellation source.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0001:Simplify Names", Justification = "The argument validation uses ArgumentExceptionHelper")]
        public ImmediateTaskSignal(
            Func<CancellationTokenSource, Task<TResult>> execution,
            Func<TResult, bool> shouldEmit,
            CancellationTokenSource? cancellationTokenSource)
        {
            _execution = execution ?? throw new ArgumentNullException(nameof(execution));
            _shouldEmit = shouldEmit ?? throw new ArgumentNullException(nameof(shouldEmit));
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
                useSynchronizationContext: false);
        }

        /// <inheritdoc/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S4462:Calls to \"async\" methods should not be blocking",
            Justification = "Synchronous read of an already-completed (RanToCompletion) task for an allocation-free fast path; await is invalid in this synchronous factory.")]
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

            if (task.Status == TaskStatus.RanToCompletion)
            {
                var result = task.Result;
                if (_shouldEmit(result))
                {
                    observer.OnNext(result);
                    observer.OnCompleted();
                    return EmptyDisposable.Instance;
                }

                observer.OnError(new OperationCanceledException());
                return EmptyDisposable.Instance;
            }

            if (task.IsCanceled || token.IsCancellationRequested)
            {
                observer.OnError(new OperationCanceledException());
                return EmptyDisposable.Instance;
            }

            if (task.IsFaulted)
            {
                observer.OnError(task.Exception!.InnerException ?? task.Exception);
                return EmptyDisposable.Instance;
            }

            ImmediateTaskSubscription subscription = new(SourceCore);
            _ = ObserveTask(
                task.WhenCancelled(token),
                _shouldEmit,
                observer,
                subscription.MarkCompleted,
                subscription.MarkFaulted,
                token);

            return subscription;
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

        /// <summary>Subscription for pending immediate tasks.</summary>
        private sealed class ImmediateTaskSubscription : IDisposable
        {
            /// <summary>Completed state marker.</summary>
            private const int Completed = 1;

            /// <summary>Faulted state marker.</summary>
            private const int Faulted = 2;

            /// <summary>Cancellation source to cancel while pending.</summary>
            private readonly CancellationTokenSource _source;

            /// <summary>Completion state.</summary>
            private int _state;

            /// <summary>Initializes a new instance of the <see cref="ImmediateTaskSubscription"/> class.</summary>
            /// <param name="source">Cancellation source.</param>
            public ImmediateTaskSubscription(CancellationTokenSource source) => _source = source;

            /// <summary>Marks the task completed.</summary>
            public void MarkCompleted() => Volatile.Write(ref _state, Completed);

            /// <summary>Marks the task faulted.</summary>
            public void MarkFaulted() => Volatile.Write(ref _state, Faulted);

            /// <inheritdoc/>
            public void Dispose()
            {
                if (Volatile.Read(ref _state) == Completed)
                {
                    return;
                }

                Cancel(_source);
            }
        }
    }
}
