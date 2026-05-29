// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using System.Threading;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Subject.
/// </summary>
/// <typeparam name="T">The Type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public partial class Signal<T> : ISignal<T>
{
    /// <summary>
    /// Holds the current observer set as a lock-free copy-on-write reference. Transitions are
    /// published with <c>Interlocked.CompareExchange</c>; observers read a stable snapshot with
    /// <c>Volatile.Read</c>.
    /// </summary>
    private SubjectState _state = SubjectState.Empty;

    /// <summary>
    /// Identifies the lifecycle phase of a <see cref="SubjectState"/>.
    /// </summary>
    private enum SubjectStatus
    {
        /// <summary>The subject is accepting values and subscriptions.</summary>
        Live,

        /// <summary>The subject has completed.</summary>
        Completed,

        /// <summary>The subject has terminated with an error.</summary>
        Errored,

        /// <summary>The subject has been disposed.</summary>
        Disposed,
    }

    /// <summary>
    /// Gets a value indicating whether indicates whether the subject has observers subscribed to it.
    /// </summary>
    public virtual bool HasObservers => Volatile.Read(ref _state).HasObservers;

    /// <summary>
    /// Gets a value indicating whether indicates whether the subject has been disposed.
    /// </summary>
    public virtual bool IsDisposed => Volatile.Read(ref _state).Status == SubjectStatus.Disposed;

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Called when [completed].
    /// </summary>
    public void OnCompleted()
    {
        SubjectState state;
        do
        {
            state = Volatile.Read(ref _state);
            if (state.Status == SubjectStatus.Disposed)
            {
                ThrowDisposed();
            }

            if (state.IsTerminal)
            {
                return;
            }
        }
        while (!ReferenceEquals(Interlocked.CompareExchange(ref _state, SubjectState.Completed, state), state));

        SubjectState.Complete(state);
    }

    /// <summary>
    /// Called when [error].
    /// </summary>
    /// <param name="error">The error.</param>
    public void OnError(Exception error)
    {
        if (error == null)
        {
            throw new ArgumentNullException(nameof(error));
        }

        var errored = SubjectState.Errored(error);
        SubjectState state;
        do
        {
            state = Volatile.Read(ref _state);
            if (state.Status == SubjectStatus.Disposed)
            {
                ThrowDisposed();
            }

            if (state.IsTerminal)
            {
                return;
            }
        }
        while (!ReferenceEquals(Interlocked.CompareExchange(ref _state, errored, state), state));

        if (!SubjectState.DispatchError(state, error))
        {
            return;
        }

        ExceptionDispatchInfo.Capture(error).Throw();
    }

    /// <summary>
    /// Called when [next].
    /// </summary>
    /// <param name="value">The value.</param>
    public void OnNext(T value)
    {
        var state = Volatile.Read(ref _state);
        var single = state.Single;
        if (single != null)
        {
            single.OnNext(value);
            return;
        }

        var many = state.Many;
        if (many != null)
        {
            for (var i = 0; i < many.Length; i++)
            {
                many[i].OnNext(value);
            }

            return;
        }

        if (state.Status != SubjectStatus.Disposed)
        {
            return;
        }

        ThrowDisposed();
    }

    /// <summary>
    /// Subscribes the specified observer.
    /// </summary>
    /// <param name="observer">The observer.</param>
    /// <returns>
    /// A IDisposable.
    /// </returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        var subscription = new SignalSubscription(this, observer);
        var blocked = TryAdd(subscription);
        if (blocked == null)
        {
            return subscription;
        }

        if (blocked.Status == SubjectStatus.Disposed)
        {
            ThrowDisposed();
        }

        if (blocked.Error != null)
        {
            observer.OnError(blocked.Error);
        }
        else
        {
            observer.OnCompleted();
        }

        return Disposable.Empty;
    }

    /// <summary>
    /// Executes the SubscribeAction operation.
    /// </summary>
    /// <param name="onNext">The onNext value.</param>
    /// <returns>The result.</returns>
    internal IDisposable SubscribeAction(Action<T> onNext)
    {
        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        var subscription = new SignalSubscription(this, onNext);
        var blocked = TryAdd(subscription);
        if (blocked == null)
        {
            return subscription;
        }

        if (blocked.Status == SubjectStatus.Disposed)
        {
            ThrowDisposed();
        }

        if (blocked.Error != null)
        {
            ExceptionDispatchInfo.Capture(blocked.Error).Throw();
        }

        return Disposable.Empty;
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        var state = Interlocked.Exchange(ref _state, SubjectState.Disposed);
        SubjectState.DisposeAll(state);
    }

    /// <summary>
    /// Executes the ThrowDisposed operation.
    /// </summary>
    private static void ThrowDisposed() => throw new ObjectDisposedException(string.Empty);

    /// <summary>
    /// Adds a subscription using a lock-free copy-on-write update.
    /// </summary>
    /// <param name="subscription">The subscription to add.</param>
    /// <returns><see langword="null"/> when added; otherwise the terminal or disposed state that blocked the add.</returns>
    private SubjectState? TryAdd(SignalSubscription subscription)
    {
        while (true)
        {
            var current = Volatile.Read(ref _state);
            if (current.Status != SubjectStatus.Live)
            {
                return current;
            }

            var next = SubjectState.Add(current, subscription);
            if (ReferenceEquals(Interlocked.CompareExchange(ref _state, next, current), current))
            {
                return null;
            }
        }
    }

    /// <summary>
    /// Removes a subscription using a lock-free copy-on-write update.
    /// </summary>
    /// <param name="subscription">The subscription value.</param>
    private void Remove(SignalSubscription subscription)
    {
        while (true)
        {
            var current = Volatile.Read(ref _state);
            var next = SubjectState.Remove(current, subscription);
            if (next == null)
            {
                return;
            }

            if (ReferenceEquals(Interlocked.CompareExchange(ref _state, next, current), current))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Immutable, lock-free observer set. A live state holds either a single subscription (the
    /// common case, allocation-free of any array) or an array; terminal and disposed states are
    /// shared singletons, except <see cref="Errored"/> which carries the terminating exception.
    /// </summary>
    private sealed class SubjectState
    {
        /// <summary>The shared live state with no observers.</summary>
        public static readonly SubjectState Empty = new(null, null, SubjectStatus.Live, null);

        /// <summary>The shared completed state.</summary>
        public static readonly SubjectState Completed = new(null, null, SubjectStatus.Completed, null);

        /// <summary>The shared disposed state.</summary>
        public static readonly SubjectState Disposed = new(null, null, SubjectStatus.Disposed, null);

        /// <summary>
        /// Initializes a new instance of the <see cref="SubjectState"/> class.
        /// </summary>
        /// <param name="single">The lone subscription, when exactly one is present.</param>
        /// <param name="many">The subscription array, when more than one is present.</param>
        /// <param name="status">The lifecycle phase.</param>
        /// <param name="error">The terminating exception, when errored.</param>
        private SubjectState(SignalSubscription? single, SignalSubscription[]? many, SubjectStatus status, Exception? error)
        {
            Single = single;
            Many = many;
            Status = status;
            Error = error;
        }

        /// <summary>
        /// Gets the lone subscription, or <see langword="null"/> when zero or more than one are present.
        /// </summary>
        public SignalSubscription? Single { get; }

        /// <summary>
        /// Gets the subscription array, or <see langword="null"/> when fewer than two are present.
        /// </summary>
        public SignalSubscription[]? Many { get; }

        /// <summary>
        /// Gets the lifecycle phase.
        /// </summary>
        public SubjectStatus Status { get; }

        /// <summary>
        /// Gets the terminating exception, or <see langword="null"/> unless the state is errored.
        /// </summary>
        public Exception? Error { get; }

        /// <summary>
        /// Gets a value indicating whether the state has at least one observer.
        /// </summary>
        public bool HasObservers => Single != null || (Many != null && Many.Length != 0);

        /// <summary>
        /// Gets a value indicating whether the state is terminal (completed or errored).
        /// </summary>
        public bool IsTerminal => Status is SubjectStatus.Completed or SubjectStatus.Errored;

        /// <summary>
        /// Creates an errored state carrying the terminating exception.
        /// </summary>
        /// <param name="error">The terminating exception.</param>
        /// <returns>The errored state.</returns>
        public static SubjectState Errored(Exception error) => new(null, null, SubjectStatus.Errored, error);

        /// <summary>
        /// Produces the live state that results from adding a subscription.
        /// </summary>
        /// <param name="state">The current live state.</param>
        /// <param name="subscription">The subscription to add.</param>
        /// <returns>The new live state.</returns>
        public static SubjectState Add(SubjectState state, SignalSubscription subscription)
        {
            var single = state.Single;
            if (single != null)
            {
                var pair = new SignalSubscription[2];
                pair[0] = single;
                pair[1] = subscription;
                return new SubjectState(null, pair, SubjectStatus.Live, null);
            }

            var many = state.Many;
            if (many == null)
            {
                return new SubjectState(subscription, null, SubjectStatus.Live, null);
            }

            var copy = new SignalSubscription[many.Length + 1];
            Array.Copy(many, copy, many.Length);
            copy[many.Length] = subscription;
            return new SubjectState(null, copy, SubjectStatus.Live, null);
        }

        /// <summary>
        /// Produces the live state that results from removing a subscription.
        /// </summary>
        /// <param name="state">The current state.</param>
        /// <param name="subscription">The subscription to remove.</param>
        /// <returns>The new state, or <see langword="null"/> when the subscription is absent.</returns>
        public static SubjectState? Remove(SubjectState state, SignalSubscription subscription)
        {
            if (state.Single != null)
            {
                return ReferenceEquals(state.Single, subscription) ? Empty : null;
            }

            var many = state.Many;
            if (many == null)
            {
                return null;
            }

            var index = Array.IndexOf(many, subscription);
            return index < 0 ? null : RemoveAt(many, index);
        }

        /// <summary>
        /// Dispatches completion to every observer in the state.
        /// </summary>
        /// <param name="state">The captured state.</param>
        public static void Complete(SubjectState state)
        {
            var single = state.Single;
            if (single != null)
            {
                single.OnCompleted();
                return;
            }

            var many = state.Many;
            if (many == null)
            {
                return;
            }

            for (var i = 0; i < many.Length; i++)
            {
                many[i].OnCompleted();
            }
        }

        /// <summary>
        /// Dispatches an error to every observer in the state.
        /// </summary>
        /// <param name="state">The captured state.</param>
        /// <param name="error">The terminating exception.</param>
        /// <returns><c>true</c> when at least one action subscriber was present; otherwise, <c>false</c>.</returns>
        public static bool DispatchError(SubjectState state, Exception error)
        {
            var single = state.Single;
            if (single != null)
            {
                single.OnError(error);
                return single.IsAction;
            }

            var many = state.Many;
            if (many == null)
            {
                return false;
            }

            var hasActionSubscribers = false;
            for (var i = 0; i < many.Length; i++)
            {
                hasActionSubscribers |= many[i].IsAction;
                many[i].OnError(error);
            }

            return hasActionSubscribers;
        }

        /// <summary>
        /// Disposes every subscription in the state.
        /// </summary>
        /// <param name="state">The captured state.</param>
        public static void DisposeAll(SubjectState state)
        {
            var single = state.Single;
            if (single != null)
            {
                single.Dispose();
                return;
            }

            var many = state.Many;
            if (many == null)
            {
                return;
            }

            for (var i = 0; i < many.Length; i++)
            {
                many[i].Dispose();
            }
        }

        /// <summary>
        /// Produces the live state with the entry at the supplied index removed, collapsing a
        /// two-element array to a single subscription.
        /// </summary>
        /// <param name="many">The source observer array.</param>
        /// <param name="index">The index to remove.</param>
        /// <returns>The new live state.</returns>
        private static SubjectState RemoveAt(SignalSubscription[] many, int index)
        {
            if (many.Length == 2)
            {
                return new SubjectState(many[index == 0 ? 1 : 0], null, SubjectStatus.Live, null);
            }

            var copy = new SignalSubscription[many.Length - 1];
            Array.Copy(many, 0, copy, 0, index);
            Array.Copy(many, index + 1, copy, index, many.Length - index - 1);
            return new SubjectState(null, copy, SubjectStatus.Live, null);
        }
    }

    /// <summary>
    /// Represents the SignalSubscription class.
    /// </summary>
    private sealed class SignalSubscription : IDisposable
    {
        /// <summary>
        /// Stores the observer or action target.
        /// </summary>
        private readonly object _target;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private Signal<T>? _subject;

        /// <summary>
        /// Initializes a new instance of the <see cref="SignalSubscription"/> class.
        /// </summary>
        /// <param name="subject">The subject value.</param>
        /// <param name="observer">The observer value.</param>
        public SignalSubscription(Signal<T> subject, IObserver<T> observer)
        {
            _subject = subject;
            _target = observer;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SignalSubscription"/> class.
        /// </summary>
        /// <param name="subject">The subject value.</param>
        /// <param name="onNext">The onNext value.</param>
        public SignalSubscription(Signal<T> subject, Action<T> onNext)
        {
            _subject = subject;
            _target = onNext;
        }

        /// <summary>
        /// Gets a value indicating whether this subscription stores an action callback.
        /// </summary>
        public bool IsAction => _target is Action<T>;

        /// <summary>
        /// Gets the observer target.
        /// </summary>
        public IObserver<T> Observer => (IObserver<T>)_target;

        /// <summary>
        /// Gets the action target.
        /// </summary>
        public Action<T> Action => (Action<T>)_target;

        /// <summary>
        /// Sends a value to the subscription target.
        /// </summary>
        /// <param name="value">The value.</param>
        public void OnNext(T value)
        {
            if (IsAction)
            {
                Action(value);
                return;
            }

            Observer.OnNext(value);
        }

        /// <summary>
        /// Sends an error to observer subscriptions.
        /// </summary>
        /// <param name="exception">The exception.</param>
        public void OnError(Exception exception)
        {
            if (IsAction)
            {
                return;
            }

            Observer.OnError(exception);
        }

        /// <summary>
        /// Sends completion to observer subscriptions.
        /// </summary>
        public void OnCompleted()
        {
            if (IsAction)
            {
                return;
            }

            Observer.OnCompleted();
        }

        /// <summary>
        /// Executes the Dispose operation.
        /// </summary>
        public void Dispose()
        {
            var subject = Interlocked.Exchange(ref _subject, null);
            subject?.Remove(this);
        }
    }
}
