// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Core.Internal;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Factory methods for allocation-conscious observers in the ReactiveUI.Primitives vocabulary.</summary>
public static class Witness
{
    /// <summary>Completion callback that does nothing.</summary>
    private static readonly Action Nop = static () => { };

    /// <summary>Error callback that rethrows with preserved exception details.</summary>
    private static readonly Action<Exception> Rethrow = static error => ExceptionDispatchInfo.Capture(error).Throw();

    /// <summary>Creates a witness from an <paramref name="onNext"/> delegate and default terminal handlers.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObserver<T> Create<T>(Action<T> onNext) =>
        Create(onNext, Rethrow, Nop);

    /// <summary>Creates a witness from value and error delegates.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onError">Callback invoked for terminal errors.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObserver<T> Create<T>(Action<T> onNext, Action<Exception> onError) =>
        Create(onNext, onError, Nop);

    /// <summary>Creates a witness from value and completion delegates.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onCompleted">Callback invoked for completion.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObserver<T> Create<T>(Action<T> onNext, Action onCompleted) =>
        Create(onNext, Rethrow, onCompleted);

    /// <summary>Creates a witness from explicit value, error, and completion callbacks.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onError">Callback invoked for terminal errors.</param>
    /// <param name="onCompleted">Callback invoked for completion.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
    /// <exception cref="ArgumentNullException">Any callback is <see langword="null"/>.</exception>
    public static IObserver<T> Create<T>(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        ArgumentExceptionHelper.ThrowIfNull(onError);

        ArgumentExceptionHelper.ThrowIfNull(onCompleted);

        return new DelegateWitness<T>(onNext, onError, onCompleted);
    }

    /// <summary>Wraps a witness so it receives at most one terminal signal and no values after termination.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="observer">Observer to protect.</param>
    /// <returns>A safe observer wrapper.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObserver<T> Safe<T>(IObserver<T> observer) =>
        Safe(observer, EmptyDisposable.Instance);

    /// <summary>Wraps a witness so it receives at most one terminal signal and no values after termination.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="observer">Observer to protect.</param>
    /// <param name="cancel">Cancellation resource disposed on terminal signals or callback exceptions.</param>
    /// <returns>A safe observer wrapper.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="cancel"/> is <see langword="null"/>.</exception>
    public static IObserver<T> Safe<T>(IObserver<T> observer, IDisposable cancel)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ArgumentExceptionHelper.ThrowIfNull(cancel);

        if (ReferenceEquals(cancel, EmptyDisposable.Instance))
        {
            if (observer is DelegateWitness<T> delegateWitness)
            {
                delegateWitness.MakeSafe();
                return delegateWitness;
            }

            return new SafeNoCancelWitness<T>(observer);
        }

        return new SafeWitness<T>(observer, cancel);
    }

    /// <summary>Rethrows <paramref name="exception"/> with its original stack trace, doing nothing when <see langword="null"/>.</summary>
    /// <param name="exception">The exception to rethrow, or <see langword="null"/>.</param>
    public static void RethrowIfNotNull(Exception? exception)
    {
        if (exception is not null)
        {
            ExceptionDispatchInfo.Capture(exception).Throw();
        }
    }

    /// <summary>Subscribes to <paramref name="source"/> without handlers so it runs for side effects alone.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <returns>A handle that unsubscribes from <paramref name="source"/> when disposed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IDisposable Subscribe<T>(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return Subscribe(source, OnNextNoOpCache<T>.Instance, Rethrow, Nop);
    }

    /// <summary>Subscribes a value callback to <paramref name="source"/>; terminal errors rethrow and completion is ignored.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The callback invoked for each value.</param>
    /// <returns>A handle that unsubscribes from <paramref name="source"/> when disposed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    public static IDisposable Subscribe<T>(IObservable<T> source, Action<T> onNext)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(onNext);

        return source switch
        {
            Signal<T> signal => signal.SubscribeAction(onNext),
            IInlineSignal<T> inline => inline.Subscribe(onNext, Rethrow, Nop),
            _ => Subscribe(source, onNext, Rethrow, Nop),
        };
    }

    /// <summary>Subscribes value and error callbacks to <paramref name="source"/>; completion is ignored.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The callback invoked for each value.</param>
    /// <param name="onError">The callback invoked with the terminal error.</param>
    /// <returns>A handle that unsubscribes from <paramref name="source"/> when disposed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="onNext"/>, or <paramref name="onError"/> is <see langword="null"/>.</exception>
    public static IDisposable Subscribe<T>(IObservable<T> source, Action<T> onNext, Action<Exception> onError)
    {
        ArgumentExceptionHelper.ThrowIfNull(onError);

        return Subscribe(source, onNext, onError, Nop);
    }

    /// <summary>Subscribes value and completion callbacks to <paramref name="source"/>; terminal errors rethrow.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The callback invoked for each value.</param>
    /// <param name="onCompleted">The callback invoked on completion.</param>
    /// <returns>A handle that unsubscribes from <paramref name="source"/> when disposed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="onNext"/>, or <paramref name="onCompleted"/> is <see langword="null"/>.</exception>
    public static IDisposable Subscribe<T>(IObservable<T> source, Action<T> onNext, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onCompleted);

        return Subscribe(source, onNext, Rethrow, onCompleted);
    }

    /// <summary>Subscribes value, error, and completion callbacks to <paramref name="source"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The callback invoked for each value.</param>
    /// <param name="onError">The callback invoked with the terminal error.</param>
    /// <param name="onCompleted">The callback invoked on completion.</param>
    /// <returns>A handle that unsubscribes from <paramref name="source"/> when disposed.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public static IDisposable Subscribe<T>(IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(onNext);

        ArgumentExceptionHelper.ThrowIfNull(onError);

        ArgumentExceptionHelper.ThrowIfNull(onCompleted);

        return source is IInlineSignal<T> inline
            ? inline.Subscribe(onNext, onError, onCompleted)
            : SubscribeCallbacks(source, onNext, onError, onCompleted);
    }

    /// <summary>Subscribes callbacks through a disposable witness that halts delivery after termination or disposal.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The callback invoked for each value.</param>
    /// <param name="onError">The callback invoked with the terminal error.</param>
    /// <param name="onCompleted">The callback invoked on completion.</param>
    /// <returns>A handle that unsubscribes from <paramref name="source"/> when disposed.</returns>
    private static IDisposable SubscribeCallbacks<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted)
    {
        SubscriptionWitness<T> witness = new(onNext, onError, onCompleted);
        var subscription = source.Subscribe(witness);
        if (witness.IsStopped)
        {
            subscription.Dispose();
            return EmptyDisposable.Instance;
        }

        witness.Attach(subscription);
        return witness;
    }

    /// <summary>Observer wrapper that prevents notifications after termination.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="observer">Wrapped observer.</param>
    /// <param name="cancel">Cancellation resource disposed on terminal notifications.</param>
    internal sealed class SafeWitness<T>(IObserver<T> observer, IDisposable cancel) : IObserver<T>
    {
        /// <summary>Wrapped observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>Cancellation resource disposed on terminal notifications.</summary>
        private IDisposable? _cancel = cancel;

        /// <summary>Non-zero after the observer has stopped.</summary>
        private int _stopped;

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                DisposeCancel();
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            ArgumentExceptionHelper.ThrowIfNull(error);

            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            try
            {
                _observer.OnError(error);
            }
            finally
            {
                DisposeCancel();
            }
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _stopped) != 0)
            {
                return;
            }

            try
            {
                _observer.OnNext(value);
            }
            catch
            {
                _ = Interlocked.Exchange(ref _stopped, 1);
                DisposeCancel();
                throw;
            }
        }

        /// <summary>Disposes the cancellation resource exactly once.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DisposeCancel() => Interlocked.Exchange(ref _cancel, null)?.Dispose();
    }

    /// <summary>Delegate-backed observer implementation.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onError">Callback invoked for an error.</param>
    /// <param name="onCompleted">Callback invoked for completion.</param>
    private sealed class DelegateWitness<T>(Action<T> onNext, Action<Exception> onError, Action onCompleted) : IObserver<T>
    {
        /// <summary>Callback invoked for each value.</summary>
        private readonly Action<T> _onNext = onNext;

        /// <summary>Callback invoked for an error.</summary>
        private readonly Action<Exception> _onError = onError;

        /// <summary>Callback invoked for completion.</summary>
        private readonly Action _onCompleted = onCompleted;

        /// <summary>Non-zero when terminal safety is enabled.</summary>
        private int _safe;

        /// <summary>Non-zero after the observer has stopped.</summary>
        private int _stopped;

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Volatile.Read(ref _safe) == 0)
            {
                _onCompleted();
                return;
            }

            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _onCompleted();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            ArgumentExceptionHelper.ThrowIfNull(error);

            if (Volatile.Read(ref _safe) == 0)
            {
                _onError(error);
                return;
            }

            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _onError(error);
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _safe) == 0)
            {
                _onNext(value);
                return;
            }

            if (Volatile.Read(ref _stopped) != 0)
            {
                return;
            }

            try
            {
                _onNext(value);
            }
            catch
            {
                _ = Interlocked.Exchange(ref _stopped, 1);
                throw;
            }
        }

        /// <summary>Enables terminal safety in-place.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MakeSafe() => Volatile.Write(ref _safe, 1);
    }

    /// <summary>Observer wrapper that prevents notifications after termination without owning a cancellation resource.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class SafeNoCancelWitness<T> : IObserver<T>
    {
        /// <summary>Wrapped observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Non-zero after the observer has stopped.</summary>
        private int _stopped;

        /// <summary>Initializes a new instance of the <see cref="SafeNoCancelWitness{T}"/> class.</summary>
        /// <param name="observer">Wrapped observer.</param>
        public SafeNoCancelWitness(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _observer.OnCompleted();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            ArgumentExceptionHelper.ThrowIfNull(error);

            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _observer.OnError(error);
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _stopped) != 0)
            {
                return;
            }

            try
            {
                _observer.OnNext(value);
            }
            catch
            {
                _ = Interlocked.Exchange(ref _stopped, 1);
                throw;
            }
        }
    }

    /// <summary>Disposable delegate-backed observer used by <c>Subscribe</c> extensions.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onError">Callback invoked for an error.</param>
    /// <param name="onCompleted">Callback invoked for completion.</param>
    private sealed class SubscriptionWitness<T>(
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted) : IObserver<T>, IDisposable
    {
        /// <summary>Upstream subscription and stop-state slot.</summary>
        private UnaryWitnessSlot _slot;

        /// <summary>Gets a value indicating whether this witness has terminated or been disposed.</summary>
        public bool IsStopped => _slot.IsStopped;

        /// <summary>Assigns the upstream subscription.</summary>
        /// <param name="subscription">The upstream subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Attach(IDisposable subscription) => _slot.Attach(subscription);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _slot.NextCallback(value, onNext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _slot.FaultCallback(error, onError);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _slot.CompleteCallback(onCompleted);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _slot.Dispose();
    }
}
