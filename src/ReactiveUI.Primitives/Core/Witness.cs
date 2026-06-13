// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Core;

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
    public static IObserver<T> Create<T>(Action<T> onNext) =>
        Create(onNext, Rethrow, Nop);

    /// <summary>Creates a witness from value and error delegates.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onError">Callback invoked for terminal errors.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
    public static IObserver<T> Create<T>(Action<T> onNext, Action<Exception> onError) =>
        Create(onNext, onError, Nop);

    /// <summary>Creates a witness from value and completion delegates.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onCompleted">Callback invoked for completion.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
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

    /// <summary>Observer wrapper that prevents notifications after termination.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    internal sealed class SafeWitness<T> : IObserver<T>
    {
        /// <summary>Wrapped observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Cancellation resource disposed on terminal notifications.</summary>
        private IDisposable? _cancel;

        /// <summary>Non-zero after the observer has stopped.</summary>
        private int _stopped;

        /// <summary>Initializes a new instance of the <see cref="SafeWitness{T}"/> class.</summary>
        /// <param name="observer">Wrapped observer.</param>
        /// <param name="cancel">Cancellation resource disposed on terminal notifications.</param>
        public SafeWitness(IObserver<T> observer, IDisposable cancel)
        {
            _observer = observer;
            _cancel = cancel;
        }

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
                Interlocked.Exchange(ref _stopped, 1);
                DisposeCancel();
                throw;
            }
        }

        /// <summary>Disposes the cancellation resource exactly once.</summary>
        private void DisposeCancel() => Interlocked.Exchange(ref _cancel, null)?.Dispose();
    }

    /// <summary>Delegate-backed observer implementation.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class DelegateWitness<T> : IObserver<T>
    {
        /// <summary>Callback invoked for each value.</summary>
        private readonly Action<T> _onNext;

        /// <summary>Callback invoked for an error.</summary>
        private readonly Action<Exception> _onError;

        /// <summary>Callback invoked for completion.</summary>
        private readonly Action _onCompleted;

        /// <summary>Non-zero when terminal safety is enabled.</summary>
        private int _safe;

        /// <summary>Non-zero after the observer has stopped.</summary>
        private int _stopped;

        /// <summary>Initializes a new instance of the <see cref="DelegateWitness{T}"/> class.</summary>
        /// <param name="onNext">Callback invoked for each value.</param>
        /// <param name="onError">Callback invoked for an error.</param>
        /// <param name="onCompleted">Callback invoked for completion.</param>
        public DelegateWitness(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            _onNext = onNext;
            _onError = onError;
            _onCompleted = onCompleted;
        }

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
                Interlocked.Exchange(ref _stopped, 1);
                throw;
            }
        }

        /// <summary>Enables terminal safety in-place.</summary>
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
                Interlocked.Exchange(ref _stopped, 1);
                throw;
            }
        }
    }
}
