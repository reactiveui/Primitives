// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Core;

/// <summary>
/// Factory methods for allocation-conscious observers in the ReactiveUI.Primitives vocabulary.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public static class Witness
{
    private static readonly Action Nop = static () => { };
    private static readonly Action<Exception> Rethrow = static error => ExceptionDispatchInfo.Capture(error).Throw();

    /// <summary>
    /// Creates a witness from an <paramref name="onNext"/> delegate and default terminal handlers.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
    public static IObserver<T> Create<T>(Action<T> onNext) =>
        Create(onNext, Rethrow, Nop);

    /// <summary>
    /// Creates a witness from value and error delegates.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onError">Callback invoked for terminal errors.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
    public static IObserver<T> Create<T>(Action<T> onNext, Action<Exception> onError) =>
        Create(onNext, onError, Nop);

    /// <summary>
    /// Creates a witness from value and completion delegates.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onCompleted">Callback invoked for completion.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
    public static IObserver<T> Create<T>(Action<T> onNext, Action onCompleted) =>
        Create(onNext, Rethrow, onCompleted);

    /// <summary>
    /// Creates a witness from explicit value, error, and completion callbacks.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onError">Callback invoked for terminal errors.</param>
    /// <param name="onCompleted">Callback invoked for completion.</param>
    /// <returns>An observer backed by the supplied callbacks.</returns>
    /// <exception cref="ArgumentNullException">Any callback is <see langword="null"/>.</exception>
    public static IObserver<T> Create<T>(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        if (onError == null)
        {
            throw new ArgumentNullException(nameof(onError));
        }

        if (onCompleted == null)
        {
            throw new ArgumentNullException(nameof(onCompleted));
        }

        return new DelegateWitness<T>(onNext, onError, onCompleted);
    }

    /// <summary>
    /// Wraps a witness so it receives at most one terminal signal and no values after termination.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="observer">Observer to protect.</param>
    /// <returns>A safe observer wrapper.</returns>
    public static IObserver<T> Safe<T>(IObserver<T> observer) =>
        Safe(observer, Disposable.Empty);

    /// <summary>
    /// Wraps a witness so it receives at most one terminal signal and no values after termination.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="observer">Observer to protect.</param>
    /// <param name="cancel">Cancellation resource disposed on terminal signals or callback exceptions.</param>
    /// <returns>A safe observer wrapper.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="cancel"/> is <see langword="null"/>.</exception>
    public static IObserver<T> Safe<T>(IObserver<T> observer, IDisposable cancel)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        if (cancel == null)
        {
            throw new ArgumentNullException(nameof(cancel));
        }

        return new SafeWitness<T>(observer, cancel);
    }

    private sealed class DelegateWitness<T> : IObserver<T>
    {
        private readonly Action<T> _onNext;
        private readonly Action<Exception> _onError;
        private readonly Action _onCompleted;

        public DelegateWitness(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            _onNext = onNext;
            _onError = onError;
            _onCompleted = onCompleted;
        }

        public void OnCompleted() => _onCompleted();

        public void OnError(Exception error) => _onError(error ?? throw new ArgumentNullException(nameof(error)));

        public void OnNext(T value) => _onNext(value);
    }

    private sealed class SafeWitness<T> : IObserver<T>
    {
        private readonly IObserver<T> _observer;
        private IDisposable? _cancel;
        private int _stopped;

        public SafeWitness(IObserver<T> observer, IDisposable cancel)
        {
            _observer = observer;
            _cancel = cancel;
        }

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

        public void OnError(Exception error)
        {
            if (error == null)
            {
                throw new ArgumentNullException(nameof(error));
            }

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

        private void DisposeCancel() => Interlocked.Exchange(ref _cancel, null)?.Dispose();
    }
}
