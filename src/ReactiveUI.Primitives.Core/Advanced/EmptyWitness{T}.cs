// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Delegate-backed observer that defaults missing handlers to no-op behavior.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
public sealed class EmptyWitness<T> : IObserver<T>
{
    /// <summary>Gets the shared no-op witness instance.</summary>
    public static readonly EmptyWitness<T> Instance = new(_ => { });

    /// <summary>Rethrows observer errors with their original stack information.</summary>
    private static readonly Action<Exception> rethrow = e => ExceptionDispatchInfo.Capture(e).Throw();

    /// <summary>Completion callback that does nothing.</summary>
    private static readonly Action nop = () => { };

    /// <summary>Error callback that does nothing.</summary>
    private static readonly Action<Exception> nope = _ => { };

    /// <summary>Callback invoked for each value.</summary>
    private readonly Action<T> _onNext;

    /// <summary>Callback invoked for an error.</summary>
    private readonly Action<Exception> _onError;

    /// <summary>Callback invoked for completion.</summary>
    private readonly Action _onCompleted;

    /// <summary>Initializes a new instance of the <see cref="EmptyWitness{T}"/> class.</summary>
    /// <param name="onNext">Callback invoked for each value.</param>
    public EmptyWitness(Action<T> onNext)
        : this(onNext, rethrow, nop)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="EmptyWitness{T}"/> class.</summary>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onError">Callback invoked for an error.</param>
    public EmptyWitness(Action<T> onNext, Action<Exception> onError)
        : this(onNext, onError, nop)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="EmptyWitness{T}"/> class.</summary>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onCompleted">Callback invoked for completion.</param>
    public EmptyWitness(Action<T> onNext, Action onCompleted)
        : this(onNext, rethrow, onCompleted)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="EmptyWitness{T}"/> class.</summary>
    /// <param name="onNext">Callback invoked for each value.</param>
    /// <param name="onError">Callback invoked for an error.</param>
    /// <param name="onCompleted">Callback invoked for completion.</param>
    public EmptyWitness(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        _onNext = onNext;
        _onError = onError;
        _onCompleted = onCompleted;
    }

    /// <summary>Calls the action implementing <see cref="IObserver{T}.OnCompleted()"/>.</summary>
    public void OnCompleted() => (_onCompleted ?? nop)();

    /// <summary>Calls the action implementing <see cref="IObserver{T}.OnError(Exception)"/>.</summary>
    /// <param name="error">Error notification.</param>
    public void OnError(Exception error) => (_onError ?? nope)(error);

    /// <summary>Calls the action implementing <see cref="IObserver{T}.OnNext(T)"/>.</summary>
    /// <param name="value">Value notification.</param>
    public void OnNext(T value) => _onNext(value);
}
