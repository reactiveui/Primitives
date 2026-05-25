// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class EmptyWitness<T> : IObserver<T>
{
    public static readonly EmptyWitness<T> Instance = new(_ => { });
    private static readonly Action<Exception> rethrow = e => ExceptionDispatchInfo.Capture(e).Throw();
    private static readonly Action nop = () => { };
    private static readonly Action<Exception> nope = _ => { };

    private readonly Action<T> _onNext;
    private readonly Action<Exception> _onError;
    private readonly Action _onCompleted;

    public EmptyWitness(Action<T> onNext)
        : this(onNext, rethrow, nop)
    {
    }

    public EmptyWitness(Action<T> onNext, Action<Exception> onError)
        : this(onNext, onError, nop)
    {
    }

    public EmptyWitness(Action<T> onNext, Action onCompleted)
        : this(onNext, rethrow, onCompleted)
    {
    }

    public EmptyWitness(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        _onNext = onNext;
        _onError = onError;
        _onCompleted = onCompleted;
    }

    /// <summary>
    /// Calls the action implementing <see cref="IObserver{T}.OnCompleted()"/>.
    /// </summary>
    public void OnCompleted() => (_onCompleted ?? nop)();

    /// <summary>
    /// Calls the action implementing <see cref="IObserver{T}.OnError(Exception)"/>.
    /// </summary>
    public void OnError(Exception error) => (_onError ?? nope)(error);

    /// <summary>
    /// Calls the action implementing <see cref="IObserver{T}.OnNext(T)"/>.
    /// </summary>
    public void OnNext(T value) => _onNext(value);
}
