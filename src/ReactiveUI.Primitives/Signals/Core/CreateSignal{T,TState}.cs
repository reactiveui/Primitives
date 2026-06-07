// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the CreateSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
/// <typeparam name="TState">The TState type.</typeparam>
internal sealed class CreateSignal<T, TState> : SignalsBase<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly TState _state;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly Func<TState, IObserver<T>, IDisposable> _subscribe;

    /// <summary>
    /// Initializes a new instance of the <see cref="CreateSignal{T,TState}"/> class.
    /// </summary>
    /// <param name="state">The state value.</param>
    /// <param name="subscribe">The subscribe value.</param>
    public CreateSignal(TState state, Func<TState, IObserver<T>, IDisposable> subscribe)
        : base(false)
    {
        _state = state;
        _subscribe = subscribe;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CreateSignal{T,TState}"/> class.
    /// </summary>
    /// <param name="state">The state value.</param>
    /// <param name="subscribe">The subscribe value.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">The isRequiredSubscribeOnCurrentThread value.</param>
    public CreateSignal(TState state, Func<TState, IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
        : base(isRequiredSubscribeOnCurrentThread)
    {
        _state = state;
        _subscribe = subscribe;
    }

    /// <summary>
    /// Executes the SubscribeCore operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Create(observer, cancel);
        return _subscribe(_state, observer) ?? EmptyDisposable.Instance;
    }

    /// <summary>
    /// Represents the Create class.
    /// </summary>
    private sealed class Create : WitnessBase<T, T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Create"/> class.
        /// </summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Create(IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel)
        {
        }

        /// <summary>
        /// Executes the OnNext operation.
        /// </summary>
        /// <param name="value">The value.</param>
        public override void OnNext(T value) => Observer.OnNext(value);

        /// <summary>
        /// Executes the OnError operation.
        /// </summary>
        /// <param name="error">The error value.</param>
        public override void OnError(Exception error)
        {
            try
            {
                Observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <summary>
        /// Executes the OnCompleted operation.
        /// </summary>
        public override void OnCompleted()
        {
            try
            {
                Observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }
}
