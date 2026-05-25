// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the ReturnSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class ReturnSignal<T> : SignalsBase<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly T _value;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly ISequencer _scheduler;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReturnSignal{T}"/> class.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    public ReturnSignal(T value, ISequencer scheduler)
        : base(scheduler == Sequencer.CurrentThread)
    {
        _value = value;
        _scheduler = scheduler;
    }

    /// <summary>
    /// Executes the SubscribeCore operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Return(observer, cancel);

        if (_scheduler == Sequencer.Immediate)
        {
            observer.OnNext(_value);
            observer.OnCompleted();
            return Disposable.Empty;
        }

        return _scheduler.Schedule(() =>
        {
            observer.OnNext(_value);
            observer.OnCompleted();
        });
    }

    /// <summary>
    /// Represents the Return class.
    /// </summary>
    private sealed class Return : WitnessBase<T, T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Return"/> class.
        /// </summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Return(IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel)
        {
        }

        /// <summary>
        /// Executes the OnNext operation.
        /// </summary>
        /// <param name="value">The value.</param>
        public override void OnNext(T value)
        {
            try
            {
                Observer.OnNext(value);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

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
