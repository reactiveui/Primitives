// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the ThrowSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class ThrowSignal<T> : SignalsBase<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly Exception _error;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly ISequencer _scheduler;

    /// <summary>
    /// Initializes a new instance of the <see cref="ThrowSignal{T}"/> class.
    /// </summary>
    /// <param name="error">The error value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    public ThrowSignal(Exception error, ISequencer scheduler)
        : base(scheduler == Sequencer.CurrentThread)
    {
        _error = error;
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
        observer = new Throw(observer, cancel);

        if (_scheduler == Sequencer.Immediate)
        {
            observer.OnError(_error);
            return Disposable.Empty;
        }

        return _scheduler.Schedule((observer, _error), static (_, state) => SignalError(state));
    }

    /// <summary>
    /// Emits the scheduled error notification.
    /// </summary>
    /// <param name="state">The observer and error state.</param>
    /// <returns>An empty disposable.</returns>
    private static IDisposable SignalError((IObserver<T> Observer, Exception Error) state)
    {
        state.Observer.OnError(state.Error);
        state.Observer.OnCompleted();
        return Disposable.Empty;
    }

    /// <summary>
    /// Represents the Throw class.
    /// </summary>
    private sealed class Throw : WitnessBase<T, T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Throw"/> class.
        /// </summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Throw(IObserver<T> observer, IDisposable cancel)
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
