// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the EmptySignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class EmptySignal<T> : SignalsBase<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly ISequencer _scheduler;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmptySignal{T}"/> class.
    /// </summary>
    /// <param name="scheduler">The scheduler value.</param>
    public EmptySignal(ISequencer scheduler)
        : base(false) => _scheduler = scheduler;

    /// <summary>
    /// Executes the SubscribeCore operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Empty(observer, cancel);

        if (_scheduler == Sequencer.Immediate)
        {
            observer.OnCompleted();
            return Disposable.Empty;
        }

        return _scheduler.Schedule(observer.OnCompleted);
    }

    /// <summary>
    /// Represents the Empty class.
    /// </summary>
    private sealed class Empty : WitnessBase<T, T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Empty"/> class.
        /// </summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Empty(IObserver<T> observer, IDisposable cancel)
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
