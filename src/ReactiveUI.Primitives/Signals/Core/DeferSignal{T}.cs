// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the DeferSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class DeferSignal<T> : SignalsBase<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly Func<IObservable<T>> _observableFactory;

    /// <summary>
    /// Initializes a new instance of the <see cref="DeferSignal{T}"/> class.
    /// </summary>
    /// <param name="observableFactory">The observableFactory value.</param>
    public DeferSignal(Func<IObservable<T>> observableFactory)
        : base(false) => _observableFactory = observableFactory;

    /// <summary>
    /// Executes the SubscribeCore operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Defer(observer, cancel);

        IObservable<T> source;
        try
        {
            source = _observableFactory();
        }
        catch (Exception ex)
        {
            source = Signal.Fail<T>(ex);
        }

        return source.Subscribe(observer);
    }

    /// <summary>
    /// Represents the Defer class.
    /// </summary>
    private sealed class Defer : WitnessBase<T, T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Defer"/> class.
        /// </summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Defer(IObserver<T> observer, IDisposable cancel)
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
