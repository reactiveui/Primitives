// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Represents the DeferSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class DeferSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Func<IObservable<T>> _observableFactory;

    /// <summary>Initializes a new instance of the <see cref="DeferSignal{T}"/> class.</summary>
    /// <param name="observableFactory">The observableFactory value.</param>
    public DeferSignal(Func<IObservable<T>> observableFactory) => _observableFactory = observableFactory;

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, false, SubscribeCore);

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
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

    /// <summary>Represents the Defer class.</summary>
    private sealed class Defer : IObserver<T>, IDisposable
    {
        /// <summary>Stores the downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Stores the upstream subscription.</summary>
        private IDisposable? _cancel;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="Defer"/> class.</summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Defer(IObserver<T> observer, IDisposable cancel)
        {
            _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
            _observer = observer;
        }

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
        public void OnNext(T value)
        {
            try
            {
                _observer.OnNext(value);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <summary>Executes the OnError operation.</summary>
        /// <param name="error">The error value.</param>
        public void OnError(Exception error)
        {
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <summary>Executes the OnCompleted operation.</summary>
        public void OnCompleted()
        {
            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <summary>Executes the Dispose operation.</summary>
        public void Dispose() => WitnessTeardown.Dispose(ref _disposed, ref _cancel);
    }
}
