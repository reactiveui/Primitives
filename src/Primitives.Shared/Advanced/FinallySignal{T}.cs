// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Represents the FinallySignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
/// <param name="source">The source value.</param>
/// <param name="finallyAction">The finallyAction value.</param>
internal sealed class FinallySignal<T>(IObservable<T> source, Action finallyAction) : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Action _finallyAction = finallyAction;

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, true, SubscribeCore);

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new Finally(this, observer, cancel).Run();

    /// <summary>Represents the Finally class.</summary>
    private sealed class Finally : IObserver<T>, IDisposable
    {
        /// <summary>Stores state for the signal implementation.</summary>
        private readonly FinallySignal<T> _parent;

        /// <summary>Stores the downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Stores the upstream subscription.</summary>
        private IDisposable? _cancel;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="Finally"/> class.</summary>
        /// <param name="parent">The parent value.</param>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        /// <exception cref="ArgumentNullException"><paramref name="cancel"/> is <see langword="null"/>.</exception>
        public Finally(FinallySignal<T> parent, IObserver<T> observer, IDisposable cancel)
        {
            _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
            _observer = observer;
            _parent = parent;
        }

        /// <summary>Executes the Run operation.</summary>
        /// <returns>The result.</returns>
        public MultipleDisposable Run()
        {
            IDisposable subscription;
            try
            {
                subscription = _parent._source.Subscribe(this);
            }
            catch
            {
                _parent._finallyAction();
                throw;
            }

            return new(subscription, new ActionDisposable(() => _parent._finallyAction()));
        }

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _observer.OnNext(value);

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => WitnessTeardown.Dispose(ref _disposed, ref _cancel);
    }
}
