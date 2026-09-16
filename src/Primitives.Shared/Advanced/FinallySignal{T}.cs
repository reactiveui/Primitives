// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Runs an action once the subscription ends, whether it terminated or was disposed.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="finallyAction">The action run when the subscription ends.</param>
[System.Diagnostics.DebuggerDisplay("FinallySignal: Source = {_source}")]
public sealed class FinallySignal<T>(IObservable<T> source, Action finallyAction) : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The action run when the subscription ends.</summary>
    private readonly Action _finallyAction = finallyAction;

    /// <summary>Reports that subscription runs on the calling thread.</summary>
    /// <returns>Always <see langword="true"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <summary>Subscribes the observer and attaches the end-of-subscription action.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that releases the subscription and runs the action.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, true, SubscribeCore);

    /// <summary>Creates the handler that forwards notifications and owns the end-of-subscription action.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    /// <returns>The disposable that releases the subscription and runs the action.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new Finally(this, observer, cancel).Run();

    /// <summary>Forwards notifications downstream and pairs the subscription with the end-of-subscription action.</summary>
    /// <param name="parent">The signal supplying the source and the action.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    private sealed class Finally(FinallySignal<T> parent, IObserver<T> observer, IDisposable cancel) : IObserver<T>, IDisposable
    {
        /// <summary>The signal supplying the source and the action.</summary>
        private readonly FinallySignal<T> _parent = parent;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>The outer subscription handle released on teardown.</summary>
        private IDisposable? _cancel = cancel;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Subscribes to the source, running the action immediately if subscription throws.</summary>
        /// <returns>The disposable that releases the source subscription and then runs the action.</returns>
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

        /// <summary>Forwards a value downstream.</summary>
        /// <param name="value">The value to forward.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _observer.OnNext(value);

        /// <summary>Forwards the error downstream and releases the subscription.</summary>
        /// <param name="error">The error to forward.</param>
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

        /// <summary>Completes downstream and releases the subscription.</summary>
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

        /// <summary>Releases the outer subscription handle once.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => WitnessTeardown.Dispose(ref _disposed, ref _cancel);
    }
}
