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
[System.Diagnostics.DebuggerDisplay("FinallySignal: Source = {_source}")]
public sealed class FinallySignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The action run when the subscription ends.</summary>
    private readonly Action _finallyAction;

    /// <summary>Initializes a new instance of the <see cref="FinallySignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="finallyAction">The action run when the subscription ends.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="finallyAction"/> is <see langword="null"/>.</exception>
    public FinallySignal(IObservable<T> source, Action finallyAction)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(finallyAction);

        _source = source;
        _finallyAction = finallyAction;
    }

    /// <summary>Reports that subscription runs on the calling thread.</summary>
    /// <returns>Always <see langword="true"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <summary>Subscribes the observer and attaches the end-of-subscription action.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that releases the subscription and runs the action.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return SubscriptionScheduling.OnCurrentThread(
            (Self: this, observer),
            static s => new Finally(s.observer, s.Self._finallyAction).Run(s.Self._source));
    }

    /// <summary>Forwards notifications downstream and runs the end-of-subscription action once on teardown.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="finallyAction">The action run when the subscription ends.</param>
    private sealed class Finally(IObserver<T> observer, Action finallyAction) : IObserver<T>, IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>The action run once when the subscription ends; nulled once claimed.</summary>
        private Action? _finallyAction = finallyAction;

        /// <summary>The upstream subscription handle.</summary>
        private IDisposable? _subscription;

        /// <summary>Subscribes to the source, running the action immediately if subscription throws.</summary>
        /// <param name="source">The source observable.</param>
        /// <returns>This subscription, which releases the source subscription and then runs the action.</returns>
        public Finally Run(IObservable<T> source)
        {
            try
            {
                SinkSubscription.Set(ref _subscription, source.Subscribe(this));
            }
            catch
            {
                Dispose();
                throw;
            }

            return this;
        }

        /// <summary>Forwards a value downstream.</summary>
        /// <param name="value">The value to forward.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => SinkDelivery.Next(_observer, value, this);

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

        /// <summary>Releases the upstream subscription and runs the action once.</summary>
        public void Dispose()
        {
            var action = Interlocked.Exchange(ref _finallyAction, null);
            if (action is null)
            {
                return;
            }

            try
            {
                SinkSubscription.Dispose(ref _subscription);
            }
            finally
            {
                action();
            }
        }
    }
}
