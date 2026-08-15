// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>
/// Dedicated cold signal for <c>Resume</c> (continue with a fixed fallback sequence after any error). Holds the
/// fallback observable directly so no per-subscription closure is allocated, mirroring the slot-based subscription
/// management of <see cref="RecoverSignal{T, TException}"/>.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="fallback">The fallback observable subscribed to after the source errors.</param>
internal sealed class ResumeSignal<T>(IObservable<T> source, IObservable<T> fallback) : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The fallback observable subscribed to after the source errors.</summary>
    private readonly IObservable<T> _fallback = fallback;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return SubscriptionScheduling.OnCurrentThread(
            (Self: this, observer),
            static s => s.Self.Run(s.observer));
    }

    /// <summary>Builds the sink and subscribes it to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The sink, which is the subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ResumeWitness Run(IObserver<T> observer) => new ResumeWitness(observer, _fallback).Run(_source);

    /// <summary>Forwards source values and, on any error, switches to the fallback sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="fallback">The fallback observable.</param>
    private sealed class ResumeWitness(IObserver<T> observer, IObservable<T> fallback) : IObserver<T>, IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>The fallback observable.</summary>
        private readonly IObservable<T> _fallback = fallback;

        /// <summary>The source subscription slot.</summary>
        private IDisposable? _sourceSubscription;

        /// <summary>The fallback subscription slot, populated after an error.</summary>
        private IDisposable? _fallbackSubscription;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _observer.OnNext(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => SetFallback(_fallback.Subscribe(_observer));

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public void Dispose()
        {
            SubscriptionSlots.Release(ref _sourceSubscription);
            SubscriptionSlots.Release(ref _fallbackSubscription);
        }

        /// <summary>Subscribes to the source and returns the sink.</summary>
        /// <param name="source">The source observable.</param>
        /// <returns>This sink, which is the subscription.</returns>
        internal ResumeWitness Run(IObservable<T> source)
        {
            SubscriptionSlots.Assign(ref _sourceSubscription, source.Subscribe(this));
            return this;
        }

        /// <summary>Stores the fallback subscription.</summary>
        /// <param name="subscription">The fallback subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetFallback(IDisposable subscription) =>
            SubscriptionSlots.Assign(ref _fallbackSubscription, subscription);
    }
}
