// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Dedicated cold signal for <c>Resume</c> (continue with a fixed fallback sequence after any error). Holds the
/// fallback observable directly so no per-subscription closure is allocated, mirroring the slot-based subscription
/// management of <see cref="RecoverSignal{T, TException}"/>.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class ResumeSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The fallback observable subscribed to after the source errors.</summary>
    private readonly IObservable<T> _fallback;

    /// <summary>Initializes a new instance of the <see cref="ResumeSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="fallback">The fallback observable subscribed to after the source errors.</param>
    internal ResumeSignal(IObservable<T> source, IObservable<T> fallback)
    {
        _source = source;
        _fallback = fallback;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer is null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        if (!CurrentThreadSequencer.IsScheduleRequired)
        {
            return Run(observer);
        }

        var subscription = new SingleDisposable();
        Sequencer.CurrentThread.Schedule(() => subscription.Create(Run(observer)));
        return subscription;
    }

    /// <summary>Builds the sink and subscribes it to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The sink, which is the subscription.</returns>
    private ResumeObserver Run(IObserver<T> observer) => new ResumeObserver(observer, _fallback).Run(_source);

    /// <summary>Forwards source values and, on any error, switches to the fallback sequence.</summary>
    private sealed class ResumeObserver : IObserver<T>, IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The fallback observable.</summary>
        private readonly IObservable<T> _fallback;

        /// <summary>The source subscription slot.</summary>
        private IDisposable? _sourceSubscription;

        /// <summary>The fallback subscription slot, populated after an error.</summary>
        private IDisposable? _fallbackSubscription;

        /// <summary>Initializes a new instance of the <see cref="ResumeObserver"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="fallback">The fallback observable.</param>
        internal ResumeObserver(IObserver<T> observer, IObservable<T> fallback)
        {
            _observer = observer;
            _fallback = fallback;
        }

        /// <inheritdoc/>
        public void OnNext(T value) => _observer.OnNext(value);

        /// <inheritdoc/>
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
        internal ResumeObserver Run(IObservable<T> source)
        {
            SubscriptionSlots.Assign(ref _sourceSubscription, source.Subscribe(this));
            return this;
        }

        /// <summary>Stores the fallback subscription.</summary>
        /// <param name="subscription">The fallback subscription.</param>
        private void SetFallback(IDisposable subscription) => SubscriptionSlots.Assign(ref _fallbackSubscription, subscription);
    }
}
