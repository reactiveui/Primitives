// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Projects each source value to an inner observable and mirrors only the latest one.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The element type of the projected inner observables.</typeparam>
/// <remarks>
/// A terminal notification raised before the subscription is disposed is still delivered and, since disposal does not
/// wait, can reach the observer on the delivering thread just after <c>Dispose</c> returns.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SwitchMapSignal: Source = {_source}, SkipNullSources = {_skipNullSources}")]
public sealed class SwitchMapSignal<TSource, TResult> : IObservable<TResult>
{
    /// <summary>The source whose values are projected to inner observables.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>Projects a source value to the inner observable to switch to.</summary>
    private readonly Func<TSource, IObservable<TResult>> _selector;

    /// <summary>Whether a null source value is skipped rather than projected.</summary>
    private readonly bool _skipNullSources;

    /// <summary>Initializes a new instance of the <see cref="SwitchMapSignal{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source whose values are projected to inner observables.</param>
    /// <param name="selector">Projects a source value to the inner observable to switch to.</param>
    public SwitchMapSignal(IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
        : this(source, selector, skipNullSources: false)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SwitchMapSignal{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source whose values are projected to inner observables.</param>
    /// <param name="selector">Projects a source value to the inner observable to switch to.</param>
    /// <param name="skipNullSources">
    /// Whether null source values retain the active subscription
    /// without invoking the selector.
    /// </param>
    internal SwitchMapSignal(
        IObservable<TSource> source,
        Func<TSource, IObservable<TResult>> selector,
        bool skipNullSources)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        _source = source;
        _selector = selector;
        _skipNullSources = skipNullSources;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Sink sink = new(_selector, observer, _skipNullSources);
        sink.Run(_source);
        return sink;
    }

    /// <summary>Subscribes to the source, switching the active inner subscription as values arrive.</summary>
    /// <param name="selector">Projects a source value to the inner observable to switch to.</param>
    /// <param name="downstream">The downstream observer that receives values from the latest inner observable.</param>
    /// <param name="skipNullSources">Whether a null source value is skipped rather than projected.</param>
    /// <remarks>
    /// The gate only guards the switching state: a notification is accepted or dropped and queued on a
    /// <see cref="SerializedDelivery{T}"/> under it, then delivered after it is released, so no lock is held while the
    /// selector, the downstream observer, an inner subscription or its disposal runs. Outer and inner notifications from
    /// different threads never overlap downstream.
    /// </remarks>
    private sealed class Sink(
        Func<TSource, IObservable<TResult>> selector,
        IObserver<TResult> downstream,
        bool skipNullSources) : IObserver<TSource>, IDisposable
    {
        /// <summary>Guards the switching state; never held while user code runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>The outer (source) subscription.</summary>
        private readonly OnceDisposable _outer = new();

        /// <summary>Projects a source value to the inner observable to switch to.</summary>
        private readonly Func<TSource, IObservable<TResult>> _selector = selector;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _downstream = downstream;

        /// <summary>Whether a null source value is skipped rather than projected.</summary>
        private readonly bool _skipNullSources = skipNullSources;

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<TResult> _delivery = new();

        /// <summary>The subscription to the latest inner observable; guarded by the gate.</summary>
        private IDisposable? _inner;

        /// <summary>Generation id of the most recent inner observable; stale inner notifications are ignored.</summary>
        private ulong _latest;

        /// <summary>Whether an inner subscription is currently active.</summary>
        private bool _hasInner;

        /// <summary>Whether the outer source has completed.</summary>
        private bool _outerCompleted;

        /// <summary>Whether a terminal notification has been queued or this sink disposed.</summary>
        private bool _done;

        /// <summary>Begins observing the source.</summary>
        /// <param name="source">The source observable.</param>
        public void Run(IObservable<TSource> source) => _outer.Disposable = source.Subscribe(this);

        /// <inheritdoc/>
        public void OnNext(TSource value)
        {
            if (_skipNullSources && value is null)
            {
                return;
            }

            IObservable<TResult> inner;
            try
            {
                inner = _selector(value);
            }
            catch (Exception ex)
            {
                OnError(ex);
                return;
            }

            ulong id;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                id = ++_latest;
                _hasInner = true;
            }

            var subscription = inner.Subscribe(new InnerWitness(this, id));

            // Only the newest subscription is kept, whichever order overlapping subscriptions return in.
            IDisposable? displaced;
            lock (_gate)
            {
                if (_done || id != _latest)
                {
                    displaced = subscription;
                }
                else
                {
                    displaced = _inner;
                    _inner = subscription;
                }
            }

            displaced?.Dispose();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _ = _delivery.PostError(error);
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _outerCompleted = true;
                if (_hasInner)
                {
                    return;
                }

                _done = true;
                _ = _delivery.PostCompleted();
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            IDisposable? inner;
            lock (_gate)
            {
                _done = true;
                inner = _inner;
                _inner = null;
            }

            _outer.Dispose();
            inner?.Dispose();
        }

        /// <summary>Forwards an inner value to the downstream observer if it belongs to the active inner subscription.</summary>
        /// <param name="id">The generation id of the inner subscription that produced the value.</param>
        /// <param name="value">The value to forward.</param>
        private void InnerOnNext(ulong id, TResult value)
        {
            lock (_gate)
            {
                if (_done || id != _latest)
                {
                    return;
                }

                _ = _delivery.Post(value);
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Forwards an inner error to the downstream observer if it belongs to the active inner subscription.</summary>
        /// <param name="id">The generation id of the inner subscription that errored.</param>
        /// <param name="error">The error to forward.</param>
        private void InnerOnError(ulong id, Exception error)
        {
            lock (_gate)
            {
                if (_done || id != _latest)
                {
                    return;
                }

                _done = true;
                _ = _delivery.PostError(error);
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Clears the active inner subscription; completes downstream only if the outer has also completed.</summary>
        /// <param name="id">The generation id of the inner subscription that completed.</param>
        private void InnerOnCompleted(ulong id)
        {
            lock (_gate)
            {
                if (_done || id != _latest)
                {
                    return;
                }

                _hasInner = false;
                if (!_outerCompleted)
                {
                    return;
                }

                _done = true;
                _ = _delivery.PostCompleted();
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Drains the sink's queued notifications and releases its subscriptions once the terminal is delivered.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(Sink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            public void Drain()
            {
                if (!Owner._delivery.DrainTo(Owner._downstream))
                {
                    return;
                }

                Owner.Dispose();
            }
        }

        /// <summary>Observes a single inner observable and routes its notifications through the parent sink.</summary>
        /// <param name="parent">The owning sink.</param>
        /// <param name="id">The generation id of this inner subscription.</param>
        private sealed class InnerWitness(Sink parent, ulong id) : IObserver<TResult>
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnNext(TResult value) => parent.InnerOnNext(id, value);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnError(Exception error) => parent.InnerOnError(id, error);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnCompleted() => parent.InnerOnCompleted(id);
        }
    }
}
