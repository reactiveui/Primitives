// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Projects each source value to an inner observable and mirrors only the latest one.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The element type of the projected inner observables.</typeparam>
/// <remarks>
/// Fuses the projection into the switch: one object and one observer hop, where a projection followed by a
/// separate switch costs two of each and an intermediate sequence of observables.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("Source = {_source}, SkipNullSources = {_skipNullSources}")]
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
    /// Whether a null source value leaves the active inner subscription in place rather than switching. A caller
    /// that projects null onto its own inner observable switches on it instead, which detaches the previous one.
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
    private sealed class Sink(
        Func<TSource, IObservable<TResult>> selector,
        IObserver<TResult> downstream,
        bool skipNullSources) : IObserver<TSource>, IDisposable
    {
        /// <summary>Guards the switching state so outer and inner notifications stay consistent.</summary>
        private readonly Lock _gate = new();

        /// <summary>The outer (source) subscription.</summary>
        private readonly OnceDisposable _outer = new();

        /// <summary>The active inner subscription; assigning a new value disposes the previous one.</summary>
        private readonly SwapDisposable _inner = new();

        /// <summary>Generation id of the most recent inner observable; stale inner notifications are ignored.</summary>
        private ulong _latest;

        /// <summary>Whether an inner subscription is currently active.</summary>
        private bool _hasInner;

        /// <summary>Whether the outer source has completed.</summary>
        private bool _outerCompleted;

        /// <summary>Whether this sink has been disposed.</summary>
        private bool _disposed;

        /// <summary>Begins observing the source.</summary>
        /// <param name="source">The source observable.</param>
        public void Run(IObservable<TSource> source) => _outer.Disposable = source.Subscribe(this);

        /// <inheritdoc/>
        public void OnNext(TSource value)
        {
            if (skipNullSources && value is null)
            {
                return;
            }

            IObservable<TResult> inner;
            try
            {
                inner = selector(value);
            }
            catch (Exception ex)
            {
                OnError(ex);
                return;
            }

            ulong id;
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                id = ++_latest;
                _hasInner = true;
            }

            _inner.Disposable = inner.Subscribe(new InnerWitness(this, id));
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }
            }

            downstream.OnError(error);
            Dispose();
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            bool complete;
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _outerCompleted = true;
                complete = !_hasInner;
            }

            if (!complete)
            {
                return;
            }

            downstream.OnCompleted();
            Dispose();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
            }

            _outer.Dispose();
            _inner.Dispose();
        }

        /// <summary>Forwards an inner value to the downstream observer if it belongs to the active inner subscription.</summary>
        /// <param name="id">The generation id of the inner subscription that produced the value.</param>
        /// <param name="value">The value to forward.</param>
        private void InnerOnNext(ulong id, TResult value)
        {
            lock (_gate)
            {
                if (_disposed || id != _latest)
                {
                    return;
                }
            }

            downstream.OnNext(value);
        }

        /// <summary>Forwards an inner error to the downstream observer if it belongs to the active inner subscription.</summary>
        /// <param name="id">The generation id of the inner subscription that errored.</param>
        /// <param name="error">The error to forward.</param>
        private void InnerOnError(ulong id, Exception error)
        {
            lock (_gate)
            {
                if (_disposed || id != _latest)
                {
                    return;
                }
            }

            downstream.OnError(error);
            Dispose();
        }

        /// <summary>Clears the active inner subscription; completes downstream only if the outer has also completed.</summary>
        /// <param name="id">The generation id of the inner subscription that completed.</param>
        private void InnerOnCompleted(ulong id)
        {
            bool complete;
            lock (_gate)
            {
                if (_disposed || id != _latest)
                {
                    return;
                }

                _hasInner = false;
                complete = _outerCompleted;
            }

            if (!complete)
            {
                return;
            }

            downstream.OnCompleted();
            Dispose();
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
