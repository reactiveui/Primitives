// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives;

/// <summary>
/// Fused <c>Blend</c> + <c>Unique</c> operator: concurrently merges a fixed set of sources and forwards a value
/// only when it differs from the previously forwarded one. Folding the merge and distinct-until-changed into a
/// single sink avoids the extra subscription hop and allocation of <c>sources.Blend().Unique()</c>.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Concurrently merges the supplied sources and forwards only values that differ from the previously
    /// forwarded value, using the default equality comparer. Errors are forwarded from the first failing source;
    /// completion is signalled once every source has completed.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable of the distinct merged values.</returns>
    public static IObservable<T> BlendUnique<T>(params IObservable<T>[] sources) =>
        BlendUnique(sources, comparer: null);

    /// <summary>
    /// Concurrently merges the supplied sources and forwards only values that differ from the previously
    /// forwarded value, using the supplied comparer (or the default when <see langword="null"/>).
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <param name="comparer">The equality comparer used to suppress duplicates, or <see langword="null"/> for the default.</param>
    /// <returns>An observable of the distinct merged values.</returns>
    public static IObservable<T> BlendUnique<T>(IObservable<T>[] sources, IEqualityComparer<T>? comparer)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return new BlendUniqueSignal<T>(sources, comparer ?? EqualityComparer<T>.Default);
    }

    /// <summary>A fused merge + distinct-until-changed observable over a fixed set of sources.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class BlendUniqueSignal<T> : IObservable<T>
    {
        /// <summary>The sources to merge.</summary>
        private readonly IObservable<T>[] _sources;

        /// <summary>The equality comparer used to suppress duplicates.</summary>
        private readonly IEqualityComparer<T> _comparer;

        /// <summary>Initializes a new instance of the <see cref="BlendUniqueSignal{T}"/> class.</summary>
        /// <param name="sources">The sources to merge.</param>
        /// <param name="comparer">The equality comparer used to suppress duplicates.</param>
        internal BlendUniqueSignal(IObservable<T>[] sources, IEqualityComparer<T> comparer)
        {
            _sources = sources;
            _comparer = comparer;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new BlendUniqueSink<T>(observer, _comparer);
            sink.Run(_sources);
            return sink;
        }
    }

    /// <summary>Forwards distinct merged values downstream and tears down every source on dispose.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class BlendUniqueSink<T> : IDisposable
    {
        /// <summary>Serializes value forwarding and guards the distinct/completion state.</summary>
        private readonly Lock _gate = new();

        /// <summary>The per-source subscriptions, torn down on dispose.</summary>
        private readonly MultipleDisposable _pocket = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _downstream;

        /// <summary>The equality comparer used to suppress duplicates.</summary>
        private readonly IEqualityComparer<T> _comparer;

        /// <summary>The most recently forwarded value (valid only once <see cref="_hasLast"/> is set).</summary>
        private T _last = default!;

        /// <summary>Whether a value has been forwarded yet.</summary>
        private bool _hasLast;

        /// <summary>The number of sources that have not yet completed.</summary>
        private int _active;

        /// <summary>Whether a terminal notification has been emitted.</summary>
        private bool _done;

        /// <summary>Initializes a new instance of the <see cref="BlendUniqueSink{T}"/> class.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="comparer">The equality comparer used to suppress duplicates.</param>
        internal BlendUniqueSink(IObserver<T> downstream, IEqualityComparer<T> comparer)
        {
            _downstream = downstream;
            _comparer = comparer;
        }

        /// <summary>Subscribes to every merged source.</summary>
        /// <param name="sources">The sources to merge.</param>
        public void Run(IObservable<T>[] sources)
        {
            lock (_gate)
            {
                _active = sources.Length;
            }

            if (sources.Length == 0)
            {
                Complete();
                return;
            }

            for (var i = 0; i < sources.Length; i++)
            {
                var source = sources[i];
                if (source == null)
                {
                    ForwardError(new InvalidOperationException("BlendUnique source contained null."));
                    return;
                }

                _pocket.Add(source.Subscribe(new Element(this)));
            }
        }

        /// <inheritdoc/>
        public void Dispose() => _pocket.Dispose();

        /// <summary>Forwards a value when it differs from the previously forwarded one.</summary>
        /// <param name="value">The merged value.</param>
        private void Forward(T value)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                if (_hasLast && _comparer.Equals(_last, value))
                {
                    return;
                }

                _last = value;
                _hasLast = true;
                _downstream.OnNext(value);
            }
        }

        /// <summary>Forwards the first terminal error and suppresses later notifications.</summary>
        /// <param name="error">The error.</param>
        private void ForwardError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _downstream.OnError(error);
            }
        }

        /// <summary>Decrements the active count and completes once every source has completed.</summary>
        private void Complete()
        {
            lock (_gate)
            {
                if (_done || --_active > 0)
                {
                    return;
                }

                _done = true;
                _downstream.OnCompleted();
            }
        }

        /// <summary>Observes a single merged source.</summary>
        /// <param name="parent">The owning sink.</param>
        private sealed class Element(BlendUniqueSink<T> parent) : IObserver<T>
        {
            /// <inheritdoc/>
            public void OnNext(T value) => parent.Forward(value);

            /// <inheritdoc/>
            public void OnError(Exception error) => parent.ForwardError(error);

            /// <inheritdoc/>
            public void OnCompleted() => parent.Complete();
        }
    }
}
