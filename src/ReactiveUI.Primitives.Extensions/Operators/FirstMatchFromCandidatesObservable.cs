// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Walks a list of candidate keys sequentially, projects each into a one-shot
/// <see cref="IObservable{TRaw}"/>, transforms the raw value into
/// <typeparamref name="TResult"/>, and emits the first transformed value that
/// satisfies a predicate. Errors from individual projections are swallowed (the
/// candidate is skipped and the next one is tried). If no candidate matches,
/// completes with a single emission of <paramref name="fallback"/>.
/// </summary>
/// <remarks>
/// <c>Subscribe</c> attempts a synchronous fast-path first: each candidate's
/// projection is subscribed and, if it completes inline, the transform + predicate
/// run on the calling thread with zero additional allocations. Only when a
/// projection completes asynchronously does the method allocate an
/// <see cref="AsyncSink"/> to track state across callbacks.
/// </remarks>
/// <typeparam name="TKey">The type of candidate keys.</typeparam>
/// <typeparam name="TRaw">The element type emitted by the projected observable.</typeparam>
/// <typeparam name="TResult">The final result type emitted to downstream after transformation.</typeparam>
/// <param name="candidates">The ordered list of candidate keys to walk.</param>
/// <param name="project">Projects a candidate key into a one-shot observable of raw values.</param>
/// <param name="transform">Synchronous transform applied to each raw value to produce the result.</param>
/// <param name="predicate">Returns <see langword="true"/> when a transformed value is a match.</param>
/// <param name="fallback">Value emitted when no candidate matches.</param>
internal sealed class FirstMatchFromCandidatesObservable<TKey, TRaw, TResult>(
    IReadOnlyList<TKey> candidates,
    Func<TKey, IObservable<TRaw>> project,
    Func<TRaw, TResult> transform,
    Func<TResult, bool> predicate,
    TResult fallback) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(candidates);
        InvalidOperationExceptionHelper.ThrowIfNull(project);
        InvalidOperationExceptionHelper.ThrowIfNull(transform);
        InvalidOperationExceptionHelper.ThrowIfNull(predicate);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (candidates.Count == 0)
        {
            observer.OnNext(fallback);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        return TrySyncLoop(observer);
    }

    /// <summary>Tries a synchronous fast-path: each candidate's projected observable is.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription disposable.</returns>
    internal IDisposable TrySyncLoop(IObserver<TResult> observer)
    {
        // Reuse the SyncProbe across subscribes on the current thread — it carries no
        // per-call state once Reset, so per-cycle allocation drops to zero on the fast path.
        // Race-free because the field is [ThreadStatic]; only one TrySyncLoop call can be
        // active per thread.
        var probe = SyncProbe.RentForCurrentThread();

        for (var i = 0; i < candidates.Count; i++)
        {
            TResult transformed;
            try
            {
                var projected = project(candidates[i]);

                probe.Reset();
                var sub = projected.Subscribe(probe);

                if (!probe.Completed)
                {
                    sub.Dispose();
                    AsyncSink sink = new(observer, candidates, project, transform, predicate, fallback, i);
                    sink.TryNext();
                    SyncProbe.ReturnToCurrentThread(probe);
                    return sink;
                }

                sub.Dispose();

                if (probe.Errored || !probe.HasValue)
                {
                    continue;
                }

                transformed = transform(probe.Value!);
            }
            catch
            {
                continue;
            }

            if (predicate(transformed))
            {
                observer.OnNext(transformed);
                observer.OnCompleted();
                SyncProbe.ReturnToCurrentThread(probe);
                return EmptyDisposable.Instance;
            }
        }

        observer.OnNext(fallback);
        observer.OnCompleted();
        SyncProbe.ReturnToCurrentThread(probe);
        return EmptyDisposable.Instance;
    }

    /// <summary>
    /// Lightweight observer used by the synchronous fast-path to capture the result
    /// of a one-shot projection. Cheaper than <see cref="AsyncSink"/> because it
    /// carries no downstream observer, candidate list, or delegate references.
    /// </summary>
    internal sealed class SyncProbe : IObserver<TRaw>
    {
        /// <summary>Per-thread cached instance; rented on entry to <c>TrySyncLoop</c> and returned
        /// on exit. Eliminates the per-subscribe allocation on the fast path.</summary>
        [ThreadStatic]
        private static SyncProbe? _cached;

        /// <summary>Gets a value indicating whether <c>OnNext</c> was called.</summary>
        internal bool HasValue { get; private set; }

        /// <summary>Gets a value indicating whether <c>OnError</c> was called.</summary>
        internal bool Errored { get; private set; }

        /// <summary>Gets a value indicating whether <c>OnCompleted</c> or <c>OnError</c> was called.</summary>
        internal bool Completed { get; private set; }

        /// <summary>Gets the value received via <c>OnNext</c>.</summary>
        internal TRaw? Value { get; private set; }

        /// <summary>Rents a probe from the current-thread cache, allocating only if the slot is empty.</summary>
        /// <returns>A fresh-reset probe ready for use.</returns>
        public static SyncProbe RentForCurrentThread()
        {
            var rented = _cached;
            if (rented is null)
            {
                return new();
            }

            _cached = null;
            rented.Reset();
            return rented;
        }

        /// <summary>Returns a probe to the current-thread cache for reuse on the next call.</summary>
        /// <param name="probe">The probe instance to cache.</param>
        public static void ReturnToCurrentThread(SyncProbe probe) => _cached = probe;

        /// <inheritdoc/>
        public void OnNext(TRaw value)
        {
            Value = value;
            HasValue = true;
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            Errored = true;
            Completed = true;
        }

        /// <inheritdoc/>
        public void OnCompleted() => Completed = true;

        /// <summary>Resets state for reuse across candidates.</summary>
        internal void Reset()
        {
            HasValue = false;
            Errored = false;
            Completed = false;
            Value = default;
        }
    }

    /// <summary>
    /// Heap-allocated observer used when a projection does not complete synchronously.
    /// Walks the remaining candidates via async callbacks.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="candidates">The candidate list.</param>
    /// <param name="project">The projection delegate.</param>
    /// <param name="transform">The transform delegate.</param>
    /// <param name="predicate">The match predicate.</param>
    /// <param name="fallback">The fallback value.</param>
    /// <param name="startIndex">Index of the first candidate to try.</param>
    private sealed class AsyncSink(
        IObserver<TResult> downstream,
        IReadOnlyList<TKey> candidates,
        Func<TKey, IObservable<TRaw>> project,
        Func<TRaw, TResult> transform,
        Func<TResult, bool> predicate,
        TResult fallback,
        int startIndex) : IObserver<TRaw>, IDisposable
    {
        /// <summary>The current candidate index.</summary>
        private int _index = startIndex;

        /// <summary>The subscription to the current candidate's projected observable.</summary>
        private IDisposable? _currentSubscription;

        /// <summary>Whether the sink has reached a terminal state.</summary>
        private bool _done;

        /// <summary>Whether the sink is currently looping through candidates.</summary>
        private bool _looping;

        /// <inheritdoc/>
        public void OnNext(TRaw value)
        {
            if (_done)
            {
                return;
            }

            TResult transformed;
            try
            {
                transformed = transform(value);
            }
            catch
            {
                return;
            }

            if (!predicate(transformed))
            {
                return;
            }

            _done = true;
            downstream.OnNext(transformed);
            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            if (_looping)
            {
                // Sync-completion is captured by the probe in TryNext; nothing more to do here.
                return;
            }

            TryNext();
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            if (_looping)
            {
                // Sync-completion is captured by the probe in TryNext; nothing more to do here.
                return;
            }

            TryNext();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _done = true;
            Interlocked.Exchange(ref _currentSubscription, null)?.Dispose();
        }

        /// <summary>Subscribes to the next candidate's projected observable, or emits the fallback if no candidates remain.</summary>
        internal void TryNext()
        {
            _looping = true;
            try
            {
                while (!_done && _index < candidates.Count)
                {
                    var key = candidates[_index++];

                    IObservable<TRaw> projected;
                    try
                    {
                        projected = project(key);
                    }
                    catch
                    {
                        continue;
                    }

                    CompletionFlagWitness probe = new(this);
                    var sub = projected.Subscribe(probe);
                    Interlocked.Exchange(ref _currentSubscription, sub);

                    if (!probe.Completed)
                    {
                        return;
                    }
                }
            }
            finally
            {
                _looping = false;
            }

            if (_done)
            {
                return;
            }

            _done = true;
            downstream.OnNext(fallback);
            downstream.OnCompleted();
        }

        /// <summary>
        /// Forwarding <see cref="IObserver{TRaw}"/> that records whether a terminal notification
        /// (<c>OnError</c> / <c>OnCompleted</c>) arrived synchronously during the <c>Subscribe</c>
        /// call. Replaces the prior <c>_syncCompleted</c> field on <see cref="AsyncSink"/> so the
        /// flag is per-iteration state rather than instance state.
        /// </summary>
        /// <param name="inner">The wrapped sink that receives forwarded notifications.</param>
        private sealed class CompletionFlagWitness(IObserver<TRaw> inner) : IObserver<TRaw>
        {
            /// <summary>Gets a value indicating whether a terminal notification was observed.</summary>
            public bool Completed { get; private set; }

            /// <inheritdoc/>
            public void OnNext(TRaw value) => inner.OnNext(value);

            /// <inheritdoc/>
            public void OnError(Exception error)
            {
                Completed = true;
                inner.OnError(error);
            }

            /// <inheritdoc/>
            public void OnCompleted()
            {
                Completed = true;
                inner.OnCompleted();
            }
        }
    }
}
