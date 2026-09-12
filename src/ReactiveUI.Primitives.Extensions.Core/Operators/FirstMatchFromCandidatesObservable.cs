// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Projects candidates sequentially and emits the first transformed value satisfying the predicate.
/// Projection errors skip the candidate. If none matches, emits the fallback value and completes.
/// </summary>
/// <typeparam name="TKey">The type of candidate keys.</typeparam>
/// <typeparam name="TRaw">The element type emitted by the projected observable.</typeparam>
/// <typeparam name="TResult">The final result type emitted to downstream after transformation.</typeparam>
/// <param name="candidates">The ordered list of candidate keys to walk.</param>
/// <param name="project">Projects a candidate key into a one-shot observable of raw values.</param>
/// <param name="transform">Synchronous transform applied to each raw value to produce the result.</param>
/// <param name="predicate">Returns <see langword="true"/> when a transformed value is a match.</param>
/// <param name="fallback">Value emitted when no candidate matches.</param>
/// <remarks>A projection that completes synchronously runs on the subscribing thread; one that does not keeps the walk alive until its callbacks arrive.</remarks>
public sealed class FirstMatchFromCandidatesObservable<TKey, TRaw, TResult>(
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

    /// <summary>Walks the candidates inline, handing over to an asynchronous sink at the first projection that does not complete synchronously.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription disposable.</returns>
    internal IDisposable TrySyncLoop(IObserver<TResult> observer)
    {
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

            if (!predicate(transformed))
            {
                continue;
            }

            observer.OnNext(transformed);
            observer.OnCompleted();
            SyncProbe.ReturnToCurrentThread(probe);
            return EmptyDisposable.Instance;
        }

        observer.OnNext(fallback);
        observer.OnCompleted();
        SyncProbe.ReturnToCurrentThread(probe);
        return EmptyDisposable.Instance;
    }

    /// <summary>Observer that records one candidate projection's synchronous outcome — value, error and termination — for the inline walk.</summary>
    [System.Diagnostics.DebuggerDisplay("SyncProbe: Completed = {Completed}, HasValue = {HasValue}, Value = {Value}")]
    public sealed class SyncProbe : IObserver<TRaw>
    {
        /// <summary>Per-thread cached instance, rented on entry to the inline walk and returned on exit.</summary>
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

    /// <summary>Observer that walks the remaining candidates through asynchronous callbacks once a projection defers.</summary>
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

        /// <summary>One once the sink has reached a terminal state; otherwise zero.</summary>
        private int _done;

        /// <summary>Set while <see cref="TryNext"/> walks candidates, so a terminal callback from the inline subscription does not advance the walk re-entrantly.</summary>
        private bool _looping;

        /// <inheritdoc/>
        public void OnNext(TRaw value)
        {
            if (Volatile.Read(ref _done) != 0)
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

            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return;
            }

            downstream.OnNext(transformed);
            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Volatile.Read(ref _done) != 0)
            {
                return;
            }

            if (_looping)
            {
                // The walk in TryNext reads this terminal notification off the probe instead.
                return;
            }

            TryNext();
        }

        /// <inheritdoc/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "SST2318:Members should not have identical bodies",
            Justification =
                "A candidate that errors and a candidate that completes both mean no match, so both channels advance the walk.")]
        public void OnCompleted()
        {
            if (Volatile.Read(ref _done) != 0)
            {
                return;
            }

            if (_looping)
            {
                // The walk in TryNext reads this terminal notification off the probe instead.
                return;
            }

            TryNext();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Volatile.Write(ref _done, 1);
            Interlocked.Exchange(ref _currentSubscription, null)?.Dispose();
        }

        /// <summary>Subscribes to the next candidate's projected observable, or emits the fallback if no candidates remain.</summary>
        internal void TryNext()
        {
            _looping = true;
            try
            {
                while (Volatile.Read(ref _done) == 0 && _index < candidates.Count)
                {
                    var key = candidates[_index];
                    _index++;

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
                    _ = Interlocked.Exchange(ref _currentSubscription, sub);

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

            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return;
            }

            downstream.OnNext(fallback);
            downstream.OnCompleted();
        }

        /// <summary>Forwards notifications and records synchronous termination for one candidate subscription.</summary>
        /// <param name="inner">The wrapped sink that receives forwarded notifications.</param>
        private sealed class CompletionFlagWitness(IObserver<TRaw> inner) : IObserver<TRaw>
        {
            /// <summary>Gets a value indicating whether a terminal notification was observed.</summary>
            public bool Completed { get; private set; }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
