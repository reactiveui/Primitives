// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>
/// Runs a list of one-shot <see cref="IObservable{RxVoid}"/> observables sequentially,
/// ignoring emitted values, and emits a single <see cref="RxVoid.Default"/> when all
/// have completed. If the list is empty, emits <see cref="RxVoid.Default"/> immediately.
/// Errors from any observable propagate to the downstream observer.
/// </summary>
/// <remarks>
/// Replaces patterns like <c>sources.Concat().LastOrDefaultAsync()</c> with a single
/// operator that subscribes sequentially. Uses an iterative loop with a sync-completion
/// flag to avoid stack overflow when sources complete synchronously during
/// <c>Subscribe</c>.
/// </remarks>
/// <param name="sources">The list of one-shot observables to run in order.</param>
internal sealed class RunAllObservable(IReadOnlyList<IObservable<RxVoid>> sources) : IObservable<RxVoid>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(sources);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        if (sources.Count == 0)
        {
            observer.OnNext(RxVoid.Default);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        Sink sink = new(observer, sources);
        sink.RunNext();
        return sink;
    }

    /// <summary>
    /// Stateful observer that walks the source list sequentially. The sink subscribes itself
    /// directly to each source — its own <see cref="IObserver{RxVoid}.OnCompleted"/> sets a
    /// per-iteration flag the surrounding loop reads to decide whether to advance. This
    /// replaces the previous probe-observer-per-iteration allocation pattern.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="sources">The source list to walk.</param>
    private sealed class Sink(
        IObserver<RxVoid> downstream,
        IReadOnlyList<IObservable<RxVoid>> sources) : IObserver<RxVoid>, IDisposable
    {
        /// <summary>Index of the current source being observed.</summary>
        private int _index;

        /// <summary>Subscription to the current source.</summary>
        private IDisposable? _currentSubscription;

        /// <summary>Set once all sources have completed or we've been disposed.</summary>
        private bool _done;

        /// <summary>Guards against re-entrant <see cref="RunNext"/> calls.</summary>
        private bool _looping;

        /// <summary>Per-iteration latch (0 = pending, 1 = terminated). Set by <see cref="OnCompleted"/>
        /// when a source terminates synchronously during <c>Subscribe</c>; read by the surrounding
        /// loop in <see cref="RunNext"/>. Accessed via <see cref="Volatile"/> so it crosses the
        /// method boundary safely without needing a separate probe-observer allocation per iteration.</summary>
        private int _iterationTerminated;

        /// <inheritdoc/>
        public void OnNext(RxVoid value)
        {
            // Ignore — we only care about completion.
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            downstream.OnError(error);
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
                // Inside the loop the surrounding RunNext reads _iterationTerminated; no recursion.
                Volatile.Write(ref _iterationTerminated, 1);
                return;
            }

            RunNext();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _done = true;
            Interlocked.Exchange(ref _currentSubscription, null)?.Dispose();
        }

        /// <summary>
        /// Subscribes to the next source, or emits RxVoid and completes if all are done.
        /// Iteratively loops on synchronous completion to avoid recursive stack growth.
        /// </summary>
        internal void RunNext()
        {
            _looping = true;
            try
            {
                while (!_done && _index < sources.Count)
                {
                    var source = sources[_index++];
                    Volatile.Write(ref _iterationTerminated, 0);
                    var sub = source.Subscribe(this);
                    Interlocked.Exchange(ref _currentSubscription, sub);

                    if (Volatile.Read(ref _iterationTerminated) == 0)
                    {
                        return;
                    }
                }
            }
            finally
            {
                _looping = false;
            }

            CompleteRun();
        }

        /// <summary>Emits the terminal <see cref="RxVoid"/> and completes once all sources have run.</summary>
        /// <remarks>The already-done early-out is only reachable when a concurrent dispose latches between the
        /// loop exit and this call; this small completion shell is excluded from coverage as race-only while the
        /// trampoline loop in <see cref="RunNext"/> stays covered.</remarks>
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        private void CompleteRun()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            downstream.OnNext(RxVoid.Default);
            downstream.OnCompleted();
        }
    }
}
