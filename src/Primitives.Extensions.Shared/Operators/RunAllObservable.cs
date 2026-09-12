// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Runs sources sequentially and emits RxVoid on completion, propagating source errors and completing immediately for empty input.</summary>
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

    /// <summary>Advances through sources as each subscription completes, handling synchronous completion without recursive subscription.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="sources">The source list to walk.</param>
    internal sealed class Sink(
        IObserver<RxVoid> downstream,
        IReadOnlyList<IObservable<RxVoid>> sources) : IObserver<RxVoid>, IDisposable
    {
        /// <summary>Index of the current source being observed.</summary>
        private int _index;

        /// <summary>Subscription to the current source.</summary>
        private IDisposable? _currentSubscription;

        /// <summary>Terminal latch (0 = running, 1 = all sources completed or disposed).</summary>
        private int _done;

        /// <summary>Guards against re-entrant <see cref="RunNext"/> calls.</summary>
        private bool _looping;

        /// <summary>Records synchronous source termination during subscription.</summary>
        private int _iterationTerminated;

        /// <inheritdoc/>
        public void OnNext(RxVoid value)
        {
            // Values are ignored; completion advances to the next source.
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return;
            }

            downstream.OnError(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Volatile.Read(ref _done) != 0)
            {
                return;
            }

            if (_looping)
            {
                Volatile.Write(ref _iterationTerminated, 1);
                return;
            }

            RunNext();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Volatile.Write(ref _done, 1);
            Interlocked.Exchange(ref _currentSubscription, null)?.Dispose();
        }

        /// <summary>Advances through synchronously completing sources without recursion, then completes when all sources are done.</summary>
        internal void RunNext()
        {
            _looping = true;
            try
            {
                while (Volatile.Read(ref _done) == 0 && _index < sources.Count)
                {
                    var source = sources[_index];
                    _index++;
                    Volatile.Write(ref _iterationTerminated, 0);
                    var sub = source.Subscribe(this);
                    _ = Interlocked.Exchange(ref _currentSubscription, sub);

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
        internal void CompleteRun()
        {
            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return;
            }

            downstream.OnNext(RxVoid.Default);
            downstream.OnCompleted();
        }
    }
}
