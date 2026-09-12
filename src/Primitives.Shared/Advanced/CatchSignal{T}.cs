// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Subscribes to each source in turn, moving to the next one whenever a source errors.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class CatchSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The sources tried in order.</summary>
    private readonly IEnumerable<IObservable<T>> _sources;

    /// <summary>Initializes a new instance of the <see cref="CatchSignal{T}"/> class.</summary>
    /// <param name="sources">The sources to try in order.</param>
    public CatchSignal(IEnumerable<IObservable<T>> sources) => _sources = sources;

    /// <summary>Reports that subscription runs on the calling thread.</summary>
    /// <returns>Always <see langword="true"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <summary>Subscribes the observer and starts walking the sources.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that tears the walk down.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, true, SubscribeCore);

    /// <summary>Creates the handler that walks the sources and starts it.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    /// <returns>The disposable that tears the walk down.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new Catch(this, observer, cancel).Run();

    /// <summary>Walks the source sequence, advancing on each error and forwarding the last error if none succeed.</summary>
    private sealed class Catch : IObserver<T>, IDisposable
    {
        /// <summary>The signal supplying the sources.</summary>
        private readonly CatchSignal<T> _parent;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Serializes advancing the enumerator against teardown.</summary>
        private readonly Lock _gate = new();

        /// <summary>The outer subscription handle released on teardown.</summary>
        private IDisposable? _cancel;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Set under <see cref="_gate"/> once teardown ran, so no further source is subscribed.</summary>
        private bool _isDisposed;

        /// <summary>The enumerator over the sources.</summary>
        private IEnumerator<IObservable<T>>? _e;

        /// <summary>The slot holding the current source subscription.</summary>
        private SingleReplaceableDisposable? _subscription;

        /// <summary>The error raised by the most recent source.</summary>
        private Exception? _lastException;

        /// <summary>The recursive continuation that advances to the next source.</summary>
        private Action? _nextSelf;

        /// <summary>Initializes a new instance of the <see cref="Catch"/> class.</summary>
        /// <param name="parent">The signal supplying the sources.</param>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="cancel">The outer subscription handle.</param>
        /// <exception cref="ArgumentNullException"><paramref name="cancel"/> is <see langword="null"/>.</exception>
        public Catch(CatchSignal<T> parent, IObserver<T> observer, IDisposable cancel)
        {
            _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
            _observer = observer;
            _parent = parent;
        }

        /// <summary>Starts the walk on the immediate sequencer.</summary>
        /// <returns>The disposable that releases the enumerator and the current source subscription.</returns>
        public MultipleDisposable Run()
        {
            _isDisposed = false;
            _e = _parent._sources.GetEnumerator();
            _subscription = new();

            var schedule = Sequencer.Immediate.Schedule(RecursiveRun);

            return new(schedule, _subscription, new ActionDisposable(() =>
            {
                lock (_gate)
                {
                    _isDisposed = true;
                    _e?.Dispose();
                    _e = null;
                }
            }));
        }

        /// <summary>Forwards a value downstream.</summary>
        /// <param name="value">The value to forward.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _observer.OnNext(value);

        /// <summary>Records the error and advances to the next source instead of terminating.</summary>
        /// <param name="error">The error raised by the current source.</param>
        public void OnError(Exception error)
        {
            _lastException = error;
            _nextSelf!();
        }

        /// <summary>Completes downstream and tears the walk down.</summary>
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

        /// <summary>Releases the enumerator, the current source subscription and the outer handle.</summary>
        public void Dispose()
        {
            _e?.Dispose();
            _e = null;
            _subscription?.Dispose();
            _subscription = null;
            _ = WitnessTeardown.Dispose(ref _disposed, ref _cancel);
        }

        /// <summary>Subscribes to the next source, or terminates once the sequence is exhausted.</summary>
        /// <param name="self">The continuation that re-enters this method for the following source.</param>
        private void RecursiveRun(Action self)
        {
            lock (_gate)
            {
                _nextSelf = self;
                if (_isDisposed)
                {
                    return;
                }

                if (!TryMoveToNextSource(out var next, out var error))
                {
                    FailAndDispose(error!);
                    return;
                }

                if (next is null)
                {
                    FinishAndDispose();
                    return;
                }

                _subscription?.Create(new SingleDisposable(next.Subscribe(this)));
            }
        }

        /// <summary>Advances the enumerator to the next source while the caller holds the gate.</summary>
        /// <param name="next">The next source, or <see langword="null"/> once the sequence is exhausted.</param>
        /// <param name="error">The exception the sequence raised, when it raised one.</param>
        /// <returns><see langword="true"/> when the sequence advanced without raising.</returns>
        /// <exception cref="InvalidOperationException">The sequence yielded a <see langword="null"/> source.</exception>
        private bool TryMoveToNextSource(out IObservable<T>? next, out Exception? error)
        {
            next = null;
            error = null;

            try
            {
                if (_e!.MoveNext())
                {
                    next = _e.Current ?? throw new InvalidOperationException("sequence is null.");
                }
                else
                {
                    _e.Dispose();
                }

                return true;
            }
            catch (Exception exception)
            {
                error = exception;
                _e?.Dispose();
                return false;
            }
        }

        /// <summary>Forwards an error downstream and tears the walk down.</summary>
        /// <param name="error">The error to forward.</param>
        private void FailAndDispose(Exception error)
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

        /// <summary>Ends the sequence once the sources are exhausted, reporting the last error one of them raised.</summary>
        private void FinishAndDispose()
        {
            if (_lastException is not null)
            {
                FailAndDispose(_lastException);
                return;
            }

            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }
}
