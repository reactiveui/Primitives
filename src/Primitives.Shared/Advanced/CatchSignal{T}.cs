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
    /// <param name="parent">The signal supplying the sources.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    /// <remarks>
    /// The gate only hands the enumerator between the walker and teardown. The enumerator is advanced, the next source is
    /// subscribed and the observer is notified without the gate held; teardown that arrives while the walker is advancing
    /// leaves disposing the enumerator to the walker, so a running enumerator is never disposed underneath it.
    /// </remarks>
    private sealed class Catch(CatchSignal<T> parent, IObserver<T> observer, IDisposable cancel) : IObserver<T>, IDisposable
    {
        /// <summary>The signal supplying the sources.</summary>
        private readonly CatchSignal<T> _parent = parent;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>Guards the enumerator hand-off and the teardown flags; never held while user code runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>The slot holding the current source subscription; an assignment after disposal is disposed at once.</summary>
        private readonly SingleReplaceableDisposable _subscription = new();

        /// <summary>The outer subscription handle released on teardown.</summary>
        private IDisposable? _cancel = cancel;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Set under <see cref="_gate"/> once teardown ran, so no further source is subscribed.</summary>
        private bool _isDisposed;

        /// <summary>Set under <see cref="_gate"/> while the walker advances the enumerator, so teardown leaves its disposal to the walker.</summary>
        private bool _isAdvancing;

        /// <summary>The enumerator over the sources.</summary>
        private IEnumerator<IObservable<T>>? _e;

        /// <summary>The error raised by the most recent source.</summary>
        private Exception? _lastException;

        /// <summary>The recursive continuation that advances to the next source.</summary>
        private Action? _nextSelf;

        /// <summary>Starts the walk on the immediate sequencer.</summary>
        /// <returns>The disposable that releases the enumerator and the current source subscription.</returns>
        public MultipleDisposable Run()
        {
            _e = _parent._sources.GetEnumerator();

            var schedule = Sequencer.Immediate.Schedule(RecursiveRun);

            return new(schedule, _subscription, new ActionDisposable(TearDown));
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
            TearDown();
            _subscription.Dispose();
            _ = WitnessTeardown.Dispose(ref _disposed, ref _cancel);
        }

        /// <summary>Advances the enumerator to the next source, recording the exception it raised when it raised one.</summary>
        /// <param name="enumerator">The enumerator to advance.</param>
        /// <param name="next">The next source, or <see langword="null"/> once the sequence is exhausted.</param>
        /// <param name="error">The exception the sequence raised, when it raised one.</param>
        /// <returns><see langword="true"/> when the sequence advanced without raising.</returns>
        private static bool TryMoveToNextSource(IEnumerator<IObservable<T>> enumerator, out IObservable<T>? next, out Exception? error)
        {
            next = null;
            error = null;

            try
            {
                if (!enumerator.MoveNext())
                {
                    enumerator.Dispose();
                    return true;
                }

                next = enumerator.Current;
                if (next is not null)
                {
                    return true;
                }

                error = new InvalidOperationException("sequence is null.");
            }
            catch (Exception exception)
            {
                error = exception;
            }

            enumerator.Dispose();
            return false;
        }

        /// <summary>Subscribes to the next source, or terminates once the sequence is exhausted.</summary>
        /// <param name="self">The continuation that re-enters this method for the following source.</param>
        private void RecursiveRun(Action self)
        {
            IEnumerator<IObservable<T>>? enumerator;
            lock (_gate)
            {
                _nextSelf = self;
                if (_isDisposed)
                {
                    return;
                }

                enumerator = _e;
                _isAdvancing = true;
            }

            var advanced = TryMoveToNextSource(enumerator!, out var next, out var error);

            bool tornDown;
            lock (_gate)
            {
                _isAdvancing = false;
                tornDown = _isDisposed;
            }

            if (tornDown)
            {
                ReleaseEnumerator();
                return;
            }

            if (!advanced)
            {
                FailAndDispose(error!);
                return;
            }

            if (next is null)
            {
                FinishAndDispose();
                return;
            }

            _subscription.Create(new SingleDisposable(next.Subscribe(this)));
        }

        /// <summary>Stops the walk, disposing the enumerator unless the walker is advancing it.</summary>
        private void TearDown()
        {
            lock (_gate)
            {
                _isDisposed = true;
                if (_isAdvancing)
                {
                    return;
                }
            }

            ReleaseEnumerator();
        }

        /// <summary>Takes the enumerator under the gate and disposes it outside the gate.</summary>
        private void ReleaseEnumerator()
        {
            IEnumerator<IObservable<T>>? enumerator;
            lock (_gate)
            {
                enumerator = _e;
                _e = null;
            }

            enumerator?.Dispose();
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
