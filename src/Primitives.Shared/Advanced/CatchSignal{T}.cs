// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Represents the CatchSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class CatchSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly IEnumerable<IObservable<T>> _sources;

    /// <summary>Initializes a new instance of the <see cref="CatchSignal{T}"/> class.</summary>
    /// <param name="sources">The sources value.</param>
    public CatchSignal(IEnumerable<IObservable<T>> sources) => _sources = sources;

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, true, SubscribeCore);

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new Catch(this, observer, cancel).Run();

    /// <summary>Represents the Catch class.</summary>
    private sealed class Catch : IObserver<T>, IDisposable
    {
        /// <summary>Stores state for the signal implementation.</summary>
        private readonly CatchSignal<T> _parent;

        /// <summary>Stores the downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Executes the new operation.</summary>
        /// <returns>The result.</returns>
        private readonly Lock _gate = new();

        /// <summary>Stores the upstream subscription.</summary>
        private IDisposable? _cancel;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Stores state for the signal implementation.</summary>
        private bool _isDisposed;

        /// <summary>Stores state for the signal implementation.</summary>
        private IEnumerator<IObservable<T>>? _e;

        /// <summary>Stores state for the signal implementation.</summary>
        private SingleReplaceableDisposable? _subscription;

        /// <summary>Stores state for the signal implementation.</summary>
        private Exception? _lastException;

        /// <summary>Stores state for the signal implementation.</summary>
        private Action? _nextSelf;

        /// <summary>Initializes a new instance of the <see cref="Catch"/> class.</summary>
        /// <param name="parent">The parent value.</param>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        /// <exception cref="ArgumentNullException"><paramref name="cancel"/> is <see langword="null"/>.</exception>
        public Catch(CatchSignal<T> parent, IObserver<T> observer, IDisposable cancel)
        {
            _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
            _observer = observer;
            _parent = parent;
        }

        /// <summary>Executes the Run operation.</summary>
        /// <returns>The result.</returns>
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

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _observer.OnNext(value);

        /// <summary>Executes the OnError operation.</summary>
        /// <param name="error">The error value.</param>
        public void OnError(Exception error)
        {
            _lastException = error;
            _nextSelf!();
        }

        /// <summary>Executes the OnCompleted operation.</summary>
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

        /// <summary>Executes the Dispose operation.</summary>
        public void Dispose()
        {
            _e?.Dispose();
            _e = null;
            _subscription?.Dispose();
            _subscription = null;
            _ = WitnessTeardown.Dispose(ref _disposed, ref _cancel);
        }

        /// <summary>Executes the RecursiveRun operation.</summary>
        /// <param name="self">The self value.</param>
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

        /// <summary>Advances the handler sequence to the next source. Call while holding the gate.</summary>
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

        /// <summary>Forwards an error downstream and tears the handler down.</summary>
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
