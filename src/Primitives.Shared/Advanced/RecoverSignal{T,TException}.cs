// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mirrors the source and switches to a handler-selected sequence on errors of the specified type.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TException">The handled exception type.</typeparam>
/// <remarks>Other error types and exceptions thrown by the handler propagate unchanged.</remarks>
[System.Diagnostics.DebuggerDisplay("RecoverSignal: Source = {_source}")]
public sealed class RecoverSignal<T, TException> : IRequireCurrentThread<T>
    where TException : Exception
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The handler that selects the fallback sequence for a caught error.</summary>
    private readonly Func<TException, IObservable<T>> _handler;

    /// <summary>Initializes a new instance of the <see cref="RecoverSignal{T, TException}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="handler">The handler that selects the fallback sequence for a caught error.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
    public RecoverSignal(IObservable<T> source, Func<TException, IObservable<T>> handler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(handler);

        _source = source;
        _handler = handler;
    }

    /// <summary>Creates a recovery sequence, eliding the operator when <paramref name="source"/> is known never to error.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="handler">The handler that selects the fallback sequence for a caught error.</param>
    /// <returns>The recovery sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, Func<TException, IObservable<T>> handler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(handler);

        return source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T> or RangeSignal or ImmediateReturnSignal<T>
            ? source
            : new RecoverSignal<T, TException>(source, handler);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return SubscriptionScheduling.OnCurrentThread(
            (Self: this, observer),
            static s => s.Self.Run(s.observer));
    }

    /// <summary>Builds the sink and subscribes it to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The sink, which is the subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private RecoverWitness Run(IObserver<T> observer) => new RecoverWitness(observer, _handler).Run(_source);

    /// <summary>Forwards source values and, on a caught error, switches to the fallback sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="handler">The handler that selects the fallback sequence.</param>
    private sealed class RecoverWitness(IObserver<T> observer, Func<TException, IObservable<T>> handler) : IObserver<T>, IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>The handler that selects the fallback sequence.</summary>
        private readonly Func<TException, IObservable<T>> _handler = handler;

        /// <summary>The source subscription slot.</summary>
        private IDisposable? _sourceSubscription;

        /// <summary>The fallback subscription slot, populated after a caught error.</summary>
        private IDisposable? _fallbackSubscription;

        /// <summary>Non-zero once the source phase has finished.</summary>
        private int _sourceDone;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _sourceDone) != 0)
            {
                return;
            }

            _observer.OnNext(value);
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _sourceDone, 1) != 0)
            {
                return;
            }

            SubscriptionSlots.Release(ref _sourceSubscription);
            if (error is TException typed)
            {
                IObservable<T> next;
                try
                {
                    next = _handler(typed);
                }
                catch (Exception handlerError) when (!FatalExceptionHelper.IsFatal(handlerError))
                {
                    try
                    {
                        _observer.OnError(handlerError);
                    }
                    finally
                    {
                        Dispose();
                    }

                    return;
                }

                SetFallback(next.Subscribe(_observer));
            }
            else
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
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _sourceDone, 1) != 0)
            {
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

        /// <inheritdoc/>
        public void Dispose()
        {
            Volatile.Write(ref _sourceDone, 1);
            SubscriptionSlots.Release(ref _sourceSubscription);
            SubscriptionSlots.Release(ref _fallbackSubscription);
        }

        /// <summary>Subscribes to the source and returns the sink.</summary>
        /// <param name="source">The source observable.</param>
        /// <returns>This sink, which is the subscription.</returns>
        internal RecoverWitness Run(IObservable<T> source)
        {
            SubscriptionSlots.Assign(ref _sourceSubscription, source.Subscribe(this));
            return this;
        }

        /// <summary>Stores the fallback subscription.</summary>
        /// <param name="subscription">The fallback subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetFallback(IDisposable subscription) =>
            SubscriptionSlots.Assign(ref _fallbackSubscription, subscription);
    }
}
