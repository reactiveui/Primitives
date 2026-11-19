// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Replaces a failed source with the fallback sequence without forwarding the source error.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ResumeSignal: Source = {_source}, Fallback = {_fallback}")]
public sealed class ResumeSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The fallback observable subscribed to after the source errors.</summary>
    private readonly IObservable<T> _fallback;

    /// <summary>Initializes a new instance of the <see cref="ResumeSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="fallback">The fallback observable subscribed to after the source errors.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="fallback"/> is <see langword="null"/>.</exception>
    public ResumeSignal(IObservable<T> source, IObservable<T> fallback)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(fallback);

        _source = source;
        _fallback = fallback;
    }

    /// <summary>Creates a fallback sequence, eliding the operator when <paramref name="source"/> is known never to error.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="fallback">The fallback observable subscribed to after the source errors.</param>
    /// <returns>The fallback-protected sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="fallback"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, IObservable<T> fallback)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(fallback);

        return source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T> or RangeSignal or ImmediateReturnSignal<T>
            ? source
            : new ResumeSignal<T>(source, fallback);
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
    private ResumeWitness Run(IObserver<T> observer) => new ResumeWitness(observer, _fallback).Run(_source);

    /// <summary>Forwards source values and, on any error, switches to the fallback sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="fallback">The fallback observable.</param>
    private sealed class ResumeWitness(IObserver<T> observer, IObservable<T> fallback) : IObserver<T>, IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>The fallback observable.</summary>
        private readonly IObservable<T> _fallback = fallback;

        /// <summary>The source subscription slot.</summary>
        private IDisposable? _sourceSubscription;

        /// <summary>The fallback subscription slot, populated after an error.</summary>
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
            SetFallback(_fallback.Subscribe(_observer));
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
        internal ResumeWitness Run(IObservable<T> source)
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
