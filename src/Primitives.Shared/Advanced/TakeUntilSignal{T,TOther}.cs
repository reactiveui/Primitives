// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>TakeUntil</c>, holding both the source and the stop arm.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <typeparam name="TOther">The cancellation value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TakeUntilSignal: Source = {_source}")]
public sealed class TakeUntilSignal<T, TOther> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The observable that stops the source when it emits.</summary>
    private readonly IObservable<TOther> _other;

    /// <summary>Initializes a new instance of the <see cref="TakeUntilSignal{T, TOther}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="other">The observable that stops the source when it emits.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="other"/> is <see langword="null"/>.</exception>
    public TakeUntilSignal(IObservable<T> source, IObservable<TOther> other)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(other);

        _source = source;
        _other = other;
    }

    /// <summary>Creates a <c>TakeUntil</c> sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="other">The observable that stops the source when it emits.</param>
    /// <returns>The bounded observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="other"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, IObservable<TOther> other)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(other);

        return other is ImmutableEmptySignal<TOther> or ImmutableNeverSignal<TOther>
            && source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T> or RangeSignal or ImmediateReturnSignal<T>
            ? source
            : new TakeUntilSignal<T, TOther>(source, other);
    }

    /// <summary>Forwards <paramref name="source"/> values until <paramref name="cancellationToken"/> is canceled.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="cancellationToken">The token whose cancellation completes the sequence.</param>
    /// <returns>The cancellation-bounded observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> CreateWithCancellation(IObservable<T> source, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (cancellationToken.IsCancellationRequested)
        {
            return ImmutableEmptySignal<T>.Instance;
        }

        return cancellationToken.CanBeCanceled && source is not ImmutableEmptySignal<T>
            ? new TakeUntilSignal<T, RxVoid>(source, new CancellationSignal(cancellationToken))
            : source;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return (!CurrentThreadRequirement.IsRequired(_source) && !CurrentThreadRequirement.IsRequired(_other))
            || !CurrentThreadSequencer.IsScheduleRequired
            ? SubscribeCore(observer)
            : CurrentThreadSequencer.RunTrampoline(
                (Self: this, observer),
                static s => s.Self.SubscribeCore(s.observer));
    }

    /// <summary>Subscribes the stop arm and, unless it has fired, the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The coordinator that owns both subscriptions.</returns>
    private TakeUntilCoordinator<T> SubscribeCore(IObserver<T> observer)
    {
        TakeUntilCoordinator<T> coordinator = new(observer);
        coordinator.Add(_other.Subscribe(new TakeUntilOtherWitness<T, TOther>(coordinator)));
        if (coordinator.IsStopped)
        {
            return coordinator;
        }

        coordinator.Add(_source.Subscribe(new TakeUntilSourceWitness<T>(coordinator)));
        return coordinator;
    }
}
