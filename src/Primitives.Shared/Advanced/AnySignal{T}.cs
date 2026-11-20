// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits whether the source produced any value.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AnySignal: Source = {_source}")]
public sealed class AnySignal<T> : IRequireCurrentThread<bool>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Initializes a new instance of the <see cref="AnySignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public AnySignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <summary>Creates an any sequence, fusing a preceding <see cref="FusedKeepSignal{T}"/> into <see cref="AnyPredicateSignal{T}"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>The any observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<bool> Create(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source is FusedKeepSignal<T> keep
            ? new AnyPredicateSignal<T>(keep.Source, keep.Predicate)
            : new AnySignal<T>(source);
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is RangeSignal range)
        {
            observer.OnNext(range.Count > 0);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        return !IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired
            ? SubscribeCore(observer)
            : CurrentThreadSequencer.RunTrampoline(
                (Self: this, observer),
                static s => s.Self.SubscribeCore(s.observer));
    }

    /// <summary>Subscribes the any sink to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The sink that owns the upstream subscription.</returns>
    private AnyWitness<T> SubscribeCore(IObserver<bool> observer)
    {
        AnyWitness<T> sink = new(observer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
