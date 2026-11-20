// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits whether any source value matches a predicate.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AnyPredicateSignal: Source = {_source}")]
public sealed class AnyPredicateSignal<T> : IRequireCurrentThread<bool>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The predicate.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="AnyPredicateSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public AnyPredicateSignal(IObservable<T> source, Func<T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        _source = source;
        _predicate = predicate;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is RangeSignal range && typeof(T) == typeof(int))
        {
            EmitAnyRange(range, _predicate, observer);
            return EmptyDisposable.Instance;
        }

        return !IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired
            ? SubscribeCore(observer)
            : CurrentThreadSequencer.RunTrampoline(
                (Self: this, observer),
                static s => s.Self.SubscribeCore(s.observer));
    }

    /// <summary>Evaluates a predicate directly over a range source and emits the any result.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="predicate">The predicate.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitAnyRange(RangeSignal range, Func<T, bool> predicate, IObserver<bool> observer)
    {
        try
        {
            var typedPredicate = (Func<int, bool>)(object)predicate;
            for (var i = 0; i < range.Count; i++)
            {
                if (!typedPredicate(range.Start + i))
                {
                    continue;
                }

                observer.OnNext(true);
                observer.OnCompleted();
                return;
            }

            observer.OnNext(false);
            observer.OnCompleted();
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            observer.OnError(error);
        }
    }

    /// <summary>Subscribes the predicated any sink to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The sink that owns the upstream subscription.</returns>
    private AnyPredicateWitness<T> SubscribeCore(IObserver<bool> observer)
    {
        AnyPredicateWitness<T> sink = new(observer, _predicate);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
