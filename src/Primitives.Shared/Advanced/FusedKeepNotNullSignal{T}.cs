// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Fusable non-null filter signal returned by <c>KeepNotNull</c> and <c>WhereNotNull</c>.</summary>
/// <typeparam name="T">The non-null reference type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FusedKeepNotNullSignal: Source = {_source}")]
public sealed class FusedKeepNotNullSignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>, ISwitchSelectFusable<T>
    where T : class
{
    /// <summary>The nullable source sequence.</summary>
    private readonly IObservable<T?> _source;

    /// <summary>Initializes a new instance of the <see cref="FusedKeepNotNullSignal{T}"/> class.</summary>
    /// <param name="source">The nullable source sequence.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public FusedKeepNotNullSignal(IObservable<T?> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <summary>Applies <c>KeepNotNull</c>/<c>WhereNotNull</c> reductions and fusions.</summary>
    /// <param name="source">The nullable source sequence.</param>
    /// <returns>The reduced or fused observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T?> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source switch
        {
            ImmutableEmptySignal<T?> => ImmutableEmptySignal<T>.Instance,
            ImmutableNeverSignal<T?> => ImmutableNeverSignal<T>.Instance,
            FusedKeepNotNullSignal<T> alreadyNonNull => alreadyNonNull,
            IMapFusable<T?> map => map.FuseKeepNotNull<T>(),
            _ => new FusedKeepNotNullSignal<T>(source),
        };
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T?> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    IObservable<TInner> ISwitchSelectFusable<T>.FuseSwitchSelect<TInner>(Func<T, IObservable<TInner>> selector) =>
        new SwitchMapSignal<T, TInner>(_source!, selector, skipNullSources: true);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is FromEnumerableSignal<T?> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitList(values, observer);
            return EmptyDisposable.Instance;
        }

        KeepNotNullWitness<T> sink = new(observer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);
        ArgumentExceptionHelper.ThrowIfNull(onError);
        ArgumentExceptionHelper.ThrowIfNull(onCompleted);

        if (_source is FromEnumerableSignal<T?> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            var count = values.Count;
            for (var i = 0; i < count; i++)
            {
                var value = values[i];
                if (value is not null)
                {
                    onNext(value);
                }
            }

            onCompleted();
            return EmptyDisposable.Instance;
        }

        return Subscribe(new EmptyWitness<T>(onNext, onError, onCompleted));
    }

    /// <summary>Forwards non-null elements from an indexable list.</summary>
    /// <param name="values">The source list.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitList(IReadOnlyList<T?> values, IObserver<T> observer)
    {
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (value is not null)
            {
                observer.OnNext(value);
            }
        }

        observer.OnCompleted();
    }
}
