// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that forwards only the source values of a given type.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("KeepTypeSignal: Source = {_source}")]
public sealed class KeepTypeSignal<TResult> : IObservable<TResult>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<object?> _source;

    /// <summary>Initializes a new instance of the <see cref="KeepTypeSignal{TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public KeepTypeSignal(IObservable<object?> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <summary>Applies <c>KeepType</c>/<c>OfType</c> reductions and fusions.</summary>
    /// <param name="source">The untyped source observable.</param>
    /// <returns>The reduced or fused observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Create(IObservable<object?> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source switch
        {
            ImmutableEmptySignal<object?> => ImmutableEmptySignal<TResult>.Instance,
            ImmutableNeverSignal<object?> => ImmutableNeverSignal<TResult>.Instance,
            IMapFusable<object?> map => map.FuseKeepType<TResult>(),
            _ => new KeepTypeSignal<TResult>(source),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is FromEnumerableSignal<object?> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitReadOnlyList(values, observer);
            return EmptyDisposable.Instance;
        }

        KeepTypeWitness<TResult> sink = new(observer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Filters matching values from an indexable synchronous list straight into <paramref name="observer"/>.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<object?> values, IObserver<TResult> observer)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (values[i] is TResult matched)
            {
                observer.OnNext(matched);
            }
        }

        observer.OnCompleted();
    }
}
