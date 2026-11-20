// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observable sink that casts object values to the requested result type.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CastSignal: Source = {Source}")]
public sealed class CastSignal<TResult> : IRequireCurrentThread<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="CastSignal{TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    public CastSignal(IObservable<object?> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        Source = source;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<object?> Source { get; }

    /// <summary>Applies <c>CastTo</c>/<c>Cast</c> reductions and fusions.</summary>
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
            IMapFusable<object?> map => map.FuseCast<TResult>(),
            _ => new CastSignal<TResult>(source),
        };
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        Source is IRequireCurrentThread<object?> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (Source is FromEnumerableSignal<object?> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitReadOnlyList(values, observer);
            return EmptyDisposable.Instance;
        }

        CastWitness<TResult> castObserver = new(observer);
        castObserver.SetSubscription(Source.Subscribe(castObserver));
        return castObserver;
    }

    /// <summary>Casts values from an indexable synchronous list straight into <paramref name="observer"/>.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<object?> values, IObserver<TResult> observer)
    {
        for (var i = 0; i < values.Count; i++)
        {
            TResult result;
            var element = values[i];
            try
            {
                result = (TResult)element!;
            }
            catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }

            observer.OnNext(result);
        }

        observer.OnCompleted();
    }
}
