// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that forwards only the non-null source values.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("KeepNotNullSignal: Source = {_source}")]
public sealed class KeepNotNullSignal<T> : IObservable<T>
    where T : class
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T?> _source;

    /// <summary>Initializes a new instance of the <see cref="KeepNotNullSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public KeepNotNullSignal(IObservable<T?> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is FromEnumerableSignal<T?> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitReadOnlyList(values, observer);
            return EmptyDisposable.Instance;
        }

        KeepNotNullWitness<T> sink = new(observer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Filters non-null values from an indexable synchronous list straight into <paramref name="observer"/>.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<T?> values, IObserver<T> observer)
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
