// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>CollectList</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CollectListSignal: Source = {_source}")]
public sealed class CollectListSignal<T> : IObservable<IList<T>>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Initializes a new instance of the <see cref="CollectListSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public CollectListSignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <summary>Creates a list-collecting signal, taking the range fast path when <paramref name="source"/> is a <see cref="RangeSignal"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A sequence that emits a single list containing all source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<IList<T>> Create(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source is RangeSignal range && typeof(T) == typeof(int)
            ? new RangeListSignal<T>(range)
            : new CollectListSignal<T>(source);
    }

    /// <summary>Collects all values from <paramref name="source"/> into a list task.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A task that completes with all source values in a list.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<IList<T>> CollectAsync(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (source is RangeSignal range && typeof(T) == typeof(int))
        {
            List<int> integers = [with(capacity: range.Count)];
            for (var i = 0; i < range.Count; i++)
            {
                integers.Add(range.Start + i);
            }

            return Task.FromResult((IList<T>)(object)integers);
        }

        TaskCompletionSource<IList<T>> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<T> values = [];
        _ = source.Subscribe(
            values.Add,
            error => completion.TrySetException(error),
            () => completion.TrySetResult(values));
        return completion.Task;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IList<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        CollectListWitness<T> sink = new(observer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
