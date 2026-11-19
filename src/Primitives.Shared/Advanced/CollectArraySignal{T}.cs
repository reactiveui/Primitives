// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>CollectArray</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CollectArraySignal: Source = {_source}")]
public sealed class CollectArraySignal<T> : IObservable<T[]>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Initializes a new instance of the <see cref="CollectArraySignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public CollectArraySignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <summary>Creates an array-collecting signal, taking the range fast path when <paramref name="source"/> is a <see cref="RangeSignal"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A sequence that emits a single array containing all source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T[]> Create(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source is RangeSignal range && typeof(T) == typeof(int)
            ? new RangeArraySignal<T>(range)
            : new CollectArraySignal<T>(source);
    }

    /// <summary>Collects all values from <paramref name="source"/> into an array task.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A task that completes with all source values in an array.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T[]> CollectAsync(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (source is RangeSignal range && typeof(T) == typeof(int))
        {
            var integers = new int[range.Count];
            for (var i = 0; i < integers.Length; i++)
            {
                integers[i] = range.Start + i;
            }

            return Task.FromResult((T[])(object)integers);
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        if (TryCollectArrayFromAsyncEnumerable(source, out var asyncEnumerableTask))
        {
            return asyncEnumerableTask;
        }
#endif

        TaskCompletionSource<T[]> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<T> values = [];
        _ = source.Subscribe(
            values.Add,
            error => completion.TrySetException(error),
            () => completion.TrySetResult([.. values]));
        return completion.Task;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T[]> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        CollectArrayWitness<T> sink = new(observer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
    /// <summary>Tries to collect values from an async-enumerable-backed signal into an array task.</summary>
    /// <param name="source">The observable source to inspect.</param>
    /// <param name="task">The collection task when the source is async-enumerable-backed.</param>
    /// <returns><see langword="true"/> when an async enumerable fast path is available; otherwise, <see langword="false"/>.</returns>
    private static bool TryCollectArrayFromAsyncEnumerable(IObservable<T> source, [NotNullWhen(true)] out Task<T[]>? task)
    {
        if (source is IAsyncEnumerableBackedSignal<T> asyncEnumerable)
        {
            task = CollectAsyncEnumerableArrayAsync(asyncEnumerable.Values, asyncEnumerable.CancellationToken);
            return true;
        }

        task = null;
        return false;
    }

    /// <summary>Collects an async enumerable into a densely packed array.</summary>
    /// <param name="values">The async enumerable values to collect.</param>
    /// <param name="cancellationToken">The token used to cancel enumeration.</param>
    /// <returns>A task that resolves to the collected array.</returns>
    private static async Task<T[]> CollectAsyncEnumerableArrayAsync(
        IAsyncEnumerable<T> values,
        CancellationToken cancellationToken)
    {
        const int InitialCapacity = 16;
        const int GrowthFactor = 2;
        var array = new T[InitialCapacity];
        var count = 0;
        var enumerator = values.GetAsyncEnumerator(cancellationToken);
        try
        {
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                if (count == array.Length)
                {
                    Array.Resize(ref array, array.Length * GrowthFactor);
                }

                array[count] = enumerator.Current;
                count++;
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }

        if (count == array.Length)
        {
            return array;
        }

        var result = new T[count];
        Array.Copy(array, result, count);

        return result;
    }
#endif
}
