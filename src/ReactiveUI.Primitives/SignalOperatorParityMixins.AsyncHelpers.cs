// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
// The async-enumerable fast path only exists on frameworks that support IAsyncEnumerable.
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives;

/// <summary>Async enumerable collection helpers for the operator surface.</summary>
public static partial class LinqExtensions
{
    private static bool TryCollectArrayFromAsyncEnumerable<T>(IObservable<T> source, [NotNullWhen(true)] out Task<T[]>? task)
    {
        if (source is IAsyncEnumerableBackedSignal<T> asyncEnumerable)
        {
            task = CollectAsyncEnumerableArrayAsync(asyncEnumerable.Values, asyncEnumerable.CancellationToken);
            return true;
        }

        task = null;
        return false;
    }

    private static async Task<T[]> CollectAsyncEnumerableArrayAsync<T>(IAsyncEnumerable<T> values, CancellationToken cancellationToken)
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

                array[count++] = enumerator.Current;
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
}
#endif
