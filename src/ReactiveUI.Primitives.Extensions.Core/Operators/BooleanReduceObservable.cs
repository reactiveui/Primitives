// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Emits whether every source's latest boolean equals <paramref name="target"/>, re-evaluating on each value once all
/// sources have produced one. An empty source list emits <c>true</c> and completes on subscribe; any source's error
/// terminates the sequence, and the sequence completes when every source completes or one completes without emitting.
/// </summary>
/// <param name="sources">The source observables.</param>
/// <param name="target">The value every source must hold for the operator to emit <c>true</c>.</param>
[System.Diagnostics.DebuggerDisplay("BooleanReduceObservable: Sources = {_sourceList}")]
public sealed class BooleanReduceObservable(IEnumerable<IObservable<bool>> sources, bool target) : IObservable<bool>
{
    /// <summary>Capacity the materialization buffer starts at when the source count is not known up front.</summary>
    private const int InitialBufferCapacity = 4;

    /// <summary>Factor the materialization buffer grows by when it fills.</summary>
    private const int BufferGrowthFactor = 2;

    /// <summary>The source list.</summary>
    private readonly IReadOnlyList<IObservable<bool>> _sourceList = MaterializeSources(sources);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_sourceList.Count == 0)
        {
            observer.OnNext(true);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        Sink sink = new(observer, _sourceList.Count, target);
        return IndexedSubscribeHelper.SubscribeIndexed(_sourceList, sink.OnNext, sink.OnError, sink.OnCompleted);
    }

    /// <summary>Copies the sources into an indexable list, enumerating the sequence at most once.</summary>
    /// <param name="sources">The sources to materialize.</param>
    /// <returns>The source list.</returns>
    private static IReadOnlyList<IObservable<bool>> MaterializeSources(IEnumerable<IObservable<bool>> sources)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(sources);

        if (sources is IReadOnlyList<IObservable<bool>> readOnlyList)
        {
            return readOnlyList;
        }

        if (sources is ICollection<IObservable<bool>> collection)
        {
            var materialized = new IObservable<bool>[collection.Count];
            collection.CopyTo(materialized, 0);
            return materialized;
        }

        IObservable<bool>[] buffer = [];
        var count = 0;
        foreach (var source in sources)
        {
            if (count == buffer.Length)
            {
                var grown = new IObservable<bool>[buffer.Length == 0 ? InitialBufferCapacity : buffer.Length * BufferGrowthFactor];
                Array.Copy(buffer, grown, count);
                buffer = grown;
            }

            buffer[count] = source;
            count++;
        }

        if (count == buffer.Length)
        {
            return buffer;
        }

        var trimmed = new IObservable<bool>[count];
        Array.Copy(buffer, trimmed, count);
        return trimmed;
    }

    /// <summary>Compares each source's latest value with the target using shared value and terminal state.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="count">The number of sources.</param>
    /// <param name="target">The value every source must hold for emit to be <c>true</c>.</param>
    private sealed class Sink(IObserver<bool> downstream, int count, bool target)
    {
        /// <summary>The shared gate, per-source value cache and terminal-state bookkeeping.</summary>
        private readonly ReduceSinkState<bool, bool> _state = new(downstream, count);

        /// <summary>Reduces the per-source latest values to whether every source holds the target value.</summary>
        private readonly Func<bool?[], bool> _reduce = values =>
        {
            for (var i = 0; i < values.Length; i++)
            {
                if (values[i] != target)
                {
                    return false;
                }
            }

            return true;
        };

        /// <summary>Records one source's latest value and re-evaluates the combined result.</summary>
        /// <param name="index">Source index.</param>
        /// <param name="value">Emitted value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(int index, bool value) => _state.HandleNext(index, value, _reduce);

        /// <summary>Forwards an error from any source downstream and terminates the sink.</summary>
        /// <param name="error">The error.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _state.HandleError(error);

        /// <summary>Records one source's completion and completes downstream when the sequence is finished.</summary>
        /// <param name="index">Source index.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(int index) => _state.HandleCompleted(index);
    }
}
