// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Combines the latest boolean values from multiple sources and emits <c>true</c> iff every latest
/// value equals <paramref name="target"/>. Backs both <c>AllTrue</c> (target=true) and <c>AllFalse</c>
/// (target=false) without the array allocations a generic <c>CombineLatest(...).Select(xs =&gt; xs.All(...))</c>
/// pipeline would incur.
/// </summary>
/// <param name="sources">The source observables.</param>
/// <param name="target">The value every source must hold for the operator to emit <c>true</c>.</param>
internal sealed class BooleanReduceObservable(IEnumerable<IObservable<bool>> sources, bool target) : IObservable<bool>
{
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

    /// <summary>Materializes source enumeration once without using LINQ in shipping code.</summary>
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
                var grown = new IObservable<bool>[buffer.Length == 0 ? 4 : buffer.Length * 2];
                Array.Copy(buffer, grown, count);
                buffer = grown;
            }

            buffer[count++] = source;
        }

        if (count == buffer.Length)
        {
            return buffer;
        }

        var trimmed = new IObservable<bool>[count];
        Array.Copy(buffer, trimmed, count);
        return trimmed;
    }

    /// <summary>
    /// Sink that holds the latest value per source and reduces them against <paramref name="target"/>.
    /// Composes <see cref="ReduceSinkState{TIn, TOut}"/> for the shared gate / value cache / OnError /
    /// OnCompleted plumbing so this class carries only the per-operator reduce step.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="count">The number of sources.</param>
    /// <param name="target">The value every source must hold for emit to be <c>true</c>.</param>
    private sealed class Sink(IObserver<bool> downstream, int count, bool target)
    {
        /// <summary>Shared gate / value cache / terminal-state plumbing.</summary>
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

        /// <summary>Handles OnNext from a source.</summary>
        /// <param name="index">Source index.</param>
        /// <param name="value">Emitted value.</param>
        public void OnNext(int index, bool value) => _state.HandleNext(index, value, _reduce);

        /// <summary>Handles OnError from any source.</summary>
        /// <param name="error">The error.</param>
        public void OnError(Exception error) => _state.HandleError(error);

        /// <summary>Handles OnCompleted from a source.</summary>
        /// <param name="index">Source index.</param>
        public void OnCompleted(int index) => _state.HandleCompleted(index);
    }
}
