// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Combines the latest values from multiple sources and emits either the maximum or minimum on each
/// tick. Backs both the <c>Max</c> (<paramref name="emitMaximum"/>=true) and <c>Min</c>
/// (<paramref name="emitMaximum"/>=false) operators without the array allocations a generic
/// <c>CombineLatest(...).Select(xs =&gt; xs.Max())</c> pipeline would incur.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="sources">The source observables.</param>
/// <param name="emitMaximum"><c>true</c> to emit the maximum; <c>false</c> to emit the minimum.</param>
internal sealed class MinMaxObservable<T>(IReadOnlyList<IObservable<T>> sources, bool emitMaximum) : IObservable<T>
    where T : struct, IComparable<T>
{
    /// <summary>The source list.</summary>
    private readonly IReadOnlyList<IObservable<T>> _sourceList = InvalidOperationExceptionHelper.Check(sources);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_sourceList.Count == 0)
        {
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        var sink = new Sink(observer, _sourceList.Count, emitMaximum);
        return IndexedSubscribeHelper.SubscribeIndexed(_sourceList, sink.OnNext, sink.OnError, sink.OnCompleted);
    }

    /// <summary>
    /// Sink that holds the latest value per source and emits either the max or the min. Composes
    /// <see cref="ReduceSinkState{TIn, TOut}"/> for the shared plumbing.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="count">The number of sources.</param>
    /// <param name="emitMaximum"><c>true</c> for max; <c>false</c> for min.</param>
    private sealed class Sink(IObserver<T> downstream, int count, bool emitMaximum)
    {
        /// <summary>Shared gate / value cache / terminal-state plumbing.</summary>
        private readonly ReduceSinkState<T, T> _state = new(downstream, count);

        /// <summary>Handles OnNext from a source.</summary>
        /// <param name="index">Source index.</param>
        /// <param name="value">Emitted value.</param>
        public void OnNext(int index, T value)
        {
            lock (_state.Gate)
            {
                if (_state.IsDone)
                {
                    return;
                }

                if (!_state.Values[index].HasValue)
                {
                    _state.HasValueCount++;
                }

                _state.Values[index] = value;

                if (!_state.AllValuesPresent)
                {
                    return;
                }

                var result = _state.Values[0]!.Value;
                for (var i = 1; i < _state.Values.Length; i++)
                {
                    var current = _state.Values[i]!.Value;
                    var cmp = current.CompareTo(result);
                    if (emitMaximum ? cmp > 0 : cmp < 0)
                    {
                        result = current;
                    }
                }

                _state.Downstream.OnNext(result);
            }
        }

        /// <summary>Handles OnError from any source.</summary>
        /// <param name="error">The error.</param>
        public void OnError(Exception error) => _state.HandleError(error);

        /// <summary>Handles OnCompleted from a source.</summary>
        /// <param name="index">Source index.</param>
        public void OnCompleted(int index) => _state.HandleCompleted(index);
    }
}
