// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Shared synchronous reduce-sink state used by <c>BooleanReduceObservable</c> (AllTrue / AllFalse)
/// and <c>MinMaxObservable</c> (Max / Min). Each per-operator sink composes one instance (has-a, not
/// is-a) and adds only its operator-specific OnNext reduce step; the boilerplate gate, value cache,
/// completion bookkeeping, OnError, and OnCompleted bodies all live here in one place.
/// </summary>
/// <typeparam name="TIn">The source element type (must be a struct so <c>TIn?</c> doubles as the
/// "value seen yet?" Optional).</typeparam>
/// <typeparam name="TOut">The downstream element type the operator emits after reducing.</typeparam>
internal sealed class ReduceSinkState<TIn, TOut>
    where TIn : struct
{
    /// <summary>The synchronization gate held across every state read/write and every downstream notification.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="ReduceSinkState{TIn, TOut}"/> class.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="count">The number of sources.</param>
    public ReduceSinkState(IObserver<TOut> downstream, int count)
    {
        Downstream = downstream;
        Values = new TIn?[count];
        Completed = new bool[count];
    }

    /// <summary>Gets the downstream observer that receives reduced values, error, and completion.</summary>
    internal IObserver<TOut> Downstream { get; }

    /// <summary>Gets the per-source latest values; index N is set on first OnNext from source N.</summary>
    internal TIn?[] Values { get; }

    /// <summary>Gets the per-source completion bookkeeping.</summary>
    internal bool[] Completed { get; }

    /// <summary>Gets or sets the number of sources that have produced at least one value.</summary>
    internal int HasValueCount { get; set; }

    /// <summary>Gets or sets the number of sources that have completed.</summary>
    internal int CompletedCount { get; set; }

    /// <summary>Gets or sets a value indicating whether the sink has reached its terminal state.</summary>
    internal bool IsDone { get; set; }

    /// <summary>Gets a value indicating whether every source has produced at least one value.</summary>
    internal bool AllValuesPresent => HasValueCount >= Values.Length;

    /// <summary>Records source <paramref name="index"/>'s latest value and emits the reduced result once every source has one. Runs under the gate.</summary>
    /// <param name="index">The 0-based source index that emitted.</param>
    /// <param name="value">The latest value from that source.</param>
    /// <param name="reduce">Projects the per-source latest values into the downstream result.</param>
    internal void HandleNext(int index, TIn value, Func<TIn?[], TOut> reduce)
    {
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            if (!Values[index].HasValue)
            {
                HasValueCount++;
            }

            Values[index] = value;

            if (!AllValuesPresent)
            {
                return;
            }

            Downstream.OnNext(reduce(Values));
        }
    }

    /// <summary>Forwards a terminal error to the downstream observer and marks the sink terminal. Idempotent.</summary>
    /// <param name="error">The error to forward.</param>
    internal void HandleError(Exception error)
    {
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            IsDone = true;
            Downstream.OnError(error);
        }
    }

    /// <summary>
    /// Records completion of the source at <paramref name="index"/>. The combined sequence terminates
    /// once every source has completed OR a source completes without ever having emitted a value.
    /// </summary>
    /// <param name="index">The 0-based source index that just completed.</param>
    internal void HandleCompleted(int index)
    {
        lock (_gate)
        {
            if (IsDone || Completed[index])
            {
                return;
            }

            Completed[index] = true;
            CompletedCount++;

            if (CompletedCount == Values.Length || !Values[index].HasValue)
            {
                IsDone = true;
                Downstream.OnCompleted();
            }
        }
    }
}
