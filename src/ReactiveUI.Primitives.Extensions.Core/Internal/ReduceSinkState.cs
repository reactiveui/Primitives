// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>Stores synchronized reduction state and coordinates terminal notifications.</summary>
/// <typeparam name="TIn">The source element type; the struct constraint lets <c>TIn?</c> record whether a value has arrived.</typeparam>
/// <typeparam name="TOut">The downstream element type the operator emits after reducing.</typeparam>
/// <remarks>Reductions and terminals are queued in order under the gate and delivered after it is released.</remarks>
internal sealed class ReduceSinkState<TIn, TOut>
    where TIn : struct
{
    /// <summary>The synchronization gate held across every state read and write; never held while the observer runs.</summary>
    private readonly Lock _gate = new();

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<TOut> _delivery = new();

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

    /// <summary>Records the source value and emits the reduction once every source has a value.</summary>
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

            _ = _delivery.Post(reduce(Values));
        }

        Flush();
    }

    /// <summary>Forwards the first terminal error and marks the sink terminal.</summary>
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
            _ = _delivery.PostError(error);
        }

        Flush();
    }

    /// <summary>Records source completion, terminating when all sources complete or one completes without a value.</summary>
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

            if (CompletedCount != Values.Length && Values[index].HasValue)
            {
                return;
            }

            IsDone = true;
            _ = _delivery.PostCompleted();
        }

        Flush();
    }

    /// <summary>Delivers the queued notifications on the calling thread, or hands them to the thread already delivering.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Flush() => _delivery.Flush(new PendingDrain(this));

    /// <summary>Drains this state's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The state.</param>
    private readonly record struct PendingDrain(ReduceSinkState<TIn, TOut> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner.Downstream);
    }
}
