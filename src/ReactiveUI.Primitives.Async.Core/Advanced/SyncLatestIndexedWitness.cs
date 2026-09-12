// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Per-source <see cref="WitnessAsync{T}"/> for a <c>CombineLatestN</c> subscription: records each
/// value into the parent's typed slot under the values-lock, then asks the parent to emit. One
/// instance is built per source at subscribe time, so the per-emission cost is a single delegate
/// invoke.
/// </summary>
/// <typeparam name="TSource">The element type of the upstream source this witness subscribes to.</typeparam>
/// <typeparam name="TResult">The downstream element type owned by the parent subscription.</typeparam>
/// <param name="parent">The parent subscription that owns the values-lock and lifecycle.</param>
/// <param name="sourceBit">The completion bitmask bit owned by this source (1 &lt;&lt; index).</param>
/// <param name="recordValue">Stores the emitted value into the parent's typed slot for this source.</param>
public sealed class SyncLatestIndexedWitness<TSource, TResult>(
    SyncLatestCoordinatorBase<TResult> parent,
    int sourceBit,
    Action<TSource> recordValue) : WitnessAsync<TSource>
{
    /// <inheritdoc/>
    protected override async ValueTask OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        lock (parent.ValuesLock)
        {
            recordValue(value);
        }

        await parent.EmitLatestAsync().ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return parent.Lifecycle.OnErrorResumeAsync(error);
    }

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) =>
        parent.Lifecycle.OnSourceCompletedAsync(result, sourceBit);
}
