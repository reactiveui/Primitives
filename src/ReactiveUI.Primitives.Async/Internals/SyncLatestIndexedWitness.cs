// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// Per-source <see cref="WitnessAsync{T}"/> used by every <c>CombineLatestN</c> subscription. The
/// per-arity class previously declared N hand-rolled <c>OnNextN</c> / <c>OnCompletedN</c> method
/// pairs whose bodies differed only in which <c>Optional&lt;TN&gt;</c> field they wrote and which
/// completion bit they passed to the lifecycle. Pre-building N of these witnesses at subscription
/// time keeps the typing exact and eliminates the per-source method declarations from the per-arity
/// files. The closure cost (one delegate per source for the value-write) is paid once at subscribe
/// and not per emission; the actual per-emission cost is one indirect delegate invoke under the
/// values-lock.
/// </summary>
/// <typeparam name="TSource">The element type of the upstream source this witness subscribes to.</typeparam>
/// <typeparam name="TResult">The downstream element type owned by the parent subscription.</typeparam>
/// <param name="parent">The parent subscription that owns the values-lock and lifecycle.</param>
/// <param name="sourceBit">The completion bitmask bit owned by this source (1 &lt;&lt; index).</param>
/// <param name="recordValue">Stores the freshly-emitted value into the parent's typed <c>_valN</c> slot.</param>
internal sealed class SyncLatestIndexedWitness<TSource, TResult>(
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
