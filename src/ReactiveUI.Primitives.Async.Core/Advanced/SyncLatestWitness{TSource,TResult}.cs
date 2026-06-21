// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Per-source observer used by arity-specific <c>SyncLatest</c> coordinators to record typed source values
/// and forward source terminal notifications to the shared lifecycle.
/// </summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The downstream element type.</typeparam>
/// <param name="parent">The parent coordinator.</param>
/// <param name="sourceBit">The completion bit owned by the source.</param>
/// <param name="recordValue">Stores the latest source value in the parent coordinator.</param>
public sealed class SyncLatestWitness<TSource, TResult>(
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
