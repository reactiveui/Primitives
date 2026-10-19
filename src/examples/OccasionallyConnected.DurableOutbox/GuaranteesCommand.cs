// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Prints delivery guarantee examples.</summary>
internal sealed record GuaranteesCommand : IOutboxCommand
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<OutboxCommandResult> ExecuteAsync(DurableOutboxApplication application, CancellationToken cancellationToken) =>
        ValueTask.FromResult(DurableOutboxApplication.ExplainGuarantees());
}
