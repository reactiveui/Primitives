// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Runs the deterministic demonstration.</summary>
internal sealed record DemoCommand : IOutboxCommand
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<OutboxCommandResult> ExecuteAsync(DurableOutboxApplication application, CancellationToken cancellationToken) =>
        application.RunDemoAsync(cancellationToken);
}
