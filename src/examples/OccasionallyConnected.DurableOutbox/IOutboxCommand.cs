// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Runs one durable-outbox example command.</summary>
internal interface IOutboxCommand
{
    /// <summary>Executes the command.</summary>
    /// <param name="application">The application services.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    ValueTask<OutboxCommandResult> ExecuteAsync(DurableOutboxApplication application, CancellationToken cancellationToken);
}
