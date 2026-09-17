// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Materializes the custom activity stream state and event from an accepted operation.</summary>
[System.Diagnostics.DebuggerDisplay("Activity domain handler")]
public sealed class ActivityDomainHandler : IServerDomainHandler
{
    /// <inheritdoc/>
    public ValueTask<ServerDomainApplyResult> ApplyAsync(
        ServerDomainApplyContext context,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(context);
        var canonical = ActivityPayloads.GetResolvedCanonical(context);
        var eventId = ActivityPayloads.CreateDeterministicEventId(context.Client.ClientId, context.Operation);
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["example-domain"] = "activity",
            ["authenticated-client"] = context.Client.ClientId,
            ["server-version"] = context.Resolution.ServerVersion,
        };

        return ValueTask.FromResult(new ServerDomainApplyResult
        {
            NewState = new(context.Operation.StreamId, context.Resolution.ServerVersion, canonical),
            Events = [new() { EventId = eventId, Payload = canonical, Metadata = metadata }],
        });
    }
}
