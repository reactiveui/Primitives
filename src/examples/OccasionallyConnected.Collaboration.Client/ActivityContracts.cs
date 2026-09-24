// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Defines the activity contract shared with the collaboration server example.</summary>
internal static class ActivityContracts
{
    /// <summary>Gets the activity stream served by the collaboration server example.</summary>
    public static StreamId StreamId { get; } = new("collaboration/activity");

    /// <summary>Gets the custom activity payload contract identifier.</summary>
    public static string ContractId => "example.collaboration.activity";

    /// <summary>Gets the custom activity payload content type.</summary>
    public static string ContentType => "application/vnd.reactiveui.oc.example.activity+json";

    /// <summary>Gets the schema version supported by the example.</summary>
    public static int SchemaVersion => 1;
}
