// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Builds receive acknowledgement case results for the CRDT loopback scenario.</summary>
internal static class CrdtLoopbackAckReportBuilder
{
    /// <summary>The acknowledged outcome.</summary>
    internal const string Acknowledged = "acknowledged";

    /// <summary>The not-acknowledged outcome.</summary>
    internal const string NotAcknowledged = "not-acknowledged";

    /// <summary>The client A ACK case name.</summary>
    internal const string ClientACaseName = "receive.client-a-acknowledges-authoritative-frontier";

    /// <summary>The client B ACK case name.</summary>
    internal const string ClientBCaseName = "receive.client-b-acknowledges-authoritative-frontier";

    /// <summary>Builds both receive ACK case results.</summary>
    /// <param name="clientAAcknowledged">Whether client A acknowledged its authoritative frontier.</param>
    /// <param name="clientBAcknowledged">Whether client B acknowledged its authoritative frontier.</param>
    /// <returns>The receive ACK case results.</returns>
    internal static IReadOnlyList<ResilienceLabCaseResult> BuildCases(
        bool clientAAcknowledged,
        bool clientBAcknowledged) =>
        [
            CreateCase(ClientACaseName, clientAAcknowledged),
            CreateCase(ClientBCaseName, clientBAcknowledged),
        ];

    /// <summary>Creates one receive ACK case result.</summary>
    /// <param name="name">The case name.</param>
    /// <param name="acknowledged">Whether the client acknowledged its authoritative frontier.</param>
    /// <returns>The receive ACK case result.</returns>
    private static ResilienceLabCaseResult CreateCase(string name, bool acknowledged) =>
        new(name, Acknowledged, acknowledged ? Acknowledged : NotAcknowledged, acknowledged);
}
