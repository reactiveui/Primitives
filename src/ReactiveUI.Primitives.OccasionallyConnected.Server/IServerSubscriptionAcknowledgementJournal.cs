// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Persists trusted subscription bindings, offered receive pages and durable acknowledgements.</summary>
internal interface IServerSubscriptionAcknowledgementJournal
{
    /// <summary>Registers or reads a trusted subscription binding.</summary>
    /// <param name="identity">The subscription identity.</param>
    /// <returns>The persisted subscription state.</returns>
    ServerSubscriptionState RegisterSubscription(ServerSubscriptionIdentity identity);

    /// <summary>Reads and durably offers a bounded page for a registered subscription.</summary>
    /// <param name="request">The subscription page request.</param>
    /// <returns>The receive page result.</returns>
    ServerReceivePageResult OfferReceivePage(ServerSubscriptionPageRequest request);

    /// <summary>Durably acknowledges a previously offered complete receive position.</summary>
    /// <param name="request">The acknowledgement request.</param>
    /// <returns>The persisted subscription state after acknowledgement.</returns>
    ServerSubscriptionState Acknowledge(ServerSubscriptionAcknowledgementRequest request);
}
