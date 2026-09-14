// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes the snapshot cursor offer outcome.</summary>
internal enum ServerSnapshotOfferStatus
{
    /// <summary>The recovered snapshot cursor was durably offered.</summary>
    Offered = 0,

    /// <summary>An identical retained proof was returned without mutation.</summary>
    AlreadyOffered = 1,

    /// <summary>The subscription was missing or expired before the offer transaction.</summary>
    MissingSubscription = 2,

    /// <summary>The stream, subscription, or operation proof changed after the view was read.</summary>
    ConcurrentChange = 3,

    /// <summary>The recovery response failed structural or proof validation.</summary>
    ValidationRejected = 4,

    /// <summary>The durable offer did not fit within configured retention limits.</summary>
    CapacityExceeded = 5,
}
