// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stable reason and fault codes reported for delivery-guarantee outcomes.</summary>
public static class SyncReasonCodes
{
    /// <summary>Gets the code reported when an exactly-once operation outlives its deduplication window and stops as <see cref="SyncOperationState.GuaranteeExpired"/>.</summary>
    public static string GuaranteeExpired => "OC.GuaranteeExpired";

    /// <summary>Gets the code reported when an exactly-once operation outlives its deduplication window and continues under explicit at-least-once retry.</summary>
    public static string GuaranteeDowngraded => "OC.GuaranteeDowngraded";

    /// <summary>Gets the code reported when an at-most-once attempt loses its outcome and stops as <see cref="SyncOperationState.Ambiguous"/> without a resend.</summary>
    public static string AtMostOnceAmbiguous => "OC.AtMostOnceAmbiguous";
}
