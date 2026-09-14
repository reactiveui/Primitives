// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace OccasionallyConnected.DurableOutbox;

/// <summary>The result to apply in the explicitly local upload-attempt simulation.</summary>
internal enum SimulatedAttemptOutcome
{
    /// <summary>Records the attempt barrier and leaves the response unresolved.</summary>
    LostResponse = 0,

    /// <summary>Records a local simulated accepted acknowledgement.</summary>
    Accepted = 1,

    /// <summary>Records a local simulated rejected acknowledgement.</summary>
    Rejected = 2,
}
