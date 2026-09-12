// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Creates stable local operation identifiers.</summary>
internal interface IOperationIdSource
{
    /// <summary>Creates a new operation identifier.</summary>
    /// <returns>The new operation identifier.</returns>
    OperationId New();
}
