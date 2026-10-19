// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Creates deterministic versions for accepted custom activity writes.</summary>
[System.Diagnostics.DebuggerDisplay("Activity version factory")]
public sealed class ActivityVersionFactory : IServerConflictVersionFactory
{
    /// <inheritdoc/>
    public string CreateNextVersion(ConflictContext context, SyncOperation operation)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(operation);
        return $"activity-{operation.ClientSequence.ToString(CultureInfo.InvariantCulture)}-{operation.OperationId.Value:N}";
    }
}
