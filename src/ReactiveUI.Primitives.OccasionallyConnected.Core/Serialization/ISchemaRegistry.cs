// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes registered payload schemas and upcast paths.</summary>
public interface ISchemaRegistry
{
    /// <summary>Determines whether a payload schema is allowlisted for a target type.</summary>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="schemaVersion">The schema version.</param>
    /// <param name="targetType">The target payload type.</param>
    /// <returns><see langword="true"/> when the schema is allowlisted; otherwise, <see langword="false"/>.</returns>
    bool IsRegistered(string contractId, int schemaVersion, Type targetType);

    /// <summary>Finds a contiguous upcast chain between two schema versions.</summary>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="fromVersion">The current schema version.</param>
    /// <param name="toVersion">The requested schema version.</param>
    /// <returns>The upcasters that convert the payload to the requested schema version.</returns>
    IReadOnlyList<IPayloadUpcaster> GetUpcastChain(string contractId, int fromVersion, int toVersion);
}
