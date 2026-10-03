// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Serializes and deserializes allowlisted payload contracts.</summary>
public interface IPayloadSerializer
{
    /// <summary>Gets the content type emitted and accepted by this serializer.</summary>
    string ContentType { get; }

    /// <summary>Serializes an allowlisted payload value.</summary>
    /// <typeparam name="T">The payload value type.</typeparam>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="schemaVersion">The positive contract schema version.</param>
    /// <param name="value">The payload value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The serialized payload envelope.</returns>
    ValueTask<PayloadEnvelope> SerializeAsync<T>(string contractId, int schemaVersion, T value, CancellationToken cancellationToken);

    /// <summary>Deserializes an allowlisted payload envelope.</summary>
    /// <param name="envelope">The payload envelope to deserialize.</param>
    /// <param name="targetType">The requested target type.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The deserialized payload value.</returns>
    ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken);
}
