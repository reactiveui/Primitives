// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Publishes owned serialized input payloads without decoding them back to caller-owned typed values.</summary>
internal interface IOccasionallyConnectedSerializedInputPublisher
{
    /// <summary>Publishes an owned serialized input payload.</summary>
    /// <param name="payload">The owned input payload.</param>
    /// <param name="options">The optional publish options.</param>
    /// <param name="cancellationToken">The token used to cancel admission and persistence.</param>
    /// <returns>The durable publish receipt.</returns>
    ValueTask<PublishReceipt> PublishSerializedInputAsync(
        PayloadEnvelope payload,
        RemotePublishOptions? options,
        CancellationToken cancellationToken);

    /// <summary>Publishes a sanitized producer fault through the owning stream.</summary>
    /// <param name="code">The stable fault code.</param>
    /// <param name="message">The nonsecret diagnostic message.</param>
    /// <param name="operationId">The optional operation identifier.</param>
    /// <param name="exception">The local exception.</param>
    void PublishInputFault(string code, string message, OperationId? operationId, Exception exception);
}
