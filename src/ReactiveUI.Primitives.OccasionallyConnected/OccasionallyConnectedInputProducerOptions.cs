// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures the concrete synchronous observer input producer.</summary>
/// <typeparam name="TInput">The input value type.</typeparam>
internal sealed record OccasionallyConnectedInputProducerOptions<TInput>
{
    /// <summary>Gets the stream receiving observer input.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the observer input admission configuration.</summary>
    public required ObserverInputOptions Admission { get; init; }

    /// <summary>Gets the owned serialized input capture provider.</summary>
    public required IOccasionallyConnectedInputCapture<TInput> Capture { get; init; }

    /// <summary>Gets the durable publish callback composed from the owning stream facade.</summary>
    public required Func<
        PayloadEnvelope,
        RemotePublishOptions?,
        CancellationToken,
        ValueTask<PublishReceipt>> PublishAsync { get; init; }

    /// <summary>Gets the bounded fault publication callback.</summary>
    public required Action<string, string, OperationId?, Exception> PublishFault { get; init; }

    /// <summary>Gets the publish options supplied for captured observer input.</summary>
    public RemotePublishOptions? PublishOptions { get; init; }

    /// <summary>Validates this option record.</summary>
    /// <exception cref="InvalidOperationException">The option record contains an invalid value.</exception>
    internal void Validate()
    {
        ValidateRequired(Admission, nameof(Admission));
        ValidateRequired(Capture, nameof(Capture));
        ValidateRequired(PublishAsync, nameof(PublishAsync));
        ValidateRequired(PublishFault, nameof(PublishFault));
        ValidateStreamId();
        Admission.Validate();
        ValidatePublishOptions();

        static void ValidateRequired(object? value, string name)
        {
            if (value is not null)
            {
                return;
            }

            throw new InvalidOperationException($"{name} must be supplied.");
        }
    }

    /// <summary>Validates publish options when observer input supplies an explicit publish policy.</summary>
    /// <exception cref="InvalidOperationException">The publish options target another stream.</exception>
    private void ValidatePublishOptions()
    {
        if (PublishOptions is null)
        {
            return;
        }

        PublishOptions.Validate();
        if (PublishOptions.StreamId == StreamId)
        {
            return;
        }

        throw new InvalidOperationException("Input producer publish options StreamId must match the stream.");
    }

    /// <summary>Validates that the stream identifier is configured.</summary>
    /// <exception cref="InvalidOperationException">The stream identifier is missing.</exception>
    private void ValidateStreamId()
    {
        if (!string.IsNullOrWhiteSpace(StreamId.Value))
        {
            return;
        }

        throw new InvalidOperationException("StreamId must be supplied.");
    }
}
