// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Defines the typed contracts and options for one occasionally connected stream.</summary>
/// <typeparam name="TState">The local projection state type.</typeparam>
/// <typeparam name="TInput">The local and remote input type.</typeparam>
[DebuggerDisplay("{StreamId.Value,nq}; Subscription={SubscriptionId,nq}")]
public sealed record StreamDefinition<TState, TInput>
{
    /// <summary>Gets the stable logical stream identity.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the optional durable subscription identity to resume.</summary>
    public SubscriptionId? SubscriptionId { get; init; }

    /// <summary>Gets the projection that computes local state.</summary>
    public required ILocalProjection<TState, TInput> Projection { get; init; }

    /// <summary>Gets the stable wire contract identifier for inputs.</summary>
    public required string InputContractId { get; init; }

    /// <summary>Gets the stable wire contract identifier for state snapshots.</summary>
    public required string StateContractId { get; init; }

    /// <summary>Gets the input payload schema version.</summary>
    public int InputSchemaVersion { get; init; } = 1;

    /// <summary>Gets the state payload schema version.</summary>
    public int StateSchemaVersion { get; init; } = 1;

    /// <summary>Gets the local projection snapshot format version.</summary>
    public int SnapshotFormatVersion { get; init; } = 1;

    /// <summary>Gets the optional remote subscription configuration.</summary>
    public RemoteSubscriptionOptions? Subscription { get; init; }

    /// <summary>Gets the optional remote publication configuration.</summary>
    public RemotePublishOptions? Publish { get; init; }

    /// <summary>Gets the optional observer input bridge configuration.</summary>
    public ObserverInputOptions? Input { get; init; }

    /// <summary>Gets the optional provider that captures observer input as owned serialized payloads.</summary>
    public IOccasionallyConnectedInputCapture<TInput>? InputCapture { get; init; }

    /// <summary>Gets the optional public typed-input admission declaration.</summary>
    public TypedInputOptions? TypedInput { get; init; }

    /// <summary>Validates this definition using the default policy capability and priority range.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Validate() => Validate(supportsCustomPolicy: false);

    /// <summary>Validates this definition using the default priority range.</summary>
    /// <param name="supportsCustomPolicy">Whether custom nested policies have been registered and supported.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Validate(bool supportsCustomPolicy) => Validate(
        supportsCustomPolicy,
        OccasionallyConnectedOptionsValidation.MinimumPriority,
        OccasionallyConnectedOptionsValidation.MaximumPriority);

    /// <summary>Validates this definition and its optional nested configurations.</summary>
    /// <param name="supportsCustomPolicy">Whether custom nested policies have been registered and supported.</param>
    /// <param name="minimumPriority">The inclusive minimum accepted publication priority.</param>
    /// <param name="maximumPriority">The inclusive maximum accepted publication priority.</param>
    /// <exception cref="InvalidOperationException">The definition contains invalid or inconsistent configuration.</exception>
    public void Validate(bool supportsCustomPolicy, int minimumPriority, int maximumPriority)
    {
        OccasionallyConnectedOptionsValidation.ValidateStreamId(StreamId, nameof(StreamId));
        ValidateSubscriptionId();
        ValidateProjection();
        OccasionallyConnectedOptionsValidation.ValidateContractId(InputContractId, nameof(InputContractId));
        OccasionallyConnectedOptionsValidation.ValidateContractId(StateContractId, nameof(StateContractId));
        ValidateVersions();
        OccasionallyConnectedOptionsValidation.ValidatePriorityRange(minimumPriority, maximumPriority);
        ValidateSubscription(supportsCustomPolicy);
        ValidatePublish(supportsCustomPolicy, minimumPriority, maximumPriority);
        ValidateInput(supportsCustomPolicy);
        ValidateTypedInput();
    }

    /// <summary>Validates the optional durable subscription identity.</summary>
    /// <exception cref="InvalidOperationException"><see cref="SubscriptionId"/> contains an empty value.</exception>
    private void ValidateSubscriptionId()
    {
        if (SubscriptionId is not { Value: { } value } || value != Guid.Empty)
        {
            return;
        }

        throw new InvalidOperationException("SubscriptionId must be non-empty when supplied.");
    }

    /// <summary>Validates the required local projection.</summary>
    /// <exception cref="InvalidOperationException"><see cref="Projection"/> is missing.</exception>
    private void ValidateProjection()
    {
        if (Projection is not null)
        {
            return;
        }

        throw new InvalidOperationException("Projection must be provided.");
    }

    /// <summary>Validates serialization and recovery format versions.</summary>
    /// <exception cref="InvalidOperationException">A configured version is not positive.</exception>
    private void ValidateVersions()
    {
        OccasionallyConnectedOptionsValidation.ValidatePositiveVersion(InputSchemaVersion, nameof(InputSchemaVersion));
        OccasionallyConnectedOptionsValidation.ValidatePositiveVersion(StateSchemaVersion, nameof(StateSchemaVersion));
        OccasionallyConnectedOptionsValidation.ValidatePositiveVersion(SnapshotFormatVersion, nameof(SnapshotFormatVersion));
    }

    /// <summary>Validates the nested remote subscription and its identity compatibility.</summary>
    /// <param name="supportsCustomPolicy">Whether custom nested policies have been registered and supported.</param>
    /// <exception cref="InvalidOperationException">Nested subscription options are invalid or incompatible.</exception>
    private void ValidateSubscription(bool supportsCustomPolicy)
    {
        if (Subscription is null)
        {
            return;
        }

        Subscription.Validate(supportsCustomPolicy);
        ValidateNestedStreamId(Subscription.StreamId, nameof(Subscription));
        ValidateNestedSubscriptionId(Subscription.SubscriptionId);
    }

    /// <summary>Validates the nested remote publish configuration and its identity compatibility.</summary>
    /// <param name="supportsCustomPolicy">Whether custom nested policies have been registered and supported.</param>
    /// <param name="minimumPriority">The inclusive minimum accepted publication priority.</param>
    /// <param name="maximumPriority">The inclusive maximum accepted publication priority.</param>
    /// <exception cref="InvalidOperationException">Nested publish options are invalid or incompatible.</exception>
    private void ValidatePublish(bool supportsCustomPolicy, int minimumPriority, int maximumPriority)
    {
        if (Publish is null)
        {
            return;
        }

        Publish.Validate(supportsCustomPolicy, minimumPriority, maximumPriority);
        ValidateNestedStreamId(Publish.StreamId, nameof(Publish));
    }

    /// <summary>Validates observer input configuration and its required capture provider.</summary>
    /// <param name="supportsCustomPolicy">Whether custom nested policies have been registered and supported.</param>
    /// <exception cref="InvalidOperationException">Observer input is incomplete or unsupported.</exception>
    private void ValidateInput(bool supportsCustomPolicy)
    {
        if (Input is null && InputCapture is null)
        {
            return;
        }

        if (Input is null)
        {
            throw new InvalidOperationException("InputCapture requires observer input options.");
        }

        if (InputCapture is null)
        {
            throw new InvalidOperationException("Input requires an owned input capture provider.");
        }

        Input.Validate(supportsCustomPolicy);
    }

    /// <summary>Validates the optional public typed-input admission declaration.</summary>
    /// <exception cref="InvalidOperationException"><see cref="TypedInput"/> is malformed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateTypedInput() => TypedInput?.Validate();

    /// <summary>Validates compatibility with an explicit nested subscription identity.</summary>
    /// <param name="nestedSubscriptionId">The optional nested subscription identity.</param>
    /// <exception cref="InvalidOperationException">The explicit subscription identities conflict.</exception>
    private void ValidateNestedSubscriptionId(SubscriptionId? nestedSubscriptionId)
    {
        if (SubscriptionId is not { } subscriptionId || nestedSubscriptionId is not { } nestedId || subscriptionId == nestedId)
        {
            return;
        }

        throw new InvalidOperationException("SubscriptionId must match the explicit nested subscription identity.");
    }

    /// <summary>Validates that nested options use the definition stream identity.</summary>
    /// <param name="nestedStreamId">The stream identity configured on nested options.</param>
    /// <param name="propertyName">The nested option property name.</param>
    /// <exception cref="InvalidOperationException"><paramref name="nestedStreamId"/> does not match the definition stream identity.</exception>
    private void ValidateNestedStreamId(StreamId nestedStreamId, string propertyName)
    {
        if (nestedStreamId == StreamId)
        {
            return;
        }

        throw new InvalidOperationException($"{propertyName} StreamId must match StreamId.");
    }
}
