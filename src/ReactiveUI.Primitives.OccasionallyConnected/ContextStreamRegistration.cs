// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores the compatibility-relevant fields for one context stream registration.</summary>
internal sealed record ContextStreamRegistration
{
    /// <summary>Gets the registered state type.</summary>
    public required Type StateType { get; init; }

    /// <summary>Gets the registered input type.</summary>
    public required Type InputType { get; init; }

    /// <summary>Gets the registered projection instance.</summary>
    public required object Projection { get; init; }

    /// <summary>Gets the registered input capture instance.</summary>
    public object? InputCapture { get; init; }

    /// <summary>Gets the registered stream facade.</summary>
    public required IOccasionallyConnectedStreamLifecycle Stream { get; init; }

    /// <summary>Gets the coalesced subscription identity.</summary>
    public SubscriptionId? SubscriptionId { get; init; }

    /// <summary>Gets the input contract identifier.</summary>
    public required string InputContractId { get; init; }

    /// <summary>Gets the state contract identifier.</summary>
    public required string StateContractId { get; init; }

    /// <summary>Gets the input schema version.</summary>
    public required int InputSchemaVersion { get; init; }

    /// <summary>Gets the state schema version.</summary>
    public required int StateSchemaVersion { get; init; }

    /// <summary>Gets the snapshot format version.</summary>
    public required int SnapshotFormatVersion { get; init; }

    /// <summary>Gets the normalized optional subscription options.</summary>
    public RemoteSubscriptionOptions? Subscription { get; init; }

    /// <summary>Gets the optional publish options.</summary>
    public RemotePublishOptions? Publish { get; init; }

    /// <summary>Gets the optional observer input options.</summary>
    public ObserverInputOptions? Input { get; init; }

    /// <summary>Gets the public typed input options.</summary>
    public required TypedInputOptions TypedInput { get; init; }

    /// <summary>Creates a registration snapshot from a stream definition and facade.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="definition">The definition.</param>
    /// <param name="typedInput">The validated public typed input options.</param>
    /// <param name="stream">The stream facade.</param>
    /// <returns>The registration snapshot.</returns>
    internal static ContextStreamRegistration Create<TState, TInput>(
        StreamDefinition<TState, TInput> definition,
        TypedInputOptions typedInput,
        OccasionallyConnectedStream<TState, TInput> stream)
    {
        var subscriptionId = GetCoalescedSubscriptionId(definition);
        return new()
        {
            StateType = typeof(TState),
            InputType = typeof(TInput),
            Projection = definition.Projection,
            InputCapture = definition.InputCapture,
            Stream = stream,
            SubscriptionId = subscriptionId,
            InputContractId = definition.InputContractId,
            StateContractId = definition.StateContractId,
            InputSchemaVersion = definition.InputSchemaVersion,
            StateSchemaVersion = definition.StateSchemaVersion,
            SnapshotFormatVersion = definition.SnapshotFormatVersion,
            Subscription = NormalizeSubscription(definition.Subscription, subscriptionId),
            Publish = definition.Publish,
            Input = definition.Input,
            TypedInput = typedInput,
        };
    }

    /// <summary>Returns whether the supplied definition is compatible with this registration.</summary>
    /// <typeparam name="TState">The candidate state type.</typeparam>
    /// <typeparam name="TInput">The candidate input type.</typeparam>
    /// <param name="definition">The candidate definition.</param>
    /// <param name="typedInput">The validated public typed input options.</param>
    /// <returns><see langword="true"/> when the candidate matches the registration.</returns>
    internal bool IsCompatible<TState, TInput>(StreamDefinition<TState, TInput> definition, TypedInputOptions typedInput)
    {
        var subscriptionId = GetCoalescedSubscriptionId(definition);
        return IsTypeAndBehaviorCompatible(definition)
            && IsContractCompatible(definition)
            && IsSubscriptionCompatible(definition, subscriptionId)
            && IsOptionCompatible(definition, typedInput);
    }

    /// <summary>Gets the definition-level subscription identity after nested identity coalescing.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="definition">The candidate definition.</param>
    /// <returns>The effective subscription identity.</returns>
    private static SubscriptionId? GetCoalescedSubscriptionId<TState, TInput>(StreamDefinition<TState, TInput> definition) =>
        definition.SubscriptionId ?? definition.Subscription?.SubscriptionId;

    /// <summary>Normalizes a nested subscription record to its effective identity.</summary>
    /// <param name="subscription">The nested subscription options.</param>
    /// <param name="subscriptionId">The effective subscription identity.</param>
    /// <returns>The normalized subscription options.</returns>
    private static RemoteSubscriptionOptions? NormalizeSubscription(RemoteSubscriptionOptions? subscription, SubscriptionId? subscriptionId) =>
        subscription is null ? null : subscription with { SubscriptionId = subscriptionId };

    /// <summary>Returns whether type and behavior identities match the registered definition.</summary>
    /// <typeparam name="TState">The candidate state type.</typeparam>
    /// <typeparam name="TInput">The candidate input type.</typeparam>
    /// <param name="definition">The candidate definition.</param>
    /// <returns><see langword="true"/> when type and behavior identity match.</returns>
    private bool IsTypeAndBehaviorCompatible<TState, TInput>(StreamDefinition<TState, TInput> definition) =>
        StateType == typeof(TState)
        && InputType == typeof(TInput)
        && ReferenceEquals(Projection, definition.Projection)
        && ReferenceEquals(InputCapture, definition.InputCapture);

    /// <summary>Returns whether wire contracts and recovery formats match the registered definition.</summary>
    /// <typeparam name="TState">The candidate state type.</typeparam>
    /// <typeparam name="TInput">The candidate input type.</typeparam>
    /// <param name="definition">The candidate definition.</param>
    /// <returns><see langword="true"/> when contracts and versions match.</returns>
    private bool IsContractCompatible<TState, TInput>(StreamDefinition<TState, TInput> definition) =>
        string.Equals(InputContractId, definition.InputContractId, StringComparison.Ordinal)
        && string.Equals(StateContractId, definition.StateContractId, StringComparison.Ordinal)
        && InputSchemaVersion == definition.InputSchemaVersion
        && StateSchemaVersion == definition.StateSchemaVersion
        && SnapshotFormatVersion == definition.SnapshotFormatVersion;

    /// <summary>Returns whether subscription identity and nested subscription options match.</summary>
    /// <typeparam name="TState">The candidate state type.</typeparam>
    /// <typeparam name="TInput">The candidate input type.</typeparam>
    /// <param name="definition">The candidate definition.</param>
    /// <param name="subscriptionId">The candidate effective subscription identity.</param>
    /// <returns><see langword="true"/> when subscription identity and options match.</returns>
    private bool IsSubscriptionCompatible<TState, TInput>(
        StreamDefinition<TState, TInput> definition,
        SubscriptionId? subscriptionId) =>
        SubscriptionId == subscriptionId
        && EqualityComparer<RemoteSubscriptionOptions?>.Default.Equals(Subscription, NormalizeSubscription(definition.Subscription, subscriptionId));

    /// <summary>Returns whether nested publish, input, and typed input options match.</summary>
    /// <typeparam name="TState">The candidate state type.</typeparam>
    /// <typeparam name="TInput">The candidate input type.</typeparam>
    /// <param name="definition">The candidate definition.</param>
    /// <param name="typedInput">The candidate typed input options.</param>
    /// <returns><see langword="true"/> when nested options match.</returns>
    private bool IsOptionCompatible<TState, TInput>(StreamDefinition<TState, TInput> definition, TypedInputOptions typedInput) =>
        EqualityComparer<RemotePublishOptions?>.Default.Equals(Publish, definition.Publish)
        && EqualityComparer<ObserverInputOptions?>.Default.Equals(Input, definition.Input)
        && EqualityComparer<TypedInputOptions>.Default.Equals(TypedInput, typedInput);
}
