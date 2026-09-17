// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures a concrete occasionally connected stream facade.</summary>
/// <typeparam name="TState">The local state type.</typeparam>
/// <typeparam name="TInput">The input value type.</typeparam>
internal sealed record OccasionallyConnectedStreamOptions<TState, TInput>
{
    /// <summary>The default notification count capacity.</summary>
    private const int DefaultNotificationCapacity = 256;

    /// <summary>The default notification byte capacity.</summary>
    private const long DefaultNotificationCapacityBytes = 4 * 1024 * 1024;

    /// <summary>Gets the public stream definition.</summary>
    public required StreamDefinition<TState, TInput> Definition { get; init; }

    /// <summary>Gets the local store dependency.</summary>
    public required ILocalStoreAdapter Store { get; init; }

    /// <summary>Gets the serializer dependency.</summary>
    public required IPayloadSerializer Serializer { get; init; }

    /// <summary>Gets the clock used for diagnostics.</summary>
    public required TimeProvider TimeProvider { get; init; }

    /// <summary>Gets the operation identifier source.</summary>
    public required IOperationIdSource OperationIdSource { get; init; }

    /// <summary>Gets the context-owned coordinator dependency.</summary>
    public required IOccasionallyConnectedStreamCoordinator Coordinator { get; init; }

    /// <summary>Gets the optional public input producer dependency.</summary>
    public IOccasionallyConnectedInputProducer<TInput>? InputProducer { get; init; }

    /// <summary>Gets the source-backed local state snapshot materializer.</summary>
    public required Func<PayloadEnvelope, CancellationToken, ValueTask<TState>> LocalStateSnapshotFactory { get; init; }

    /// <summary>Gets the source-backed remote input snapshot materializer.</summary>
    public required Func<PayloadEnvelope, CancellationToken, ValueTask<TInput>> RemoteInputSnapshotFactory { get; init; }

    /// <summary>Gets the scheduler used for observer notifications.</summary>
    public required IObserverNotificationScheduler NotificationScheduler { get; init; }

    /// <summary>Gets the observer notification queue options.</summary>
    public ObserverNotificationSubscriptionOptions NotificationOptions { get; init; } =
        new(DefaultNotificationCapacity, DefaultNotificationCapacityBytes, ObserverNotificationOverflowMode.CoalesceLatest);

    /// <summary>Gets the maximum admitted stream mutation work items.</summary>
    public int WorkCapacity { get; init; } = 64;

    /// <summary>Gets the initialized client identity associated with local commits.</summary>
    public string? ClientId { get; init; }

    /// <summary>Gets the inclusive minimum operation priority.</summary>
    public int MinimumPriority { get; init; } = OperationPolicy.MinimumPriority;

    /// <summary>Gets the inclusive maximum operation priority.</summary>
    public int MaximumPriority { get; init; } = OperationPolicy.MaximumPriority;

    /// <summary>Validates the option set.</summary>
    /// <exception cref="InvalidOperationException">The option set is malformed.</exception>
    internal void Validate()
    {
        ValidateRequired(Definition, nameof(Definition));
        ValidateRequired(Store, nameof(Store));
        ValidateRequired(Serializer, nameof(Serializer));
        ValidateRequired(TimeProvider, nameof(TimeProvider));
        ValidateRequired(OperationIdSource, nameof(OperationIdSource));
        ValidateRequired(Coordinator, nameof(Coordinator));
        ValidateRequired(LocalStateSnapshotFactory, nameof(LocalStateSnapshotFactory));
        ValidateRequired(RemoteInputSnapshotFactory, nameof(RemoteInputSnapshotFactory));
        ValidateRequired(NotificationScheduler, nameof(NotificationScheduler));
        Definition.Validate();
        NotificationOptions.Validate();
        if (WorkCapacity <= 0)
        {
            throw new InvalidOperationException("WorkCapacity must be positive.");
        }

        if (MinimumPriority <= MaximumPriority)
        {
            return;
        }

        throw new InvalidOperationException("MinimumPriority must not exceed MaximumPriority.");
    }

    /// <summary>Validates a required option dependency.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="name">The option display name.</param>
    /// <exception cref="InvalidOperationException">The value is null.</exception>
    private static void ValidateRequired(object? value, string name)
    {
        if (value is not null)
        {
            return;
        }

        throw new InvalidOperationException($"{name} must be supplied.");
    }
}
