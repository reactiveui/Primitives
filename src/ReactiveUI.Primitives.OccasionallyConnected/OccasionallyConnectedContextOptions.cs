// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Captures immutable dependencies used by an <see cref="OccasionallyConnectedContext"/>.</summary>
internal sealed record OccasionallyConnectedContextOptions
{
    /// <summary>Gets the shared engine dependency.</summary>
    public required SyncEngine Engine { get; init; }

    /// <summary>Gets the local store dependency.</summary>
    public required ILocalStoreAdapter Store { get; init; }

    /// <summary>Gets the payload serializer dependency.</summary>
    public required IPayloadSerializer Serializer { get; init; }

    /// <summary>Gets the clock used for stream diagnostics.</summary>
    public required TimeProvider TimeProvider { get; init; }

    /// <summary>Gets the operation identifier source.</summary>
    public required IOperationIdSource OperationIdSource { get; init; }

    /// <summary>Gets the observer notification scheduler.</summary>
    public required IObserverNotificationScheduler NotificationScheduler { get; init; }

    /// <summary>Gets the occasionally connected behavior options.</summary>
    public required OccasionallyConnectedOptions Options { get; init; }

    /// <summary>Gets the initialized client identity.</summary>
    public required ClientIdentity Client { get; init; }

    /// <summary>Gets the finite maximum stream registry size.</summary>
    public required int RegistryCapacity { get; init; }

    /// <summary>Gets a value indicating whether startup should be scheduled after construction.</summary>
    public bool AutoStart { get; init; }

    /// <summary>Gets the optional schema registry snapshot used for descriptor validation.</summary>
    public SchemaRegistry? SchemaRegistry { get; init; }

    /// <summary>Validates the construction option record.</summary>
    /// <exception cref="InvalidOperationException">The option record is malformed.</exception>
    internal void Validate()
    {
        ValidateRequired(Engine, nameof(Engine));
        ValidateRequired(Store, nameof(Store));
        ValidateRequired(Serializer, nameof(Serializer));
        ValidateRequired(TimeProvider, nameof(TimeProvider));
        ValidateRequired(OperationIdSource, nameof(OperationIdSource));
        ValidateRequired(NotificationScheduler, nameof(NotificationScheduler));
        ValidateRequired(Options, nameof(Options));
        ValidateRequired(Client, nameof(Client));
        Options.Validate();
        if (RegistryCapacity > 0)
        {
            return;
        }

        throw new InvalidOperationException("RegistryCapacity must be positive.");
    }

    /// <summary>Validates a required dependency.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="name">The option name.</param>
    /// <exception cref="InvalidOperationException">The dependency is missing.</exception>
    private static void ValidateRequired(object? value, string name)
    {
        if (value is not null)
        {
            return;
        }

        throw new InvalidOperationException($"{name} must be supplied.");
    }
}
