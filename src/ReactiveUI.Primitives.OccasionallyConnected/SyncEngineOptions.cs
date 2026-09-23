// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures the internal synchronization engine.</summary>
internal sealed record SyncEngineOptions
{
    /// <summary>The default finite stream registration cap.</summary>
    internal const int DefaultMaxRegisteredStreams = 1024;

    /// <summary>The default finite transport subscription cap.</summary>
    internal const int DefaultMaxActiveSubscriptions = 1024;

    /// <summary>The default finite diagnostic observer cap per engine observable.</summary>
    internal const int DefaultMaxDiagnosticSubscriptions = 256;

    /// <summary>The default finite retained byte budget for scheduler descriptors.</summary>
    internal const long DefaultMaxSchedulerDescriptorBytes = 1024L * 1024L;

    /// <summary>Gets the local store dependency.</summary>
    public required ILocalStoreAdapter Store { get; init; }

    /// <summary>Gets the remote transport dependency.</summary>
    public required IRemoteTransportAdapter Transport { get; init; }

    /// <summary>Gets whether the engine owns the local store lifetime.</summary>
    public SyncEngineDependencyOwnership StoreOwnership { get; init; } = SyncEngineDependencyOwnership.Owned;

    /// <summary>Gets whether the engine owns the remote transport lifetime.</summary>
    public SyncEngineDependencyOwnership TransportOwnership { get; init; } = SyncEngineDependencyOwnership.Owned;

    /// <summary>Gets the clock used for engine lifecycle diagnostics.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Gets the configured observer notification scheduler.</summary>
    public IObserverNotificationScheduler NotificationScheduler { get; init; } = ThreadPoolObserverNotificationScheduler.Instance;

    /// <summary>Gets the occasionally connected behavior options.</summary>
    public OccasionallyConnectedOptions Options { get; init; } = OccasionallyConnectedOptions.Default;

    /// <summary>Gets the local store initialization requirements.</summary>
    public required LocalStoreInitialization StoreInitialization { get; init; }

    /// <summary>Gets the client identity supplied to the remote transport.</summary>
    public required ClientIdentity Client { get; init; }

    /// <summary>Gets the supported protocol version range.</summary>
    public VersionRange SupportedProtocolVersions { get; init; } = new(new(1, 0), new(1, 0));

    /// <summary>Gets the retry random source used by engine retry policies.</summary>
    public IRetryRandomSource RetryRandomSource { get; init; } = SyncEngine.EngineRetryRandomSource.Instance;

    /// <summary>Gets the finite maximum number of registered stream participants.</summary>
    public int MaxRegisteredStreams { get; init; } = DefaultMaxRegisteredStreams;

    /// <summary>Gets the finite maximum number of active receive subscriptions.</summary>
    public int MaxActiveSubscriptions { get; init; } = DefaultMaxActiveSubscriptions;

    /// <summary>Gets the finite maximum number of subscribers per engine diagnostic observable.</summary>
    public int MaxDiagnosticSubscriptions { get; init; } = DefaultMaxDiagnosticSubscriptions;

    /// <summary>Gets the finite retained byte budget for fair scheduler stream and head descriptors.</summary>
    public long MaxSchedulerDescriptorBytes { get; init; } = DefaultMaxSchedulerDescriptorBytes;

    /// <summary>Validates the option record.</summary>
    /// <exception cref="InvalidOperationException">The option record is malformed.</exception>
    internal void Validate()
    {
        ValidateRequired(Store, nameof(Store));
        ValidateRequired(Transport, nameof(Transport));
        ValidateRequired(TimeProvider, nameof(TimeProvider));
        ValidateRequired(NotificationScheduler, nameof(NotificationScheduler));
        ValidateRequired(Options, nameof(Options));
        ValidateRequired(StoreInitialization, nameof(StoreInitialization));
        ValidateRequired(Client, nameof(Client));
        ValidateRequired(SupportedProtocolVersions, nameof(SupportedProtocolVersions));
        ValidateRequired(RetryRandomSource, nameof(RetryRandomSource));
        ValidateOwnership(StoreOwnership, nameof(StoreOwnership));
        ValidateOwnership(TransportOwnership, nameof(TransportOwnership));
        Options.Validate();
        ValidatePositive(MaxRegisteredStreams, nameof(MaxRegisteredStreams));
        ValidatePositive(MaxActiveSubscriptions, nameof(MaxActiveSubscriptions));
        ValidatePositive(MaxDiagnosticSubscriptions, nameof(MaxDiagnosticSubscriptions));
        ValidatePositive(MaxSchedulerDescriptorBytes, nameof(MaxSchedulerDescriptorBytes));
        if (SupportedProtocolVersions.Minimum <= SupportedProtocolVersions.Maximum)
        {
            return;
        }

        throw new InvalidOperationException("SupportedProtocolVersions minimum must not exceed maximum.");
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

    /// <summary>Validates dependency lifetime ownership.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="name">The option name.</param>
    /// <exception cref="InvalidOperationException">The ownership value is undefined.</exception>
    private static void ValidateOwnership(SyncEngineDependencyOwnership value, string name)
    {
        if (value is SyncEngineDependencyOwnership.Owned or SyncEngineDependencyOwnership.Borrowed)
        {
            return;
        }

        throw new InvalidOperationException($"{name} must be a defined value.");
    }

    /// <summary>Validates a positive count.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="name">The option name.</param>
    /// <exception cref="InvalidOperationException">The value is not positive.</exception>
    private static void ValidatePositive(int value, string name)
    {
        if (value > 0)
        {
            return;
        }

        throw new InvalidOperationException($"{name} must be positive.");
    }

    /// <summary>Validates a positive byte count.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="name">The option name.</param>
    /// <exception cref="InvalidOperationException">The value is not positive.</exception>
    private static void ValidatePositive(long value, string name)
    {
        if (value > 0)
        {
            return;
        }

        throw new InvalidOperationException($"{name} must be positive.");
    }
}
