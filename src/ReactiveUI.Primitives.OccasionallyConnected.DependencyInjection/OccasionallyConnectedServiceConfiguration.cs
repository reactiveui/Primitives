// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.DependencyInjection;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Stores an immutable dependency-injection configuration snapshot.</summary>
internal sealed record OccasionallyConnectedServiceConfiguration
{
    /// <summary>Gets the service collection that produced the provider.</summary>
    internal required IServiceCollection Services { get; init; }

    /// <summary>Gets the client identity.</summary>
    internal required ClientIdentity Client { get; init; }

    /// <summary>Gets the optional local store identity.</summary>
    internal string? StoreIdentity { get; init; }

    /// <summary>Gets explicit local store initialization.</summary>
    internal LocalStoreInitialization? StoreInitialization { get; init; }

    /// <summary>Gets the immutable runtime options.</summary>
    internal required OccasionallyConnectedOptions Options { get; init; }

    /// <summary>Gets the maximum named stream count.</summary>
    internal required int MaximumNamedStreams { get; init; }

    /// <summary>Gets the maximum stream name length.</summary>
    internal required int MaximumStreamNameLength { get; init; }

    /// <summary>Gets the selected local store service type.</summary>
    internal required Type StoreType { get; init; }

    /// <summary>Gets the selected local store descriptor.</summary>
    internal required ServiceDescriptor StoreDescriptor { get; init; }

    /// <summary>Gets the selected remote transport service type.</summary>
    internal required Type TransportType { get; init; }

    /// <summary>Gets the selected remote transport descriptor.</summary>
    internal required ServiceDescriptor TransportDescriptor { get; init; }

    /// <summary>Gets the optional explicit payload serializer selection.</summary>
    internal ExplicitSerializerSelection? ExplicitSerializerSelection { get; init; }

    /// <summary>Gets the optional maximum JSON payload size.</summary>
    internal int? MaximumPayloadBytes { get; init; }

    /// <summary>Gets the named stream registrations.</summary>
    internal required NamedStreamRegistration[] Streams { get; init; }

    /// <summary>Gets the generated JSON contract registrations.</summary>
    internal required JsonContractRegistration[] JsonContracts { get; init; }
}
