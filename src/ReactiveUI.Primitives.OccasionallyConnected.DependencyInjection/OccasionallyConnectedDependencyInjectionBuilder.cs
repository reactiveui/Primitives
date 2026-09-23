// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Configures occasionally connected services for dependency injection.</summary>
[DebuggerDisplay("Streams = {_streams.Count}, JsonContracts = {_jsonContracts.Count}")]
public sealed class OccasionallyConnectedDependencyInjectionBuilder
{
    /// <summary>Stores named stream registrations.</summary>
    private readonly List<NamedStreamRegistration> _streams = [];

    /// <summary>Stores generated JSON contract registrations.</summary>
    private readonly List<JsonContractRegistration> _jsonContracts = [];

    /// <summary>Stores the service collection being configured.</summary>
    private readonly IServiceCollection _services;

    /// <summary>Stores the selected client identity.</summary>
    private ClientIdentity? _client;

    /// <summary>Stores the selected local store identity.</summary>
    private string? _storeIdentity;

    /// <summary>Stores explicit local store initialization.</summary>
    private LocalStoreInitialization? _storeInitialization;

    /// <summary>Stores the immutable runtime options snapshot.</summary>
    private OccasionallyConnectedOptions _options = OccasionallyConnectedOptions.Default;

    /// <summary>Stores the selected local store service type.</summary>
    private Type? _storeType;

    /// <summary>Stores the effective local store descriptor captured during configuration.</summary>
    private ServiceDescriptor? _storeDescriptor;

    /// <summary>Stores the selected remote transport service type.</summary>
    private Type? _transportType;

    /// <summary>Stores the effective remote transport descriptor captured during configuration.</summary>
    private ServiceDescriptor? _transportDescriptor;

    /// <summary>Stores the optional explicit payload serializer selection.</summary>
    private ExplicitSerializerSelection? _explicitSerializerSelection;

    /// <summary>Stores whether generated JSON metadata should be used.</summary>
    private bool _useJsonSerializer;

    /// <summary>Stores the optional maximum JSON payload size.</summary>
    private int? _maximumPayloadBytes;

    /// <summary>Stores the maximum number of named streams.</summary>
    private int _maximumNamedStreams = 128;

    /// <summary>Stores the maximum named stream length.</summary>
    private int _maximumStreamNameLength = 128;

    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedDependencyInjectionBuilder"/> class.</summary>
    /// <param name="services">The service collection being configured.</param>
    internal OccasionallyConnectedDependencyInjectionBuilder(IServiceCollection services) => _services = services;

    /// <summary>Configures the client identity.</summary>
    /// <param name="client">The client identity.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedDependencyInjectionBuilder UseClient(ClientIdentity client)
    {
        ArgumentExceptionHelper.ThrowIfNull(client);
        _client = client;
        return this;
    }

    /// <summary>Configures the local store identity.</summary>
    /// <param name="storeIdentity">The store identity.</param>
    /// <returns>The current builder.</returns>
    /// <exception cref="ArgumentException">The store identity is blank.</exception>
    public OccasionallyConnectedDependencyInjectionBuilder UseStoreIdentity(string storeIdentity)
    {
#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(storeIdentity);
#else
        ArgumentExceptionHelper.ThrowIfNull(storeIdentity);
        if (string.IsNullOrWhiteSpace(storeIdentity))
        {
            throw new ArgumentException("Store identity must be supplied.", nameof(storeIdentity));
        }
#endif
        _storeIdentity = storeIdentity;
        return this;
    }

    /// <summary>Configures explicit local store initialization.</summary>
    /// <param name="initialization">The store initialization.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedDependencyInjectionBuilder UseStoreInitialization(
        LocalStoreInitialization initialization)
    {
        ArgumentExceptionHelper.ThrowIfNull(initialization);
        _storeInitialization = initialization;
        return this;
    }

    /// <summary>Configures runtime options.</summary>
    /// <param name="options">The runtime options.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedDependencyInjectionBuilder UseOptions(OccasionallyConnectedOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        _options = options;
        return this;
    }

    /// <summary>Configures runtime options with a transform.</summary>
    /// <param name="configure">The option transform.</param>
    /// <returns>The current builder.</returns>
    /// <exception cref="InvalidOperationException">The transform returned null.</exception>
    public OccasionallyConnectedDependencyInjectionBuilder ConfigureOptions(
        Func<OccasionallyConnectedOptions, OccasionallyConnectedOptions> configure)
    {
        ArgumentExceptionHelper.ThrowIfNull(configure);
        _options = configure(_options) ?? throw new InvalidOperationException("Options transform returned null.");
        return this;
    }

    /// <summary>Configures the maximum named stream count.</summary>
    /// <param name="maximumNamedStreams">The maximum named stream count.</param>
    /// <returns>The current builder.</returns>
    /// <exception cref="InvalidOperationException">The existing stream count exceeds the new limit.</exception>
    public OccasionallyConnectedDependencyInjectionBuilder WithMaximumNamedStreams(int maximumNamedStreams)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maximumNamedStreams);
        if (_streams.Count > maximumNamedStreams)
        {
            throw new InvalidOperationException("The configured named stream count exceeds the new limit.");
        }

        _maximumNamedStreams = maximumNamedStreams;
        return this;
    }

    /// <summary>Configures the maximum stream name length.</summary>
    /// <param name="maximumStreamNameLength">The maximum stream name length.</param>
    /// <returns>The current builder.</returns>
    /// <exception cref="InvalidOperationException">A registered stream exceeds the new limit.</exception>
    public OccasionallyConnectedDependencyInjectionBuilder WithMaximumStreamNameLength(int maximumStreamNameLength)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maximumStreamNameLength);
        for (var i = 0; i < _streams.Count; i++)
        {
            if (_streams[i].Name.Length > maximumStreamNameLength)
            {
                throw new InvalidOperationException("A registered stream name exceeds the new length limit.");
            }
        }

        _maximumStreamNameLength = maximumStreamNameLength;
        return this;
    }

    /// <summary>Selects a singleton local store service.</summary>
    /// <param name="storeType">The store service type.</param>
    /// <returns>The current builder.</returns>
    /// <exception cref="ArgumentException">The type is not a local store adapter.</exception>
    /// <exception cref="InvalidOperationException">The selected service is missing or not singleton.</exception>
    public OccasionallyConnectedDependencyInjectionBuilder UseStore(Type storeType)
    {
        ValidateServiceType(storeType, typeof(ILocalStoreAdapter), nameof(storeType));
        var descriptor = ValidateSingletonService(storeType);
        _storeType = storeType;
        _storeDescriptor = descriptor;
        return this;
    }

    /// <summary>Selects a singleton remote transport service.</summary>
    /// <param name="transportType">The transport service type.</param>
    /// <returns>The current builder.</returns>
    /// <exception cref="ArgumentException">The type is not a remote transport adapter.</exception>
    /// <exception cref="InvalidOperationException">The selected service is missing or not singleton.</exception>
    public OccasionallyConnectedDependencyInjectionBuilder UseTransport(Type transportType)
    {
        ValidateServiceType(transportType, typeof(IRemoteTransportAdapter), nameof(transportType));
        var descriptor = ValidateSingletonService(transportType);
        _transportType = transportType;
        _transportDescriptor = descriptor;
        return this;
    }

    /// <summary>Selects a singleton payload serializer service.</summary>
    /// <param name="serializerType">The serializer service type.</param>
    /// <returns>The current builder.</returns>
    /// <exception cref="ArgumentException">The type is not a payload serializer.</exception>
    /// <exception cref="InvalidOperationException">The selected service is missing or not singleton.</exception>
    public OccasionallyConnectedDependencyInjectionBuilder UseSerializer(Type serializerType)
    {
        ValidateServiceType(serializerType, typeof(IPayloadSerializer), nameof(serializerType));
        var descriptor = ValidateSingletonService(serializerType);
        _explicitSerializerSelection = new(serializerType, descriptor);
        _useJsonSerializer = false;
        return this;
    }

    /// <summary>Configures JSON serialization from registered generated metadata.</summary>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedDependencyInjectionBuilder UseJsonSerializer()
    {
        _useJsonSerializer = true;
        _explicitSerializerSelection = null;
        _maximumPayloadBytes = null;
        return this;
    }

    /// <summary>Configures JSON serialization from registered generated metadata.</summary>
    /// <param name="maximumPayloadBytes">The maximum payload size.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedDependencyInjectionBuilder UseJsonSerializer(int maximumPayloadBytes)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maximumPayloadBytes);
        _useJsonSerializer = true;
        _explicitSerializerSelection = null;
        _maximumPayloadBytes = maximumPayloadBytes;
        return this;
    }

    /// <summary>Adds generated JSON contract metadata.</summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="contractId">The contract identifier.</param>
    /// <param name="schemaVersion">The schema version.</param>
    /// <param name="jsonTypeInfo">The generated JSON metadata.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedDependencyInjectionBuilder AddJsonContract<T>(
        string contractId,
        int schemaVersion,
        JsonTypeInfo<T> jsonTypeInfo)
    {
        _jsonContracts.Add(JsonContractRegistration.Create(contractId, schemaVersion, jsonTypeInfo));
        return this;
    }

    /// <summary>Adds a named stream registration.</summary>
    /// <typeparam name="TState">The stream state type.</typeparam>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="name">The stream name.</param>
    /// <param name="factory">The stream definition factory.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedDependencyInjectionBuilder AddStream<TState, TInput>(
        string name,
        Func<IServiceProvider, StreamDefinition<TState, TInput>> factory)
    {
        ValidateStreamName(name);
        ArgumentExceptionHelper.ThrowIfNull(factory);
        ValidateRegistrationCapacity(name, typeof(TState), typeof(TInput));
        _streams.Add(NamedStreamRegistration.Create(name, factory));
        return this;
    }

    /// <summary>Builds an immutable configuration snapshot.</summary>
    /// <returns>The configuration snapshot.</returns>
    /// <exception cref="InvalidOperationException">A required dependency was not supplied.</exception>
    internal OccasionallyConnectedServiceConfiguration BuildConfiguration()
    {
        var client = _client ?? throw new InvalidOperationException($"{nameof(ClientIdentity)} must be supplied.");
        if (_storeType is null || _storeDescriptor is null)
        {
            throw new InvalidOperationException($"{nameof(ILocalStoreAdapter)} must be supplied.");
        }

        if (_transportType is null || _transportDescriptor is null)
        {
            throw new InvalidOperationException($"{nameof(IRemoteTransportAdapter)} must be supplied.");
        }

        if (!_useJsonSerializer && _explicitSerializerSelection is null)
        {
            throw new InvalidOperationException($"{nameof(IPayloadSerializer)} must be supplied.");
        }

        return new()
        {
            Services = _services,
            Client = client,
            StoreIdentity = _storeIdentity,
            StoreInitialization = _storeInitialization,
            Options = _options,
            MaximumNamedStreams = _maximumNamedStreams,
            MaximumStreamNameLength = _maximumStreamNameLength,
            StoreType = _storeType,
            StoreDescriptor = _storeDescriptor,
            TransportType = _transportType,
            TransportDescriptor = _transportDescriptor,
            ExplicitSerializerSelection = _explicitSerializerSelection,
            MaximumPayloadBytes = _maximumPayloadBytes,
            Streams = [.. _streams],
            JsonContracts = [.. _jsonContracts],
        };
    }

    /// <summary>Validates that a selected service type implements the required contract.</summary>
    /// <param name="serviceType">The selected service type.</param>
    /// <param name="requiredType">The required contract type.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The type does not implement the required contract.</exception>
    private static void ValidateServiceType(Type serviceType, Type requiredType, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(serviceType);
        if (!requiredType.IsAssignableFrom(serviceType))
        {
            throw new ArgumentException("The selected service type does not implement the required contract.", parameterName);
        }
    }

    /// <summary>Finds the effective unkeyed descriptor for the requested service type.</summary>
    /// <param name="services">The service collection to inspect.</param>
    /// <param name="serviceType">The service type.</param>
    /// <returns>The effective descriptor, or <see langword="null"/> when none exists.</returns>
    private static ServiceDescriptor? FindEffectiveUnkeyedDescriptor(IServiceCollection services, Type serviceType)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            var descriptor = services[i];
            if (descriptor.ServiceType == serviceType && !descriptor.IsKeyedService)
            {
                return descriptor;
            }
        }

        return null;
    }

    /// <summary>Finds and validates the effective unkeyed service descriptor.</summary>
    /// <param name="serviceType">The selected service type.</param>
    /// <returns>The effective singleton descriptor.</returns>
    /// <exception cref="InvalidOperationException">The selected service is missing or not singleton.</exception>
    private ServiceDescriptor ValidateSingletonService(Type serviceType)
    {
        var descriptor = FindEffectiveUnkeyedDescriptor(_services, serviceType)
            ?? throw new InvalidOperationException(
                "Selected occasionally connected dependencies must be registered first.");

        if (descriptor.Lifetime == ServiceLifetime.Singleton)
        {
            return descriptor;
        }

        throw new InvalidOperationException("Selected occasionally connected dependencies must be singleton services.");
    }

    /// <summary>Validates a named stream name.</summary>
    /// <param name="name">The stream name.</param>
    /// <exception cref="ArgumentException">The name is blank or too long.</exception>
    private void ValidateStreamName(string name)
    {
#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
#else
        ArgumentExceptionHelper.ThrowIfNull(name);
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Stream name must be supplied.", nameof(name));
        }
#endif

        if (name.Length > _maximumStreamNameLength)
        {
            throw new ArgumentException("Stream name exceeds the configured maximum length.", nameof(name));
        }
    }

    /// <summary>Validates named stream capacity and duplicate registrations.</summary>
    /// <param name="name">The stream name.</param>
    /// <param name="stateType">The stream state type.</param>
    /// <param name="inputType">The stream input type.</param>
    /// <exception cref="InvalidOperationException">The registration exceeds capacity or duplicates another name.</exception>
    private void ValidateRegistrationCapacity(string name, Type stateType, Type inputType)
    {
        if (_streams.Count >= _maximumNamedStreams)
        {
            throw new InvalidOperationException("The named stream registry capacity has been reached.");
        }

        for (var i = 0; i < _streams.Count; i++)
        {
            var registration = _streams[i];
            if (!string.Equals(registration.Name, name, StringComparison.Ordinal))
            {
                continue;
            }

            if (registration.StateType == stateType && registration.InputType == inputType)
            {
                throw new InvalidOperationException("A stream with the same name and type is already registered.");
            }

            throw new InvalidOperationException("A stream with the same name is already registered with another type.");
        }
    }
}
