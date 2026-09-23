// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Resolves named streams from an occasionally connected context.</summary>
internal sealed class OccasionallyConnectedStreamRegistry : IOccasionallyConnectedStreamRegistry
{
    /// <summary>Stores the gate for cache and in-flight resolution state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores resolved streams by configured name.</summary>
    private readonly Dictionary<string, object> _cache = [];

    /// <summary>Stores in-flight stream resolutions by configured name.</summary>
    private readonly Dictionary<string, StreamResolutionSlot> _resolutions = [];

    /// <summary>Stores registrations by configured name.</summary>
    private readonly Dictionary<string, NamedStreamRegistration> _registrations;

    /// <summary>Stores the context that owns stream instances.</summary>
    private readonly OccasionallyConnectedContext _context;

    /// <summary>Stores the service provider for user factories.</summary>
    private readonly IServiceProvider _services;

    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedStreamRegistry"/> class.</summary>
    /// <param name="configuration">The immutable configuration.</param>
    /// <param name="context">The context that owns stream instances.</param>
    /// <param name="services">The service provider.</param>
    internal OccasionallyConnectedStreamRegistry(
        OccasionallyConnectedServiceConfiguration configuration,
        OccasionallyConnectedContext context,
        IServiceProvider services)
    {
        _context = context;
        _services = services;
        _registrations = [with(comparer: StringComparer.Ordinal)];
        var streams = configuration.Streams;
        for (var i = 0; i < streams.Length; i++)
        {
            _registrations.Add(streams[i].Name, streams[i]);
        }
    }

    /// <inheritdoc />
    public IOccasionallyConnectedStream<TState, TInput> GetRequiredStream<TState, TInput>(
        OccasionallyConnectedStreamKey<TState, TInput> key)
    {
        var name = key.Name;
        var registration = GetTypedRegistration<TState, TInput>(name);
        var slot = EnterResolution(name, out var ownsResolution, out var cached);
        if (cached is not null)
        {
            return (IOccasionallyConnectedStream<TState, TInput>)cached;
        }

        return ownsResolution
            ? ResolveStream<TState, TInput>(name, registration, slot)
            : (IOccasionallyConnectedStream<TState, TInput>)slot.Completion.Task.GetAwaiter().GetResult();
    }

    /// <summary>Gets and validates the named stream registration.</summary>
    /// <typeparam name="TState">The stream state type.</typeparam>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="name">The stream name.</param>
    /// <returns>The named stream registration.</returns>
    /// <exception cref="ArgumentException">The stream name is blank.</exception>
    /// <exception cref="InvalidOperationException">The stream name is missing or registered with another type.</exception>
    private NamedStreamRegistration GetTypedRegistration<TState, TInput>(string name)
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
        if (!_registrations.TryGetValue(name, out var registration))
        {
            throw new InvalidOperationException("The named stream is not registered.");
        }

        if (registration.StateType == typeof(TState) && registration.InputType == typeof(TInput))
        {
            return registration;
        }

        throw new InvalidOperationException("The named stream was registered with another type.");
    }

    /// <summary>Enters or joins stream resolution for one name.</summary>
    /// <param name="name">The stream name.</param>
    /// <param name="ownsResolution">A value indicating whether the caller owns the resolution.</param>
    /// <param name="cached">The cached stream, if the name is already resolved.</param>
    /// <returns>The in-flight resolution slot.</returns>
    /// <exception cref="InvalidOperationException">The same stream is resolved recursively on one thread.</exception>
    private StreamResolutionSlot EnterResolution(string name, out bool ownsResolution, out object? cached)
    {
        var currentThreadId = Environment.CurrentManagedThreadId;
        lock (_gate)
        {
            if (_cache.TryGetValue(name, out cached))
            {
                ownsResolution = false;
                return StreamResolutionSlot.Cached;
            }

            if (_resolutions.TryGetValue(name, out var existing))
            {
                if (existing.OwnerThreadId == currentThreadId)
                {
                    throw new InvalidOperationException("Recursive resolution of the same named stream is not supported.");
                }

                ownsResolution = false;
                cached = null;
                return existing;
            }

            var created = new StreamResolutionSlot(currentThreadId);
            _resolutions.Add(name, created);
            ownsResolution = true;
            cached = null;
            return created;
        }
    }

    /// <summary>Resolves and caches a named stream owned by this caller.</summary>
    /// <typeparam name="TState">The stream state type.</typeparam>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="name">The stream name.</param>
    /// <param name="registration">The named stream registration.</param>
    /// <param name="slot">The in-flight resolution slot.</param>
    /// <returns>The resolved stream.</returns>
    private IOccasionallyConnectedStream<TState, TInput> ResolveStream<TState, TInput>(
        string name,
        NamedStreamRegistration registration,
        StreamResolutionSlot slot)
    {
        try
        {
            var definition = (StreamDefinition<TState, TInput>)registration.CreateDefinition(_services);
            var stream = _context.GetOrCreateStream(definition);
            lock (_gate)
            {
                _cache.Add(name, stream);
                _ = _resolutions.Remove(name);
            }

            slot.Completion.SetResult(stream);
            return stream;
        }
        catch (Exception exception)
        {
            lock (_gate)
            {
                _ = _resolutions.Remove(name);
            }

            slot.Completion.SetException(exception);
            _ = slot.Completion.Task.Exception;
            throw;
        }
    }

    /// <summary>Stores a resolved or in-flight named stream resolution.</summary>
    private sealed class StreamResolutionSlot
    {
        /// <summary>Gets a sentinel slot used when a cached stream is returned directly.</summary>
        public static readonly StreamResolutionSlot Cached = new(0);

        /// <summary>Initializes a new instance of the <see cref="StreamResolutionSlot"/> class.</summary>
        /// <param name="ownerThreadId">The resolving thread identifier.</param>
        public StreamResolutionSlot(int ownerThreadId)
        {
            OwnerThreadId = ownerThreadId;
            Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        /// <summary>Gets the resolving thread identifier.</summary>
        public int OwnerThreadId { get; }

        /// <summary>Gets the completion source for this resolution.</summary>
        public TaskCompletionSource<object> Completion { get; }
    }
}
