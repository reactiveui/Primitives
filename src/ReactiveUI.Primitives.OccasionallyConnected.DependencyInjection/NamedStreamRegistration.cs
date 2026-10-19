// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Stores a named stream registration.</summary>
internal sealed class NamedStreamRegistration
{
    /// <summary>Stores the erased stream definition factory.</summary>
    private readonly Func<IServiceProvider, object> _factory;

    /// <summary>Initializes a new instance of the <see cref="NamedStreamRegistration"/> class.</summary>
    /// <param name="name">The stream name.</param>
    /// <param name="stateType">The stream state type.</param>
    /// <param name="inputType">The stream input type.</param>
    /// <param name="factory">The erased stream definition factory.</param>
    private NamedStreamRegistration(
        string name,
        Type stateType,
        Type inputType,
        Func<IServiceProvider, object> factory)
    {
        Name = name;
        StateType = stateType;
        InputType = inputType;
        _factory = factory;
    }

    /// <summary>Gets the stream name.</summary>
    internal string Name { get; }

    /// <summary>Gets the stream state type.</summary>
    internal Type StateType { get; }

    /// <summary>Gets the stream input type.</summary>
    internal Type InputType { get; }

    /// <summary>Creates a named stream registration.</summary>
    /// <typeparam name="TState">The stream state type.</typeparam>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="name">The stream name.</param>
    /// <param name="factory">The typed stream definition factory.</param>
    /// <returns>The named stream registration.</returns>
    internal static NamedStreamRegistration Create<TState, TInput>(
        string name,
        Func<IServiceProvider, StreamDefinition<TState, TInput>> factory) =>
        new(name, typeof(TState), typeof(TInput), services => CreateDefinition(services, factory));

    /// <summary>Creates a stream definition.</summary>
    /// <param name="services">The service provider.</param>
    /// <returns>The stream definition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal object CreateDefinition(IServiceProvider services) => _factory(services);

    /// <summary>Creates and validates a typed stream definition.</summary>
    /// <typeparam name="TState">The stream state type.</typeparam>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="services">The service provider.</param>
    /// <param name="factory">The typed stream definition factory.</param>
    /// <returns>The typed stream definition.</returns>
    /// <exception cref="InvalidOperationException">The factory returned null.</exception>
    private static StreamDefinition<TState, TInput> CreateDefinition<TState, TInput>(
        IServiceProvider services,
        Func<IServiceProvider, StreamDefinition<TState, TInput>> factory) =>
        factory(services) ?? throw new InvalidOperationException("Stream definition factory returned null.");
}
