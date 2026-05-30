// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Provides factory methods for creating asynchronous Signal instances with configurable publishing and state
/// retention behaviors.
/// </summary>
/// <remarks>The Signal class offers a variety of static methods to create Signals that support different
/// publishing strategies (such as serial or concurrent) and state management options (stateful or stateless). These
/// Signals can be used to broadcast values to multiple observers in asynchronous scenarios. Use the provided creation
/// options to customize the Signal's behavior according to your application's requirements.</remarks>
public static class Signal
{
    /// <summary>
    /// Creates a new asynchronous Signal instance for the specified type.
    /// </summary>
    /// <remarks>The created Signal uses the default Signal creation options. Use the overload that accepts
    /// <see cref="SignalCreationOptions"/> to customize Signal behavior.</remarks>
    /// <typeparam name="T">The type of elements processed by the Signal.</typeparam>
    /// <returns>An <see cref="ISignalAsync{T}"/> that represents the newly created asynchronous Signal.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: Signal.Create<int>().")]
    public static ISignalAsync<T> Create<T>() => Create<T>(SignalCreationOptions.Default);

    /// <summary>
    /// Creates a new asynchronous Signal instance with the specified publishing and state options.
    /// </summary>
    /// <remarks>Use this method to create an ISignalAsync{T} with the desired concurrency and state
    /// management characteristics. The returned Signal type depends on the values provided in the options
    /// parameter.</remarks>
    /// <typeparam name="T">The type of elements processed by the Signal.</typeparam>
    /// <param name="options">The options that configure the publishing behavior and statefulness of the Signal. Must specify valid values
    /// for publishing and statelessness.</param>
    /// <returns>An asynchronous Signal instance configured according to the specified options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the specified combination of publishing and statelessness options is not supported.</exception>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: Signal.Create<int>(options).")]
    public static ISignalAsync<T> Create<T>(SignalCreationOptions? options) =>
        (options?.PublishingOption, options?.IsStateless) switch
        {
            (PublishingOption.Serial, false) => new SerialSignalAsync<T>(),
            (PublishingOption.Concurrent, false) => new ConcurrentSignalAsync<T>(),
            (PublishingOption.Serial, true) => new SerialStatelessSignalAsync<T>(),
            (PublishingOption.Concurrent, true) => new ConcurrentStatelessSignalAsync<T>(),
            _ => throw new ArgumentOutOfRangeException()
        };

    /// <summary>
    /// Creates a new asynchronous behavior Signal initialized with the specified starting value.
    /// </summary>
    /// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
    /// <param name="startValue">The initial value to be emitted to new subscribers and stored as the current value of the Signal.</param>
    /// <returns>An asynchronous behavior Signal that holds the specified starting value and emits it to new subscribers.</returns>
    public static ISignalAsync<T> CreateBehavior<T>(T startValue) =>
        CreateBehavior(startValue, BehaviorSignalCreationOptions.Default);

    /// <summary>
    /// Creates a new asynchronous Signal that replays the latest value to new subscribers, using the specified initial
    /// value and creation options.
    /// </summary>
    /// <typeparam name="T">The type of the values published by the Signal.</typeparam>
    /// <param name="startValue">The initial value to be published by the Signal before any values are pushed.</param>
    /// <param name="options">The options that control the Signal's publishing behavior and state management.</param>
    /// <returns>An asynchronous Signal that replays the latest value to new subscribers, configured according to the specified
    /// options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the specified options contain an unsupported publishing configuration.</exception>
    public static ISignalAsync<T> CreateBehavior<T>(T startValue, BehaviorSignalCreationOptions? options) =>
        (options?.PublishingOption, options?.IsStateless) switch
        {
            (PublishingOption.Serial, false) => new SerialReplayLatestSignalAsync<T>(new(startValue)),
            (PublishingOption.Concurrent, false) => new ConcurrentReplayLatestSignalAsync<T>(new(startValue)),
            (PublishingOption.Serial, true) => new SerialStatelessReplayLatestSignalAsync<T>(new(startValue)),
            (PublishingOption.Concurrent, true) => new ConcurrentStatelessReplayLatestSignalAsync<T>(new(startValue)),
            _ => throw new ArgumentOutOfRangeException()
        };

    /// <summary>
    /// Creates a new asynchronous Signal that replays only the most recent value to new subscribers.
    /// </summary>
    /// <remarks>The returned Signal will only retain the most recent value published. When a new subscriber
    /// subscribes, it immediately receives the latest value, if any, followed by subsequent values. This is useful for
    /// scenarios where only the most recent state is relevant to new observers.</remarks>
    /// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
    /// <returns>An asynchronous Signal that stores and replays the latest value to each new subscriber.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: Signal.CreateReplayLatest<int>().")]
    public static ISignalAsync<T> CreateReplayLatest<T>() =>
        CreateReplayLatest<T>(ReplayLatestSignalCreationOptions.Default);

    /// <summary>
    /// Creates a new asynchronous Signal that replays the latest value to new subscribers, with configuration options
    /// for publishing behavior and statefulness.
    /// </summary>
    /// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
    /// <param name="options">The options that specify the publishing mode and whether the Signal maintains state. Cannot be null.</param>
    /// <returns>An asynchronous Signal that replays the latest value to new subscribers, configured according to the specified
    /// options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the combination of options specified in the <paramref name="options"/> parameter is not supported.</exception>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: Signal.CreateReplayLatest<int>(options).")]
    public static ISignalAsync<T> CreateReplayLatest<T>(ReplayLatestSignalCreationOptions? options) =>
        (options?.PublishingOption, options?.IsStateless) switch
        {
            (PublishingOption.Serial, false) => new SerialReplayLatestSignalAsync<T>(Optional<T>.Empty),
            (PublishingOption.Concurrent, false) => new ConcurrentReplayLatestSignalAsync<T>(Optional<T>.Empty),
            (PublishingOption.Serial, true) => new SerialStatelessReplayLatestSignalAsync<T>(Optional<T>.Empty),
            (PublishingOption.Concurrent, true) =>
                new ConcurrentStatelessReplayLatestSignalAsync<T>(Optional<T>.Empty),
            _ => throw new ArgumentOutOfRangeException()
        };
}
