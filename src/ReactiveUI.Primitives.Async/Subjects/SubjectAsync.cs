// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Subjects;

/// <summary>
/// Provides factory methods for creating asynchronous subject instances with configurable publishing and state
/// retention behaviors.
/// </summary>
/// <remarks>The SubjectAsync class offers a variety of static methods to create subjects that support different
/// publishing strategies (such as serial or concurrent) and state management options (stateful or stateless). These
/// subjects can be used to broadcast values to multiple observers in asynchronous scenarios. Use the provided creation
/// options to customize the subject's behavior according to your application's requirements.</remarks>
public static class SubjectAsync
{
    /// <summary>
    /// Creates a new asynchronous subject instance for the specified type.
    /// </summary>
    /// <remarks>The created subject uses the default subject creation options. Use the overload that accepts
    /// <see cref="SubjectCreationOptions"/> to customize subject behavior.</remarks>
    /// <typeparam name="T">The type of elements processed by the subject.</typeparam>
    /// <returns>An <see cref="ISubjectAsync{T}"/> that represents the newly created asynchronous subject.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: SubjectAsync.Create<int>().")]
    public static ISubjectAsync<T> Create<T>() => Create<T>(SubjectCreationOptions.Default);

    /// <summary>
    /// Creates a new asynchronous subject instance with the specified publishing and state options.
    /// </summary>
    /// <remarks>Use this method to create an ISubjectAsync{T} with the desired concurrency and state
    /// management characteristics. The returned subject type depends on the values provided in the options
    /// parameter.</remarks>
    /// <typeparam name="T">The type of elements processed by the subject.</typeparam>
    /// <param name="options">The options that configure the publishing behavior and statefulness of the subject. Must specify valid values
    /// for publishing and statelessness.</param>
    /// <returns>An asynchronous subject instance configured according to the specified options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the specified combination of publishing and statelessness options is not supported.</exception>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: SubjectAsync.Create<int>(options).")]
    public static ISubjectAsync<T> Create<T>(SubjectCreationOptions? options) =>
        (options?.PublishingOption, options?.IsStateless) switch
        {
            (PublishingOption.Serial, false) => new SerialSubjectAsync<T>(),
            (PublishingOption.Concurrent, false) => new ConcurrentSubjectAsync<T>(),
            (PublishingOption.Serial, true) => new SerialStatelessSubjectAsync<T>(),
            (PublishingOption.Concurrent, true) => new ConcurrentStatelessSubjectAsync<T>(),
            _ => throw new ArgumentOutOfRangeException()
        };

    /// <summary>
    /// Creates a new asynchronous behavior subject initialized with the specified starting value.
    /// </summary>
    /// <typeparam name="T">The type of the elements processed by the subject.</typeparam>
    /// <param name="startValue">The initial value to be emitted to new subscribers and stored as the current value of the subject.</param>
    /// <returns>An asynchronous behavior subject that holds the specified starting value and emits it to new subscribers.</returns>
    public static ISubjectAsync<T> CreateBehavior<T>(T startValue) =>
        CreateBehavior(startValue, BehaviorSubjectCreationOptions.Default);

    /// <summary>
    /// Creates a new asynchronous subject that replays the latest value to new subscribers, using the specified initial
    /// value and creation options.
    /// </summary>
    /// <typeparam name="T">The type of the values published by the subject.</typeparam>
    /// <param name="startValue">The initial value to be published by the subject before any values are pushed.</param>
    /// <param name="options">The options that control the subject's publishing behavior and state management.</param>
    /// <returns>An asynchronous subject that replays the latest value to new subscribers, configured according to the specified
    /// options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the specified options contain an unsupported publishing configuration.</exception>
    public static ISubjectAsync<T> CreateBehavior<T>(T startValue, BehaviorSubjectCreationOptions? options) =>
        (options?.PublishingOption, options?.IsStateless) switch
        {
            (PublishingOption.Serial, false) => new SerialReplayLatestSubjectAsync<T>(new(startValue)),
            (PublishingOption.Concurrent, false) => new ConcurrentReplayLatestSubjectAsync<T>(new(startValue)),
            (PublishingOption.Serial, true) => new SerialStatelessReplayLastSubjectAsync<T>(new(startValue)),
            (PublishingOption.Concurrent, true) => new ConcurrentStatelessReplayLatestSubjectAsync<T>(new(startValue)),
            _ => throw new ArgumentOutOfRangeException()
        };

    /// <summary>
    /// Creates a new asynchronous subject that replays only the most recent value to new subscribers.
    /// </summary>
    /// <remarks>The returned subject will only retain the most recent value published. When a new subscriber
    /// subscribes, it immediately receives the latest value, if any, followed by subsequent values. This is useful for
    /// scenarios where only the most recent state is relevant to new observers.</remarks>
    /// <typeparam name="T">The type of the elements processed by the subject.</typeparam>
    /// <returns>An asynchronous subject that stores and replays the latest value to each new subscriber.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: SubjectAsync.CreateReplayLatest<int>().")]
    public static ISubjectAsync<T> CreateReplayLatest<T>() =>
        CreateReplayLatest<T>(ReplayLatestSubjectCreationOptions.Default);

    /// <summary>
    /// Creates a new asynchronous subject that replays the latest value to new subscribers, with configuration options
    /// for publishing behavior and statefulness.
    /// </summary>
    /// <typeparam name="T">The type of the elements processed by the subject.</typeparam>
    /// <param name="options">The options that specify the publishing mode and whether the subject maintains state. Cannot be null.</param>
    /// <returns>An asynchronous subject that replays the latest value to new subscribers, configured according to the specified
    /// options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the combination of options specified in the <paramref name="options"/> parameter is not supported.</exception>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: SubjectAsync.CreateReplayLatest<int>(options).")]
    public static ISubjectAsync<T> CreateReplayLatest<T>(ReplayLatestSubjectCreationOptions? options) =>
        (options?.PublishingOption, options?.IsStateless) switch
        {
            (PublishingOption.Serial, false) => new SerialReplayLatestSubjectAsync<T>(Optional<T>.Empty),
            (PublishingOption.Concurrent, false) => new ConcurrentReplayLatestSubjectAsync<T>(Optional<T>.Empty),
            (PublishingOption.Serial, true) => new SerialStatelessReplayLastSubjectAsync<T>(Optional<T>.Empty),
            (PublishingOption.Concurrent, true) =>
                new ConcurrentStatelessReplayLatestSubjectAsync<T>(Optional<T>.Empty),
            _ => throw new ArgumentOutOfRangeException()
        };
}
