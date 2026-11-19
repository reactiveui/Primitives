// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async;

/// <summary>An asynchronous observable that multicasts a cold source through a signal, leaving the caller to decide when the source subscription is established.</summary>
/// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
/// <remarks>Subscribing attaches the observer to the signal without touching the source, so observers that subscribe
/// before <see cref="ConnectAsync"/> all share the one source subscription it creates.</remarks>
[System.Diagnostics.DebuggerDisplay("ConnectableSignalAsync: State = {State}")]
public sealed class ConnectableSignalAsync<T> : IObservableAsync<T>, IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="ConnectableSignalAsync{T}"/> class.</summary>
    /// <param name="source">The source signal to multicast.</param>
    /// <param name="signal">The signal that multicasts source notifications to the subscribed observers.</param>
    public ConnectableSignalAsync(IObservableAsync<T> source, ISignalAsync<T> signal) =>
        State = new(source, signal);

    /// <summary>Gets the mutable connection state owned by this wrapper.</summary>
    private ConnectableSignalAsyncState<T> State { get; }

    /// <summary>Creates a connectable sequence multicasting <paramref name="source"/> serially.</summary>
    /// <param name="source">The source sequence to multicast.</param>
    /// <returns>A connectable observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConnectableSignalAsync<T> Publish(IObservableAsync<T> source) =>
        new(source, new SerialSignalAsync<T>());

    /// <summary>Creates a connectable sequence multicasting <paramref name="source"/> with <paramref name="options"/>.</summary>
    /// <param name="source">The source sequence to multicast.</param>
    /// <param name="options">The signal creation options.</param>
    /// <returns>A connectable observable sequence.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="options"/> is unsupported.</exception>
    public static ConnectableSignalAsync<T> Publish(IObservableAsync<T> source, SignalCreationOptions options) =>
        new(
            source,
            options switch
            {
                { PublishingOption: PublishingOption.Serial, IsStateless: false } => new SerialSignalAsync<T>(),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: false } => new ConcurrentSignalAsync<T>(),
                { PublishingOption: PublishingOption.Serial, IsStateless: true } => new SerialStatelessSignalAsync<T>(),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: true } =>
                    new ConcurrentStatelessSignalAsync<T>(),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options,
                    "Unsupported signal creation options."),
            });

    /// <summary>Creates a connectable sequence multicasting <paramref name="source"/> and replaying the latest value starting from <paramref name="initialValue"/>.</summary>
    /// <param name="source">The source sequence to multicast.</param>
    /// <param name="initialValue">The initial value replayed before any source emission.</param>
    /// <returns>A connectable observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConnectableSignalAsync<T> Publish(IObservableAsync<T> source, T initialValue) =>
        new(source, new SerialReplayLatestSignalAsync<T>(new(initialValue)));

    /// <summary>
    /// Creates a connectable sequence multicasting <paramref name="source"/> and replaying the latest value starting from
    /// <paramref name="initialValue"/> with <paramref name="options"/>.
    /// </summary>
    /// <param name="source">The source sequence to multicast.</param>
    /// <param name="initialValue">The initial value replayed before any source emission.</param>
    /// <param name="options">The behavior signal creation options.</param>
    /// <returns>A connectable observable sequence.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="options"/> is unsupported.</exception>
    public static ConnectableSignalAsync<T> Publish(
        IObservableAsync<T> source,
        T initialValue,
        BehaviorSignalCreationOptions options) =>
        new(
            source,
            options switch
            {
                { PublishingOption: PublishingOption.Serial, IsStateless: false } =>
                    new SerialReplayLatestSignalAsync<T>(new(initialValue)),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: false } =>
                    new ConcurrentReplayLatestSignalAsync<T>(new(initialValue)),
                { PublishingOption: PublishingOption.Serial, IsStateless: true } =>
                    new SerialStatelessReplayLatestSignalAsync<T>(new(initialValue)),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: true } =>
                    new ConcurrentStatelessReplayLatestSignalAsync<T>(new(initialValue)),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options,
                    "Unsupported behavior signal creation options."),
            });

    /// <summary>Creates a stateless connectable sequence multicasting <paramref name="source"/>.</summary>
    /// <param name="source">The source sequence to multicast.</param>
    /// <returns>A connectable observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConnectableSignalAsync<T> StatelessPublish(IObservableAsync<T> source) =>
        new(source, new SerialStatelessSignalAsync<T>());

    /// <summary>Creates a stateless connectable sequence multicasting <paramref name="source"/> with <paramref name="initialValue"/> per connection.</summary>
    /// <param name="source">The source sequence to multicast.</param>
    /// <param name="initialValue">The initial value per connection.</param>
    /// <returns>A connectable observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConnectableSignalAsync<T> StatelessPublish(IObservableAsync<T> source, T initialValue) =>
        new(source, new SerialStatelessReplayLatestSignalAsync<T>(new(initialValue)));

    /// <summary>Creates a connectable sequence replaying the latest value of <paramref name="source"/>.</summary>
    /// <param name="source">The source sequence to multicast.</param>
    /// <returns>A connectable observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConnectableSignalAsync<T> ReplayLatestPublish(IObservableAsync<T> source) =>
        new(source, new SerialReplayLatestSignalAsync<T>(Optional<T>.Empty));

    /// <summary>Creates a connectable sequence replaying the latest value of <paramref name="source"/> with <paramref name="options"/>.</summary>
    /// <param name="source">The source sequence to multicast.</param>
    /// <param name="options">The replay-latest signal creation options.</param>
    /// <returns>A connectable observable sequence.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="options"/> is unsupported.</exception>
    public static ConnectableSignalAsync<T> ReplayLatestPublish(
        IObservableAsync<T> source,
        ReplayLatestSignalCreationOptions options) =>
        new(
            source,
            options switch
            {
                { PublishingOption: PublishingOption.Serial, IsStateless: false } =>
                    new SerialReplayLatestSignalAsync<T>(Optional<T>.Empty),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: false } =>
                    new ConcurrentReplayLatestSignalAsync<T>(Optional<T>.Empty),
                { PublishingOption: PublishingOption.Serial, IsStateless: true } =>
                    new SerialStatelessReplayLatestSignalAsync<T>(Optional<T>.Empty),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: true } =>
                    new ConcurrentStatelessReplayLatestSignalAsync<T>(Optional<T>.Empty),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options,
                    "Unsupported replay-latest signal creation options."),
            });

    /// <summary>Creates a stateless connectable sequence replaying only the latest value per connection.</summary>
    /// <param name="source">The source sequence to multicast.</param>
    /// <returns>A connectable observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConnectableSignalAsync<T> StatelessReplayLatestPublish(IObservableAsync<T> source) =>
        new(source, new SerialStatelessReplayLatestSignalAsync<T>(Optional<T>.Empty));

    /// <summary>Subscribes the signal to the source, or returns the live connection when one exists.</summary>
    /// <param name="cancellationToken">A token that cancels connection establishment.</param>
    /// <returns>A handle whose disposal drops the source subscription, allowing a later call to reconnect.</returns>
    /// <exception cref="OperationCanceledException">This instance has been disposed, or
    /// <paramref name="cancellationToken"/> was cancelled.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<IAsyncDisposable> ConnectAsync(CancellationToken cancellationToken) =>
        ConnectableSignalAsyncHelper.ConnectAsync(State, cancellationToken);

    /// <summary>Drops any live connection and blocks further ones.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Concurrency",
        "PSH1315:A blocking wait on an awaitable that may not be done",
        Justification = "The synchronous dispose contract leaves no way to await teardown of the async connection state.")]
    public void Dispose() => ConnectableSignalAsyncHelper.Dispose(State);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        ConnectableSignalAsyncHelper.SubscribeAsync(State, observer, cancellationToken);
}
