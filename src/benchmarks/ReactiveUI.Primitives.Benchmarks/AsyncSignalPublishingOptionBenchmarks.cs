// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Signals;
using ExtensionsResult = ReactiveUI.Extensions.Async.Result;
using ExtensionsSubject = ReactiveUI.Extensions.Async.Subjects.ISubjectAsync<int>;
using ExtensionsSubjects = ReactiveUI.Extensions.Async.Subjects;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures broadcasting through async signals built from each serial/concurrent and stateful/stateless creation option.</summary>
[MemoryDiagnoser]
public class AsyncSignalPublishingOptionBenchmarks
{
    /// <summary>The number of values broadcast by each case.</summary>
    private const int Count = 32;

    /// <summary>The number of subscribers attached by each case.</summary>
    private const int SubscriberCount = 4;

    /// <summary>The value a behavior signal starts with and a replay-latest signal holds before subscribers arrive.</summary>
    private const int StartValue = -1;

    /// <summary>Plain signal options: concurrent publishing with completion state retained.</summary>
    private static readonly SignalCreationOptions ConcurrentSignal =
        new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };

    /// <summary>Plain signal options: serial publishing without completion state.</summary>
    private static readonly SignalCreationOptions SerialStatelessSignal =
        new() { PublishingOption = PublishingOption.Serial, IsStateless = true };

    /// <summary>Plain signal options: concurrent publishing without completion state.</summary>
    private static readonly SignalCreationOptions ConcurrentStatelessSignal =
        new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };

    /// <summary>Behavior signal options: concurrent publishing with completion state retained.</summary>
    private static readonly BehaviorSignalCreationOptions ConcurrentBehavior =
        new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };

    /// <summary>Behavior signal options: serial publishing without completion state.</summary>
    private static readonly BehaviorSignalCreationOptions SerialStatelessBehavior =
        new() { PublishingOption = PublishingOption.Serial, IsStateless = true };

    /// <summary>Behavior signal options: concurrent publishing without completion state.</summary>
    private static readonly BehaviorSignalCreationOptions ConcurrentStatelessBehavior =
        new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };

    /// <summary>Replay-latest signal options: concurrent publishing with completion state retained.</summary>
    private static readonly ReplayLatestSignalCreationOptions ConcurrentReplay =
        new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };

    /// <summary>Replay-latest signal options: serial publishing without completion state.</summary>
    private static readonly ReplayLatestSignalCreationOptions SerialStatelessReplay =
        new() { PublishingOption = PublishingOption.Serial, IsStateless = true };

    /// <summary>Replay-latest signal options: concurrent publishing without completion state.</summary>
    private static readonly ReplayLatestSignalCreationOptions ConcurrentStatelessReplay =
        new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };

    /// <summary>ReactiveUI.Extensions subject options: concurrent publishing with completion state retained.</summary>
    private static readonly ExtensionsSubjects.SubjectCreationOptions ConcurrentSubject =
        new() { PublishingOption = ExtensionsSubjects.PublishingOption.Concurrent, IsStateless = false };

    /// <summary>ReactiveUI.Extensions subject options: serial publishing without completion state.</summary>
    private static readonly ExtensionsSubjects.SubjectCreationOptions SerialStatelessSubject =
        new() { PublishingOption = ExtensionsSubjects.PublishingOption.Serial, IsStateless = true };

    /// <summary>ReactiveUI.Extensions subject options: concurrent publishing without completion state.</summary>
    private static readonly ExtensionsSubjects.SubjectCreationOptions ConcurrentStatelessSubject =
        new() { PublishingOption = ExtensionsSubjects.PublishingOption.Concurrent, IsStateless = true };

    /// <summary>ReactiveUI.Extensions behavior subject options: concurrent publishing with completion state retained.</summary>
    private static readonly ExtensionsSubjects.BehaviorSubjectCreationOptions ConcurrentBehaviorSubject =
        new() { PublishingOption = ExtensionsSubjects.PublishingOption.Concurrent, IsStateless = false };

    /// <summary>ReactiveUI.Extensions behavior subject options: serial publishing without completion state.</summary>
    private static readonly ExtensionsSubjects.BehaviorSubjectCreationOptions SerialStatelessBehaviorSubject =
        new() { PublishingOption = ExtensionsSubjects.PublishingOption.Serial, IsStateless = true };

    /// <summary>ReactiveUI.Extensions behavior subject options: concurrent publishing without completion state.</summary>
    private static readonly ExtensionsSubjects.BehaviorSubjectCreationOptions ConcurrentStatelessBehaviorSubject =
        new() { PublishingOption = ExtensionsSubjects.PublishingOption.Concurrent, IsStateless = true };

    /// <summary>ReactiveUI.Extensions replay-latest subject options: concurrent publishing with completion state retained.</summary>
    private static readonly ExtensionsSubjects.ReplayLatestSubjectCreationOptions ConcurrentReplaySubject =
        new() { PublishingOption = ExtensionsSubjects.PublishingOption.Concurrent, IsStateless = false };

    /// <summary>ReactiveUI.Extensions replay-latest subject options: serial publishing without completion state.</summary>
    private static readonly ExtensionsSubjects.ReplayLatestSubjectCreationOptions SerialStatelessReplaySubject =
        new() { PublishingOption = ExtensionsSubjects.PublishingOption.Serial, IsStateless = true };

    /// <summary>ReactiveUI.Extensions replay-latest subject options: concurrent publishing without completion state.</summary>
    private static readonly ExtensionsSubjects.ReplayLatestSubjectCreationOptions ConcurrentStatelessReplaySubject =
        new() { PublishingOption = ExtensionsSubjects.PublishingOption.Concurrent, IsStateless = true };

    /// <summary>Broadcasts through a serial signal that retains completion state.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark(Baseline = true)]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesSerialSignalBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.Create<int>(SignalCreationOptions.Default), false);

    /// <summary>Broadcasts through a serial ReactiveUI.Extensions subject that retains completion state.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsSerialSubjectBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.Create<int>(ExtensionsSubjects.SubjectCreationOptions.Default), false);

    /// <summary>Broadcasts through a concurrent signal that retains completion state.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesConcurrentSignalBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.Create<int>(ConcurrentSignal), false);

    /// <summary>Broadcasts through a concurrent ReactiveUI.Extensions subject that retains completion state.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsConcurrentSubjectBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.Create<int>(ConcurrentSubject), false);

    /// <summary>Broadcasts through a serial stateless signal.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesSerialStatelessSignalBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.Create<int>(SerialStatelessSignal), false);

    /// <summary>Broadcasts through a serial stateless ReactiveUI.Extensions subject.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsSerialStatelessSubjectBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.Create<int>(SerialStatelessSubject), false);

    /// <summary>Broadcasts through a concurrent stateless signal.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesConcurrentStatelessSignalBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.Create<int>(ConcurrentStatelessSignal), false);

    /// <summary>Broadcasts through a concurrent stateless ReactiveUI.Extensions subject.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsConcurrentStatelessSubjectBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.Create<int>(ConcurrentStatelessSubject), false);

    /// <summary>Broadcasts through a serial behavior signal that retains completion state.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed start value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesSerialBehaviorBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.CreateBehavior(StartValue, BehaviorSignalCreationOptions.Default), false);

    /// <summary>Broadcasts through a serial ReactiveUI.Extensions behavior subject that retains completion state.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed start value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsSerialBehaviorBroadcastAsync() =>
        BroadcastExtensionsAsync(
            ExtensionsSubjects.SubjectAsync.CreateBehavior(StartValue, ExtensionsSubjects.BehaviorSubjectCreationOptions.Default),
            false);

    /// <summary>Broadcasts through a concurrent behavior signal that retains completion state.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed start value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesConcurrentBehaviorBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.CreateBehavior(StartValue, ConcurrentBehavior), false);

    /// <summary>Broadcasts through a concurrent ReactiveUI.Extensions behavior subject that retains completion state.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed start value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsConcurrentBehaviorBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.CreateBehavior(StartValue, ConcurrentBehaviorSubject), false);

    /// <summary>Broadcasts through a serial stateless behavior signal.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed start value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesSerialStatelessBehaviorBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.CreateBehavior(StartValue, SerialStatelessBehavior), false);

    /// <summary>Broadcasts through a serial stateless ReactiveUI.Extensions behavior subject.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed start value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsSerialStatelessBehaviorBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.CreateBehavior(StartValue, SerialStatelessBehaviorSubject), false);

    /// <summary>Broadcasts through a concurrent stateless behavior signal.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed start value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesConcurrentStatelessBehaviorBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.CreateBehavior(StartValue, ConcurrentStatelessBehavior), false);

    /// <summary>Broadcasts through a concurrent stateless ReactiveUI.Extensions behavior subject.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed start value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsConcurrentStatelessBehaviorBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.CreateBehavior(StartValue, ConcurrentStatelessBehaviorSubject), false);

    /// <summary>Publishes a value before subscribers arrive, then broadcasts through a serial replay-latest signal.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesSerialReplayLatestBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.CreateReplayLatest<int>(ReplayLatestSignalCreationOptions.Default), true);

    /// <summary>Publishes a value before subscribers arrive, then broadcasts through a serial ReactiveUI.Extensions replay-latest subject.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsSerialReplayLatestBroadcastAsync() =>
        BroadcastExtensionsAsync(
            ExtensionsSubjects.SubjectAsync.CreateReplayLatest<int>(ExtensionsSubjects.ReplayLatestSubjectCreationOptions.Default),
            true);

    /// <summary>Publishes a value before subscribers arrive, then broadcasts through a concurrent replay-latest signal.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesConcurrentReplayLatestBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.CreateReplayLatest<int>(ConcurrentReplay), true);

    /// <summary>Publishes a value before subscribers arrive, then broadcasts through a concurrent ReactiveUI.Extensions replay-latest subject.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsConcurrentReplayLatestBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.CreateReplayLatest<int>(ConcurrentReplaySubject), true);

    /// <summary>Publishes a value before subscribers arrive, then broadcasts through a serial stateless replay-latest signal.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesSerialStatelessReplayLatestBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.CreateReplayLatest<int>(SerialStatelessReplay), true);

    /// <summary>Publishes a value before subscribers arrive, then broadcasts through a serial stateless ReactiveUI.Extensions replay-latest subject.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsSerialStatelessReplayLatestBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.CreateReplayLatest<int>(SerialStatelessReplaySubject), true);

    /// <summary>Publishes a value before subscribers arrive, then broadcasts through a concurrent stateless replay-latest signal.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesConcurrentStatelessReplayLatestBroadcastAsync() =>
        BroadcastAsync(PrimitivesAsyncSignalFactory.CreateReplayLatest<int>(ConcurrentStatelessReplay), true);

    /// <summary>Publishes a value before subscribers arrive, then broadcasts through a concurrent stateless ReactiveUI.Extensions replay-latest subject.</summary>
    /// <returns>The total number of values observed by all subscribers, including the replayed value.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsConcurrentStatelessReplayLatestBroadcastAsync() =>
        BroadcastExtensionsAsync(ExtensionsSubjects.SubjectAsync.CreateReplayLatest<int>(ConcurrentStatelessReplaySubject), true);

    /// <summary>Broadcasts through a signal whose subscribers see a mapped view of its values.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesMappedSignalBroadcastAsync() =>
        BroadcastAsync(
            PrimitivesAsyncSignalFactory.Create<int>().MapValues(static values => values.Map(static v => v + 1)),
            false);

    /// <summary>Feeds a sequence into a signal through its observer adapter and broadcasts it to the subscribers.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSignalAsObserverFeedAsync()
    {
        var signal = PrimitivesAsyncSignalFactory.Create<int>();
        var witnesses = new AsyncTallyWitness<int>[SubscriberCount];
        var subscriptions = new IAsyncDisposable[SubscriberCount];
        for (var i = 0; i < SubscriberCount; i++)
        {
            witnesses[i] = new();
            subscriptions[i] = await signal.SubscribeAsync(witnesses[i], CancellationToken.None).ConfigureAwait(false);
        }

        var feed = await SignalAsync.Range(0, Count)
            .SubscribeAsync(signal.AsObserverAsync(), CancellationToken.None)
            .ConfigureAwait(false);
        var total = await SumCompletionsAsync(witnesses).ConfigureAwait(false);
        await feed.DisposeAsync().ConfigureAwait(false);
        await DisposeAllAsync(subscriptions).ConfigureAwait(false);
        await signal.DisposeAsync().ConfigureAwait(false);
        return total;
    }

    /// <summary>Subscribes the subscribers, broadcasts a run of values, completes the signal and totals what each observed.</summary>
    /// <param name="signal">The signal to broadcast through.</param>
    /// <param name="publishBeforeSubscribe">Whether to publish one value before the subscribers arrive.</param>
    /// <returns>The total number of values observed by all subscribers.</returns>
    private static async Task<int> BroadcastAsync(ISignalAsync<int> signal, bool publishBeforeSubscribe)
    {
        if (publishBeforeSubscribe)
        {
            await signal.OnNextAsync(StartValue, CancellationToken.None).ConfigureAwait(false);
        }

        var witnesses = new AsyncTallyWitness<int>[SubscriberCount];
        var subscriptions = new IAsyncDisposable[SubscriberCount];
        for (var i = 0; i < SubscriberCount; i++)
        {
            witnesses[i] = new();
            subscriptions[i] = await signal.SubscribeAsync(witnesses[i], CancellationToken.None).ConfigureAwait(false);
        }

        for (var i = 0; i < Count; i++)
        {
            await signal.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
        }

        await signal.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        var total = await SumCompletionsAsync(witnesses).ConfigureAwait(false);
        await DisposeAllAsync(subscriptions).ConfigureAwait(false);
        await signal.DisposeAsync().ConfigureAwait(false);
        return total;
    }

    /// <summary>Subscribes the subscribers to a ReactiveUI.Extensions subject, broadcasts a run of values, completes it and totals what each observed.</summary>
    /// <param name="subject">The subject to broadcast through.</param>
    /// <param name="publishBeforeSubscribe">Whether to publish one value before the subscribers arrive.</param>
    /// <returns>The total number of values observed by all subscribers.</returns>
    private static async Task<int> BroadcastExtensionsAsync(ExtensionsSubject subject, bool publishBeforeSubscribe)
    {
        if (publishBeforeSubscribe)
        {
            await subject.OnNextAsync(StartValue, CancellationToken.None).ConfigureAwait(false);
        }

        var observers = new ExtensionsAsyncTallyWitness<int>[SubscriberCount];
        var handles = new IAsyncDisposable[SubscriberCount];
        for (var index = 0; index < SubscriberCount; index++)
        {
            observers[index] = new();
            handles[index] = await subject.SubscribeAsync(observers[index], CancellationToken.None).ConfigureAwait(false);
        }

        for (var value = 0; value < Count; value++)
        {
            await subject.OnNextAsync(value, CancellationToken.None).ConfigureAwait(false);
        }

        await subject.OnCompletedAsync(ExtensionsResult.Success).ConfigureAwait(false);
        var observed = 0;
        foreach (var observer in observers)
        {
            observed += await observer.Completion.ConfigureAwait(false);
        }

        foreach (var handle in handles)
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }

        await subject.DisposeAsync().ConfigureAwait(false);
        return observed;
    }

    /// <summary>Waits for every witness to complete and totals their value counts.</summary>
    /// <param name="witnesses">The witnesses to wait on.</param>
    /// <returns>The total value count.</returns>
    private static async Task<int> SumCompletionsAsync(AsyncTallyWitness<int>[] witnesses)
    {
        var total = 0;
        for (var i = 0; i < witnesses.Length; i++)
        {
            total += await witnesses[i].Completion.ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Disposes every subscription in order.</summary>
    /// <param name="subscriptions">The subscriptions to dispose.</param>
    /// <returns>A task that completes once every subscription is disposed.</returns>
    private static async Task DisposeAllAsync(IAsyncDisposable[] subscriptions)
    {
        for (var i = 0; i < subscriptions.Length; i++)
        {
            await subscriptions[i].DisposeAsync().ConfigureAwait(false);
        }
    }
}
