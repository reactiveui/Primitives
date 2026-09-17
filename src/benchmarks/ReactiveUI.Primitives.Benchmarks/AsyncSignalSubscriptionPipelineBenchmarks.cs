// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Disposables;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsDisposableAsync = ReactiveUI.Extensions.Async.Disposables.DisposableAsync;
using ExtensionsResult = ReactiveUI.Extensions.Async.Result;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures async pipelines whose subscriptions own inner state: flattening, leading values, scoped resources, retries and error logging.</summary>
[MemoryDiagnoser]
public class AsyncSignalSubscriptionPipelineBenchmarks
{
    /// <summary>The number of values produced by the outer and plain sources.</summary>
    private const int Count = 32;

    /// <summary>The number of values each flattened inner source produces.</summary>
    private const int InnerCount = 4;

    /// <summary>The number of failed attempts before the retried source succeeds.</summary>
    private const int FailedAttempts = 3;

    /// <summary>The failure carried by failing sources.</summary>
    private static readonly InvalidOperationException Fault = new("attempt failure");

    /// <summary>The leading values prepended by the multi-value case.</summary>
    private static readonly int[] LeadingValues = [-4, -3, -2, -1];

    /// <summary>Flattens an inner range per outer value.</summary>
    /// <returns>The number of flattened values.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesFlatMapCountAsync() =>
        await SignalAsync.Range(0, Count)
            .FlatMap(static i => SignalAsync.Range(i, InnerCount))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Flattens an inner range per outer value in ReactiveUI.Extensions.</summary>
    /// <returns>The number of flattened values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSelectManyCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.SelectMany(
                    ExtensionsAsyncObservable.Range(0, Count),
                    static value => ExtensionsAsyncObservable.Range(value, InnerCount)))
            .ConfigureAwait(false);

    /// <summary>Prepends one value to a range.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesStartWithCountAsync() =>
        await SignalAsync.Range(0, Count).StartWith(-1).CountAsync().ConfigureAwait(false);

    /// <summary>Prepends one value to a range in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsStartWithCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.StartWith(ExtensionsAsyncObservable.Range(0, Count), -1))
            .ConfigureAwait(false);

    /// <summary>Prepends several values to a range.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesPrependValuesCountAsync() =>
        await SignalAsync.Range(0, Count).Prepend(LeadingValues).CountAsync().ConfigureAwait(false);

    /// <summary>Prepends several values to a range in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsPrependValuesCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Prepend(ExtensionsAsyncObservable.Range(0, Count), LeadingValues))
            .ConfigureAwait(false);

    /// <summary>Drains a range scoped to a per-subscription resource that is released on completion.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesUsingCountAsync() =>
        await SignalAsync.Using<int, IAsyncDisposable>(
                static _ => new(DisposableAsync.Create(static () => default)),
                static _ => SignalAsync.Range(0, Count))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Drains a range scoped to a per-subscription resource in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsUsingCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Using<int, IAsyncDisposable>(
                    static _ => new(ExtensionsDisposableAsync.Create(static () => default)),
                    static _ => ExtensionsAsyncObservable.Range(0, Count)))
            .ConfigureAwait(false);

    /// <summary>Retries a source that fails its first attempts and then succeeds.</summary>
    /// <returns>The number of values observed across every attempt.</returns>
    [Benchmark]
    public async Task<int> PrimitivesRetryCountAsync()
    {
        var attempts = 0;
        var source = SignalAsync.Create<int>(async (observer, cancellationToken) =>
        {
            attempts++;
            await observer.OnNextAsync(attempts, cancellationToken).ConfigureAwait(false);
            await observer.OnCompletedAsync(attempts <= FailedAttempts ? Result.Failure(Fault) : Result.Success)
                .ConfigureAwait(false);
            return DisposableAsync.Empty;
        });

        return await source.Retry(FailedAttempts).CountAsync().ConfigureAwait(false);
    }

    /// <summary>Retries a source that fails its first attempts and then succeeds in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed across every attempt.</returns>
    [Benchmark]
    public async Task<int> ExtensionsRetryCountAsync()
    {
        var attempt = 0;
        var source = ExtensionsAsyncObservable.Create<int>(async (observer, cancellationToken) =>
        {
            attempt++;
            await observer.OnNextAsync(attempt, cancellationToken).ConfigureAwait(false);
            var result = attempt <= FailedAttempts ? ExtensionsResult.Failure(Fault) : ExtensionsResult.Success;
            await observer.OnCompletedAsync(result).ConfigureAwait(false);
            return ExtensionsDisposableAsync.Create(static () => default);
        });

        return await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.Retry(source, FailedAttempts))
            .ConfigureAwait(false);
    }

    /// <summary>Logs every resumable error of a source before forwarding it.</summary>
    /// <returns>The number of errors logged.</returns>
    [Benchmark]
    public async Task<int> PrimitivesLogErrorsAsync()
    {
        var logged = 0;
        AsyncTallyWitness<int> witness = new();
        var source = SignalAsync.Create<int>(static async (observer, cancellationToken) =>
        {
            for (var i = 0; i < Count; i++)
            {
                await observer.OnErrorResumeAsync(Fault, cancellationToken).ConfigureAwait(false);
            }

            await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            return DisposableAsync.Empty;
        });

        var subscription = await source.LogErrors(_ => logged++)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return logged;
    }

    /// <summary>Logs every resumable error of a source before forwarding it in ReactiveUI.Extensions.</summary>
    /// <returns>The number of errors logged.</returns>
    [Benchmark]
    public async Task<int> ExtensionsLogErrorsAsync()
    {
        var errors = 0;
        ExtensionsAsyncTallyWitness<int> witness = new();
        var source = ExtensionsAsyncObservable.Create<int>(static async (observer, cancellationToken) =>
        {
            for (var value = 0; value < Count; value++)
            {
                await observer.OnErrorResumeAsync(Fault, cancellationToken).ConfigureAwait(false);
            }

            await observer.OnCompletedAsync(ExtensionsResult.Success).ConfigureAwait(false);
            return ExtensionsDisposableAsync.Create(static () => default);
        });

        var subscription = await ExtensionsAsyncObservable.LogErrors(source, _ => errors++)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return errors;
    }
}
