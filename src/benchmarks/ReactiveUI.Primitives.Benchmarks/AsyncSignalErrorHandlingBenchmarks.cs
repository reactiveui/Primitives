// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsAsyncSubject = ReactiveUI.Extensions.Async.Subjects.SubjectAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks async operators that recover from, retry after or convert source failures.</summary>
[MemoryDiagnoser]
public class AsyncSignalErrorHandlingBenchmarks
{
    /// <summary>The number of values produced before and after a failure.</summary>
    private const int Count = 32;

    /// <summary>The number of failed attempts before the source succeeds.</summary>
    private const int FailedAttempts = 2;

    /// <summary>The failure raised by the failing sources.</summary>
    private static readonly InvalidOperationException Failure = new("benchmark failure");

    /// <summary>Recovers a failing primitive async sequence with a fallback sequence.</summary>
    /// <returns>The number of values from the source and the fallback.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesRecoverCountAsync() =>
        await PrimitivesFailingSource()
            .Recover(static _ => PrimitivesAsyncSignal.Sequence(0, Count))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Recovers a failing primitive async sequence with the Rx-named operator.</summary>
    /// <returns>The number of values from the source and the fallback.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCatchCountAsync() =>
        await PrimitivesFailingSource()
            .Catch(static _ => PrimitivesAsyncSignal.Sequence(0, Count))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Recovers a failing primitive async sequence with the rescue-named operator.</summary>
    /// <returns>The number of values from the source and the fallback.</returns>
    [Benchmark]
    public async Task<int> PrimitivesRescueCountAsync() =>
        await PrimitivesFailingSource()
            .Rescue(static _ => PrimitivesAsyncSignal.Sequence(0, Count))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Recovers a failing ReactiveUI.Extensions async range with a fallback range.</summary>
    /// <returns>The number of values from the source and the fallback.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCatchCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Catch(
                    ExtensionsFailingSource(),
                    static _ => ExtensionsAsyncObservable.Range(0, Count)))
            .ConfigureAwait(false);

    /// <summary>Replaces the failure of a primitive async sequence with a single fallback value.</summary>
    /// <returns>The number of values including the fallback.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCatchAndReturnCountAsync() =>
        await PrimitivesFailingSource()
            .CatchAndReturn(-1)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Replaces the failure of a ReactiveUI.Extensions async range with a single fallback value.</summary>
    /// <returns>The number of values including the fallback.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCatchAndReturnCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.CatchAndReturn(ExtensionsFailingSource(), -1))
            .ConfigureAwait(false);

    /// <summary>Resubscribes a primitive async source that fails twice before succeeding.</summary>
    /// <returns>The number of values observed across every attempt.</returns>
    [Benchmark]
    public async Task<int> PrimitivesReattemptCountAsync()
    {
        var attempts = 0;
        return await PrimitivesAsyncSignal.Defer(
                () =>
                {
                    attempts++;
                    return attempts <= FailedAttempts
                        ? PrimitivesFailingSource()
                        : PrimitivesAsyncSignal.Sequence(0, Count);
                })
            .Reattempt(FailedAttempts)
            .CountAsync()
            .ConfigureAwait(false);
    }

    /// <summary>Resubscribes a ReactiveUI.Extensions async source that fails twice before succeeding.</summary>
    /// <returns>The number of values observed across every attempt.</returns>
    [Benchmark]
    public async Task<int> ExtensionsRetryCountAsync()
    {
        var attempts = 0;
        return await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Retry(
                    ExtensionsAsyncObservable.Defer(
                        () =>
                        {
                            attempts++;
                            return attempts <= FailedAttempts
                                ? ExtensionsFailingSource()
                                : ExtensionsAsyncObservable.Range(0, Count);
                        }),
                    FailedAttempts))
            .ConfigureAwait(false);
    }

    /// <summary>Converts a resumable error on a primitive async signal into a failed completion.</summary>
    /// <returns>The number of values observed plus one when the completion was received.</returns>
    [Benchmark]
    public async Task<int> PrimitivesOnErrorResumeAsFailureAsync()
    {
        var source = PrimitivesAsyncSignalFactory.Create<int>();
        var observed = 0;
        var subscription = await source.OnErrorResumeAsFailure()
            .SubscribeAsync(_ => observed++, null, _ => observed++, CancellationToken.None)
            .ConfigureAwait(false);
        try
        {
            for (var i = 0; i < Count; i++)
            {
                await source.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
            }

            await source.OnErrorResumeAsync(Failure, CancellationToken.None).ConfigureAwait(false);
            return observed;
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            await source.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Converts a resumable error on a ReactiveUI.Extensions async subject into a failed completion.</summary>
    /// <returns>The number of values observed plus one when the completion was received.</returns>
    [Benchmark]
    public async Task<int> ExtensionsOnErrorResumeAsFailureAsync()
    {
        var source = ExtensionsAsyncSubject.Create<int>();
        var observed = 0;
        var subscription = await ExtensionsAsyncObservable.SubscribeAsync(
                ExtensionsAsyncObservable.OnErrorResumeAsFailure(source),
                _ => observed++,
                null,
                _ => observed++,
                CancellationToken.None)
            .ConfigureAwait(false);
        try
        {
            for (var i = 0; i < Count; i++)
            {
                await source.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
            }

            await source.OnErrorResumeAsync(Failure, CancellationToken.None).ConfigureAwait(false);
            return observed;
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            await source.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Creates a primitive async sequence that emits values and then fails.</summary>
    /// <returns>The failing sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservableAsync<int> PrimitivesFailingSource() =>
        PrimitivesAsyncSignal.Sequence(0, Count).Chain(PrimitivesAsyncSignal.Throw<int>(Failure));

    /// <summary>Creates a ReactiveUI.Extensions async range that emits values and then fails.</summary>
    /// <returns>The failing range.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReactiveUI.Extensions.Async.IObservableAsync<int> ExtensionsFailingSource() =>
        ExtensionsAsyncObservable.Concat(
            ExtensionsAsyncObservable.Range(0, Count),
            ExtensionsAsyncObservable.Throw<int>(Failure));
}
