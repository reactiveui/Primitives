// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Concurrency;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the retry extension operators re-subscribing to a source that fails a fixed number of times before succeeding.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ExtensionRetryBenchmarks
{
    /// <summary>The number of retried pipelines each case runs.</summary>
    private const int Count = 200;

    /// <summary>The number of failed attempts before the source succeeds.</summary>
    private const int Failures = 3;

    /// <summary>The retry budget, large enough to reach the successful attempt.</summary>
    private const int RetryBudget = 4;

    /// <summary>The backoff multiplier between retries.</summary>
    private const double BackoffFactor = 2.0;

    /// <summary>The initial retry delay.</summary>
    private static readonly TimeSpan InitialDelay = TimeSpan.FromTicks(1);

    /// <summary>The cap applied to the growing backoff delay.</summary>
    private static readonly TimeSpan MaxDelay = TimeSpan.FromTicks(4);

    /// <summary>The virtual time that covers every scheduled retry of one pipeline.</summary>
    private static readonly TimeSpan RetryHorizon = TimeSpan.FromTicks(16);

    /// <summary>Benchmarks RetryWithBackoff with capped exponential delays on virtual time.</summary>
    /// <returns>The sum of the values delivered after recovery.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("RetryWithBackoff")]
    public int PrimitivesRetryWithBackoff()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            FlakySource source = new(Failures);
            using var subscription = PrimitivesExtensions.RetryWithBackoff(
                    source,
                    RetryBudget,
                    InitialDelay,
                    BackoffFactor,
                    MaxDelay,
                    clock)
                .Subscribe(observer);
            clock.AdvanceBy(RetryHorizon);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks RetryWithBackoff with capped exponential delays using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the values delivered after recovery.</returns>
    [Benchmark]
    [BenchmarkCategory("RetryWithBackoff")]
    public int PackageRetryWithBackoff()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            FlakySource source = new(Failures);
            using var subscription = PackageExtensions.RetryWithBackoff(
                    source,
                    RetryBudget,
                    InitialDelay,
                    BackoffFactor,
                    MaxDelay,
                    scheduler)
                .Subscribe(observer);
            scheduler.AdvanceBy(RetryHorizon);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks the typed OnErrorRetry with a fixed delay on virtual time, reporting each failure.</summary>
    /// <returns>The sum of the delivered values plus the number of reported failures.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("OnErrorRetry")]
    public int PrimitivesOnErrorRetry()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        FailureCounter failures = new();
        for (var i = 0; i < Count; i++)
        {
            FlakySource source = new(Failures);
            using var subscription = PrimitivesExtensions.OnErrorRetry<int, InvalidOperationException>(
                    source,
                    _ => failures.Record(),
                    RetryBudget,
                    InitialDelay,
                    clock)
                .Subscribe(observer);
            clock.AdvanceBy(RetryHorizon);
        }

        return observer.Total + failures.Reported;
    }

    /// <summary>Benchmarks the typed OnErrorRetry with a fixed delay using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the delivered values plus the number of reported failures.</returns>
    [Benchmark]
    [BenchmarkCategory("OnErrorRetry")]
    public int PackageOnErrorRetry()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        FailureCounter failures = new();
        for (var i = 0; i < Count; i++)
        {
            FlakySource source = new(Failures);
            using var subscription = PackageExtensions.OnErrorRetry<int, InvalidOperationException>(
                    source,
                    _ => failures.Record(),
                    RetryBudget,
                    InitialDelay,
                    scheduler)
                .Subscribe(observer);
            scheduler.AdvanceBy(RetryHorizon);
        }

        return observer.Total + failures.Reported;
    }

    /// <summary>Benchmarks RetryWithDelay with a selector that retries immediately.</summary>
    /// <returns>The sum of the values delivered after recovery.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("RetryWithDelay")]
    public int PrimitivesRetryWithDelay()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            FlakySource source = new(Failures);
            using var subscription = PrimitivesExtensions.RetryWithDelay(source, RetryBudget, static _ => TimeSpan.Zero)
                .Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks RetryWithDelay with an immediate selector using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the values delivered after recovery.</returns>
    [Benchmark]
    [BenchmarkCategory("RetryWithDelay")]
    public int PackageRetryWithDelay()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            FlakySource source = new(Failures);
            using var subscription = PackageExtensions.RetryWithDelay(source, RetryBudget, static _ => TimeSpan.Zero)
                .Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks RetryWithFixedDelay with a zero delay, re-subscribing inline.</summary>
    /// <returns>The sum of the values delivered after recovery.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("RetryWithFixedDelay")]
    public int PrimitivesRetryWithFixedDelay()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            FlakySource source = new(Failures);
            using var subscription = PrimitivesExtensions.RetryWithFixedDelay(source, RetryBudget, TimeSpan.Zero)
                .Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks RetryWithFixedDelay with a zero delay using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the values delivered after recovery.</returns>
    [Benchmark]
    [BenchmarkCategory("RetryWithFixedDelay")]
    public int PackageRetryWithFixedDelay()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            FlakySource source = new(Failures);
            using var subscription = PackageExtensions.RetryWithFixedDelay(source, RetryBudget, TimeSpan.Zero)
                .Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Counts the failures reported through a retry error callback.</summary>
    private sealed class FailureCounter
    {
        /// <summary>Gets the number of reported failures.</summary>
        internal int Reported { get; private set; }

        /// <summary>Records one reported failure.</summary>
        internal void Record() => Reported++;
    }

    /// <summary>Cold source that fails its first subscriptions and then emits one value and completes.</summary>
    /// <param name="failures">The number of subscriptions that fail before one succeeds.</param>
    private sealed class FlakySource(int failures) : IObservable<int>
    {
        /// <summary>The value emitted by the successful attempt.</summary>
        private const int SuccessValue = 7;

        /// <summary>The failure delivered to every failing subscription.</summary>
        private static readonly InvalidOperationException Failure = new("Transient failure.");

        /// <summary>The number of subscriptions made so far.</summary>
        private int _attempts;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            var attempt = _attempts;
            _attempts = attempt + 1;
            if (attempt < failures)
            {
                observer.OnError(Failure);
                return EmptySubscription.Instance;
            }

            observer.OnNext(SuccessValue);
            observer.OnCompleted();
            return EmptySubscription.Instance;
        }
    }

    /// <summary>Subscription handle for a source that finishes during subscribe.</summary>
    private sealed class EmptySubscription : IDisposable
    {
        /// <summary>The shared instance.</summary>
        internal static readonly EmptySubscription Instance = new();

        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}
