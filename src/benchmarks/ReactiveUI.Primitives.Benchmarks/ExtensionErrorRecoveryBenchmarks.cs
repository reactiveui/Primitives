// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using RxIntSubject = System.Reactive.Subjects.Subject<int>;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures recovery operators that replace, swallow or retry a failing stream.</summary>
[MemoryDiagnoser]
public class ExtensionErrorRecoveryBenchmarks
{
    /// <summary>The number of values emitted before each failure.</summary>
    private const int Count = 256;

    /// <summary>The fallback value substituted for a failure.</summary>
    private const int Fallback = 42;

    /// <summary>The failure raised by every failing source.</summary>
    private static readonly InvalidOperationException Boom = new("benchmark");

    /// <summary>Replaces a failure with a fallback value.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesCatchReturn()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.CatchReturn(source, Fallback).Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Replaces a failure with a fallback value using System.Reactive.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int SystemReactiveCatchReturn()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Catch(source, RxObservable.Return(Fallback)).Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Replaces a failure with a fallback value using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsCatchReturn()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.CatchReturn(source, Fallback).Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Maps a typed failure to a computed fallback value.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int PrimitivesCatchAndReturnFactory()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.CatchAndReturn<int, InvalidOperationException>(source, static ex => ex.Message.Length)
            .Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Maps a typed failure to a computed fallback value using System.Reactive.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int SystemReactiveCatchAndReturnFactory()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Catch<int, InvalidOperationException>(
                source,
                static ex => RxObservable.Return(ex.Message.Length))
            .Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Maps a typed failure to a computed fallback value using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsCatchAndReturnFactory()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.CatchAndReturn<int, InvalidOperationException>(source, static ex => ex.Message.Length)
            .Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Reports a typed failure to a callback and completes empty.</summary>
    /// <returns>The sum of the values plus completions and reported errors.</returns>
    [Benchmark]
    public int PrimitivesCatchIgnoreTyped()
    {
        var errors = 0;
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.CatchIgnore<int, InvalidOperationException>(source, _ => errors++)
            .Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount + errors;
    }

    /// <summary>Reports a typed failure to a callback and completes empty using System.Reactive.</summary>
    /// <returns>The sum of the values plus completions and reported errors.</returns>
    [Benchmark]
    public int SystemReactiveCatchIgnoreTyped()
    {
        var errors = 0;
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Catch<int, InvalidOperationException>(
                source,
                _ =>
                {
                    errors++;
                    return RxObservable.Empty<int>();
                })
            .Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount + errors;
    }

    /// <summary>Reports a typed failure to a callback and completes empty using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the values plus completions and reported errors.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsCatchIgnoreTyped()
    {
        var errors = 0;
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.CatchIgnore<int, InvalidOperationException>(source, _ => errors++)
            .Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount + errors;
    }

    /// <summary>Swallows any failure and completes empty.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int PrimitivesCatchIgnore()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.CatchIgnore(source).Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Swallows any failure and completes empty using System.Reactive.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int SystemReactiveCatchIgnore()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Catch(source, RxObservable.Empty<int>()).Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Swallows any failure and completes empty using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsCatchIgnore()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.CatchIgnore(source).Subscribe(observer);
        PushThenFail(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Resubscribes a cold source after every failure until it completes.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int PrimitivesRetryForever()
    {
        IntSignalWitness observer = new();
        using var subscription = PrimitivesExtensions.OnErrorRetry(new FlakySource()).Subscribe(observer);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Resubscribes a cold source after every failure until it completes using System.Reactive.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int SystemReactiveRetryForever()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Retry(new FlakySource()).Subscribe(observer);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Resubscribes a cold source after every failure until it completes using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the values plus completions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsRetryForever()
    {
        IntSignalWitness observer = new();
        using var subscription = PackageExtensions.OnErrorRetry(new FlakySource()).Subscribe(observer);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Pushes the ascending range into the subject and then fails it.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushThenFail(IObserver<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnError(Boom);
    }

    /// <summary>Cold source that emits a batch and fails for its first attempts, then emits a batch and completes.</summary>
    private sealed class FlakySource : IObservable<int>
    {
        /// <summary>The number of failing attempts before the source succeeds.</summary>
        private const int FailingAttempts = 8;

        /// <summary>The number of values each attempt emits.</summary>
        private const int ValuesPerAttempt = 16;

        /// <summary>The number of subscriptions made so far.</summary>
        private int _attempts;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            var attempt = _attempts++;
            for (var i = 0; i < ValuesPerAttempt; i++)
            {
                observer.OnNext(i);
            }

            if (attempt < FailingAttempts)
            {
                observer.OnError(Boom);
            }
            else
            {
                observer.OnCompleted();
            }

            return NoopSubscription.Instance;
        }
    }

    /// <summary>Subscription handle with nothing to release.</summary>
    private sealed class NoopSubscription : IDisposable
    {
        /// <summary>Gets the shared instance.</summary>
        public static NoopSubscription Instance { get; } = new();

        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}
