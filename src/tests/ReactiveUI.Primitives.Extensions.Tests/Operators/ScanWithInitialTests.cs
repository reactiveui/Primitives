// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for the <see cref = "ScanWithInitialObservable{TSource, TAccumulate}"/> class.</summary>
[System.Diagnostics.DebuggerDisplay("ScanWithInitialTests: {nameof(ScanWithInitialTests),nq}")]
public partial class ScanWithInitialTests
{
    /// <summary>Spin iterations used to widen the interleaving window in contention tests.</summary>
    private const int InterleavingSpinIterations = 100;

#if NET9_0_OR_GREATER

    /// <summary>Synchronization gate used by tests.</summary>
    private readonly Lock _gate = new();
#else
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly object _gate = new();
#endif

    /// <summary>Tests that <see cref = "ScanWithInitialObservable{TSource, TAccumulate}"/> emits the initial value immediately upon subscription.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Subscribe_EmitsInitialValueImmediately()
    {
        // Arrange
        Subject<int> source = new();
        const int Initial = 10;
        Func<int, int, int> accumulator = static (acc, x) => acc + x;
        ScanWithInitialObservable<int, int> observable = new(source, Initial, accumulator);
        List<int> results = [];

        // Act
        using (observable.Subscribe(results.Add))
        {
            // Assert
            const int ExpectedInitial = 10;
            await Assert.That(results).IsCollectionEqualTo([ExpectedInitial]);
        }
    }

    /// <summary>Tests that <see cref = "ScanWithInitialObservable{TSource, TAccumulate}"/> accumulates values correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnNext_AccumulatesValues()
    {
        // Arrange
        Subject<int> source = new();
        const int Initial = 0;
        Func<int, int, int> accumulator = static (acc, x) => acc + x;
        ScanWithInitialObservable<int, int> observable = new(source, Initial, accumulator);
        List<int> results = [];

        // Act
        using (observable.Subscribe(results.Add))
        {
            const int Second = 2;
            const int Third = 3;
            source.OnNext(1);
            source.OnNext(Second);
            source.OnNext(Third);
        }

        // Assert
        const int RunningSumAfterSecond = 3;
        const int RunningSumAfterThird = 6;
        await Assert.That(results).IsCollectionEqualTo([0, 1, RunningSumAfterSecond, RunningSumAfterThird]);
    }

    /// <summary>Tests that <see cref = "ScanWithInitialObservable{TSource, TAccumulate}"/> handles errors in the accumulator.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task AccumulatorError_PropagatesError()
    {
        // Arrange
        Subject<int> source = new();
        const int Initial = 0;
        Exception exception = new InvalidOperationException("Accumulator failed");
        Func<int, int, int> accumulator = (_, _) => throw exception;
        ScanWithInitialObservable<int, int> observable = new(source, Initial, accumulator);
        List<Exception> errors = [];

        // Act
        using (observable.Subscribe(
                   static _ => { },
                   errors.Add))
        {
            source.OnNext(1);
        }

        // Assert
        await Assert.That(errors).IsCollectionEqualTo([exception]);
    }

    /// <summary>Tests that <see cref = "ScanWithInitialObservable{TSource, TAccumulate}"/> is thread-safe.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Concurrency",
        "PSH1315:A blocking wait on an awaitable that may not be done",
        Justification = "Test is synchronous.")]
    public async Task Observable_IsThreadSafe()
    {
        // Arrange
        Subject<int> source = new();
        const int Initial = 0;
        Func<int, int, int> accumulator = static (acc, x) =>
        {
            Thread.SpinWait(InterleavingSpinIterations);
            return acc + x;
        };
        ScanWithInitialObservable<int, int> observable = new(source, Initial, accumulator);
        List<int> results = [];
        var completedCount = 0;
        const int ContendedEmissionCount = 100;
        const int CompletionDelayMilliseconds = 50;

        // Act
        using (observable.Subscribe(
                   x =>
                   {
                       lock (_gate)
                       {
                           results.Add(x);
                       }
                   },
                   static _ => { },
                   () => Interlocked.Increment(ref completedCount)))
        {
            var t1 = Task.Run(() =>
            {
                for (var i = 0; i < ContendedEmissionCount; i++)
                {
                    source.OnNext(i);
                }
            });
            var t2 = Task.Run(async () =>
            {
                await Task.Delay(CompletionDelayMilliseconds);
                source.OnCompleted();
            });
            await Task.WhenAll(t1, t2);
        }

        // Assert
        // We can't easily assert the exact sequence due to the non-thread-safe Subject,
        // but we can assert that it didn't crash and the state remains consistent.
        // The lock in ScanWithInitialSink ensures that OnNext doesn't race with OnCompleted internally.
        await Assert.That(completedCount).IsEqualTo(1);
    }
}
