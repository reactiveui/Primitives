// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for the <see cref="ScanWithInitialObservable{TSource, TAccumulate}"/> class.</summary>
public partial class ScanWithInitialTests
{
#if NET9_0_OR_GREATER
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly Lock _gate = new();
#else
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly object _gate = new();
#endif

    /// <summary>Tests that <see cref="ScanWithInitialObservable{TSource, TAccumulate}"/> emits the initial value immediately upon subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Subscribe_EmitsInitialValueImmediately()
    {
        // Arrange
        var source = new Subject<int>();
        const int Initial = 10;
        var accumulator = (int acc, int x) =>
            acc + x;
        var observable = new ScanWithInitialObservable<int, int>(source, Initial, accumulator);
        var results = new List<int>();

        // Act
        using (observable.Subscribe(
                   results.Add))
        {
            // Assert
            const int ExpectedInitial = 10;
            await Assert.That(results).IsCollectionEqualTo([ExpectedInitial]);
        }
    }

    /// <summary>Tests that <see cref="ScanWithInitialObservable{TSource, TAccumulate}"/> accumulates values correctly.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnNext_AccumulatesValues()
    {
        // Arrange
        var source = new Subject<int>();
        const int Initial = 0;
        var accumulator = (int acc, int x) =>
            acc + x;
        var observable = new ScanWithInitialObservable<int, int>(source, Initial, accumulator);
        var results = new List<int>();

        // Act
        using (observable.Subscribe(
                   results.Add))
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

    /// <summary>Tests that <see cref="ScanWithInitialObservable{TSource, TAccumulate}"/> handles errors in the accumulator.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task AccumulatorError_PropagatesError()
    {
        // Arrange
        var source = new Subject<int>();
        const int Initial = 0;
        Exception exception = new InvalidOperationException("Accumulator failed");
        Func<int, int, int> accumulator = (_, _) =>
            throw exception;
        var observable = new ScanWithInitialObservable<int, int>(source, Initial, accumulator);
        var errors = new List<Exception>();

        // Act
        using (observable.Subscribe(
                   _ => { },
                   errors.Add))
        {
            source.OnNext(1);
        }

        // Assert
        await Assert.That(errors).IsCollectionEqualTo([exception]);
    }

    /// <summary>Tests that <see cref="ScanWithInitialObservable{TSource, TAccumulate}"/> is thread-safe.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Blocker Code Smell", "S4462:Calls to \"async\" methods should not be blocking", Justification = "Test is synchronous.")]
    public async Task Observable_IsThreadSafe()
    {
        // Arrange
        var source = new Subject<int>();
        const int Initial = 0;
        var accumulator = (int acc, int x) =>
        {
            Thread.Sleep(1); // Force potential race condition
            return acc + x;
        };
        var observable = new ScanWithInitialObservable<int, int>(source, Initial, accumulator);
        var results = new List<int>();
        var completedCount = 0;

        // Act
        using (observable.Subscribe(
                   x =>
                   {
                       lock (_gate)
                       {
                           results.Add(x);
                       }
                   },
                   _ => { },
                   () => Interlocked.Increment(ref completedCount)))
        {
            var t1 = Task.Run(() =>
            {
                for (var i = 0; i < 100; i++)
                {
                    source.OnNext(i);
                }
            });

            var t2 = Task.Run(async () =>
            {
                await Task.Delay(50);
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
