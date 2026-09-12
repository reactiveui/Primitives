// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for the <see cref = "ScanWithInitialObservable{TSource, TAccumulate}"/> class.</summary>
[System.Diagnostics.DebuggerDisplay("ScanWithInitialTests: {nameof(ScanWithInitialTests),nq}")]
public partial class ScanWithInitialTests
{
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

    /// <summary>Tests that a terminal notification raised from inside an emission completes the
    /// sink exactly once and stops accumulating, which is the interleaving the sink's gate and
    /// <c>_done</c> latch exist to serialize.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnCompletedDuringEmission_CompletesOnceAndStopsAccumulating()
    {
        // Arrange
        Subject<int> source = new();
        const int Initial = 0;
        const int EmissionsBeforeCompletion = 3;
        const int EmissionCount = 100;
        Func<int, int, int> accumulator = static (acc, x) => acc + x;
        ScanWithInitialObservable<int, int> observable = new(source, Initial, accumulator);
        List<int> results = [];
        var completedCount = 0;

        // Act
        using (observable.Subscribe(
                   x =>
                   {
                       results.Add(x);
                       if (results.Count != EmissionsBeforeCompletion)
                       {
                           return;
                       }

                       source.OnCompleted();
                   },
                   static _ => { },
                   () => completedCount++))
        {
            for (var i = 0; i < EmissionCount; i++)
            {
                source.OnNext(i);
            }
        }

        // Assert
        using (Assert.Multiple())
        {
            await Assert.That(completedCount).IsEqualTo(1);
            await Assert.That(results).Count().IsEqualTo(EmissionsBeforeCompletion);
        }
    }
}
