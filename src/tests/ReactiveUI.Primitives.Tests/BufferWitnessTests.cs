// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests buffered observer termination.</summary>
public class BufferWitnessTests
{
    /// <summary>A window holding two values.</summary>
    private const int WindowOfTwo = 2;

    /// <summary>A window holding three values.</summary>
    private const int WindowOfThree = 3;

    /// <summary>The length of the shorter source sequence.</summary>
    private const int FiveValues = 5;

    /// <summary>The length of the longer source sequence.</summary>
    private const int SevenValues = 7;

    /// <summary>A duplicate error cannot emit a partial batch or notify the observer twice.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_Repeated_ForwardsOnlyFirstError()
    {
        const int WindowSize = 2;
        RecordingWitness<IList<int>> observer = new();
        RecordingDisposable subscription = new();
        using BufferWitness<int> witness = new(observer, WindowSize, 0);
        witness.SetSubscription(subscription);
        witness.OnNext(1);
        InvalidOperationException error = new("source");

        witness.OnError(error);
        witness.OnError(new InvalidOperationException("late"));
        witness.OnCompleted();

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A skip below the window size opens a new window every skip values, so the windows overlap.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Buffer_SkipBelowCount_OverlapsWindows()
    {
        var batches = Collect(Sequence(FiveValues), WindowOfThree, skip: 1);

        await Assert.That(batches).IsEqualTo("[1,2,3] [2,3,4] [3,4,5] [4,5] [5]");
    }

    /// <summary>A skip below the window size that does not divide it still steps one window per skip values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Buffer_SkipBelowCountAndNotAFactor_StepsBySkip()
    {
        var batches = Collect(Sequence(SevenValues), WindowOfThree, WindowOfTwo);

        await Assert.That(batches).IsEqualTo("[1,2,3] [3,4,5] [5,6,7] [7]");
    }

    /// <summary>A skip above the window size leaves a gap between windows, dropping the values in it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Buffer_SkipAboveCount_LeavesAGapBetweenWindows()
    {
        var batches = Collect(Sequence(SevenValues), WindowOfTwo, WindowOfThree);

        await Assert.That(batches).IsEqualTo("[1,2] [4,5] [7]");
    }

    /// <summary>A skip equal to the window size keeps the windows consecutive and non-overlapping.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Buffer_SkipEqualToCount_KeepsWindowsConsecutive()
    {
        var batches = Collect(Sequence(SevenValues), WindowOfThree, WindowOfThree);

        await Assert.That(batches).IsEqualTo("[1,2,3] [4,5,6] [7]");
    }

    /// <summary>The window-size overload keeps the windows consecutive and non-overlapping.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Buffer_CountOnly_KeepsWindowsConsecutive()
    {
        var batches = Collect(Sequence(SevenValues), WindowOfThree, skip: 0);

        await Assert.That(batches).IsEqualTo("[1,2,3] [4,5,6] [7]");
    }

    /// <summary>Builds the values 1 through <paramref name="length"/>.</summary>
    /// <param name="length">The number of values.</param>
    /// <returns>The values to push.</returns>
    private static int[] Sequence(int length) => [.. Enumerable.Range(1, length)];

    /// <summary>Pushes the values through a buffering sink and renders the batches it emits.</summary>
    /// <param name="source">The values to push.</param>
    /// <param name="count">The window size.</param>
    /// <param name="skip">The number of values between window starts.</param>
    /// <returns>The batches the sink emitted, rendered in order.</returns>
    private static string Collect(int[] source, int count, int skip)
    {
        RecordingWitness<IList<int>> observer = new();
        using BufferWitness<int> witness = new(observer, count, skip);
        witness.SetSubscription(new RecordingDisposable());

        foreach (var value in source)
        {
            witness.OnNext(value);
        }

        witness.OnCompleted();
        return string.Join(" ", observer.Values.Select(static batch => $"[{string.Join(",", batch)}]"));
    }
}
