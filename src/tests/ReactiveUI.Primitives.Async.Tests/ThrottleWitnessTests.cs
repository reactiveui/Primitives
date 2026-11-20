// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="ThrottleWitness{T}"/>.</summary>
public sealed class ThrottleWitnessTests
{
    /// <summary>The value sent without a cancellable token.</summary>
    private const int FirstValue = 1;

    /// <summary>The value sent with a cancellable token.</summary>
    private const int SecondValue = 2;

    /// <summary>The throttle window.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromMilliseconds(20);

    /// <summary>How long a test waits for an asynchronous hand-off before failing.</summary>
    private static readonly TimeSpan HandOffTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies a value sent without a token and a value sent with a cancellable token are both delivered once the window elapses.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNextAsync_WithAndWithoutCancellableToken_DeliversAfterWindow()
    {
        ManualTimeProvider time = new();
        TaskCompletionSource first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource second = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> values = [];
        CallbackWitnessAsync<int> observer = new((value, _) =>
        {
            lock (values)
            {
                values.Add(value);
            }

            (value == FirstValue ? first : second).SetResult();
            return default;
        });
        await using ThrottleWitness<int> witness = new(observer, Window, time);
        using CancellationTokenSource cancellable = new();

        await witness.OnNextAsync(FirstValue, CancellationToken.None);
        await time.FireNextAsync();
        await Assert.That(first.Task).CompletesWithin(HandOffTimeout);

        await witness.OnNextAsync(SecondValue, cancellable.Token);
        await time.FireNextAsync();
        await Assert.That(second.Task).CompletesWithin(HandOffTimeout);

        await Assert.That(values).IsCollectionEqualTo([FirstValue, SecondValue]);
    }
}
