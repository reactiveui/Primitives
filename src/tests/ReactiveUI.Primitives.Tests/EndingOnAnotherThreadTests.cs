// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies that operators which subscribe to one source after another keep going when a source ends on another thread. The
/// ending can arrive before the subscribing returns or at the same moment as it, and must not be lost either way.
/// </summary>
public sealed class EndingOnAnotherThreadTests
{
    /// <summary>The sources subscribed to one after another, enough for an ending to meet a subscribing many times.</summary>
    private const int Sources = 50_000;

    /// <summary>The longest a run may take before the operator is taken to be waiting for an ending it lost.</summary>
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    /// <summary>Verifies a repeated source is subscribed to the requested number of times when each run ends on another thread.</summary>
    /// <param name="waitsForEnd">Whether each run has ended on the other thread before its subscribing returns.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task RepeatSubscribesAgainAfterAnEndingOnAnotherThread(bool waitsForEnd)
    {
        var values = await Count(new EndsElsewhere(waitsForEnd).Repeat(Sources));

        await Assert.That(values).IsEqualTo(Sources);
    }

    /// <summary>Verifies an unbounded repeat stopped by <c>Take</c> gives every value when each run ends on another thread.</summary>
    /// <param name="waitsForEnd">Whether each run has ended on the other thread before its subscribing returns.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task UnboundedRepeatKeepsGoingAfterAnEndingOnAnotherThread(bool waitsForEnd)
    {
        var values = await Count(new EndsElsewhere(waitsForEnd).Repeat().Take(Sources));

        await Assert.That(values).IsEqualTo(Sources);
    }

    /// <summary>Verifies a concatenation reads every source when each ends on another thread.</summary>
    /// <param name="waitsForEnd">Whether each source has ended on the other thread before its subscribing returns.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task ConcatReadsEverySourceThatEndsOnAnotherThread(bool waitsForEnd)
    {
        var values = await Count(Signal.Concat(Enumerable.Range(0, Sources).Select(_ => (IObservable<int>)new EndsElsewhere(waitsForEnd))));

        await Assert.That(values).IsEqualTo(Sources);
    }

    /// <summary>Counts the values of a source, failing when it does not end in time.</summary>
    /// <param name="source">The source.</param>
    /// <returns>The number of values.</returns>
    private static async Task<int> Count(IObservable<int> source)
    {
        var count = 0;
        TaskCompletionSource<int> ended = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = source.Subscribe(
            _ => Interlocked.Increment(ref count),
            ended.SetException,
            () => ended.SetResult(Volatile.Read(ref count)));
        return await ended.Task.WaitAsync(Patience);
    }

    /// <summary>A source that gives one value and ends on a pool thread.</summary>
    /// <param name="waitsForEnd">Whether subscribing returns only once the source has ended on the other thread.</param>
    private sealed class EndsElsewhere(bool waitsForEnd) : IObservable<int>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            using ManualResetEventSlim ended = new();
            var done = ended;
            _ = ThreadPool.UnsafeQueueUserWorkItem(
                _ =>
                {
                    observer.OnNext(1);
                    observer.OnCompleted();
                    if (waitsForEnd)
                    {
                        done.Set();
                    }
                },
                null);
            if (waitsForEnd)
            {
                ended.Wait();
            }

            return Nothing.Instance;
        }
    }

    /// <summary>A subscription with nothing to stop.</summary>
    private sealed class Nothing : IDisposable
    {
        /// <summary>The one instance.</summary>
        public static readonly Nothing Instance = new();

        /// <inheritdoc/>
        public void Dispose()
        {
            // The source has ended or ends by itself.
        }
    }
}
