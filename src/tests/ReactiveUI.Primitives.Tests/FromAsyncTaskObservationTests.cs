// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies observer failures during task result delivery.</summary>
public class FromAsyncTaskObservationTests
{
    /// <summary>The task result passed to the observer.</summary>
    private const int ResultValue = 7;

    /// <summary>A throwing value callback receives no additional terminal notification.</summary>
    /// <param name="dispose">Whether the observer disposes its lifetime before throwing.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ThrowingValueCallbackSuppressesTerminalNotification(bool dispose)
    {
        using AsyncSubscriptionLifetime lifetime = new();
        FailingObserver observer = new(lifetime, dispose);
        using FromAsyncExternalCancellation<int> cancellation = new(observer, lifetime, CancellationToken.None);
        FromAsyncTaskObservation<int> observation = new(observer, lifetime, cancellation, null);
        observation.Observe(Task.FromResult(ResultValue));
        await Assert.That(observer.Values).IsEqualTo(1);
        await Assert.That(observer.Errors).IsEqualTo(0);
        await Assert.That(observer.Completions).IsEqualTo(0);
        await Assert.That(lifetime.IsCompleted).IsTrue();
    }

    /// <summary>An uncancelable external token permits synchronous factory execution.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task UncancelableTokenRunsFactory()
    {
        TaskNotificationObserver observer = new();
        using var subscription = Signal.FromAsync(static _ => Task.FromResult(ResultValue), CancellationToken.None).Subscribe(observer);
        await Assert.That(observer.Values.SequenceEqual([ResultValue])).IsTrue();
        await Assert.That(observer.Completions).IsEqualTo(1);
        await Assert.That(observer.Error).IsNull();
    }

    /// <summary>A task fault is forwarded once and completes the subscription lifetime.</summary>
    /// <param name="aggregate">Whether the supplied failure is an aggregate with no inner exceptions.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Observe_FaultedTask_ForwardsOriginalFailure(bool aggregate)
    {
        using AsyncSubscriptionLifetime lifetime = new();
        RecordingWitness<int> observer = new();
        using FromAsyncExternalCancellation<int> cancellation = new(observer, lifetime, CancellationToken.None);
        FromAsyncTaskObservation<int> observation = new(observer, lifetime, cancellation, null);
        Exception expected = aggregate ? new AggregateException() : new InvalidOperationException("task failed");

        observation.Observe(Task.FromException<int>(expected));

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(lifetime.IsCompleted).IsTrue();
    }

    /// <summary>A fault arriving after disposal does not notify the canceled observer.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task Observe_DisposedLifetime_DropsTaskFailure()
    {
        AsyncSubscriptionLifetime lifetime = new();
        RecordingWitness<int> observer = new();
        using FromAsyncExternalCancellation<int> cancellation = new(observer, lifetime, CancellationToken.None);
        FromAsyncTaskObservation<int> observation = new(observer, lifetime, cancellation, null);
        lifetime.Dispose();

        observation.Observe(Task.FromException<int>(new InvalidOperationException("task failed")));

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Errors).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(lifetime.IsCompleted).IsTrue();
    }

    /// <summary>Throws after recording a value and optionally disposing its lifetime.</summary>
    /// <param name="lifetime">The subscription lifetime.</param>
    /// <param name="dispose">Whether to dispose before throwing.</param>
    private sealed class FailingObserver(AsyncSubscriptionLifetime lifetime, bool dispose) : IObserver<int>
    {
        /// <summary>Gets the number of value callbacks.</summary>
        public int Values { get; private set; }

        /// <summary>Gets the number of error callbacks.</summary>
        public int Errors { get; private set; }

        /// <summary>Gets the number of completion callbacks.</summary>
        public int Completions { get; private set; }

        /// <inheritdoc/>
        public void OnNext(int value)
        {
            Values++;
            if (dispose)
            {
                lifetime.Dispose();
            }

            throw new InvalidOperationException("observer failure");
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => Errors++;

        /// <inheritdoc/>
        public void OnCompleted() => Completions++;
    }
}
