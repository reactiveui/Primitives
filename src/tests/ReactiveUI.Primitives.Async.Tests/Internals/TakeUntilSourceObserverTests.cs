// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests.Internals;

/// <summary>Tests for <see cref="TakeUntilSourceWitness{T}"/>, the shared async observer that forwards every source notification into a <see cref="TakeUntilLifecycle{T}"/>.</summary>
public class TakeUntilSourceObserverTests
{
    /// <summary>Sentinel value used by the value-forward test.</summary>
    private const int SentinelValue = 42;

    /// <summary>Verifies that OnNextAsync forwards the value through the lifecycle to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAsync_ThenLifecycleForwardsValueToDownstream()
    {
        CaptureObserverAsync<int> captured = new();
        TakeUntilLifecycle<int> lifecycle = new(captured);
        TakeUntilSourceWitness<int> observer = new(lifecycle);

        await observer.OnNextAsync(SentinelValue, CancellationToken.None);

        await Assert.That(captured.Values).IsCollectionEqualTo([SentinelValue]);

        await lifecycle.DisposeAsync();
    }

    /// <summary>Verifies that OnErrorResumeAsync forwards the error through the lifecycle to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsync_ThenLifecycleForwardsErrorToDownstream()
    {
        CaptureObserverAsync<int> captured = new();
        TakeUntilLifecycle<int> lifecycle = new(captured);
        TakeUntilSourceWitness<int> observer = new(lifecycle);
        InvalidOperationException expected = new("forward");

        await observer.OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(captured.Errors).Count().IsEqualTo(1);
        await Assert.That(captured.Errors[0]).IsEqualTo(expected);

        await lifecycle.DisposeAsync();
    }

    /// <summary>Verifies that OnCompletedAsync forwards the terminal result through the lifecycle.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAsync_ThenLifecycleForwardsCompletionToDownstream()
    {
        CaptureObserverAsync<int> captured = new();
        TakeUntilLifecycle<int> lifecycle = new(captured);
        TakeUntilSourceWitness<int> observer = new(lifecycle);

        await observer.OnCompletedAsync(Result.Success);

        await Assert.That(captured.Completions).Count().IsEqualTo(1);
        await Assert.That(captured.Completions[0].IsSuccess).IsTrue();

        await lifecycle.DisposeAsync();
    }

    /// <summary>Verifies that <c>LinkExternalCancellation</c> short-circuits for a non-cancellable token.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkExternalCancellationNonCancellable_ThenNoOp()
    {
        CaptureObserverAsync<int> captured = new();
        TakeUntilLifecycle<int> lifecycle = new(captured);

        lifecycle.LinkExternalCancellation(CancellationToken.None);

        await Assert.That(lifecycle.DisposeToken.IsCancellationRequested).IsFalse();
        await lifecycle.DisposeAsync();
    }

    /// <summary>Verifies that an already-cancelled external token immediately fires the dispose CTS.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkExternalCancellationAlreadyCancelled_ThenDisposeTokenFires()
    {
        CaptureObserverAsync<int> captured = new();
        TakeUntilLifecycle<int> lifecycle = new(captured);
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        lifecycle.LinkExternalCancellation(cts.Token);

        await Assert.That(lifecycle.DisposeToken.IsCancellationRequested).IsTrue();
        await lifecycle.DisposeAsync();
    }

    /// <summary>Verifies that a later cancellation on the linked external token propagates to the dispose CTS.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkExternalCancellationCancellable_ThenLaterCancelPropagates()
    {
        CaptureObserverAsync<int> captured = new();
        TakeUntilLifecycle<int> lifecycle = new(captured);
        using CancellationTokenSource cts = new();

        lifecycle.LinkExternalCancellation(cts.Token);
        await Assert.That(lifecycle.DisposeToken.IsCancellationRequested).IsFalse();

        await cts.CancelAsync();
        await Assert.That(lifecycle.DisposeToken.IsCancellationRequested).IsTrue();

        await lifecycle.DisposeAsync();
    }

    /// <summary>Capture observer used by the helper tests.</summary>
    /// <typeparam name="T">Element type captured.</typeparam>
    private sealed class CaptureObserverAsync<T> : IObserverAsync<T>
    {
        /// <summary>Gets the captured OnNext values in order.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the captured OnErrorResume exceptions in order.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the captured OnCompleted results in order.</summary>
        public List<Result> Completions { get; } = [];

        /// <inheritdoc/>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            Values.Add(value);
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            Errors.Add(error);
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result)
        {
            Completions.Add(result);
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
