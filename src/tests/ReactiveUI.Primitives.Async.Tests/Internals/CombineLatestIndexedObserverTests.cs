// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests.Internals;

/// <summary>Tests for <see cref="SyncLatestIndexedWitness{TSource, TResult}"/>, the shared per-source observer that backs every per-arity CombineLatest subscription.</summary>
public class CombineLatestIndexedObserverTests
{
    /// <summary>The bitmask bit (bit 0) used by this test's single virtual source.</summary>
    private const int SourceBit = 1;

    /// <summary>Sentinel value driven into the observer's OnNextAsync.</summary>
    private const int Sentinel = 42;

    /// <summary>Verifies that OnNextAsync records the value via the closure and forwards the
    /// notification through the parent's <c>EmitLatestAsync</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAsync_ThenRecordsValueAndCallsEmitLatestAsync()
    {
        CaptureObserverAsync<int> captured = new();
        TestSubscription parent = new(captured);
        int? stored = null;
        SyncLatestIndexedWitness<int, int> observer = new(parent, SourceBit, v => stored = v);

        await observer.OnNextAsync(Sentinel, CancellationToken.None);

        await Assert.That(stored).IsEqualTo(Sentinel);
        await Assert.That(parent.EmitLatestCount).IsEqualTo(1);

        await parent.DisposeAsync();
    }

    /// <summary>Verifies that OnErrorResumeAsync forwards the error through the parent's lifecycle to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsync_ThenLifecycleForwardsError()
    {
        CaptureObserverAsync<int> captured = new();
        TestSubscription parent = new(captured);
        SyncLatestIndexedWitness<int, int> observer = new(parent, SourceBit, static _ => { });
        InvalidOperationException expected = new("forward");

        await observer.OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(captured.Errors).Count().IsEqualTo(1);
        await Assert.That(captured.Errors[0]).IsEqualTo(expected);

        await parent.DisposeAsync();
    }

    /// <summary>Verifies that OnCompletedAsync forwards completion through the parent's lifecycle,
    /// setting the matching source-bit in the completion bitmask.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAsync_ThenLifecycleForwardsCompletion()
    {
        CaptureObserverAsync<int> captured = new();
        TestSubscription parent = new(captured);
        SyncLatestIndexedWitness<int, int> observer = new(parent, SourceBit, static _ => { });

        await observer.OnCompletedAsync(Result.Success);

        // With sourceCount=1 (a single-source parent), the one source completing satisfies the
        // all-done mask so the downstream observer's OnCompletedAsync fires immediately.
        await Assert.That(captured.Completions).Count().IsEqualTo(1);
        await Assert.That(captured.Completions[0].IsSuccess).IsTrue();

        await parent.DisposeAsync();
    }

    /// <summary>Minimal concrete subclass exposing the base's EmitLatestAsync invocation count.</summary>
    /// <param name="observer">The downstream observer.</param>
    private sealed class TestSubscription(IObserverAsync<int> observer) : SyncLatestCoordinatorBase<int>(observer, 1)
    {
        /// <summary>Gets the number of times <see cref="EmitLatestAsync"/> has been invoked.</summary>
        public int EmitLatestCount { get; private set; }

        /// <inheritdoc/>
        internal override ValueTask EmitLatestAsync()
        {
            EmitLatestCount++;
            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken)
        {
            _ = index;
            _ = cancellationToken;
            return new(NoopDisposable.Instance);
        }
    }

    /// <summary>Capture observer used by the helper tests.</summary>
    /// <typeparam name="T">Element type captured.</typeparam>
    private sealed class CaptureObserverAsync<T> : IObserverAsync<T>
    {
        /// <summary>Gets the captured OnErrorResume exceptions in order.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the captured OnCompleted results in order.</summary>
        public List<Result> Completions { get; } = [];

        /// <summary>Gets the captured OnNext values in order.</summary>
        private List<T> Values { get; } = [];

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
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>No-op async disposable used as the SubscribeAtAsync return value.</summary>
    private sealed class NoopDisposable : IAsyncDisposable
    {
        /// <summary>Gets the shared no-op singleton.</summary>
        public static IAsyncDisposable Instance { get; } = new NoopDisposable();

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }
}
