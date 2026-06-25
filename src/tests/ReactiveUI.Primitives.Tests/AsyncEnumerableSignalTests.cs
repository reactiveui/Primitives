// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="AsyncEnumerableSignal{T}"/>.</summary>
public sealed class AsyncEnumerableSignalTests
{
    /// <summary>Verifies a value buffered while disposal tears down the observer is not delivered.</summary>
    /// <param name="token">The test cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Timeout(30_000)]
    public async Task DisposeDuringMoveNextSuppressesBufferedOnNext(CancellationToken token)
    {
        GatedAsyncEnumerable source = new();
        AsyncEnumerableSignal<int> signal = new(source, CancellationToken.None);
        RecordingWitness<int> observer = new();

        var subscription = signal.Subscribe(observer);

        // Wait until the pump is parked inside MoveNextAsync with a value ready to emit.
        await source.MoveNextEntered.Task.WaitAsync(token);

        // Dispose mid-flight, then let the buffered MoveNextAsync complete with a value.
        subscription.Dispose();
        source.ReleaseMoveNext(result: true);

        // Wait for the pump to drain so any (incorrect) emission would already have happened.
        await source.Disposed.Task.WaitAsync(token);

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).IsEmpty();
    }

    /// <summary>Verifies the enumerator is disposed exactly once when disposal races the pump.</summary>
    /// <param name="token">The test cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Timeout(30_000)]
    public async Task DisposeAfterCompletionDisposesEnumeratorExactlyOnce(CancellationToken token)
    {
        GatedAsyncEnumerable source = new();
        AsyncEnumerableSignal<int> signal = new(source, CancellationToken.None);
        RecordingWitness<int> observer = new();

        var subscription = signal.Subscribe(observer);

        await source.MoveNextEntered.Task.WaitAsync(token);

        // Dispose while parked, then end the sequence; both disposal and the pump's finally run.
        subscription.Dispose();
        source.ReleaseMoveNext(result: false);

        await source.Disposed.Task.WaitAsync(token);

        // Dispose again to confirm the idempotent disposer never reaches the enumerator twice.
        subscription.Dispose();

        await Assert.That(source.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies a normally completing sequence delivers all values then completes.</summary>
    /// <param name="token">The test cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Timeout(30_000)]
    public async Task CompletesNormallyAndDeliversAllValues(CancellationToken token)
    {
        int[] values = [1, 2, 3];
        AsyncEnumerableSignal<int> signal = new(new CountingAsyncEnumerable(values), CancellationToken.None);
        RecordingWitness<int> observer = new();

        _ = signal.Subscribe(observer);

        await observer.CompletedSignal.Task.WaitAsync(token);

        await Assert.That(observer.Values.SequenceEqual(values)).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors).IsEmpty();
    }

    /// <summary>An async enumerable whose single <c>MoveNextAsync</c> blocks on an external gate.</summary>
    private sealed class GatedAsyncEnumerable : IAsyncEnumerable<int>, IAsyncEnumerator<int>
    {
        /// <summary>The gate released to complete the pending <c>MoveNextAsync</c>.</summary>
        private readonly TaskCompletionSource<bool> _moveNextGate =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal completed once the pump enters <c>MoveNextAsync</c>.</summary>
        public TaskCompletionSource MoveNextEntered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal completed once the enumerator is disposed.</summary>
        public TaskCompletionSource Disposed { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of times the enumerator has been disposed.</summary>
        public int DisposeCount { get; private set; }

        /// <summary>Gets the current value.</summary>
        public int Current { get; } = 42;

        /// <summary>Releases the gated <c>MoveNextAsync</c> with the given outcome.</summary>
        /// <param name="result">The value returned by <c>MoveNextAsync</c>.</param>
        public void ReleaseMoveNext(bool result) => _moveNextGate.TrySetResult(result);

        /// <inheritdoc/>
        public IAsyncEnumerator<int> GetAsyncEnumerator(CancellationToken cancellationToken = default) => this;

        /// <inheritdoc/>
        public async ValueTask<bool> MoveNextAsync()
        {
            _ = MoveNextEntered.TrySetResult();
            return await _moveNextGate.Task.ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            _ = Disposed.TrySetResult();
            return default;
        }
    }

    /// <summary>An async enumerable that yields a fixed set of values.</summary>
    private sealed class CountingAsyncEnumerable : IAsyncEnumerable<int>, IAsyncEnumerator<int>
    {
        /// <summary>The values to yield in order.</summary>
        private readonly int[] _values;

        /// <summary>The current zero-based position, or -1 before the first move.</summary>
        private int _index = -1;

        /// <summary>Initializes a new instance of the <see cref="CountingAsyncEnumerable"/> class.</summary>
        /// <param name="values">The values to yield.</param>
        public CountingAsyncEnumerable(int[] values) => _values = values;

        /// <summary>Gets the current value.</summary>
        public int Current => _values[_index];

        /// <inheritdoc/>
        public IAsyncEnumerator<int> GetAsyncEnumerator(CancellationToken cancellationToken = default) => this;

        /// <inheritdoc/>
        public ValueTask<bool> MoveNextAsync() => new(++_index < _values.Length);

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Records observer values and terminal signals.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingWitness<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the completion count.</summary>
        public int Completed { get; private set; }

        /// <summary>Gets the observed errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the signal completed when the observer is completed.</summary>
        public TaskCompletionSource CompletedSignal { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public void OnCompleted()
        {
            Completed++;
            _ = CompletedSignal.TrySetResult();
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }
}
