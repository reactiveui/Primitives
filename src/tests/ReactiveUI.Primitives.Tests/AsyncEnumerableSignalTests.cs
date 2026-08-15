// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="AsyncEnumerableSignal{T}"/>.</summary>
public sealed class AsyncEnumerableSignalTests
{
    /// <summary>Values the source enumerable yields, and therefore the values the observer must observe.</summary>
    private static readonly int[] SourceValues = [1, 2, 3];

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
        source.ReleaseMoveNext(true);

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
        source.ReleaseMoveNext(false);

        await source.Disposed.Task.WaitAsync(token);

        // Dispose again to confirm the idempotent disposer never reaches the enumerator twice.
        subscription.Dispose();

        await Assert.That(source.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies disposal disposes a non-cooperative enumerator without waiting on its <c>MoveNextAsync</c>.</summary>
    /// <param name="token">The test cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Timeout(30_000)]
    public async Task DisposeDisposesNonCooperativeEnumeratorWithoutHanging(CancellationToken token)
    {
        NeverCompletingAsyncEnumerable source = new();
        AsyncEnumerableSignal<int> signal = new(source, CancellationToken.None);
        RecordingWitness<int> observer = new();

        var subscription = signal.Subscribe(observer);

        // Wait until the pump is parked inside the MoveNextAsync that never completes.
        await source.MoveNextEntered.Task.WaitAsync(token);

        // Disposal must dispose the enumerator promptly even though MoveNextAsync ignores cancellation.
        subscription.Dispose();

        await source.Disposed.Task.WaitAsync(token);

        // Dispose again to confirm the second call never reaches the enumerator.
        subscription.Dispose();

        await Assert.That(source.DisposeCount).IsEqualTo(1);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).IsEmpty();
    }

    /// <summary>Verifies a normally completing sequence delivers all values then completes.</summary>
    /// <param name="token">The test cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Timeout(30_000)]
    public async Task CompletesNormallyAndDeliversAllValues(CancellationToken token)
    {
        AsyncEnumerableSignal<int> signal = new(new CountingAsyncEnumerable(SourceValues), CancellationToken.None);
        RecordingWitness<int> observer = new();

        _ = signal.Subscribe(observer);

        await observer.CompletedSignal.Task.WaitAsync(token);

        await Assert.That(observer.Values.SequenceEqual(SourceValues)).IsTrue();
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>An async enumerable whose <c>MoveNextAsync</c> never completes and ignores cancellation.</summary>
    private sealed class NeverCompletingAsyncEnumerable : IAsyncEnumerable<int>, IAsyncEnumerator<int>
    {
        /// <summary>The task that never completes, modelling a non-cooperative source.</summary>
        private readonly TaskCompletionSource<bool> _never =
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
        public int Current => 0;

        /// <inheritdoc/>
        public IAsyncEnumerator<int> GetAsyncEnumerator(CancellationToken cancellationToken = default) => this;

        /// <inheritdoc/>
        public async ValueTask<bool> MoveNextAsync()
        {
            _ = MoveNextEntered.TrySetResult();

            // Deliberately ignores the cancellation token: the pump must not depend on this completing.
            return await _never.Task.ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            _ = Disposed.TrySetResult();

            // Release the parked MoveNextAsync so the pump task can drain without leaking.
            _ = _never.TrySetResult(false);
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
        public ValueTask<bool> MoveNextAsync()
        {
            _index++;
            return new(_index < _values.Length);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => Values.Add(value);
    }
}
