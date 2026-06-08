// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Direct RxVoid tests for <see cref="ObserverArrayHelpers"/> — both the broadcast
/// loop and the remove-or-null short-circuit paths. The helpers are pure functions over
/// their inputs, so each branch is exercised by passing synthesized arrays rather than
/// relying on operator-level scheduler races.</summary>
public class ObserverArrayHelpersTests
{
    /// <summary>Sentinel value broadcast through the helper.</summary>
    private const int Sentinel = 7;

    /// <summary>Expected length of the array after removing one observer from three.</summary>
    private const int RemainingLengthAfterRemoveFromThree = 2;

    /// <summary>Verifies <see cref="ObserverArrayHelpers.Broadcast{T}"/> short-circuits when the observer array is empty.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBroadcastEmpty_ThenNoOp()
    {
        var observers = Array.Empty<IObserver<int>>();

        ObserverArrayHelpers.Broadcast(observers, Sentinel);

        await Assert.That(observers).IsEmpty();
    }

    /// <summary>Verifies <see cref="ObserverArrayHelpers.Broadcast{T}"/> fans the value out to every observer in order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBroadcastMultiple_ThenEveryObserverReceivesValue()
    {
        var first = new RecordingObserver<int>();
        var second = new RecordingObserver<int>();
        var third = new RecordingObserver<int>();
        IObserver<int>[] observers = [first, second, third];

        ObserverArrayHelpers.Broadcast(observers, Sentinel);

        await Assert.That(first.Values).IsCollectionEqualTo([Sentinel]);
        await Assert.That(second.Values).IsCollectionEqualTo([Sentinel]);
        await Assert.That(third.Values).IsCollectionEqualTo([Sentinel]);
    }

    /// <summary>Verifies <see cref="ObserverArrayHelpers.RemoveOrNull{T}"/> returns <see langword="null"/> when the observer is not present in the array.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRemoveNotPresent_ThenReturnsNull()
    {
        var empty = Array.Empty<IObserver<int>>();
        var resident = new RecordingObserver<int>();
        var stranger = new RecordingObserver<int>();
        IObserver<int>[] current = [resident];

        var result = ObserverArrayHelpers.RemoveOrNull(current, stranger, empty);

        await Assert.That(result).IsNull();
    }

    /// <summary>Verifies <see cref="ObserverArrayHelpers.RemoveOrNull{T}"/> returns the empty sentinel when the array contains exactly one observer (the one being removed).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRemoveSingleton_ThenReturnsEmptySentinel()
    {
        var empty = Array.Empty<IObserver<int>>();
        var only = new RecordingObserver<int>();
        IObserver<int>[] current = [only];

        var result = ObserverArrayHelpers.RemoveOrNull(current, only, empty);

        await Assert.That(result).IsSameReferenceAs(empty);
    }

    /// <summary>Verifies <see cref="ObserverArrayHelpers.RemoveOrNull{T}"/> removes the first observer from a multi-element array (no left copy, full right copy).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRemoveFirstFromThree_ThenLeavesTrailingPair()
    {
        var empty = Array.Empty<IObserver<int>>();
        var a = new RecordingObserver<int>();
        var b = new RecordingObserver<int>();
        var c = new RecordingObserver<int>();
        IObserver<int>[] current = [a, b, c];

        var result = ObserverArrayHelpers.RemoveOrNull(current, a, empty);

        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Length).IsEqualTo(RemainingLengthAfterRemoveFromThree);
        await Assert.That(ReferenceEquals(result[0], b)).IsTrue();
        await Assert.That(ReferenceEquals(result[1], c)).IsTrue();
    }

    /// <summary>Verifies <see cref="ObserverArrayHelpers.RemoveOrNull{T}"/> removes the middle observer (both left and right copies non-empty).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRemoveMiddleFromThree_ThenLeavesFirstAndLast()
    {
        var empty = Array.Empty<IObserver<int>>();
        var a = new RecordingObserver<int>();
        var b = new RecordingObserver<int>();
        var c = new RecordingObserver<int>();
        IObserver<int>[] current = [a, b, c];

        var result = ObserverArrayHelpers.RemoveOrNull(current, b, empty);

        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Length).IsEqualTo(RemainingLengthAfterRemoveFromThree);
        await Assert.That(ReferenceEquals(result[0], a)).IsTrue();
        await Assert.That(ReferenceEquals(result[1], c)).IsTrue();
    }

    /// <summary>Verifies <see cref="ObserverArrayHelpers.RemoveOrNull{T}"/> removes the last observer (full left copy, no right copy).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRemoveLastFromThree_ThenLeavesLeadingPair()
    {
        var empty = Array.Empty<IObserver<int>>();
        var a = new RecordingObserver<int>();
        var b = new RecordingObserver<int>();
        var c = new RecordingObserver<int>();
        IObserver<int>[] current = [a, b, c];

        var result = ObserverArrayHelpers.RemoveOrNull(current, c, empty);

        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Length).IsEqualTo(RemainingLengthAfterRemoveFromThree);
        await Assert.That(ReferenceEquals(result[0], a)).IsTrue();
        await Assert.That(ReferenceEquals(result[1], b)).IsTrue();
    }

    /// <summary>Recording observer used to verify <c>Broadcast</c> reaches each slot.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>Gets the captured <c>OnNext</c> values in order.</summary>
        public List<T> Values { get; } = [];

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }
}
