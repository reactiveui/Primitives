// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the cold signal that emits a value on subscription and again whenever it changes.</summary>
public sealed class CurrentValueSignalTests
{
    /// <summary>The second value a test sets.</summary>
    private const int Second = 2;

    /// <summary>Every subscriber attaches its own hook and receives its own initial value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EverySubscriberReceivesItsOwnInitialValue()
    {
        ObservedValue source = new() { Value = 1 };
        CurrentValueSignal<int> signal = new(() => source.Value, source.Attach);
        RecordingWitness<int> first = new();
        RecordingWitness<int> second = new();

        using var firstSubscription = signal.Subscribe(first);
        using var secondSubscription = signal.Subscribe(second);

        await Assert.That(string.Join(",", first.Values)).IsEqualTo("1");
        await Assert.That(string.Join(",", second.Values)).IsEqualTo("1");
        await Assert.That(source.AttachedCount).IsEqualTo(Second);
    }

    /// <summary>A raised change is delivered, and after disposal the hook is detached and nothing more arrives.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ChangesAreDeliveredUntilDisposal()
    {
        ObservedValue source = new() { Value = 1 };
        RecordingWitness<int> downstream = new();
        var subscription = new CurrentValueSignal<int>(() => source.Value, source.Attach).Subscribe(downstream);

        source.Set(Second);
        subscription.Dispose();
        subscription.Dispose();
        source.Set(1);

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("1,2");
        await Assert.That(source.AttachedCount).IsEqualTo(0);
    }

    /// <summary>With a comparer, a change to an equal value is skipped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ComparerSkipsEqualValues()
    {
        ObservedValue source = new() { Value = 1 };
        RecordingWitness<int> downstream = new();
        using var subscription = new CurrentValueSignal<int>(() => source.Value, source.Attach, EqualityComparer<int>.Default)
            .Subscribe(downstream);

        source.Set(1);
        source.Set(Second);

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("1,2");
    }

    /// <summary>A change raised while the hook is attached is covered by the initial read and emitted once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ChangeRaisedWhileAttachingIsEmittedOnce()
    {
        ObservedValue source = new() { Value = 1 };
        RecordingWitness<int> downstream = new();
        using var subscription = new CurrentValueSignal<int>(
                () => source.Value,
                changed =>
                {
                    var handle = source.Attach(changed);
                    source.Set(Second);
                    return handle;
                })
            .Subscribe(downstream);

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("2");
    }

    /// <summary>A read that throws during subscription detaches the hook and propagates out of subscribe.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ThrowingInitialReadDetachesTheHook()
    {
        ObservedValue source = new();
        CurrentValueSignal<int> signal = new(static () => throw new InvalidOperationException("read"), source.Attach);

        await Assert.That(() => signal.Subscribe(new RecordingWitness<int>())).Throws<InvalidOperationException>();
        await Assert.That(source.AttachedCount).IsEqualTo(0);
    }

    /// <summary>The constructor and subscribe reject null arguments.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NullArgumentsAreRejected()
    {
        ObservedValue source = new();
        CurrentValueSignal<int> signal = new(() => source.Value, source.Attach);

        await Assert.That(() => new CurrentValueSignal<int>(null!, source.Attach)).Throws<ArgumentNullException>();
        await Assert.That(() => new CurrentValueSignal<int>(() => source.Value, null!)).Throws<ArgumentNullException>();
        await Assert.That(() => signal.Subscribe(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>A value with change callbacks, standing in for a property with a change event.</summary>
    private sealed class ObservedValue
    {
        /// <summary>The attached change callbacks.</summary>
        private readonly List<Action> _callbacks = [];

        /// <summary>Gets or sets the current value.</summary>
        public int Value { get; set; }

        /// <summary>Gets the number of attached callbacks.</summary>
        public int AttachedCount => _callbacks.Count;

        /// <summary>Attaches a change callback.</summary>
        /// <param name="changed">The callback.</param>
        /// <returns>The handle that detaches it.</returns>
        public IDisposable Attach(Action changed)
        {
            _callbacks.Add(changed);
            return new Detach(this, changed);
        }

        /// <summary>Sets the value and raises the change.</summary>
        /// <param name="value">The new value.</param>
        public void Set(int value)
        {
            Value = value;
            foreach (var callback in _callbacks.ToArray())
            {
                callback();
            }
        }

        /// <summary>Removes a callback when disposed.</summary>
        /// <param name="owner">The value the callback is attached to.</param>
        /// <param name="changed">The callback.</param>
        private sealed class Detach(ObservedValue owner, Action changed) : IDisposable
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => owner._callbacks.Remove(changed);
        }
    }
}
