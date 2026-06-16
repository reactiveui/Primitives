// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="DelayableNotificationSignal{T}"/>.</summary>
public sealed class DelayableNotificationSignalTests
{
    /// <summary>A buffered notification value used across tests.</summary>
    private const string Buffered = "buffered";

    /// <summary>Constructor validates its delegate arguments.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDelegates()
    {
        await Assert.That(() => new DelayableNotificationSignal<string>(null!, static items => items)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new DelayableNotificationSignal<string>(static () => false, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Notifications are buffered while delayed and emitted as a de-duplicated batch on flush.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BuffersThenFlushes()
    {
        var delayed = true;
        var signal = new DelayableNotificationSignal<string>(() => delayed, static items => items);
        var recorder = new RecordingObserver<string>();
        using var subscription = signal.Subscribe(recorder);

        signal.OnNext(Buffered);
        await Assert.That(recorder.Values.Length).IsEqualTo(0);

        delayed = false;
        signal.Flush();

        await Assert.That(recorder.Values.Contains(Buffered)).IsTrue();
    }

    /// <summary>The flush delegate de-duplicates the buffered batch before emission.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FlushDeDuplicatesBatch()
    {
        var signal = new DelayableNotificationSignal<string>(static () => true, static items => items.Distinct());
        var recorder = new RecordingObserver<string>();
        using var subscription = signal.Subscribe(recorder);

        signal.OnNext("a");
        signal.OnNext("a");
        signal.OnNext("b");
        signal.Flush();

        await Assert.That(recorder.Values.SequenceEqual(["a", "b"])).IsTrue();
    }

    /// <summary>Notifications pass through immediately when not delayed and terminal events stop further emission.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateAndTerminal()
    {
        var immediate = new DelayableNotificationSignal<string>(static () => false, static items => items);
        var recorder = new RecordingObserver<string>();
        using var subscription = immediate.Subscribe(recorder);
        immediate.OnNext("now");
        immediate.OnCompleted();
        immediate.OnNext("ignored");

        var errored = new DelayableNotificationSignal<string>(static () => false, static items => items);
        var error = new InvalidOperationException("delayable-error");
        errored.OnError(error);
        var afterError = new RecordingObserver<string>();
        errored.Subscribe(afterError);

        await Assert.That(recorder.Values.Contains("now")).IsTrue();
        await Assert.That(recorder.Completed).IsEqualTo(1);
        await Assert.That(afterError.Errors.Contains(error)).IsTrue();
    }

    /// <summary>HasObservers reflects subscription state and IsDisposed flips on disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ObserverStateAndDisposal()
    {
        var signal = new DelayableNotificationSignal<string>(static () => false, static items => items);
        await Assert.That(signal.HasObservers).IsFalse();
        await Assert.That(signal.IsDisposed).IsFalse();

        var subscription = signal.Subscribe(new RecordingObserver<string>());
        await Assert.That(signal.HasObservers).IsTrue();

        subscription.Dispose();
        await Assert.That(signal.HasObservers).IsFalse();

        signal.Dispose();
        await Assert.That(signal.IsDisposed).IsTrue();
    }

    /// <summary>The factory helpers build working signal instances.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FactoryHelpersBuildWorkingSignals()
    {
        var delayed = true;
        var signal = Signal.Delayable<string>(() => delayed, static items => items);
        var recorder = new RecordingObserver<string>();
        using var subscription = signal.Subscribe(recorder);

        signal.OnNext(Buffered);
        await Assert.That(recorder.Values.Length).IsEqualTo(0);

        delayed = false;
        signal.Flush();
        await Assert.That(recorder.Values.Contains(Buffered)).IsTrue();

        using var scheduled = Signal.Scheduled<int>(Sequencer.Immediate);
        await Assert.That(scheduled.HasObservers).IsFalse();
    }

    /// <summary>A factory-built signal de-duplicates the buffered batch via the flush delegate.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FactoryFlushDeDuplicatesBatch()
    {
        var signal = Signal.Delayable<string>(static () => true, static items => items.Distinct());
        var recorder = new RecordingObserver<string>();
        using var subscription = signal.Subscribe(recorder);

        signal.OnNext("a");
        signal.OnNext("a");
        signal.OnNext("b");
        signal.Flush();

        await Assert.That(recorder.Values.SequenceEqual(["a", "b"])).IsTrue();
    }

    /// <summary>A factory-built signal passes notifications through immediately when not delayed and stops after completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FactoryImmediatePassthroughAndTerminal()
    {
        var signal = Signal.Delayable<string>(static () => false, static items => items);
        var recorder = new RecordingObserver<string>();
        using var subscription = signal.Subscribe(recorder);

        signal.OnNext("now");
        signal.OnCompleted();
        signal.OnNext("ignored");

        await Assert.That(recorder.Values.SequenceEqual(["now"])).IsTrue();
        await Assert.That(recorder.Completed).IsEqualTo(1);
    }

    /// <summary>A factory-built signal replays its terminal error to a subscriber that arrives after the error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FactoryReplaysErrorToLateSubscriber()
    {
        var signal = Signal.Delayable<string>(static () => false, static items => items);
        var error = new InvalidOperationException("delayable-error");
        signal.OnError(error);

        var recorder = new RecordingObserver<string>();
        signal.Subscribe(recorder);

        await Assert.That(recorder.Errors.Contains(error)).IsTrue();
    }

    /// <summary>A factory-built signal reports observer state and flips IsDisposed on disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FactoryObserverStateAndDisposal()
    {
        var signal = Signal.Delayable<string>(static () => false, static items => items);
        await Assert.That(signal.HasObservers).IsFalse();
        await Assert.That(signal.IsDisposed).IsFalse();

        var subscription = signal.Subscribe(new RecordingObserver<string>());
        await Assert.That(signal.HasObservers).IsTrue();

        subscription.Dispose();
        await Assert.That(signal.HasObservers).IsFalse();

        signal.Dispose();
        await Assert.That(signal.IsDisposed).IsTrue();
    }

    /// <summary>Records the notifications delivered to an observer.</summary>
    /// <typeparam name="T">The notification type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>The values delivered via OnNext.</summary>
        private readonly List<T> _values = [];

        /// <summary>The errors delivered via OnError.</summary>
        private readonly List<Exception> _errors = [];

        /// <summary>The number of times OnCompleted was called.</summary>
        private int _completed;

        /// <summary>Gets the values delivered via <see cref="OnNext"/>.</summary>
        public T[] Values
        {
            get
            {
                lock (_values)
                {
                    return [.. _values];
                }
            }
        }

        /// <summary>Gets the errors delivered via <see cref="OnError"/>.</summary>
        public Exception[] Errors
        {
            get
            {
                lock (_errors)
                {
                    return [.. _errors];
                }
            }
        }

        /// <summary>Gets the number of times <see cref="OnCompleted"/> was invoked.</summary>
        public int Completed => Volatile.Read(ref _completed);

        /// <inheritdoc/>
        public void OnCompleted() => Interlocked.Increment(ref _completed);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_errors)
            {
                _errors.Add(error);
            }
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_values)
            {
                _values.Add(value);
            }
        }
    }
}
