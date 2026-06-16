// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Direct tests for the <see cref = "SinkTerminal"/> terminal-forwarding helper.</summary>
public class SinkTerminalTests
{
    /// <summary>The value pushed through the sink under test.</summary>
    private const int Value = 7;

    /// <summary>Verifies <see cref = "SinkTerminal.Fault{TResult}(IObserver{TResult}, Exception, IDisposable)"/> forwards the error downstream and disposes the sink.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FaultForwardsTheErrorAndDisposesTheSink()
    {
        Recorder<int> observer = new();
        TrackingDisposable sink = new();
        InvalidOperationException error = new("boom");
        SinkTerminal.Fault(observer, error, sink);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(sink.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies <see cref = "SinkTerminal.Complete{TResult}(IObserver{TResult}, TResult, IDisposable)"/> emits the value, completes, then disposes the sink.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompleteWithValueEmitsThenCompletesAndDisposesTheSink()
    {
        Recorder<int> observer = new();
        TrackingDisposable sink = new();
        SinkTerminal.Complete(observer, Value, sink);
        await Assert.That(observer.Values.SequenceEqual([Value])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(sink.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies <see cref = "SinkTerminal.Complete{TResult}(IObserver{TResult}, IDisposable)"/> completes without a value, then disposes the sink.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompleteWithoutValueCompletesAndDisposesTheSink()
    {
        Recorder<int> observer = new();
        TrackingDisposable sink = new();
        SinkTerminal.Complete(observer, sink);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(sink.DisposeCount).IsEqualTo(1);
    }

    /// <summary>The sink is disposed via the <c>finally</c> even when the downstream observer throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FaultStillDisposesTheSinkWhenTheObserverThrows()
    {
        TrackingDisposable sink = new();
        ThrowingObserver<int> observer = new();
        Assert.Throws<InvalidOperationException>(() =>
            SinkTerminal.Fault(observer, new InvalidOperationException("downstream"), sink));
        await Assert.That(sink.DisposeCount).IsEqualTo(1);
    }

    /// <summary>The latched <c>Fault</c> overload forwards the error, disposes the sink, and sets the latch when it is not yet set.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FaultWithLatchForwardsAndSetsTheLatchWhenNotDone()
    {
        Recorder<int> observer = new();
        TrackingDisposable sink = new();
        InvalidOperationException error = new("boom");
        var done = false;
        SinkTerminal.Fault(observer, error, sink, ref done);
        await Assert.That(done).IsTrue();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(sink.DisposeCount).IsEqualTo(1);
    }

    /// <summary>The latched <c>Fault</c> overload does nothing once the latch is already set.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FaultWithLatchIsNoOpWhenAlreadyDone()
    {
        Recorder<int> observer = new();
        TrackingDisposable sink = new();
        var done = true;
        SinkTerminal.Fault(observer, new InvalidOperationException("ignored"), sink, ref done);
        await Assert.That(done).IsTrue();
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(sink.DisposeCount).IsEqualTo(0);
    }

    /// <summary>The latched value <c>Complete</c> overload emits, completes, disposes, and sets the latch when it is not yet set.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompleteWithValueAndLatchEmitsAndSetsTheLatchWhenNotDone()
    {
        Recorder<int> observer = new();
        TrackingDisposable sink = new();
        var done = false;
        SinkTerminal.Complete(observer, Value, sink, ref done);
        await Assert.That(done).IsTrue();
        await Assert.That(observer.Values.SequenceEqual([Value])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(sink.DisposeCount).IsEqualTo(1);
    }

    /// <summary>The latched value <c>Complete</c> overload does nothing once the latch is already set.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompleteWithValueAndLatchIsNoOpWhenAlreadyDone()
    {
        Recorder<int> observer = new();
        TrackingDisposable sink = new();
        var done = true;
        SinkTerminal.Complete(observer, Value, sink, ref done);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(sink.DisposeCount).IsEqualTo(0);
    }

    /// <summary>The latched valueless <c>Complete</c> overload completes, disposes, and sets the latch when it is not yet set.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompleteWithoutValueAndLatchCompletesAndSetsTheLatchWhenNotDone()
    {
        Recorder<int> observer = new();
        TrackingDisposable sink = new();
        var done = false;
        SinkTerminal.Complete(observer, sink, ref done);
        await Assert.That(done).IsTrue();
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(sink.DisposeCount).IsEqualTo(1);
    }

    /// <summary>The latched valueless <c>Complete</c> overload does nothing once the latch is already set.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompleteWithoutValueAndLatchIsNoOpWhenAlreadyDone()
    {
        Recorder<int> observer = new();
        TrackingDisposable sink = new();
        var done = true;
        SinkTerminal.Complete(observer, sink, ref done);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(sink.DisposeCount).IsEqualTo(0);
    }

    /// <summary>An observer that records all values, errors, and completion counts.</summary>
    /// <typeparam name = "T">The type of the observed values.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets the recorded values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the recorded errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of completion callbacks observed.</summary>
        public int Completed { get; private set; }

        /// <summary>Records a completion callback.</summary>
        public void OnCompleted() => Completed++;

        /// <summary>Records an error callback.</summary>
        /// <param name = "error">The error to record.</param>
        public void OnError(Exception error) => Errors.Add(error);

        /// <summary>Records a value callback.</summary>
        /// <param name = "value">The value to record.</param>
        public void OnNext(T value) => Values.Add(value);
    }

    /// <summary>An observer whose <see cref = "OnError"/> throws, to exercise the helper's <c>finally</c> branch.</summary>
    /// <typeparam name = "T">The type of the observed values.</typeparam>
    private sealed class ThrowingObserver<T> : IObserver<T>
    {
        /// <summary>Does nothing.</summary>
        public void OnCompleted()
        {
        }

        /// <summary>Throws to simulate a faulting downstream observer.</summary>
        /// <param name = "error">The forwarded error (ignored).</param>
        public void OnError(Exception error) => throw new InvalidOperationException("observer faulted");

        /// <summary>Does nothing.</summary>
        /// <param name = "value">The forwarded value (ignored).</param>
        public void OnNext(T value)
        {
        }
    }

    /// <summary>A disposable that counts how many times it has been disposed.</summary>
    private sealed class TrackingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref = "Dispose"/> has been called.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
