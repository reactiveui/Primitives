// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Direct tests for the <see cref="SinkTerminal"/> terminal-forwarding helper.</summary>
public class SinkTerminalTests
{
    private const int Value = 7;

    /// <summary><see cref="SinkTerminal.Fault{TResult}"/> forwards the error downstream and disposes the sink.</summary>
    [Test]
    public void FaultForwardsTheErrorAndDisposesTheSink()
    {
        var observer = new Recorder<int>();
        var sink = new TrackingDisposable();
        var error = new InvalidOperationException("boom");

        SinkTerminal.Fault(observer, error, sink);

        Assert.Same(error, observer.Errors[0]);
        Assert.Equal(0, observer.Values.Count);
        Assert.Equal(0, observer.Completed);
        Assert.Equal(1, sink.DisposeCount);
    }

    /// <summary><see cref="SinkTerminal.Complete{TResult}(IObserver{TResult}, TResult, IDisposable)"/> emits the value, completes, then disposes the sink.</summary>
    [Test]
    public void CompleteWithValueEmitsThenCompletesAndDisposesTheSink()
    {
        var observer = new Recorder<int>();
        var sink = new TrackingDisposable();

        SinkTerminal.Complete(observer, Value, sink);

        Assert.Equal<int>([Value], observer.Values);
        Assert.Equal(1, observer.Completed);
        Assert.Equal(0, observer.Errors.Count);
        Assert.Equal(1, sink.DisposeCount);
    }

    /// <summary><see cref="SinkTerminal.Complete{TResult}(IObserver{TResult}, IDisposable)"/> completes without a value, then disposes the sink.</summary>
    [Test]
    public void CompleteWithoutValueCompletesAndDisposesTheSink()
    {
        var observer = new Recorder<int>();
        var sink = new TrackingDisposable();

        SinkTerminal.Complete(observer, sink);

        Assert.Equal(0, observer.Values.Count);
        Assert.Equal(1, observer.Completed);
        Assert.Equal(1, sink.DisposeCount);
    }

    /// <summary>The sink is disposed via the <c>finally</c> even when the downstream observer throws.</summary>
    [Test]
    public void FaultStillDisposesTheSinkWhenTheObserverThrows()
    {
        var sink = new TrackingDisposable();
        var observer = new ThrowingObserver<int>();

        Assert.Throws<InvalidOperationException>(() =>
            SinkTerminal.Fault(observer, new InvalidOperationException("downstream"), sink));

        Assert.Equal(1, sink.DisposeCount);
    }

    /// <summary>An observer that records all values, errors, and completion counts.</summary>
    /// <typeparam name="T">The type of the observed values.</typeparam>
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
        /// <param name="error">The error to record.</param>
        public void OnError(Exception error) => Errors.Add(error);

        /// <summary>Records a value callback.</summary>
        /// <param name="value">The value to record.</param>
        public void OnNext(T value) => Values.Add(value);
    }

    /// <summary>An observer whose <see cref="OnError"/> throws, to exercise the helper's <c>finally</c> branch.</summary>
    /// <typeparam name="T">The type of the observed values.</typeparam>
    private sealed class ThrowingObserver<T> : IObserver<T>
    {
        /// <summary>No-op.</summary>
        public void OnCompleted()
        {
        }

        /// <summary>Throws to simulate a faulting downstream observer.</summary>
        /// <param name="error">The forwarded error (ignored).</param>
        public void OnError(Exception error) => throw new InvalidOperationException("observer faulted");

        /// <summary>No-op.</summary>
        /// <param name="value">The forwarded value (ignored).</param>
        public void OnNext(T value)
        {
        }
    }

    /// <summary>A disposable that counts how many times it has been disposed.</summary>
    private sealed class TrackingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref="Dispose"/> has been called.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
