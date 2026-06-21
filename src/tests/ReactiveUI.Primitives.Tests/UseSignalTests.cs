// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Signal"/> resource-scoped use contracts.</summary>
public class UseSignalTests
{
    /// <summary>Covers resource disposal when the subscription forwards a null error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UseValidatesSubscriptionError()
    {
        RecordingDisposable resource = new();
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Use(
                () => resource,
                _ => new ScriptedObservable<int>(static observer => observer.OnError(null!)))
            .Subscribe(new Recorder<int>()));
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Covers resource disposal when the inner subscription is null.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UseDisposesResourceWhenSubscriptionIsNull()
    {
        RecordingDisposable resource = new();
        _ = Assert.Throws<ArgumentNullException>(() => Signal
            .Use(() => resource, _ => new NullSubscriptionObservable<int>())
            .Subscribe(new Recorder<int>()));
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Observable that runs a supplied subscription script synchronously.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="script">The subscription script.</param>
    private sealed class ScriptedObservable<T>(Action<IObserver<T>> script) : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            script(observer);
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Observable that returns a null subscription.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class NullSubscriptionObservable<T> : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnNext(default!);
            return null!;
        }
    }

    /// <summary>Records observer notifications.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets observed errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of completion notifications.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }
}
