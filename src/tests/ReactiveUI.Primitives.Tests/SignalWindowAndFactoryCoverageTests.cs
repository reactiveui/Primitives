// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Covers recently added signal windowing and factory behavior that is reported by CI coverage gates.
/// </summary>
public sealed class SignalWindowAndFactoryCoverageTests
{
    /// <summary>
    /// Verifies <see cref="Signal.Collect{TSource}(IObservable{TSource}, TimeSpan)"/> immediate, scheduled,
    /// terminal, and error paths.
    /// </summary>
    [Test]
    public void CollectCoversImmediateScheduledCompletionErrorAndDisposePaths()
    {
        const int first = 1;
        const int second = 2;
        const int third = 3;
        const int expectedBatchCount = 2;
        var immediateBatches = new List<int[]>();

        Signal.FromEnumerable([first, second])
            .Collect(TimeSpan.Zero)
            .Subscribe(batch => immediateBatches.Add([.. batch]));

        Assert.Equal(expectedBatchCount, immediateBatches.Count);
        Assert.Equal<int>([first], immediateBatches[0]);
        Assert.Equal<int>([second], immediateBatches[1]);

        var clock = new TestClock();
        var source = new Signal<int>();
        var scheduledBatches = new List<int[]>();
        var completed = 0;
        var subscription = source
            .Collect(TimeSpan.FromTicks(second), clock)
            .Subscribe(batch => scheduledBatches.Add([.. batch]), ex => throw ex, () => completed++);

        source.OnNext(first);
        source.OnNext(second);
        clock.AdvanceBy(TimeSpan.FromTicks(second));
        source.OnNext(third);
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(second));
        subscription.Dispose();

        Assert.Equal(expectedBatchCount, scheduledBatches.Count);
        Assert.Equal<int>([first, second], scheduledBatches[0]);
        Assert.Equal<int>([third], scheduledBatches[1]);
        Assert.Equal(1, completed);

        var errorClock = new TestClock();
        var errorSource = new Signal<int>();
        var expected = new InvalidOperationException("collect");
        Exception? observed = null;

        errorSource.Collect(TimeSpan.FromTicks(first), errorClock)
            .Subscribe(_ => { }, ex => observed = ex);
        errorSource.OnNext(first);
        errorSource.OnError(expected);
        errorClock.AdvanceBy(TimeSpan.FromTicks(first));

        Assert.Same(expected, observed!);
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Collect(TimeSpan.FromTicks(first)));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(first).Collect(TimeSpan.FromTicks(first), null!));

        var stoppedGuardCompleted = 0;
        new ScriptedObservable<int>(observer =>
        {
            observer.OnCompleted();
            observer.OnNext(first);
        }).Collect(TimeSpan.FromTicks(first), new TestClock())
            .Subscribe(_ => { }, ex => throw ex, () => stoppedGuardCompleted++);
        Assert.Equal(1, stoppedGuardCompleted);
    }

    /// <summary>
    /// Verifies <see cref="Signal.EmitIfQuiet{TSource}(IObservable{TSource}, TimeSpan)"/> immediate, scheduled,
    /// completion, stale emission, and error paths.
    /// </summary>
    [Test]
    public void EmitIfQuietCoversImmediateScheduledCompletionStaleAndErrorPaths()
    {
        const int first = 1;
        const int second = 2;
        const int third = 3;
        var immediateValues = new List<int>();

        Signal.FromEnumerable([first, second])
            .EmitIfQuiet(TimeSpan.Zero)
            .Subscribe(immediateValues.Add);

        Assert.Equal<int>([first, second], immediateValues);

        var clock = new TestClock();
        var source = new Signal<int>();
        var delayedValues = new List<int>();
        var completed = 0;

        source.EmitIfQuiet(TimeSpan.FromTicks(third), clock)
            .Subscribe(delayedValues.Add, ex => throw ex, () => completed++);
        source.OnNext(first);
        clock.AdvanceBy(TimeSpan.FromTicks(second));
        source.OnNext(second);
        clock.AdvanceBy(TimeSpan.FromTicks(first));
        clock.AdvanceBy(TimeSpan.FromTicks(second));
        source.OnNext(third);
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(third));

        Assert.Equal<int>([second, third], delayedValues);
        Assert.Equal(1, completed);

        var emptyCompletion = 0;
        var emptySource = new Signal<int>();
        emptySource.EmitIfQuiet(TimeSpan.FromTicks(first), new TestClock())
            .Subscribe(_ => { }, ex => throw ex, () => emptyCompletion++);
        emptySource.OnCompleted();
        Assert.Equal(1, emptyCompletion);

        var errorClock = new TestClock();
        var errorSource = new Signal<int>();
        var expected = new InvalidOperationException("quiet");
        Exception? observed = null;

        errorSource.EmitIfQuiet(TimeSpan.FromTicks(first), errorClock)
            .Subscribe(_ => { }, ex => observed = ex);
        errorSource.OnNext(first);
        errorSource.OnError(expected);
        errorClock.AdvanceBy(TimeSpan.FromTicks(first));

        Assert.Same(expected, observed!);
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).EmitIfQuiet(TimeSpan.FromTicks(first)));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(first).EmitIfQuiet(TimeSpan.FromTicks(first), null!));

        var stoppedGuardCompleted = 0;
        new ScriptedObservable<int>(observer =>
        {
            observer.OnCompleted();
            observer.OnNext(first);
        }).EmitIfQuiet(TimeSpan.FromTicks(first), new TestClock())
            .Subscribe(_ => { }, ex => throw ex, () => stoppedGuardCompleted++);
        Assert.Equal(1, stoppedGuardCompleted);
    }

    /// <summary>
    /// Verifies deferred sources and blocking enumeration surface success, factory failure, and source failure paths.
    /// </summary>
    [Test]
    public void DeferAndToEnumerableCoverSuccessAndErrorPaths()
    {
        const int first = 1;
        const int second = 2;
        const int expectedSubscriptionCount = 2;
        var subscriptions = 0;
        var values = new List<int>();

        var deferred = Signal.Defer(() =>
        {
            subscriptions++;
            return Signal.FromEnumerable([first, second]);
        });

        deferred.Subscribe(values.Add);
        deferred.Subscribe(_ => { });

        Assert.Equal<int>([first, second], values);
        Assert.Equal(expectedSubscriptionCount, subscriptions);
        Assert.Equal<int>([first, second], Signal.FromEnumerable([first, second]).ToEnumerable());

        var factoryError = new InvalidOperationException("defer-factory");
        Exception? observedFactoryError = null;
        Signal.Defer<int>(() => throw factoryError).Subscribe(_ => { }, ex => observedFactoryError = ex);

        Assert.Same(factoryError, observedFactoryError!);
        Assert.Throws<InvalidOperationException>(() => Signal.Fail<int>(new InvalidOperationException("enumerable")).ToEnumerable());
        Assert.Throws<ArgumentNullException>(() => Signal.Defer<int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ToEnumerable());
    }

    /// <summary>
    /// Verifies generic event factory overloads for supported and unsupported handler shapes.
    /// </summary>
    [Test]
    public void GenericFromEventPatternCoversPropertyChangedGenericAndUnsupportedHandlers()
    {
        const int eventValue = 7;
        var source = new GenericEventSource();
        var values = new List<int>();
        var genericSubscription = Signal
            .FromEventPattern<EventHandler<TestEventArgs>, TestEventArgs>(
                handler => source.Changed += handler,
                handler => source.Changed -= handler)
            .Subscribe(pattern => values.Add(pattern.EventArgs.Value));

        source.Raise(eventValue);
        genericSubscription.Dispose();
        source.Raise(eventValue + 1);

        Assert.Equal<int>([eventValue], values);

        var propertySource = new PropertyChangedEventSource();
        var propertyNames = new List<string?>();
        var propertySubscription = Signal
            .FromEventPattern<PropertyChangedEventHandler, PropertyChangedEventArgs>(
                handler => propertySource.PropertyChanged += handler,
                handler => propertySource.PropertyChanged -= handler)
            .Subscribe(pattern => propertyNames.Add(pattern.EventArgs.PropertyName));

        propertySource.Raise(nameof(PropertyChangedEventSource.Value));
        propertySubscription.Dispose();
        propertySource.Raise("ignored");

        Assert.Equal<string?>([nameof(PropertyChangedEventSource.Value)], propertyNames);
        Assert.Throws<ArgumentNullException>(() => Signal.FromEventPattern<EventHandler<TestEventArgs>, TestEventArgs>(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEventPattern<EventHandler<TestEventArgs>, TestEventArgs>(_ => { }, null!));
        Assert.Throws<NotSupportedException>(() =>
            Signal.FromEventPattern<Action, EventArgs>(_ => { }, _ => { }).Subscribe(_ => { }));
    }

    /// <summary>
    /// Event arguments carrying a deterministic integer value.
    /// </summary>
    /// <param name="value">The value supplied by the event.</param>
    private sealed class TestEventArgs(int value) : EventArgs
    {
        /// <summary>
        /// Gets the event value.
        /// </summary>
        public int Value { get; } = value;
    }

    /// <summary>
    /// Observable that runs a supplied subscription script synchronously.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="script">The script invoked with the observer.</param>
    private sealed class ScriptedObservable<T>(Action<IObserver<T>> script) : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            script(observer);
            return Disposable.Empty;
        }
    }

    /// <summary>
    /// Source used to exercise generic <see cref="EventHandler{TEventArgs}"/> event conversion.
    /// </summary>
    private sealed class GenericEventSource
    {
        /// <summary>
        /// Raised by the test source.
        /// </summary>
        public event EventHandler<TestEventArgs>? Changed;

        /// <summary>
        /// Raises <see cref="Changed"/> with the supplied value.
        /// </summary>
        /// <param name="value">The value supplied to the event arguments.</param>
        public void Raise(int value) => Changed?.Invoke(this, new TestEventArgs(value));
    }

    /// <summary>
    /// Source used to exercise <see cref="PropertyChangedEventHandler"/> event conversion.
    /// </summary>
    private sealed class PropertyChangedEventSource
    {
        /// <summary>
        /// Raised by the test source.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Roslynator",
            "RCS1159:Use EventHandler<T>",
            Justification = "This test deliberately covers the PropertyChangedEventHandler branch of the factory overload.")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S3908:Refactor this delegate to use 'System.EventHandler<TEventArgs>'.",
            Justification = "This test deliberately covers the PropertyChangedEventHandler branch of the factory overload.")]
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>
        /// Gets a placeholder property name used by the event test.
        /// </summary>
        public static int Value => default;

        /// <summary>
        /// Raises <see cref="PropertyChanged"/> with the supplied property name.
        /// </summary>
        /// <param name="propertyName">The property name supplied to the event arguments.</param>
        public void Raise(string propertyName) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}
