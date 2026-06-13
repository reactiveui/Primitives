// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#pragma warning disable S103 // Coverage tests intentionally group branch-heavy scenarios.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Covers recently added signal windowing and factory behavior that is reported by CI coverage gates.</summary>
public sealed class SignalWindowAndFactoryCoverageTests
{
    /// <summary>Verifies the Collect method covers immediate, scheduled, terminal, and error paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CollectCoversImmediateScheduledCompletionErrorAndDisposePaths()
    {
        const int First = 1;
        const int Second = 2;
        const int Third = 3;
        const int ExpectedBatchCount = 2;
        var immediateBatches = new List<int[]>();
        Signal.FromEnumerable([First, Second]).Collect(TimeSpan.Zero).Subscribe(batch => immediateBatches.Add([.. batch]));
        await Assert.That(immediateBatches.Count).IsEqualTo(ExpectedBatchCount);
        await Assert.That(immediateBatches[0].SequenceEqual([First])).IsTrue();
        await Assert.That(immediateBatches[1].SequenceEqual([Second])).IsTrue();
        var clock = new TestClock();
        var source = new Signal<int>();
        var scheduledBatches = new List<int[]>();
        var completed = 0;
        var subscription = source.Collect(TimeSpan.FromTicks(Second), clock).Subscribe(batch => scheduledBatches.Add([.. batch]), ex => throw ex, () => completed++);
        source.OnNext(First);
        source.OnNext(Second);
        clock.AdvanceBy(TimeSpan.FromTicks(Second));
        source.OnNext(Third);
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(Second));
        subscription.Dispose();
        await Assert.That(scheduledBatches.Count).IsEqualTo(ExpectedBatchCount);
        await Assert.That(scheduledBatches[0].SequenceEqual([First, Second])).IsTrue();
        await Assert.That(scheduledBatches[1].SequenceEqual([Third])).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        var errorClock = new TestClock();
        var errorSource = new Signal<int>();
        var expected = new InvalidOperationException("collect");
        Exception? observed = null;
        errorSource.Collect(TimeSpan.FromTicks(First), errorClock).Subscribe(
            _ =>
        {
        },
            ex => observed = ex);
        errorSource.OnNext(First);
        errorSource.OnError(expected);
        errorClock.AdvanceBy(TimeSpan.FromTicks(First));
        await Assert.That(observed!).IsSameReferenceAs(expected);
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Collect(TimeSpan.FromTicks(First)));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(First).Collect(TimeSpan.FromTicks(First), null!));
        var stoppedGuardCompleted = 0;
        new ScriptedObservable<int>(observer =>
        {
            observer.OnCompleted();
            observer.OnNext(First);
        }).Collect(TimeSpan.FromTicks(First), new TestClock()).Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () => stoppedGuardCompleted++);
        await Assert.That(stoppedGuardCompleted).IsEqualTo(1);
    }

    /// <summary>Verifies the EmitIfQuiet method covers immediate, scheduled, completion, stale emission, and error paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EmitIfQuietCoversImmediateScheduledCompletionStaleAndErrorPaths()
    {
        const int First = 1;
        const int Second = 2;
        const int Third = 3;
        var immediateValues = new List<int>();
        Signal.FromEnumerable([First, Second]).EmitIfQuiet(TimeSpan.Zero).Subscribe(immediateValues.Add);
        await Assert.That(immediateValues.SequenceEqual([First, Second])).IsTrue();
        var clock = new TestClock();
        var source = new Signal<int>();
        var delayedValues = new List<int>();
        var completed = 0;
        source.EmitIfQuiet(TimeSpan.FromTicks(Third), clock).Subscribe(delayedValues.Add, ex => throw ex, () => completed++);
        source.OnNext(First);
        clock.AdvanceBy(TimeSpan.FromTicks(Second));
        source.OnNext(Second);
        clock.AdvanceBy(TimeSpan.FromTicks(First));
        clock.AdvanceBy(TimeSpan.FromTicks(Second));
        source.OnNext(Third);
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(Third));
        await Assert.That(delayedValues.SequenceEqual([Second, Third])).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        var emptyCompletion = 0;
        var emptySource = new Signal<int>();
        emptySource.EmitIfQuiet(TimeSpan.FromTicks(First), new TestClock()).Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () => emptyCompletion++);
        emptySource.OnCompleted();
        await Assert.That(emptyCompletion).IsEqualTo(1);
        var errorClock = new TestClock();
        var errorSource = new Signal<int>();
        var expected = new InvalidOperationException("quiet");
        Exception? observed = null;
        errorSource.EmitIfQuiet(TimeSpan.FromTicks(First), errorClock).Subscribe(
            _ =>
        {
        },
            ex => observed = ex);
        errorSource.OnNext(First);
        errorSource.OnError(expected);
        errorClock.AdvanceBy(TimeSpan.FromTicks(First));
        await Assert.That(observed!).IsSameReferenceAs(expected);
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).EmitIfQuiet(TimeSpan.FromTicks(First)));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(First).EmitIfQuiet(TimeSpan.FromTicks(First), null!));
        var stoppedGuardCompleted = 0;
        new ScriptedObservable<int>(observer =>
        {
            observer.OnCompleted();
            observer.OnNext(First);
        }).EmitIfQuiet(TimeSpan.FromTicks(First), new TestClock()).Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () => stoppedGuardCompleted++);
        await Assert.That(stoppedGuardCompleted).IsEqualTo(1);
    }

    /// <summary>Verifies deferred sources and blocking enumeration surface success, factory failure, and source failure paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeferAndToEnumerableCoverSuccessAndErrorPaths()
    {
        const int First = 1;
        const int Second = 2;
        const int ExpectedSubscriptionCount = 2;
        var subscriptions = 0;
        var values = new List<int>();
        var deferred = Signal.Defer(() =>
        {
            subscriptions++;
            return Signal.FromEnumerable([First, Second]);
        });
        deferred.Subscribe(values.Add);
        deferred.Subscribe(_ =>
        {
        });
        await Assert.That(values.SequenceEqual([First, Second])).IsTrue();
        await Assert.That(subscriptions).IsEqualTo(ExpectedSubscriptionCount);
        await Assert.That(Signal.FromEnumerable([First, Second]).ToEnumerable().SequenceEqual([First, Second])).IsTrue();
        var factoryError = new InvalidOperationException("defer-factory");
        Exception? observedFactoryError = null;
        Signal.Defer<int>(() => throw factoryError).Subscribe(
            _ =>
        {
        },
            ex => observedFactoryError = ex);
        await Assert.That(observedFactoryError!).IsSameReferenceAs(factoryError);
        Assert.Throws<InvalidOperationException>(() => Signal.Fail<int>(new InvalidOperationException("enumerable")).ToEnumerable());
        Assert.Throws<ArgumentNullException>(() => Signal.Defer<int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ToEnumerable());
    }

    /// <summary>Verifies generic event factory overloads for supported and unsupported handler shapes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GenericFromEventPatternCoversPropertyChangedGenericAndUnsupportedHandlers()
    {
        const int EventValue = 7;
        var source = new GenericEventSource();
        var values = new List<int>();
        var genericSubscription = Signal.FromEventPattern<EventHandler<TestEventArgs>, TestEventArgs>(handler => source.Changed += handler, handler => source.Changed -= handler).Subscribe(pattern => values.Add(pattern.EventArgs.Value));
        source.Raise(EventValue);
        genericSubscription.Dispose();
        source.Raise(EventValue + 1);
        await Assert.That(values.SequenceEqual([EventValue])).IsTrue();
        var propertySource = new PropertyChangedEventSource();
        var propertyNames = new List<string?>();
        var propertySubscription = Signal.FromEventPattern<PropertyChangedEventHandler, PropertyChangedEventArgs>(handler => propertySource.PropertyChanged += handler, handler => propertySource.PropertyChanged -= handler).Subscribe(pattern => propertyNames.Add(pattern.EventArgs.PropertyName));
        propertySource.Raise(nameof(PropertyChangedEventSource.Value));
        propertySubscription.Dispose();
        propertySource.Raise("ignored");
        await Assert.That(propertyNames.SequenceEqual([nameof(PropertyChangedEventSource.Value)])).IsTrue();
        Assert.Throws<ArgumentNullException>(() => Signal.FromEventPattern<EventHandler<TestEventArgs>, TestEventArgs>(null!, _ =>
{
}));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEventPattern<EventHandler<TestEventArgs>, TestEventArgs>(
            _ =>
{
},
            null!));
        Assert.Throws<NotSupportedException>(() => Signal.FromEventPattern<Action, EventArgs>(
            _ =>
{
},
            _ =>
{
}).Subscribe(_ =>
{
}));
    }

    /// <summary>Event arguments carrying a deterministic integer value.</summary>
    /// <param name = "value">The value supplied by the event.</param>
    private sealed class TestEventArgs(int value) : EventArgs
    {
        /// <summary>Gets the event value.</summary>
        public int Value { get; } = value;
    }

    /// <summary>Observable that runs a supplied subscription script synchronously.</summary>
    /// <typeparam name = "T">The value type.</typeparam>
    /// <param name = "script">The script invoked with the observer.</param>
    private sealed class ScriptedObservable<T>(Action<IObserver<T>> script) : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            script(observer);
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Source used to exercise generic <see cref = "EventHandler{TEventArgs}"/> event conversion.</summary>
    private sealed class GenericEventSource
    {
        /// <summary>Raised by the test source.</summary>
        public event EventHandler<TestEventArgs>? Changed;

        /// <summary>Raises <see cref = "Changed"/> with the supplied value.</summary>
        /// <param name = "value">The value supplied to the event arguments.</param>
        public void Raise(int value) => Changed?.Invoke(this, new TestEventArgs(value));
    }

    /// <summary>Source used to exercise <see cref = "PropertyChangedEventHandler"/> event conversion.</summary>
    private sealed class PropertyChangedEventSource
    {
        /// <summary>Raised by the test source.</summary>
        [SuppressMessage("Roslynator", "RCS1159:Use EventHandler<T>", Justification = "This test deliberately covers the PropertyChangedEventHandler branch of the factory overload.")]
        [SuppressMessage("Major Code Smell", "S3908:Refactor this delegate to use 'System.EventHandler<TEventArgs>'.", Justification = "This test deliberately covers the PropertyChangedEventHandler branch of the factory overload.")]
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>Gets a placeholder property name used by the event test.</summary>
        public static int Value => 0;

        /// <summary>Raises <see cref = "PropertyChanged"/> with the supplied property name.</summary>
        /// <param name = "propertyName">The property name supplied to the event arguments.</param>
        public void Raise(string propertyName) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}
