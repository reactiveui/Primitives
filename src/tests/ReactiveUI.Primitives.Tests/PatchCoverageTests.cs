// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Covers PR patch paths that are otherwise hard to reach through larger scenario tests.</summary>
public class PatchCoverageTests
{
    /// <summary>First test value.</summary>
    private const int First = 1;

    /// <summary>Second test value.</summary>
    private const int Second = 2;

    /// <summary>Shared state value.</summary>
    private const string State = "state";

    /// <summary>Covers callback, forwarding, and stateful witness contracts.</summary>
    [Test]
    public void WitnessImplementationsForwardNotificationsAndFallbackErrors()
    {
        Assert.Throws<ArgumentNullException>(() => _ = new CallbackWitness<int>(null!, null, null));
        Assert.Throws<ArgumentNullException>(() => _ = new ForwardingWitness<int>(null!));
        Assert.Throws<ArgumentNullException>(() => _ = new StatefulWitness<int, string>(State, null!, null, null));

        var callbackValues = new List<int>();
        var callbackErrors = new List<Exception>();
        var callbackCompletions = new List<Result>();
        var callback = new CallbackWitness<int>(callbackValues.Add, callbackErrors.Add, callbackCompletions.Add);
        var callbackError = new InvalidOperationException("callback");

        callback.OnNext(First);
        callback.OnError(callbackError);
        callback.OnCompleted();

        Assert.True(callbackValues.SequenceEqual([First]));
        Assert.Same(callbackError, callbackErrors[0]);
        Assert.True(callbackCompletions[0].IsSuccess);

        var callbackFallback = new InvalidOperationException("callback fallback");
        Assert.Throws<InvalidOperationException>(() => new CallbackWitness<int>(_ => { }, null, null).OnError(callbackFallback));
        new CallbackWitness<int>(_ => { }, null, null).OnCompleted();

        var forwarded = new Recorder<int>();
        var forwarding = new ForwardingWitness<int>(forwarded);
        var forwardingError = new InvalidOperationException("forwarding");

        forwarding.OnNext(Second);
        forwarding.OnError(forwardingError);
        forwarding.OnCompleted();

        Assert.True(forwarded.Values.SequenceEqual([Second]));
        Assert.Same(forwardingError, forwarded.Errors[0]);
        Assert.Equal(1, forwarded.Completed);

        var statefulValues = new List<string>();
        var statefulErrors = new List<string>();
        var statefulCompletions = new List<string>();
        var stateful = new StatefulWitness<int, string>(
            State,
            (value, state) => statefulValues.Add($"{state}:{value}"),
            (error, state) => statefulErrors.Add($"{state}:{error.Message}"),
            (result, state) => statefulCompletions.Add($"{state}:{result.IsSuccess}"));
        var statefulError = new InvalidOperationException("stateful");

        stateful.OnNext(First);
        stateful.OnError(statefulError);
        stateful.OnCompleted();

        Assert.True(statefulValues.SequenceEqual([$"{State}:{First}"]));
        Assert.True(statefulErrors.SequenceEqual([$"{State}:{statefulError.Message}"]));
        Assert.True(statefulCompletions.SequenceEqual([$"{State}:True"]));

        var statefulFallback = new InvalidOperationException("stateful fallback");
        Assert.Throws<InvalidOperationException>(() => new StatefulWitness<int, string>(State, (_, _) => { }, null, null).OnError(statefulFallback));
        new StatefulWitness<int, string>(State, (_, _) => { }, null, null).OnCompleted();

        var safeValues = new List<int>();
        var safeErrors = new List<Exception>();
        var safeCompleted = 0;
        var safe = Witness.Safe(Witness.Create<int>(safeValues.Add, safeErrors.Add, () => safeCompleted++));

        safe.OnNext(First);
        safe.OnCompleted();
        safe.OnNext(Second);
        safe.OnError(new InvalidOperationException("ignored"));
        safe.OnCompleted();

        Assert.True(safeValues.SequenceEqual([First]));
        Assert.Equal(0, safeErrors.Count);
        Assert.Equal(1, safeCompleted);
    }

    /// <summary>Covers optional value creation and empty value access.</summary>
    [Test]
    public void OptionalCoversEmptyAndValueContracts()
    {
        var defaultOptional = new Optional<int>();
        Assert.False(defaultOptional.HasValue);
        Assert.Throws<InvalidOperationException>(() => _ = defaultOptional.Value);

        Assert.False(Optional<int>.Empty.HasValue);
        Assert.False(Optional<int>.None.HasValue);

        var constructed = new Optional<int>(First);
        Assert.True(constructed.HasValue);
        Assert.Equal(First, constructed.Value);

        var some = Optional<int>.Some(Second);
        Assert.True(some.HasValue);
        Assert.Equal(Second, some.Value);
    }

    /// <summary>Covers null-validation paths touched by the refactor.</summary>
    [Test]
    public void RefactoredNullValidationBranchesThrow()
    {
        var sequencer = new SynchronizationContextSequencer(new InlineSynchronizationContext());
        Assert.Throws<ArgumentNullException>(() => sequencer.Schedule(null!, long.MaxValue));

        Assert.Throws<ArgumentNullException>(() => Signal.Unfold(First, static value => value < Second, static value => value + 1, static value => value).Subscribe(null!));

        var resource = new RecordingDisposable();
        Assert.Throws<ArgumentNullException>(() =>
            Signal.Use(() => resource, _ => new ScriptedObservable<int>(static observer => observer.OnError(null!)))
                .Subscribe(new Recorder<int>()));
        Assert.Equal(1, resource.DisposeCount);

        var source = new StateSignal<int>(First);
        using var projection = source.ToReadOnlyState(static value => value);
        Assert.Throws<ArgumentNullException>(() => projection.OnError(null!));

        var taskSignal = Signal.FromTask(_ => Task.FromResult(First), Sequencer.Immediate);
        try
        {
            Assert.Throws<ArgumentNullException>(() => taskSignal.GetOperationCanceled(null!));
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }

        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(null!, _ => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, _ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Safe<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Safe(Witness.Create<int>(_ => { }), null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Safe(new Recorder<int>(), new RecordingDisposable()).OnError(null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Safe(new Recorder<int>()).OnError(null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, _ => { }, () => { }).OnError(null!));
    }

    /// <summary>Covers small runtime branches touched by the patch.</summary>
    [Test]
    public void RefactoredRuntimeBranchesExecute()
    {
        var sequencer = new SynchronizationContextSequencer(new InlineSynchronizationContext());
        var workItem = new CountingWorkItem();
        var dueTimestamp = sequencer.Timestamp;

        sequencer.Schedule(workItem, dueTimestamp);
        sequencer.Schedule(workItem, dueTimestamp - 1);

        Assert.Equal(Second, workItem.ExecuteCount);

        var unfolded = new List<int>();
        var unfoldCompleted = 0;
        Signal.Unfold(First, static value => value <= Second, static value => value + 1, static value => value)
            .Subscribe(unfolded.Add, error => throw error, () => unfoldCompleted++);

        Assert.True(unfolded.SequenceEqual([First, Second]));
        Assert.Equal(1, unfoldCompleted);

        var resource = new RecordingDisposable();
        Assert.Throws<ArgumentNullException>(() =>
            Signal.Use(() => resource, _ => new NullSubscriptionObservable<int>())
                .Subscribe(new Recorder<int>()));
        Assert.Equal(1, resource.DisposeCount);

        var source = new StateSignal<int>(First);
        using var projection = source.ToReadOnlyState(value => value == Second ? throw new InvalidOperationException("selector") : value);
        var projected = new Recorder<int>();
        projection.Subscribe(projected);

        source.Value = Second;

        Assert.Equal(1, projected.Errors.Count);
        Assert.Equal("selector", projected.Errors[0].Message);

        var lateProjected = new Recorder<int>();
        projection.Subscribe(lateProjected);
        Assert.Equal(1, lateProjected.Errors.Count);
        Assert.Same(projected.Errors[0], lateProjected.Errors[0]);

        var taskSignal = Signal.FromTask(_ => Task.FromResult(Second), Sequencer.Immediate);
        try
        {
            var taskValues = new List<int>();
            var taskCompleted = 0;

            taskSignal.Subscribe(taskValues.Add, error => throw error, () => taskCompleted++);

            Assert.True(taskValues.SequenceEqual([Second]));
            Assert.Equal(1, taskCompleted);
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>Synchronization context that executes posted work inline.</summary>
    private sealed class InlineSynchronizationContext : SynchronizationContext
    {
        /// <inheritdoc/>
        public override void Post(SendOrPostCallback callback, object? state) => callback(state);
    }

    /// <summary>Counts work item executions.</summary>
    private sealed class CountingWorkItem : IWorkItem
    {
        /// <summary>Gets the number of executions.</summary>
        public int ExecuteCount { get; private set; }

        /// <inheritdoc/>
        public void Execute() => ExecuteCount++;
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

    /// <summary>Records disposal calls.</summary>
    private sealed class RecordingDisposable : IDisposable
    {
        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
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
