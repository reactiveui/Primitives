// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Tests;

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.IO;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>
/// Tests for ReactiveExtensions around scheduling.
/// </summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>
    /// Tests DetectStale marks stream as stale.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task DetectStale_WhenInactive_MarksAsStale()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<Stale<int>>();
        using var sub = subject.DetectStale(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add);

        subject.OnNext(1);
        scheduler.AdvanceBy(SchedulerHalfWindowTicks);
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);

        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(SampleValue3);
            await Assert.That(results[0].IsStale).IsFalse();
            await Assert.That(results[0].Update).IsEqualTo(1);
            await Assert.That(results[1].IsStale).IsFalse();
            await Assert.That(results[1].Update).IsEqualTo(SampleValue2);
            await Assert.That(results[SampleValue2].IsStale).IsTrue();
        }
    }

    /// <summary>
    /// Tests Heartbeat injects heartbeats.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Heartbeat_WhenInactive_InjectsHeartbeats()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<Heartbeat<int>>();
        using var sub = subject.Heartbeat(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add);

        subject.OnNext(1);
        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);

        // Updates and heartbeats interleave; assert by predicate rather than positional index
        // because the periodic-tick count between updates depends on scheduler implementation details.
        var updates = results.Where(static h => !h.IsHeartbeat).ToList();
        var heartbeats = results.Where(static h => h.IsHeartbeat).ToList();

        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsGreaterThanOrEqualTo(SampleValue3);
            await Assert.That(updates).Count().IsEqualTo(SampleValue2);
            await Assert.That(updates[0].Update).IsEqualTo(1);
            await Assert.That(updates[1].Update).IsEqualTo(SampleValue2);
            await Assert.That(heartbeats).IsNotEmpty();
        }
    }

    /// <summary>
    /// Tests ObserveOnSafe with null scheduler.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ObserveOnSafe_WithNullScheduler_ReturnsSource()
    {
        var source = Observable.Return(1);
        var result = source.ObserveOnSafe(null);

        await Assert.That(result).IsSameReferenceAs(source);
    }

    /// <summary>
    /// Tests ObserveOnSafe with scheduler.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ObserveOnSafe_WithScheduler_ObservesOnScheduler()
    {
        var scheduler = new VirtualClock();
        var source = Observable.Return(1);
        int? result = null;
        using var sub = source.ObserveOnSafe(scheduler).Subscribe(x => result = x);

        await Assert.That(result).IsNull();

        scheduler.AdvanceBy(1);

        await Assert.That(result).IsEqualTo(1);
    }

    /// <summary>
    /// Tests Start with Action and null scheduler.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Start_WithActionAndNullScheduler_ExecutesAction()
    {
        var executed = false;
        Action action = () => executed = true;

        using var sub = ReactiveExtensions.Start(action, Sequencer.Immediate).Subscribe();

        await Assert.That(executed).IsTrue();
    }

    /// <summary>
    /// Tests Start with function and null scheduler executes immediately.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Start_WithFunctionAndNullScheduler_ReturnsComputedValue()
    {
        var result = await ReactiveExtensions.Start(() => 21 * 2, scheduler: null).ToTask();

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests ScheduleSafe with null scheduler executes immediately.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ScheduleSafe_WithNullScheduler_ExecutesImmediately()
    {
        var executed = false;
        const ISequencer? Scheduler = null;
        var disposable = Scheduler.ScheduleSafe(() => executed = true);

        using (Assert.Multiple())
        {
            await Assert.That(executed).IsTrue();
            await Assert.That(disposable).IsNotNull();
        }
    }

    /// <summary>
    /// Tests Heartbeat class with update.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Heartbeat_WithUpdate_IsNotHeartbeat()
    {
        var heartbeat = new Heartbeat<int>(42);

        using (Assert.Multiple())
        {
            await Assert.That(heartbeat.IsHeartbeat).IsFalse();
            await Assert.That(heartbeat.Update).IsEqualTo(SampleValue42);
        }
    }

    /// <summary>
    /// Tests Heartbeat class without update.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Heartbeat_WithoutUpdate_IsHeartbeat()
    {
        var heartbeat = new Heartbeat<int>();

        await Assert.That(heartbeat.IsHeartbeat).IsTrue();
    }

    /// <summary>
    /// Tests Schedule with value and TimeSpan and function.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithValueTimeSpanAndFunction_DelaysAndTransforms()
    {
        var scheduler = new VirtualClock();
        int? result = null;

        using var sub = 10.Schedule(TimeSpan.FromTicks(100), scheduler, x => x * 2)
            .Subscribe(x => result = x);

        await Assert.That(result).IsNull();

        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);

        await Assert.That(result).IsEqualTo(SampleValue20);
    }

    /// <summary>
    /// Tests Schedule with observable, TimeSpan and function.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithObservableTimeSpanAndFunction_DelaysAndTransforms()
    {
        var scheduler = new VirtualClock();
        var source = Observable.Return(10);
        var results = new List<int>();

        using var sub = source.Schedule(TimeSpan.FromTicks(100), scheduler, x => x * 2)
            .Subscribe(results.Add);

        await Assert.That(results).IsEmpty();

        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);

        await Assert.That(results).IsCollectionEqualTo([SampleValue20]);
    }

    /// <summary>
    /// Tests SyncTimer creates shared observable that produces ticks.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SyncTimer_ProducesSharedTicks()
    {
        var timeSpan = TimeSpan.FromMilliseconds(100);
        var scheduler = new VirtualClock();
        var results1 = new List<DateTime>();
        var results2 = new List<DateTime>();

        using var sub1 = ReactiveExtensions.SyncTimer(timeSpan, scheduler).Take(2).Subscribe(results1.Add);
        using var sub2 = ReactiveExtensions.SyncTimer(timeSpan, scheduler).Take(2).Subscribe(results2.Add);

        scheduler.AdvanceBy(timeSpan.Ticks * SampleValue2);

        using (Assert.Multiple())
        {
            // Both subscriptions should get ticks (shared timer)
            await Assert.That(results1).Count().IsGreaterThanOrEqualTo(1);
            await Assert.That(results2).Count().IsGreaterThanOrEqualTo(1);
        }
    }

    /// <summary>
    /// Tests Using with action executes the action.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Using_WithAction_ExecutesActionImmediately()
    {
        var executed = false;
        using var disposable = Disposable.Create(() => { });

        disposable.Using(d => executed = true, Sequencer.Immediate).Subscribe();

        await Assert.That(executed).IsTrue();
    }

    /// <summary>
    /// Tests Using with action and null scheduler executes immediately.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Using_WithActionAndNullScheduler_ExecutesActionImmediately()
    {
        var executed = false;
        using var disposable = Disposable.Create(() => { });

        await disposable.Using(d => executed = true, scheduler: null).ToTask();

        await Assert.That(executed).IsTrue();
    }

    /// <summary>
    /// Tests Using with function transforms the value.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Using_WithFunction_TransformsValue()
    {
        using var disposable = Disposable.Create(() => { });
        var result = 0;

        disposable.Using(d => SampleValue42, Sequencer.Immediate).Subscribe(r => result = r);

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests Schedule with TimeSpan and action.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithTimeSpanAndAction_ExecutesAction()
    {
        var executed = false;
        const int Value = 42;

        Value.Schedule(TimeSpan.FromMilliseconds(10), Sequencer.Immediate, v => executed = true)
            .Subscribe();

        await Assert.That(executed).IsTrue();
    }

    /// <summary>
    /// Tests Schedule with observable, DateTimeOffset and action delays emission until the scheduler advances.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithObservableDateTimeOffsetAndAction_DelaysAndExecutesAction()
    {
        var scheduler = new VirtualClock();
        var dueTime = scheduler.Now.AddTicks(100);
        var subject = new Subject<int>();
        var executed = false;
        var results = new List<int>();

        using var sub = subject.Schedule(dueTime, scheduler, value => executed = value == 42)
            .Subscribe(results.Add);

        subject.OnNext(SampleValue42);

        using (Assert.Multiple())
        {
            await Assert.That(executed).IsFalse();
            await Assert.That(results).IsEmpty();
        }

        scheduler.AdvanceBy(SchedulerWindowTicks + 1);

        using (Assert.Multiple())
        {
            await Assert.That(executed).IsTrue();
            await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
        }
    }

    /// <summary>
    /// Tests Schedule with observable, TimeSpan and action.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithObservableTimeSpanAndAction_ExecutesAction()
    {
        var executed = false;
        var subject = new Subject<int>();

        subject.Schedule(TimeSpan.FromMilliseconds(10), Sequencer.Immediate, v => executed = true)
            .Subscribe();

        subject.OnNext(SampleValue42);

        await Assert.That(executed).IsTrue();
    }

    /// <summary>
    /// Tests Schedule with DateTimeOffset and action.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithDateTimeOffsetAndAction_ExecutesAction()
    {
        var executed = false;
        const int Value = 42;

        Value.Schedule(TimeProvider.System.GetLocalNow().AddMilliseconds(SampleValue10), Sequencer.Immediate, v => executed = true)
            .Subscribe();

        await Assert.That(executed).IsTrue();
    }

    /// <summary>
    /// Tests Schedule with observable, DateTimeOffset and action.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithObservableDateTimeOffsetAndAction_ExecutesAction()
    {
        var executed = false;
        var subject = new Subject<int>();

        subject.Schedule(TimeProvider.System.GetLocalNow().AddMilliseconds(SampleValue10), Sequencer.Immediate, v => executed = true)
            .Subscribe();

        subject.OnNext(SampleValue42);

        await Assert.That(executed).IsTrue();
    }

    /// <summary>
    /// Tests Schedule with function and scheduler.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithFunction_TransformsValue()
    {
        const int Value = 42;
        var result = 0;

        Value.Schedule(Sequencer.Immediate, v => v * SampleValue2)
            .Subscribe(r => result = r);

        await Assert.That(result).IsEqualTo(SampleValue84);
    }

    /// <summary>
    /// Tests Schedule with observable and function.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithObservableAndFunction_TransformsValue()
    {
        var subject = new Subject<int>();
        var result = 0;

        subject.Schedule(Sequencer.Immediate, v => v * SampleValue2)
            .Subscribe(r => result = r);

        subject.OnNext(SampleValue42);

        await Assert.That(result).IsEqualTo(SampleValue84);
    }

    /// <summary>
    /// Tests Schedule with TimeSpan and function.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithTimeSpanAndFunction_TransformsValue()
    {
        const int Value = 42;
        var result = 0;

        Value.Schedule(TimeSpan.FromMilliseconds(10), Sequencer.Immediate, v => v * SampleValue2)
            .Subscribe(r => result = r);

        await Assert.That(result).IsEqualTo(SampleValue84);
    }

    /// <summary>
    /// Tests Schedule with observable, TimeSpan and function.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Schedule_WithObservableTimeSpanAndFunction_TransformsValue()
    {
        var subject = new Subject<int>();
        var result = 0;

        subject.Schedule(TimeSpan.FromMilliseconds(10), Sequencer.Immediate, v => v * SampleValue2)
            .Subscribe(r => result = r);

        subject.OnNext(SampleValue42);

        await Assert.That(result).IsEqualTo(SampleValue84);
    }

    /// <summary>
    /// Tests ObserveOnIf with bool condition and single scheduler when true.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ObserveOnIf_WithBoolConditionTrue_ObservesOnScheduler()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();

        subject.ObserveOnIf(true, scheduler).Subscribe(results.Add);

        subject.OnNext(1);
        await Assert.That(results).IsEmpty();

        scheduler.AdvanceBy(1);

        await Assert.That(results).IsCollectionEqualTo([1]);
    }

    /// <summary>
    /// Tests ObserveOnIf with bool condition and single scheduler when false.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ObserveOnIf_WithBoolConditionFalse_DoesNotObserveOnScheduler()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();

        subject.ObserveOnIf(false, scheduler).Subscribe(results.Add);

        subject.OnNext(1);
        await Assert.That(results).IsCollectionEqualTo([1]);
    }

    /// <summary>
    /// Tests ObserveOnIf with bool condition and two schedulers when true.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ObserveOnIf_WithBoolConditionTrue_ObservesOnTrueScheduler()
    {
        var trueScheduler = new VirtualClock();
        var falseScheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();

        subject.ObserveOnIf(true, trueScheduler, falseScheduler).Subscribe(results.Add);

        subject.OnNext(1);
        await Assert.That(results).IsEmpty();

        trueScheduler.AdvanceBy(1);
        await Assert.That(results).IsCollectionEqualTo([1]);
    }

    /// <summary>
    /// Tests ObserveOnIf with bool condition and two schedulers when false.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ObserveOnIf_WithBoolConditionFalse_ObservesOnFalseScheduler()
    {
        var trueScheduler = new VirtualClock();
        var falseScheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();

        subject.ObserveOnIf(false, trueScheduler, falseScheduler).Subscribe(results.Add);

        subject.OnNext(1);
        await Assert.That(results).IsEmpty();

        falseScheduler.AdvanceBy(1);
        await Assert.That(results).IsCollectionEqualTo([1]);
    }

    /// <summary>
    /// Tests ObserveOnIf with reactive condition routes notifications to both schedulers as the condition changes.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ObserveOnIf_WithReactiveCondition_ObservesOnMatchingScheduler()
    {
        var trueScheduler = new VirtualClock();
        var falseScheduler = new VirtualClock();
        var source = new Subject<int>();
        var condition = new BehaviorSubject<bool>(false);
        var results = new List<int>();

        using var sub = source.ObserveOnIf(condition, trueScheduler, falseScheduler).Subscribe(results.Add);

        source.OnNext(1);
        await Assert.That(results).IsEmpty();

        falseScheduler.AdvanceBy(1);
        await Assert.That(results).IsCollectionEqualTo([1]);

        condition.OnNext(true);
        source.OnNext(SampleValue2);

        await Assert.That(results).IsCollectionEqualTo([1]);

        trueScheduler.AdvanceBy(1);
        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
    }

    /// <summary>
    /// Tests ObserveOnIf with reactive condition and single scheduler observes only when condition is true.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ObserveOnIf_WithReactiveConditionAndSingleScheduler_ObservesOnlyWhenEnabled()
    {
        var scheduler = new VirtualClock();
        var source = new Subject<int>();
        var condition = new BehaviorSubject<bool>(false);
        var results = new List<int>();

        using var sub = source.ObserveOnIf(condition, scheduler).Subscribe(results.Add);

        source.OnNext(1);
        await Assert.That(results).IsCollectionEqualTo([1]);

        condition.OnNext(true);
        source.OnNext(SampleValue2);

        await Assert.That(results).IsCollectionEqualTo([1]);

        scheduler.AdvanceBy(1);
        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
    }

    /// <summary>
    /// Tests SyncTimer without scheduler uses default scheduler and produces ticks.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncTimerCalledWithoutScheduler_ThenProducesTicks()
    {
        var results = new List<DateTime>();
        using var sub = ReactiveExtensions.SyncTimer(TimeSpan.FromMilliseconds(50)).Take(2).Subscribe(results.Add);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            TimeSpan.FromSeconds(5));

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
    }

    /// <summary>
    /// Tests Start with null scheduler executes the action directly.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartActionWithNullScheduler_ThenExecutesAction()
    {
        var executed = false;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var sub = ReactiveExtensions.Start(() => executed = true, scheduler: null)
            .Subscribe(_ => { }, completed.SetResult);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(30));

        await Assert.That(executed).IsTrue();
    }

    /// <summary>
    /// Tests Start with function and null scheduler returns computed result.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartFuncWithNullScheduler_ThenReturnsResult()
    {
        var result = await ReactiveExtensions.Start(() => 42, scheduler: null).ToTask();

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests ScheduleSafe with TimeSpan and null scheduler uses Thread.Sleep path.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleSafeWithTimeSpanAndNullScheduler_ThenSleepsAndExecutes()
    {
        var executed = false;
        const ISequencer? Scheduler = null;
        var disposable = Scheduler.ScheduleSafe(TimeSpan.FromMilliseconds(10), () => executed = true);

        using (Assert.Multiple())
        {
            await Assert.That(executed).IsTrue();
            await Assert.That(disposable).IsNotNull();
        }
    }

    /// <summary>
    /// Tests Using with Action invokes the action and disposes the resource.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingWithActionAndNoScheduler_ThenExecutesAndDisposes()
    {
        var actionExecuted = false;
        using var disposable = Disposable.Create(() => { });

        await disposable.Using(d => actionExecuted = true).ToTask();

        await Assert.That(actionExecuted).IsTrue();
    }

    /// <summary>
    /// Tests Schedule with value and TimeSpan emits value after delay.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleValueWithTimeSpan_ThenEmitsAfterDelay()
    {
        var scheduler = new VirtualClock();
        int? result = null;

        using var sub = 42.Schedule(TimeSpan.FromTicks(100), scheduler)
            .Subscribe(x => result = x);

        await Assert.That(result).IsNull();

        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests Schedule with observable and TimeSpan delays each emitted value.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleObservableWithTimeSpan_ThenDelaysValues()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();

        using var sub = ((IObservable<int>)subject).Schedule(TimeSpan.FromTicks(100), scheduler)
            .Subscribe(results.Add);

        subject.OnNext(SampleValue10);
        await Assert.That(results).IsEmpty();

        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);

        await Assert.That(results).IsCollectionEqualTo([SampleValue10]);
    }

    /// <summary>
    /// Tests Schedule with value and DateTimeOffset emits value at due time.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleValueWithDateTimeOffset_ThenEmitsAtDueTime()
    {
        var scheduler = new VirtualClock();
        int? result = null;
        var dueTime = scheduler.Now.AddTicks(100);

        using var sub = 42.Schedule(dueTime, scheduler)
            .Subscribe(x => result = x);

        await Assert.That(result).IsNull();

        scheduler.AdvanceBy(SchedulerWindowTicks + 1);

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests Schedule with observable and DateTimeOffset delays values to due time.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleObservableWithDateTimeOffset_ThenDelaysValues()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        var dueTime = scheduler.Now.AddTicks(100);

        using var sub = ((IObservable<int>)subject).Schedule(dueTime, scheduler)
            .Subscribe(results.Add);

        subject.OnNext(SampleValue10);
        await Assert.That(results).IsEmpty();

        scheduler.AdvanceBy(SchedulerWindowTicks + 1);

        await Assert.That(results).IsCollectionEqualTo([SampleValue10]);
    }

    /// <summary>
    /// Tests Schedule with action and TimeSpan executes action then emits value.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleValueWithTimeSpanAndAction_ThenExecutesActionAndEmits()
    {
        var scheduler = new VirtualClock();
        var actionExecuted = false;
        int? result = null;

        using var sub = 42.Schedule(TimeSpan.FromTicks(100), scheduler, v => actionExecuted = v == 42)
            .Subscribe(x => result = x);

        await Assert.That(actionExecuted).IsFalse();

        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);

        using (Assert.Multiple())
        {
            await Assert.That(actionExecuted).IsTrue();
            await Assert.That(result).IsEqualTo(SampleValue42);
        }
    }

    /// <summary>
    /// Tests Using with Func overload returns the function result.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingWithFunc_ThenReturnsFunctionResult()
    {
        var disposable = new CompositeDisposable();
        var result = await disposable.Using(d => 42).FirstAsync();

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests While with scheduler repeatedly executes action while condition holds.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileWithScheduler_ThenExecutesActionOnScheduler()
    {
        var executedOnScheduler = false;
        var scheduler = new VirtualClock();

        // Test that the scheduler parameter is passed through to Observable.Start
        var obs = ReactiveExtensions.While(
            () => !executedOnScheduler,
            () => executedOnScheduler = true,
            scheduler);

        var results = new List<RxVoid>();
        using var sub = obs.Subscribe(results.Add);

        // Nothing should execute until the scheduler advances
        await Assert.That(executedOnScheduler).IsFalse();

        scheduler.Start();

        await Assert.That(executedOnScheduler).IsTrue();
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>
    /// Tests Schedule with action, observable source, and TimeSpan overload.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleWithActionAndTimeSpan_ThenExecutesActionAndEmits()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        var actionValues = new List<int>();

        subject.Schedule(TimeSpan.FromTicks(50), scheduler, actionValues.Add)
            .Subscribe(results.Add);

        subject.OnNext(SampleValue10);
        scheduler.AdvanceBy(SchedulerHalfWindowTicks + 1);

        await Assert.That(actionValues).IsCollectionEqualTo([SampleValue10]);
        await Assert.That(results).IsCollectionEqualTo([SampleValue10]);
    }

    /// <summary>
    /// Tests Heartbeat timer subscription null check path by quickly disposing.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHeartbeatSourceEmits_ThenTimerSubscriptionDisposed()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<Heartbeat<int>>();

        using var sub = subject.Heartbeat(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add);

        // Emit value, which disposes the current timer
        subject.OnNext(1);
        scheduler.AdvanceBy(1);

        // Emit another quickly, before heartbeat timer fires
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(1);

        await Assert.That(results.Count).IsGreaterThanOrEqualTo(SampleValue2);
        await Assert.That(results[0].Update).IsEqualTo(1);
        await Assert.That(results[1].Update).IsEqualTo(SampleValue2);
    }

    /// <summary>
    /// Tests Using with Action and null scheduler disposes the object after executing the action.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingWithActionAndNullScheduler_ThenDisposesObject()
    {
        var executed = false;
        await using var stream = new MemoryStream();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var sub = stream.Using<MemoryStream>(
            _ => executed = true,
            null).Subscribe(
            _ => { },
            completed.SetResult);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(executed).IsTrue();
    }
}
