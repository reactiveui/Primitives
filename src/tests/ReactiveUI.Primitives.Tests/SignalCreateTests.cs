// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the signal creation factories.</summary>
public class SignalCreateTests
{
    /// <summary>Value emitted by create-signal tests.</summary>
    private const int CreatedValue = 42;

    /// <summary>The first expected value.</summary>
    private const int First = 1;

    /// <summary>The second expected value.</summary>
    private const int Second = 2;

    /// <summary>The third expected value.</summary>
    private const int Third = 3;

    /// <summary>The fourth expected value.</summary>
    private const int Fourth = 4;

    /// <summary>Expected values for create-with-state tests.</summary>
    private static readonly int[] CreateWithStateExpected = [Third];

    /// <summary>A delay long enough that the subscription is always disposed before it elapses.</summary>
    private static readonly TimeSpan NeverElapsingDelay = TimeSpan.FromSeconds(30);

    /// <summary>How long the test waits for the cancellation callback before failing.</summary>
    private static readonly TimeSpan CancellationTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Creates the argument checking.</summary>
    [Test]
    public void Create_ArgumentChecking()
    {
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Create(default(Func<IObserver<int>, IDisposable>)!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Create((Func<IObserver<int>, IDisposable>)null!).Subscribe(null!));
    }

    /// <summary>Creates the null coalescing action.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Create_NullCoalescingAction()
    {
        var xs = Signal.Create<int>(static o =>
        {
            o.OnNext(CreatedValue);
            return new ActionDisposable(null!);
        });
        List<int> lst = [];
        var d = xs.Subscribe(lst.Add);
        d.Dispose();
        await Assert.That(lst.SequenceEqual([CreatedValue])).IsTrue();
    }

    /// <summary>Creates the exception.</summary>
    [Test]
    public void Create_Exception() => Assert.Throws<InvalidOperationException>(static () => Signal.Create(
            new Func<IObserver<int>, IDisposable>(static _ => throw new InvalidOperationException()))
        .Subscribe());

    /// <summary>Creates the observer throws.</summary>
    [Test]
    public void Create_ObserverThrows()
    {
        _ = Assert.Throws<InvalidOperationException>(static () => Signal.Create<int>(static o =>
        {
            o.OnNext(1);
            return EmptyDisposable.Instance;
        }).Subscribe(static x => throw new InvalidOperationException()));
        _ = Assert.Throws<InvalidOperationException>(static () => Signal.Create<int>(static o =>
        {
            o.OnError(new InvalidOperationException("source"));
            return EmptyDisposable.Instance;
        }).Subscribe(static x => { }, static ex => throw new InvalidOperationException()));
        _ = Assert.Throws<InvalidOperationException>(static () => Signal.Create<int>(static o =>
        {
            o.OnCompleted();
            return EmptyDisposable.Instance;
        }).Subscribe(static x => { }, static ex => { }, static () => throw new InvalidOperationException()));
    }

    /// <summary>Creates the with disposable argument checking.</summary>
    [Test]
    public void CreateWithDisposable_ArgumentChecking()
    {
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Create(default(Func<IObserver<int>, IDisposable>)!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Create<int>(static _ => DummyDisposable.Instance).Subscribe(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Create<int>(static o =>
        {
            o.OnError(null!);
            return DummyDisposable.Instance;
        }).Subscribe(null!));
    }

    /// <summary>Creates the with disposable null coalescing action.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CreateWithDisposable_NullCoalescingAction()
    {
        var xs = Signal.Create<int>(static o =>
        {
            o.OnNext(CreatedValue);
            return null!;
        });
        List<int> lst = [];
        var d = xs.Subscribe(lst.Add);
        d.Dispose();
        await Assert.That(lst.SequenceEqual([CreatedValue])).IsTrue();
    }

    /// <summary>Creates the with disposable exception.</summary>
    [Test]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "A separate named regression test covering the disposable-returning Create overload's subscribe-time throw. "
            + "It shares its assertion shape with Create_Exception but is kept as its own [Test] so the two entry points "
            + "have independent, named coverage.")]
    public void CreateWithDisposable_Exception() => Assert.Throws<InvalidOperationException>(static () => Signal.Create(
            new Func<IObserver<int>, IDisposable>(static _ => throw new InvalidOperationException()))
        .Subscribe());

    /// <summary>Anonymous signals validate the subscribe delegate.</summary>
    [Test]
    public void AnonymousSignal_ArgumentChecking()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            AnonymousSignal<int> invalid = new(null!);
            GC.KeepAlive(invalid);
        });
        AnonymousSignal<int> signal = new(static _ => EmptyDisposable.Instance);
        _ = Assert.Throws<ArgumentNullException>(() => signal.Subscribe(null!));
    }

    /// <summary>Anonymous signals forward subscriptions to the supplied delegate.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnonymousSignal_ForwardsObserverAndDisposable()
    {
        var disposed = false;
        IObserver<int>? seenObserver = null;
        ActionDisposable expectedDisposable = new(() => disposed = true);
        AnonymousSignal<int> signal = new(observer =>
        {
            seenObserver = observer;
            observer.OnNext(CreatedValue);
            observer.OnCompleted();
            return expectedDisposable;
        });
        Recorder<int> observer = new();
        var disposable = signal.Subscribe(observer);
        disposable.Dispose();
        await Assert.That(ReferenceEquals(observer, seenObserver)).IsTrue();
        await Assert.That(disposable).IsSameReferenceAs(expectedDisposable);
        await Assert.That(observer.Values.SequenceEqual([CreatedValue])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Anonymous signals use the empty disposable when a delegate returns null.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnonymousSignal_ReturnsEmptyDisposableForNullDelegateResult()
    {
        AnonymousSignal<int> signal = new(static _ => null!);
        var disposable = signal.Subscribe(new Recorder<int>());
        await Assert.That(disposable).IsSameReferenceAs(EmptyDisposable.Instance);
    }

    /// <summary>Covers create-with-state overloads and null validation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CreateWithStateFactoriesInvokeStatefulSubscribeCallbacks()
    {
        List<int> values = [];
        var completed = 0;
        var disposed = 0;
        _ = Signal.CreateWithState<int, int>(
            Third,
            static (state, observer) =>
            {
                observer.OnNext(state);
                observer.OnCompleted();
                return new ActionDisposable(static () => { });
            },
            false).Subscribe(values.Add, static ex => throw ex, () => completed++);
        var subscription = Signal.CreateWithState<int, int>(Fourth, (state, observer) =>
        {
            observer.OnNext(state);
            return new ActionDisposable(() => disposed++);
        }).Subscribe(static _ => { });
        subscription.Dispose();
        await Assert.That(values.SequenceEqual(CreateWithStateExpected)).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(disposed).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Create<int>(null!, true));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.CreateSafe<int>(null!, true));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.CreateWithState<int, int>(First, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.CreateWithState<int, int>(First, null!, true));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Lazy<int>(null!));
    }

    /// <summary>Verifies create overloads preserve current-thread subscription requirements.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CreateWithCurrentThreadRequirementReportsAndEmitsSynchronously()
    {
        var created = Signal.Create<int>(
            static observer =>
            {
                observer.OnNext(CreatedValue);
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            },
            true);

        RecordingWitness<int> observer = new();
        _ = created.Subscribe(observer);

        await Assert.That(((IRequireCurrentThread<int>)created).IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(observer.Values.SequenceEqual([CreatedValue])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies asynchronous create overloads assign lifetimes, forward failures, and honor cancellation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AsyncCreateFactoriesAssignDisposablesAndForwardFailures()
    {
        var disposed = 0;
        List<int> values = [];
        var created = Signal.Create<int>(observer =>
        {
            observer.OnNext(CreatedValue);
            return Task.FromResult<IDisposable>(new ActionDisposable(() => disposed++));
        });

        var subscription = created.Subscribe(values.Add);
        await Task.Yield();
        subscription.Dispose();

        await Assert.That(values.SequenceEqual([CreatedValue])).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);

        Exception? observed = null;
        InvalidOperationException expected = new("async-create");
        _ = Signal.Create<int>((_, _) => Task.FromException<IDisposable>(expected))
            .Subscribe(static _ => { }, error => observed = error);
        await Task.Yield();

        await Assert.That(observed).IsSameReferenceAs(expected);

        TaskCompletionSource canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellable = Signal.Create<int>(async (_, cancellationToken) =>
        {
            try
            {
                await Task.Delay(NeverElapsingDelay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                canceled.SetResult();
                throw;
            }

            return EmptyDisposable.Instance;
        });

        var cancellableSubscription = cancellable.Subscribe(static _ => { });
        cancellableSubscription.Dispose();
        await canceled.Task.WaitAsync(CancellationTimeout).ConfigureAwait(false);

        var nullDisposable = Signal.Create<int>(static (_, _) => Task.FromResult<IDisposable>(null!));
        var nullSubscription = nullDisposable.Subscribe(static _ => { });
        await Task.Yield();
        nullSubscription.Dispose();

        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Create((Func<IObserver<int>, Task<IDisposable>>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Create((Func<IObserver<int>, CancellationToken, Task<IDisposable>>)null!));
    }

    /// <summary>Verifies asynchronous defer overloads emit, fail, and skip subscription after cancellation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AsyncDeferFactoriesEmitFailAndHonorCancellation()
    {
        List<int> values = [];
        _ = Signal.Defer(static () => Task.FromResult(Signal.Emit(CreatedValue))).Subscribe(values.Add);
        _ = Signal.Defer(static _ => Task.FromResult(Signal.Emit(First))).Subscribe(values.Add);
        await Task.Yield();

        await Assert.That(values.SequenceEqual([CreatedValue, First])).IsTrue();

        Exception? observed = null;
        InvalidOperationException expected = new("defer");
        _ = Signal.Defer(() => Task.FromException<IObservable<int>>(expected))
            .Subscribe(static _ => { }, error => observed = error);
        await Task.Yield();

        await Assert.That(observed).IsSameReferenceAs(expected);

        TaskCompletionSource<IObservable<int>> delayedFactory = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> canceledValues = [];
        var deferred = Signal.Defer(_ => delayedFactory.Task);
        var subscription = deferred.Subscribe(canceledValues.Add);
        subscription.Dispose();
        delayedFactory.SetResult(Signal.Emit(Fourth));
        await Task.Yield();

        await Assert.That(canceledValues.Count).IsEqualTo(0);

        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Defer((Func<Task<IObservable<int>>>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Defer((Func<CancellationToken, Task<IObservable<int>>>)null!));
    }

    /// <summary>Verifies Rx-named signal factories route to the corresponding Primitives factories.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RxSignalFactoryAliasesRouteToPrimitiveFactories()
    {
        List<int> values = [];
        _ = Signal.Return(CreatedValue, Sequencer.Immediate).Subscribe(values.Add);
        _ = Signal.Merge(Signal.Return(First), Signal.Return(Third)).Subscribe(values.Add);
        _ = Signal.Merge((IEnumerable<IObservable<int>>)[Signal.Return(Fourth)]).Subscribe(values.Add);
        _ = Signal.Switch(Signal.Return(Signal.Return(First))).Subscribe(values.Add);
        _ = Signal.Range(Second, Second).Subscribe(values.Add);
        _ = Signal.Range(Second, Second, Sequencer.Immediate).Subscribe(values.Add);
        _ = Signal.Concat(Signal.Return(Fourth), Signal.Return(First)).Subscribe(values.Add);
        _ = Signal.Concat((IEnumerable<IObservable<int>>)[Signal.Return(Second)]).Subscribe(values.Add);

        await Assert.That(values.SequenceEqual([
            CreatedValue,
            First,
            Third,
            Fourth,
            First,
            Second,
            Third,
            Second,
            Third,
            Fourth,
            First,
            Second
        ])).IsTrue();

        var completions = 0;
        _ = Signal.Empty<int>().Subscribe(static _ => { }, static ex => throw ex, () => completions++);
        _ = Signal.Empty<int>(Sequencer.Immediate).Subscribe(static _ => { }, static ex => throw ex, () => completions++);

        await Assert.That(completions).IsEqualTo(Second);

        List<Exception> errors = [];
        InvalidOperationException expected = new("throw");
        _ = Signal.Throw<int>(expected, Sequencer.Immediate).Subscribe(static _ => { }, errors.Add);

        await Assert.That(errors.Count).IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);

        await Assert.That(Signal.Never<int>()).IsNotNull();
        await Assert.That(Signal.Timer(TimeSpan.FromTicks(1))).IsNotNull();
        await Assert.That(Signal.Timer(TimeSpan.FromTicks(1), Sequencer.Immediate)).IsNotNull();
        await Assert.That(Signal.Timer(DateTimeOffset.UnixEpoch.AddTicks(1))).IsNotNull();
        await Assert.That(Signal.Timer(DateTimeOffset.UnixEpoch.AddTicks(1), Sequencer.Immediate)).IsNotNull();
        await Assert.That(Signal.Timer(TimeSpan.FromTicks(1), TimeSpan.FromTicks(1))).IsNotNull();
        await Assert.That(Signal.Timer(TimeSpan.FromTicks(1), TimeSpan.FromTicks(1), Sequencer.Immediate))
            .IsNotNull();
        await Assert.That(Signal.Interval(TimeSpan.FromTicks(1))).IsNotNull();

        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        List<long> intervalValues = [];
        using var interval = Signal.Interval(TimeSpan.FromTicks(1), clock).Take(Third).Subscribe(intervalValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Third));

        long[] expectedIntervalValues = [0L, First, Second];
        await Assert.That(intervalValues.SequenceEqual(expectedIntervalValues)).IsTrue();
    }

    /// <summary>The guarded create factory honours a current-thread requirement and releases its subscription once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CreateSafeWithCurrentThreadRequirementEmitsAndReleasesItsSubscription()
    {
        RecordingDisposable inner = new();
        var created = Signal.CreateSafe<int>(
            observer =>
            {
                observer.OnNext(CreatedValue);
                observer.OnCompleted();
                return inner;
            },
            true);

        await Assert.That(((IRequireCurrentThread<int>)created).IsRequiredSubscribeOnCurrentThread()).IsTrue();

        RecordingWitness<int> witness = new();
        var subscription = created.Subscribe(witness);

        await Assert.That(witness.Values.SequenceEqual([CreatedValue])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);

        subscription.Dispose();
        subscription.Dispose();

        await Assert.That(inner.DisposeCount).IsEqualTo(1);
    }

    /// <summary>The guarded create factory tolerates a subscribe callback that returns no disposable.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CreateSafeToleratesASubscribeCallbackThatReturnsNoDisposable()
    {
        var created = Signal.CreateSafe<int>(
            static observer =>
            {
                observer.OnNext(CreatedValue);
                observer.OnCompleted();
                return null!;
            },
            true);

        RecordingWitness<int> witness = new();
        using var subscription = created.Subscribe(witness);

        await Assert.That(witness.Values.SequenceEqual([CreatedValue])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>The stateful create factory reports the current-thread requirement it was built with.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CreateWithStateReportsItsCurrentThreadRequirement()
    {
        var required = Signal.CreateWithState<int, int>(
            CreatedValue,
            static (state, observer) =>
            {
                observer.OnNext(state);
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            },
            true);

        var optional = Signal.CreateWithState<int, int>(
            CreatedValue,
            static (state, observer) =>
            {
                observer.OnNext(state);
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            },
            false);

        await Assert.That(((IRequireCurrentThread<int>)required).IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(((IRequireCurrentThread<int>)optional).IsRequiredSubscribeOnCurrentThread()).IsFalse();

        RecordingWitness<int> witness = new();
        using var subscription = required.Subscribe(witness);

        await Assert.That(witness.Values.SequenceEqual([CreatedValue])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>Records observer notifications.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the number of completion notifications.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        public void OnError(Exception error) => throw error;

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }
}
