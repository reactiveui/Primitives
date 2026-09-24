// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Maui;
using Microsoft.Maui.Dispatching;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Maui.Tests;

/// <summary>Tests <see cref="MauiDispatcherSequencer"/>'s immediate and time-based dispatch paths through a fake <see cref="IDispatcher"/>.</summary>
public sealed class MauiDispatcherSequencerTests
{
    /// <summary>The values an immediate burst produces, in the FIFO order asserted.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Installs a dispatcher provider that returns the dispatcher each test thread registers.</summary>
    [Before(Class)]
    public static void InstallThreadDispatcherProvider() => _ = DispatcherProvider.SetCurrent(new ThreadDispatcherProvider());

    /// <summary>Verifies the constructor rejects a null dispatcher.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher() =>
        await Assert.That(static () => new MauiDispatcherSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the dispatcher extension method validates and adapts dispatchers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ToSequencerValidatesAndAdaptsDispatcher()
    {
        const IDispatcher nullDispatcher = null!;
        await Assert.That(static () => nullDispatcher!.ToSequencer()).ThrowsExactly<ArgumentNullException>();

        FakeDispatcher dispatcher = new();
        var sequencer = dispatcher.ToSequencer();

        await Assert.That(sequencer).IsNotNull();
    }

    /// <summary>Verifies immediate work is marshalled through <see cref="IDispatcher.Dispatch(Action)"/> and executed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleDispatchesAndExecutes()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer sequencer = new(dispatcher);
        var executed = false;

        sequencer.Schedule(new DelegateWorkItem(() => executed = true));

        await Assert.That(executed).IsTrue();
        await Assert.That(dispatcher.DispatchCount).IsGreaterThan(0);
        await Assert.That(dispatcher.DispatchDelayedCount).IsEqualTo(0);
    }

    /// <summary>Verifies future work routes through <see cref="IDispatcher.DispatchDelayed(TimeSpan, Action)"/> with a positive delay.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleUsesDispatchDelayed()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer sequencer = new(dispatcher);
        var executed = false;

        sequencer.Schedule(new DelegateWorkItem(() => executed = true), long.MaxValue);

        await Assert.That(executed).IsTrue();
        await Assert.That(dispatcher.DispatchDelayedCount).IsEqualTo(1);
        await Assert.That(dispatcher.LastDelay).IsGreaterThan(TimeSpan.Zero);
    }

    /// <summary>Verifies a due timestamp that is not in the future takes the immediate path rather than the delayed timer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task PastDueTimestampUsesImmediatePath()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer sequencer = new(dispatcher);
        var executed = false;

        // A timestamp read before the call cannot be later than the monotonic clock the sequencer reads inside it.
        var due = sequencer.Timestamp;
        sequencer.Schedule(new DelegateWorkItem(() => executed = true), due);

        await Assert.That(executed).IsTrue();
        await Assert.That(dispatcher.DispatchCount).IsGreaterThan(0);
        await Assert.That(dispatcher.DispatchDelayedCount).IsEqualTo(0);
    }

    /// <summary>Verifies a burst of immediate work items all execute in FIFO order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateBurstExecutesInOrder()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer sequencer = new(dispatcher);
        List<int> values = [];

        foreach (var value in ExpectedBurst)
        {
            var captured = value;
            sequencer.Schedule(new DelegateWorkItem(() => values.Add(captured)));
        }

        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    /// <summary>Verifies the sequencer surfaces the shared dispatch clock through both clock properties.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ClockPropertiesReportTheSharedDispatchClock()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer sequencer = new(dispatcher);
        var before = sequencer.Timestamp;

        await Assert.That(sequencer.Now).IsGreaterThan(DateTimeOffset.MinValue);
        await Assert.That(sequencer.Timestamp).IsGreaterThanOrEqualTo(before);
    }

    /// <summary>A thread without a dispatcher gets an error rather than a sequencer that could never run work.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task CurrentThrowsWhenTheThreadHasNoDispatcher() =>
        await Assert.That(static () => RunOnNewThread<MauiDispatcherSequencer?>(null, static () => MauiDispatcherSequencer.Current))
            .ThrowsExactly<InvalidOperationException>();

    /// <summary>Each thread gets its own cached sequencer bound to that thread's dispatcher.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task CurrentIsCachedPerThreadAndBoundToThatThreadsDispatcher()
    {
        FakeDispatcher firstDispatcher = new();
        FakeDispatcher secondDispatcher = new();

        var first = await RunOnNewThread(firstDispatcher, CaptureCurrent);
        var second = await RunOnNewThread(secondDispatcher, CaptureCurrent);

        await Assert.That(first.Repeat).IsSameReferenceAs(first.Sequencer);
        await Assert.That(first.Sequencer.Dispatcher).IsSameReferenceAs(firstDispatcher);
        await Assert.That(second.Sequencer).IsNotSameReferenceAs(first.Sequencer);
        await Assert.That(second.Sequencer.Dispatcher).IsSameReferenceAs(secondDispatcher);
    }

    /// <summary>Before an application exists, Main uses the calling thread's dispatcher and never the thread pool.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task MainFallsBackToCurrentBeforeAnApplicationExists()
    {
        if (IPlatformApplication.Current is not null)
        {
            return;
        }

        FakeDispatcher dispatcher = new();
        var captured = await RunOnNewThread(
            dispatcher,
            static () => (MauiDispatcherSequencer.Main, MauiDispatcherSequencer.Current));

        await Assert.That(captured.Main).IsSameReferenceAs(captured.Current);
        await Assert.That(captured.Main.Dispatcher).IsSameReferenceAs(dispatcher);
        await Assert.That(static () => RunOnNewThread<MauiDispatcherSequencer?>(null, static () => MauiDispatcherSequencer.Main))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>The application's dispatcher comes from the running application's services.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ResolveApplicationDispatcherReadsTheApplicationServices()
    {
        FakeDispatcher dispatcher = new();

        await Assert.That(MauiDispatcherSequencer.ResolveApplicationDispatcher(null)).IsNull();
        await Assert.That(MauiDispatcherSequencer.ResolveApplicationDispatcher(new FakePlatformApplication(null))).IsNull();
        await Assert.That(MauiDispatcherSequencer.ResolveApplicationDispatcher(new FakePlatformApplication(new DispatcherServices(null))))
            .IsNull();
        await Assert.That(MauiDispatcherSequencer.ResolveApplicationDispatcher(new FakePlatformApplication(new DispatcherServices(dispatcher))))
            .IsSameReferenceAs(dispatcher);
    }

    /// <summary>Without an application dispatcher there is nothing to bind, and nothing is cached.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task BindMainReturnsNullWithoutAnApplicationDispatcher()
    {
        MauiDispatcherSequencer? slot = null;
        await Assert.That(MauiDispatcherSequencer.BindMain(ref slot, null)).IsNull();
        await Assert.That(slot).IsNull();
    }

    /// <summary>The first application dispatcher bound stays bound.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task BindMainKeepsTheFirstBinding()
    {
        FakeDispatcher first = new();
        FakeDispatcher second = new();
        MauiDispatcherSequencer? slot = null;

        var bound = MauiDispatcherSequencer.BindMain(ref slot, first);
        var rebound = MauiDispatcherSequencer.BindMain(ref slot, second);

        await Assert.That(bound).IsNotNull();
        await Assert.That(bound!.Dispatcher).IsSameReferenceAs(first);
        await Assert.That(slot).IsSameReferenceAs(bound);
        await Assert.That(rebound).IsSameReferenceAs(bound);
    }

    /// <summary>Reads <see cref="MauiDispatcherSequencer.Current"/> twice on the calling thread.</summary>
    /// <returns>Both reads.</returns>
    private static (MauiDispatcherSequencer Sequencer, MauiDispatcherSequencer Repeat) CaptureCurrent() =>
        (MauiDispatcherSequencer.Current, MauiDispatcherSequencer.Current);

    /// <summary>Runs a function on a fresh thread that reports the given dispatcher as its own.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="dispatcher">The dispatcher the thread reports, or <see langword="null"/> for none.</param>
    /// <param name="func">The function to run.</param>
    /// <returns>The function's result.</returns>
    private static Task<T> RunOnNewThread<T>(IDispatcher? dispatcher, Func<T> func)
    {
        TaskCompletionSource<T> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Thread thread = new(() =>
        {
            ThreadDispatcherProvider.ForThread = dispatcher;
            try
            {
                completion.SetResult(func());
            }
            catch (Exception ex)
            {
                completion.SetException(ex);
            }
        });
        thread.Start();
        return completion.Task;
    }

    /// <summary>Dispatcher provider that reports the dispatcher registered by the calling thread.</summary>
    private sealed class ThreadDispatcherProvider : IDispatcherProvider
    {
        /// <summary>The dispatcher registered by the calling thread.</summary>
        [ThreadStatic]
        private static IDispatcher? _forThread;

        /// <summary>Gets or sets the dispatcher registered by the calling thread.</summary>
        public static IDispatcher? ForThread
        {
            get => _forThread;
            set => _forThread = value;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDispatcher? GetForCurrentThread() => _forThread;
    }

    /// <summary>Platform application that exposes the given services.</summary>
    /// <param name="services">The application services, or <see langword="null"/> before MAUI sets them.</param>
    private sealed class FakePlatformApplication(IServiceProvider? services) : IPlatformApplication
    {
        /// <inheritdoc/>
        public IServiceProvider Services => services!;

        /// <inheritdoc/>
        public IApplication Application => null!;
    }

    /// <summary>Service provider that resolves only <see cref="IDispatcher"/>.</summary>
    /// <param name="dispatcher">The dispatcher to return, or <see langword="null"/> for none.</param>
    private sealed class DispatcherServices(IDispatcher? dispatcher) : IServiceProvider
    {
        /// <inheritdoc/>
        public object? GetService(Type serviceType) => serviceType == typeof(IDispatcher) ? dispatcher : null;
    }

    /// <summary>Work item that invokes a delegate when executed.</summary>
    private sealed class DelegateWorkItem : IWorkItem
    {
        /// <summary>The action to run on execution.</summary>
        private readonly Action _action;

        /// <summary>Initializes a new instance of the <see cref="DelegateWorkItem"/> class.</summary>
        /// <param name="action">The action to run on execution.</param>
        public DelegateWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => _action();
    }

    /// <summary>Fake MAUI dispatcher that runs marshalled work synchronously and records how it was dispatched.</summary>
    private sealed class FakeDispatcher : IDispatcher
    {
        /// <summary>Gets the number of times <see cref="Dispatch(Action)"/> was called.</summary>
        public int DispatchCount { get; private set; }

        /// <summary>Gets the number of times <see cref="DispatchDelayed(TimeSpan, Action)"/> was called.</summary>
        public int DispatchDelayedCount { get; private set; }

        /// <summary>Gets the delay passed to the most recent <see cref="DispatchDelayed(TimeSpan, Action)"/> call.</summary>
        public TimeSpan LastDelay { get; private set; }

        /// <inheritdoc/>
        public bool IsDispatchRequired => true;

        /// <inheritdoc/>
        public bool Dispatch(Action action)
        {
            DispatchCount++;
            action();
            return true;
        }

        /// <inheritdoc/>
        public bool DispatchDelayed(TimeSpan delay, Action action)
        {
            DispatchDelayedCount++;
            LastDelay = delay;
            action();
            return true;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDispatcherTimer CreateTimer() => new FakeDispatcherTimer();

        /// <summary>Fake dispatcher timer that fires its tick immediately on start.</summary>
        private sealed class FakeDispatcherTimer : IDispatcherTimer
        {
            /// <inheritdoc/>
            public event EventHandler? Tick;

            /// <inheritdoc/>
            public TimeSpan Interval { get; set; }

            /// <inheritdoc/>
            public bool IsRepeating { get; set; }

            /// <inheritdoc/>
            public bool IsRunning { get; private set; }

            /// <inheritdoc/>
            public void Start()
            {
                IsRunning = true;
                Tick?.Invoke(this, EventArgs.Empty);
            }

            /// <inheritdoc/>
            public void Stop() => IsRunning = false;
        }
    }
}
