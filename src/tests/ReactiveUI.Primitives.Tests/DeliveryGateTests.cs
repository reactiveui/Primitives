// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the non-blocking delivery serializer.</summary>
[DebuggerDisplay("DeliveryGateTests")]
public sealed class DeliveryGateTests
{
    /// <summary>The number of drains a test expects when one piece of work follows another.</summary>
    private const int TwoDrains = 2;

    /// <summary>The default wait budget, in milliseconds.</summary>
    private const int DefaultBudgetMilliseconds = 20;

    /// <summary>The number of producers in the ordering test.</summary>
    private const int Producers = 8;

    /// <summary>The number of items each producer queues in the ordering test.</summary>
    private const int ItemsPerProducer = 5_000;

    /// <summary>A signal with nothing delivering drains once on the calling thread.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SignalOnAnIdleGateDrainsOnTheCallingThread()
    {
        ConcurrentQueue<int> threads = new();
        GateHost host = new(() => threads.Enqueue(Environment.CurrentManagedThreadId));

        host.Signal();

        await Assert.That(Describe(threads)).IsEqualTo(Environment.CurrentManagedThreadId.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>A signal raised by the drain itself is drained after the drain returns, never inside it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SignalFromTheDrainRunsAfterTheDrainReturns()
    {
        var depth = 0;
        var maxDepth = 0;
        var drains = 0;
        StrongBox<GateHost> host = new();
        host.Value = new(() =>
        {
            depth++;
            drains++;
            maxDepth = Math.Max(maxDepth, depth);
            if (drains == 1)
            {
                host.Value!.Signal();
            }

            depth--;
        });

        host.Value.Signal();

        await Assert.That(drains).IsEqualTo(TwoDrains);
        await Assert.That(maxDepth).IsEqualTo(1);
    }

    /// <summary>
    /// A producer that finds a delivery outlasting its budget hands its work over and returns, and the delivering thread
    /// drains it once its observer returns.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SignalPastTheBudgetHandsWorkToTheDeliveringThread()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        ConcurrentQueue<int> threads = new();
        GateHost host = new(() => RecordAndBlockFirst(threads, inside, release));

        var owner = BackgroundThread.Start(host.Signal);
        inside.Wait();

        var producer = BackgroundThread.Start(() => host.Signal(TimeSpan.Zero));
        await Assert.That(await BackgroundThread.FinishesPromptly(producer)).IsTrue();

        release.Set();
        await Assert.That(await BackgroundThread.FinishesPromptly(owner)).IsTrue();
        var ownerThread = await owner;
        await Assert.That(Describe(threads)).IsEqualTo($"{ownerThread},{ownerThread}");
    }

    /// <summary>A producer waiting within its budget delivers its work on its own thread once the delivery finishes.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SignalWithinTheBudgetDrainsOnTheWaitingThread()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        ConcurrentQueue<int> threads = new();
        GateHost host = new(() => RecordAndBlockFirst(threads, inside, release));

        var owner = BackgroundThread.Start(host.Signal);
        inside.Wait();
        var producer = BackgroundThread.Start(() => host.Signal(TimeSpan.FromMinutes(1)));

        release.Set();
        await Assert.That(await BackgroundThread.FinishesPromptly(owner)).IsTrue();
        await Assert.That(await BackgroundThread.FinishesPromptly(producer)).IsTrue();
        await Assert.That(Describe(threads)).IsEqualTo($"{await owner},{await producer}");
    }

    /// <summary>A delivering thread whose observer marshals to a producer's thread is not deadlocked by that producer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverMarshallingToAProducerThreadDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
        ConcurrentQueue<int> threads = new();
        GateHost host = new(() =>
        {
            threads.Enqueue(Environment.CurrentManagedThreadId);
            if (Environment.CurrentManagedThreadId == dispatcher.ManagedThreadId || inside.IsSet)
            {
                return;
            }

            inside.Set();
            dispatcher.Invoke(static () => { });
        });

        dispatcher.Post(() =>
        {
            inside.Wait();
            host.Signal();
        });
        var worker = BackgroundThread.Start(host.Signal);

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(threads.Count).IsEqualTo(TwoDrains);
    }

    /// <summary>An enter refuses while another delivery runs, and the exit drains work handed over meanwhile.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ExitDrainsWorkHandedOverWhileEntered()
    {
        var drains = 0;
        GateHost host = new(() => drains++);

        var entered = host.TryEnter();
        var reentered = host.TryEnter();
        host.Signal();
        await Assert.That(drains).IsEqualTo(0);

        host.Exit();

        await Assert.That(entered).IsTrue();
        await Assert.That(reentered).IsFalse();
        await Assert.That(drains).IsEqualTo(1);
        await Assert.That(host.TryEnter()).IsTrue();
    }

    /// <summary>A producer waiting for the gate is not overtaken by a later direct enter.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryEnterDefersToAWaitingProducer()
    {
        var waiterDrained = 0;
        GateHost host = new(() => Volatile.Write(ref waiterDrained, 1));
        _ = host.TryEnter();
        var waiter = BackgroundThread.Start(() => host.Signal(TimeSpan.FromMinutes(1)));
        SpinWait.SpinUntil(() => host.Waiters != 0);

        host.Exit();
        var barged = host.TryEnter();
        var drainedBeforeEnter = Volatile.Read(ref waiterDrained) == 1;
        if (barged)
        {
            _ = host.Reset();
        }

        await Assert.That(await BackgroundThread.FinishesPromptly(waiter)).IsTrue();
        await Assert.That(!barged || drainedBeforeEnter).IsTrue();
        await Assert.That(waiterDrained).IsEqualTo(1);
    }

    /// <summary>A reentrant enter on the delivering thread runs the nested delivery inside the outer one.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryEnterReentrantRunsTheNestedDeliveryInside()
    {
        List<string> log = [];
        GateHost host = new(() => log.Add("drain"));

        var outer = host.TryEnterReentrant();
        log.Add("outer");
        var nested = host.TryEnterReentrant();
        log.Add("nested");
        host.Exit();
        var heldAfterNestedExit = !host.TryEnter();
        log.Add("outer-end");
        host.Exit();

        await Assert.That(outer).IsTrue();
        await Assert.That(nested).IsTrue();
        await Assert.That(heldAfterNestedExit).IsTrue();
        await Assert.That(string.Join(",", log)).IsEqualTo("outer,nested,outer-end");
        await Assert.That(host.TryEnter()).IsTrue();
    }

    /// <summary>The outer exit of a reentrant delivery still drains work another thread handed over.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryEnterReentrantOuterExitDrainsHandedOverWork()
    {
        var drains = 0;
        GateHost host = new(() => drains++);

        var drainsAfterNestedExit = -1;
        await BackgroundThread.Start(() =>
        {
            _ = host.TryEnterReentrant();
            Thread producer = new(() => host.Signal(TimeSpan.Zero));
            producer.Start();
            producer.Join();
            _ = host.TryEnterReentrant();
            host.Exit();
            drainsAfterNestedExit = drains;
            host.Exit();
        });

        await Assert.That(drainsAfterNestedExit).IsEqualTo(0);
        await Assert.That(drains).IsEqualTo(1);
    }

    /// <summary>A reentrant enter from a thread that is not delivering is refused.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryEnterReentrantRefusesAnotherThread()
    {
        GateHost host = new(static () => { });
        _ = host.TryEnter();

        var entered = true;
        await BackgroundThread.Start(() => entered = host.TryEnterReentrant());

        await Assert.That(entered).IsFalse();
    }

    /// <summary>A drain that throws leaves the gate idle, so the next signal drains again.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ThrowingDrainLeavesTheGateIdle()
    {
        var drains = 0;
        GateHost host = new(() =>
        {
            drains++;
            if (drains != 1)
            {
                return;
            }

            throw new InvalidOperationException("drain");
        });

        await Assert.That(host.Signal).Throws<InvalidOperationException>();
        host.Signal();

        await Assert.That(drains).IsEqualTo(TwoDrains);
    }

    /// <summary>A reset with nothing handed over reports no pending work and frees the gate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ResetReleasesAnEnteredGate()
    {
        GateHost host = new(static () => { });

        _ = host.TryEnter();
        var pending = host.Reset();

        await Assert.That(pending).IsFalse();
        await Assert.That(host.TryEnter()).IsTrue();
    }

    /// <summary>A reset keeps work handed over during the failed delivery until a signal drains it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ResetKeepsHandedOverWorkForTheNextSignal()
    {
        var drains = 0;
        GateHost host = new(() => drains++);

        _ = host.TryEnter();
        await BackgroundThread.Start(() => host.Signal(TimeSpan.Zero));
        var pending = host.Reset();
        var directEnterWhilePending = host.TryEnter();
        host.Signal();

        await Assert.That(pending).IsTrue();
        await Assert.That(directEnterWhilePending).IsFalse();
        await Assert.That(drains).IsEqualTo(1);
        await Assert.That(host.TryEnter()).IsTrue();
    }

    /// <summary>A reset of a nested reentrant delivery keeps the outer delivery's hold on the gate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ResetInsideANestedEnterKeepsTheOuterDelivery()
    {
        GateHost host = new(static () => { });

        _ = host.TryEnterReentrant();
        _ = host.TryEnterReentrant();
        var pending = host.Reset();
        var heldByOuter = !host.TryEnter();
        host.Exit();

        await Assert.That(pending).IsFalse();
        await Assert.That(heldByOuter).IsTrue();
        await Assert.That(host.TryEnter()).IsTrue();
    }

    /// <summary>Producers that always hand off still have every queued item delivered once, in order, with no overlap.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZeroBudgetProducersDeliverEveryItemInOrder()
    {
        Queue<(int Producer, int Sequence)> queue = new();
        var queueGate = new object();
        var lastSeen = new int[Producers];
        var inFlight = 0;
        var overlaps = 0;
        var outOfOrder = 0;
        var delivered = 0;
        for (var i = 0; i < Producers; i++)
        {
            lastSeen[i] = -1;
        }

        GateHost host = new(() =>
        {
            if (Interlocked.Increment(ref inFlight) != 1)
            {
                overlaps++;
            }

            while (TryDequeue(queue, queueGate, out var item))
            {
                if (item.Sequence != lastSeen[item.Producer] + 1)
                {
                    outOfOrder++;
                }

                lastSeen[item.Producer] = item.Sequence;
                delivered++;
            }

            _ = Interlocked.Decrement(ref inFlight);
        });

        var producers = new Task<int>[Producers];
        for (var p = 0; p < Producers; p++)
        {
            var producer = p;
            producers[p] = BackgroundThread.Start(() =>
            {
                for (var sequence = 0; sequence < ItemsPerProducer; sequence++)
                {
                    lock (queueGate)
                    {
                        queue.Enqueue((producer, sequence));
                    }

                    host.Signal(TimeSpan.Zero);
                }
            });
        }

        await Task.WhenAll(producers);

        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(outOfOrder).IsEqualTo(0);
        await Assert.That(delivered).IsEqualTo(Producers * ItemsPerProducer);
    }

    /// <summary>A second hand-off onto a delivery already handed work returns at once, and the work is drained once more.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task HandOffOntoAnAlreadyDirtyDeliveryDrainsOnce()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        ConcurrentQueue<int> threads = new();
        GateHost host = new(() => RecordAndBlockFirst(threads, inside, release));

        var owner = BackgroundThread.Start(host.Signal);
        inside.Wait();
        await BackgroundThread.Start(() => host.Signal(TimeSpan.Zero));
        await BackgroundThread.Start(() => host.Signal(TimeSpan.Zero));
        release.Set();
        var ownerThread = await owner;

        await Assert.That(Describe(threads)).IsEqualTo($"{ownerThread},{ownerThread}");
    }

    /// <summary>A delivery handed work yields it to a producer already waiting, which drains it on its own thread.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DeliveryHandedWorkYieldsItToAWaitingProducer()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        ConcurrentQueue<int> threads = new();
        GateHost host = new(() => RecordAndBlockFirst(threads, inside, release));

        var owner = BackgroundThread.Start(host.Signal);
        inside.Wait();
        await BackgroundThread.Start(() => host.Signal(TimeSpan.Zero));
        var waiter = BackgroundThread.Start(() => host.Signal(TimeSpan.FromMinutes(1)));
        SpinWait.SpinUntil(() => host.Waiters != 0);
        release.Set();

        await Assert.That(await BackgroundThread.FinishesPromptly(waiter)).IsTrue();
        await Assert.That(Describe(threads)).IsEqualTo($"{await owner},{await waiter}");
    }

    /// <summary>An exit that finds work handed over yields it to a producer already waiting, which drains it on its own thread.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ExitYieldsHandedOverWorkToAWaitingProducer()
    {
        ConcurrentQueue<int> threads = new();
        GateHost host = new(() => threads.Enqueue(Environment.CurrentManagedThreadId));
        Task<int>? waiter = null;

        _ = await BackgroundThread.Start(() =>
        {
            _ = host.TryEnter();
            Thread producer = new(() => host.Signal(TimeSpan.Zero));
            producer.Start();
            producer.Join();
            waiter = BackgroundThread.Start(() => host.Signal(TimeSpan.FromMinutes(1)));
            SpinWait.SpinUntil(() => host.Waiters != 0);
            host.Exit();
        });

        await Assert.That(await BackgroundThread.FinishesPromptly(waiter!)).IsTrue();
        await Assert.That(Describe(threads)).IsEqualTo($"{await waiter!}");
    }

    /// <summary>A negative wait budget is rejected, and the default budget is twenty milliseconds.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SignalValidatesTheWaitBudget()
    {
        GateHost host = new(static () => { });

        await Assert.That(() => host.Signal(TimeSpan.FromTicks(-1))).Throws<ArgumentOutOfRangeException>();
        await Assert.That(DeliveryGate.DefaultWaitBudget).IsEqualTo(TimeSpan.FromMilliseconds(DefaultBudgetMilliseconds));
    }

    /// <summary>Records the draining thread, and blocks the first drain until released.</summary>
    /// <param name="threads">The draining threads, in order.</param>
    /// <param name="inside">Set once the first drain is running.</param>
    /// <param name="release">Releases the first drain.</param>
    private static void RecordAndBlockFirst(ConcurrentQueue<int> threads, ManualResetEventSlim inside, ManualResetEventSlim release)
    {
        threads.Enqueue(Environment.CurrentManagedThreadId);
        if (threads.Count != 1)
        {
            return;
        }

        inside.Set();
        release.Wait();
    }

    /// <summary>Takes the next queued item under the queue's lock.</summary>
    /// <param name="queue">The queue.</param>
    /// <param name="queueGate">The lock guarding the queue.</param>
    /// <param name="item">The dequeued item.</param>
    /// <returns><see langword="true"/> when an item was dequeued.</returns>
    private static bool TryDequeue(Queue<(int Producer, int Sequence)> queue, object queueGate, out (int Producer, int Sequence) item)
    {
        lock (queueGate)
        {
            return queue.TryDequeue(out item);
        }
    }

    /// <summary>Renders the recorded thread ids as a comma-separated list.</summary>
    /// <param name="threads">The recorded thread ids.</param>
    /// <returns>The ids in order.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string Describe(ConcurrentQueue<int> threads) => string.Join(",", threads);

    /// <summary>Holds a gate state field and the drain that serves it.</summary>
    /// <param name="drain">Delivers the recorded work.</param>
    [DebuggerDisplay("GateHost: {_state}")]
    private sealed class GateHost(Action drain) : IDrainTarget
    {
        /// <summary>The gate state under test.</summary>
        private DeliveryGateState _state;

        /// <summary>Gets the number of producers waiting to deliver on their own thread.</summary>
        internal int Waiters => _state.Waiters;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => drain();

        /// <summary>Takes the gate when it is free.</summary>
        /// <returns><see langword="true"/> when the gate was taken.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryEnter() => DeliveryGate.TryEnter(ref _state);

        /// <summary>Takes the gate when it is free or already held by this thread.</summary>
        /// <returns><see langword="true"/> when the gate was taken.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryEnterReentrant() => DeliveryGate.TryEnterReentrant(ref _state);

        /// <summary>Resets the gate after a failed delivery.</summary>
        /// <returns><see langword="true"/> when handed-over work is still pending.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Reset() => DeliveryGate.Reset(ref _state);

        /// <summary>Signals with the default budget.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Signal() => DeliveryGate.Signal(ref _state, this);

        /// <summary>Signals with an explicit budget.</summary>
        /// <param name="waitBudget">The wait budget.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Signal(TimeSpan waitBudget) => DeliveryGate.Signal(ref _state, this, waitBudget);

        /// <summary>Exits the gate.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Exit() => DeliveryGate.Exit(ref _state, this);
    }
}
