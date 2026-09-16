// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Serializes deliveries to a downstream observer without holding a lock while the observer runs, so an observer that
/// blocks on another producer's thread cannot deadlock it.
/// </summary>
/// <remarks>
/// The owner holds a <see cref="DeliveryGateState"/> field and passes it by reference. A producer delivers directly
/// when <see cref="TryEnter"/> succeeds; otherwise it records its work where its drain can reach it and signals. A
/// producer that finds another thread delivering waits up to the wait budget to drain on its own thread; past the
/// budget it hands its work to the running delivery and returns. A signal raised on the delivering thread is drained
/// after the observer returns. The gate identifies its owner by thread, so enter, deliver and exit on one thread with
/// no await in between.
/// </remarks>
public static class DeliveryGate
{
    /// <summary>The wait budget used when none is supplied, in milliseconds.</summary>
    private const int DefaultWaitBudgetMilliseconds = 20;

    /// <summary>The number of milliseconds in a second.</summary>
    private const int MillisecondsPerSecond = 1000;

    /// <summary>The default wait budget, in <see cref="Stopwatch"/> ticks.</summary>
    private static readonly long DefaultWaitBudgetTimestamp = Stopwatch.Frequency * DefaultWaitBudgetMilliseconds / MillisecondsPerSecond;

    /// <summary>Gets the wait budget used when none is supplied.</summary>
    public static TimeSpan DefaultWaitBudget { get; } = TimeSpan.FromMilliseconds(DefaultWaitBudgetMilliseconds);

    /// <summary>Takes the gate on the calling thread when nothing is delivering and no producer is waiting.</summary>
    /// <param name="gate">The gate state.</param>
    /// <returns>
    /// <see langword="true"/> when the caller may deliver directly and must then call
    /// <see cref="Exit{TDrain}(ref DeliveryGateState, TDrain)"/>, or <see cref="Reset"/> if the delivery throws;
    /// <see langword="false"/> when the caller must record its work and signal.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryEnter(ref DeliveryGateState gate) => gate.TryEnter();

    /// <summary>
    /// Takes the gate like <see cref="TryEnter"/>, and also succeeds on the thread that is already delivering, so a
    /// nested delivery runs inside the outer one instead of after it.
    /// </summary>
    /// <param name="gate">The gate state.</param>
    /// <returns><see langword="true"/> when the caller may deliver directly and must then exit or reset once.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryEnterReentrant(ref DeliveryGateState gate) => gate.TryEnter() || gate.TryEnterNested();

    /// <summary>Releases a gate taken by an enter, first draining any work handed over meanwhile.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="gate">The gate state.</param>
    /// <param name="drain">Delivers every piece of recorded work that has not been delivered yet.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Exit<TDrain>(ref DeliveryGateState gate, TDrain drain)
        where TDrain : IDrainTarget =>
        gate.Exit(drain);

    /// <summary>Drains recorded work with the default wait budget.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="gate">The gate state.</param>
    /// <param name="drain">Delivers every piece of recorded work that has not been delivered yet.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Signal<TDrain>(ref DeliveryGateState gate, TDrain drain)
        where TDrain : IDrainTarget =>
        gate.Signal(drain, DefaultWaitBudgetTimestamp);

    /// <summary>Drains recorded work on the calling thread, or hands it to the thread already delivering.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="gate">The gate state.</param>
    /// <param name="drain">Delivers every piece of recorded work that has not been delivered yet.</param>
    /// <param name="waitBudget">How long to wait for another thread's delivery before handing the work over.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="waitBudget"/> is negative.</exception>
    public static void Signal<TDrain>(ref DeliveryGateState gate, TDrain drain, TimeSpan waitBudget)
        where TDrain : IDrainTarget
    {
        if (waitBudget < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(waitBudget), waitBudget, "The wait budget cannot be negative.");
        }

        gate.Signal(drain, (long)(waitBudget.TotalSeconds * Stopwatch.Frequency));
    }

    /// <summary>Releases a gate whose direct delivery threw.</summary>
    /// <param name="gate">The gate state.</param>
    /// <returns>
    /// <see langword="true"/> when work handed over during the failed delivery is still waiting; the gate then refuses
    /// direct delivery until a signal drains it.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool Reset(ref DeliveryGateState gate) => gate.Reset();
}
