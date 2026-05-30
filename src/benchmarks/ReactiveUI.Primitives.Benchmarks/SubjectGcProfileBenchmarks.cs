// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// GC-verbose allocation baselines for the subject scenarios across Primitives, System.Reactive,
/// and R3 (throughput, subscribe/dispose churn, behavior/state, replay). Delegates to the
/// comparison benchmarks. Opt in with <c>--filter "*GcProfile*"</c>.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
public class SubjectGcProfileBenchmarks
{
    /// <summary>
    /// The delegate target for the subject throughput scenarios.
    /// </summary>
    private readonly SubjectThroughputBenchmarks _throughput = new();

    /// <summary>
    /// The delegate target for the subscribe/dispose churn scenarios.
    /// </summary>
    private readonly SubjectSubscriptionBenchmarks _subscription = new();

    /// <summary>
    /// The delegate target for the behavior/state scenarios.
    /// </summary>
    private readonly StatefulSignalBenchmarks _stateful = new();

    /// <summary>
    /// The delegate target for the bounded replay scenarios.
    /// </summary>
    private readonly ReplaySignalBenchmarks _replay = new();

    /// <summary>Subject emit 1024 (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_Emit1024() => _throughput.PrimitivesSubjectEmit1024();

    /// <summary>Subject emit 1024 (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_Emit1024() => _throughput.SystemReactiveSubjectEmit1024();

    /// <summary>Subject emit 1024 (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_Emit1024() => _throughput.R3SubjectEmit1024();

    /// <summary>Subscribe/dispose 64 (Primitives).</summary>
    /// <returns>The churn count.</returns>
    [Benchmark]
    public int Primitives_SubscribeDispose64() => _subscription.PrimitivesSubjectSubscribeDispose64();

    /// <summary>Subscribe/dispose 64 (System.Reactive).</summary>
    /// <returns>The churn count.</returns>
    [Benchmark]
    public int Rx_SubscribeDispose64() => _subscription.SystemReactiveSubjectSubscribeDispose64();

    /// <summary>Subscribe/dispose 64 (R3).</summary>
    /// <returns>The churn count.</returns>
    [Benchmark]
    public int R3_SubscribeDispose64() => _subscription.R3SubjectSubscribeDispose64();

    /// <summary>Behavior/state emit 1024 (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_State1024() => _stateful.PrimitivesStateSignal1024();

    /// <summary>BehaviorSubject emit 1024 (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_State1024() => _stateful.SystemReactiveBehaviorSubject1024();

    /// <summary>BehaviorSubject emit 1024 (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_State1024() => _stateful.R3BehaviorSubject1024();

    /// <summary>Bounded replay late-subscribe (Primitives).</summary>
    /// <returns>The replayed total.</returns>
    [Benchmark]
    public int Primitives_Replay() => _replay.PrimitivesHistorySubscribe();

    /// <summary>Bounded replay late-subscribe (System.Reactive).</summary>
    /// <returns>The replayed total.</returns>
    [Benchmark]
    public int Rx_Replay() => _replay.SystemReactiveReplaySubscribe();

    /// <summary>Bounded replay late-subscribe (R3).</summary>
    /// <returns>The replayed total.</returns>
    [Benchmark]
    public int R3_Replay() => _replay.R3ReplaySubscribe();
}
