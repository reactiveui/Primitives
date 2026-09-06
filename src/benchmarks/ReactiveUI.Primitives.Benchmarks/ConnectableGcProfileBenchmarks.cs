// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// GC-verbose allocation baselines for the connectable/multicast operators across Primitives,
/// System.Reactive, and R3. Delegates to <see cref="ConnectableShareBenchmarks"/>. Opt in with
/// <c>--filter "*GcProfile*"</c>.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
[System.Diagnostics.DebuggerDisplay("ConnectableGcProfileBenchmarks: Delegate = {_b}")]
public class ConnectableGcProfileBenchmarks
{
    /// <summary>The delegate benchmark instance that performs the measured work.</summary>
    private readonly ConnectableShareBenchmarks _b = new();

    /// <summary>Publish + Connect (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_Publish() => _b.PrimitivesPublishLiveConnect();

    /// <summary>Publish + Connect (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_Publish() => _b.SystemReactivePublishLiveConnect();

    /// <summary>Publish + Connect (R3).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_Publish() => _b.R3PublishLiveConnect();

    /// <summary>Share (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_Share() => _b.PrimitivesShareLiveSubscribe();

    /// <summary>Share (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_Share() => _b.SystemReactiveShareLiveSubscribe();

    /// <summary>Share (R3).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_Share() => _b.R3ShareLiveSubscribe();

    /// <summary>Replay + late subscribe (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_Replay() => _b.PrimitivesReplayLiveLateSubscribe();

    /// <summary>Replay + late subscribe (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_Replay() => _b.SystemReactiveReplayLiveLateSubscribe();

    /// <summary>Replay + late subscribe (R3).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_Replay() => _b.R3ReplayLiveLateSubscribe();

    /// <summary>RefCount (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_RefCount() => _b.PrimitivesRefCountSubscribe();

    /// <summary>RefCount (R3).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_RefCount() => _b.R3RefCountSubscribe();

    /// <summary>AutoConnect (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_AutoConnect() => _b.PrimitivesAutoConnectSubscribe();

    /// <summary>AutoConnect (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_AutoConnect() => _b.SystemReactiveAutoConnectSubscribe();
}
