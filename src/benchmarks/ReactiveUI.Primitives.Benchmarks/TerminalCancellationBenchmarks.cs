// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxTask = System.Reactive.Threading.Tasks.TaskObservableExtensions;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the CancellationToken-accepting terminal and operator overloads (FirstAsync,
/// LastOrDefaultAsync, ToTask, TakeUntil) against their System.Reactive and R3 equivalents,
/// covering the live-token pass-through cost, the task-shim wrapper, and the cancel path.
/// </summary>
[MemoryDiagnoser]
[System.Diagnostics.DebuggerDisplay("TerminalCancellationBenchmarks: Canceled = {_liveSource.IsCancellationRequested}")]
public class TerminalCancellationBenchmarks : IDisposable
{
    /// <summary>The inclusive start value of the range used by each benchmark.</summary>
    private const int Start = 1;

    /// <summary>The number of elements produced by the range used by each benchmark.</summary>
    private const int Count = 16;

    /// <summary>Source of the live token that is never canceled during the run.</summary>
    private CancellationTokenSource _liveSource = new();

    /// <summary>Creates the live cancellation source.</summary>
    [GlobalSetup]
    public void Setup() => _liveSource = new();

    /// <summary>Disposes the live cancellation source.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [GlobalCleanup]
    public void Cleanup() => Dispose();

    /// <summary>Disposes the live cancellation source.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Benchmarks awaiting the first value of a synchronous pipeline with a live token.</summary>
    /// <returns>The first value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark(Baseline = true)]
    public Task<int> PrimitivesFirstAsyncWithToken() =>
        Signal.Sequence(Start, Count).Map(static x => x).FirstAsync(_liveSource.Token);

    /// <summary>Benchmarks awaiting the first value with a live token using System.Reactive.</summary>
    /// <returns>The first value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public Task<int> SystemReactiveFirstAsyncWithToken() =>
        RxTask.ToTask(
            RxObservable.FirstAsync(RxObservable.Range(Start, Count).Select(static x => x)),
            _liveSource.Token);

    /// <summary>Benchmarks awaiting the first value with a live token using R3.</summary>
    /// <returns>The first value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public Task<int> R3FirstAsyncWithToken() =>
        R3.ObservableExtensions.FirstAsync(
            R3.ObservableExtensions.Select(
                R3.Observable.Range(Start, Count, _liveSource.Token),
                static x => x),
            _liveSource.Token);

    /// <summary>Benchmarks the task-shim ToTask overload that wraps a token around an existing task.</summary>
    /// <returns>The first value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public Task<int> PrimitivesTaskToTaskWithToken() =>
        Signal.Sequence(Start, Count).Map(static x => x).FirstAsync(CancellationToken.None).ToTask(_liveSource.Token);

    /// <summary>Benchmarks awaiting the last-or-default value with a live token.</summary>
    /// <returns>The last value or default.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public Task<int> PrimitivesLastOrDefaultAsyncWithToken() =>
        Signal.Sequence(Start, Count).LastOrDefaultAsync(_liveSource.Token);

    /// <summary>Benchmarks awaiting the last-or-default value with a live token using System.Reactive.</summary>
    /// <returns>The last value or default.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public Task<int> SystemReactiveLastOrDefaultAsyncWithToken() =>
        RxTask.ToTask(RxObservable.LastOrDefaultAsync(RxObservable.Range(Start, Count)), _liveSource.Token);

    /// <summary>Benchmarks awaiting the last-or-default value with a live token using R3.</summary>
    /// <returns>The last value or default.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public Task<int> R3LastOrDefaultAsyncWithToken() =>
        R3.ObservableExtensions.LastOrDefaultAsync(
            R3.Observable.Range(Start, Count, _liveSource.Token),
            cancellationToken: _liveSource.Token);

    /// <summary>Benchmarks forwarding a completing range through TakeUntil with a live token.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesTakeUntilToken()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(Start, Count).TakeUntil(_liveSource.Token).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks forwarding a completing range through TakeUntil with a live token using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveTakeUntilToken()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(Start, Count).TakeUntil(_liveSource.Token).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks forwarding a completing range through TakeUntil with a live token using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3TakeUntilToken()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.TakeUntil(
                R3.Observable.Range(Start, Count, _liveSource.Token),
                _liveSource.Token)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks the cancel path: canceling a pending FirstAsync on a silent source.</summary>
    /// <returns>One when the await was canceled.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCancelPendingFirstAsync()
    {
        using CancellationTokenSource cancellation = new();
        var pending = Signal.Silent<int>().FirstAsync(cancellation.Token);
        await cancellation.CancelAsync().ConfigureAwait(false);
        try
        {
            _ = await pending.ConfigureAwait(false);
            return 0;
        }
        catch (OperationCanceledException)
        {
            return 1;
        }
    }

    /// <summary>Benchmarks the cancel path using System.Reactive.</summary>
    /// <returns>One when the await was canceled.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveCancelPendingFirstAsync()
    {
        using CancellationTokenSource cancellation = new();
        var pending = RxTask.ToTask(RxObservable.FirstAsync(RxObservable.Never<int>()), cancellation.Token);
        await cancellation.CancelAsync().ConfigureAwait(false);
        try
        {
            _ = await pending.ConfigureAwait(false);
            return 0;
        }
        catch (OperationCanceledException)
        {
            return 1;
        }
    }

    /// <summary>Benchmarks the cancel path using R3.</summary>
    /// <returns>One when the await was canceled.</returns>
    [Benchmark]
    public async Task<int> R3CancelPendingFirstAsync()
    {
        using CancellationTokenSource cancellation = new();
        var pending = R3.ObservableExtensions.FirstAsync(R3.Observable.Never<int>(), cancellation.Token);
        await cancellation.CancelAsync().ConfigureAwait(false);
        try
        {
            _ = await pending.ConfigureAwait(false);
            return 0;
        }
        catch (OperationCanceledException)
        {
            return 1;
        }
    }

    /// <summary>Releases resources owned by the benchmark instance.</summary>
    /// <param name="disposing"><see langword="true"/> when managed resources should be released.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        _liveSource.Dispose();
    }
}
