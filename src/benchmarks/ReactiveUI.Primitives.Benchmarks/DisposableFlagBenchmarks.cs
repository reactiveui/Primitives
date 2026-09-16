// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Disposables;
using RxBooleanDisposable = System.Reactive.Disposables.BooleanDisposable;
using RxCancellationDisposable = System.Reactive.Disposables.CancellationDisposable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures disposables that expose their disposed state as a flag or a cancellation token.</summary>
[MemoryDiagnoser]
public class DisposableFlagBenchmarks
{
    /// <summary>The number of disposables created and disposed per case.</summary>
    private const int Count = 256;

    /// <summary>Creates, disposes twice and checks a boolean disposable per iteration.</summary>
    /// <returns>The number of instances reporting disposed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesBooleanDisposableCycle()
    {
        var disposed = 0;
        for (var i = 0; i < Count; i++)
        {
            BooleanDisposable disposable = new();
            disposable.Dispose();
            disposable.Dispose();
            if (disposable.IsDisposed)
            {
                disposed++;
            }
        }

        return disposed;
    }

    /// <summary>Creates, disposes twice and checks a boolean disposable per iteration using System.Reactive.</summary>
    /// <returns>The number of instances reporting disposed.</returns>
    [Benchmark]
    public int SystemReactiveBooleanDisposableCycle()
    {
        var disposed = 0;
        for (var i = 0; i < Count; i++)
        {
            RxBooleanDisposable disposable = new();
            disposable.Dispose();
            disposable.Dispose();
            if (disposable.IsDisposed)
            {
                disposed++;
            }
        }

        return disposed;
    }

    /// <summary>Links a token to a cancellation disposable and cancels it by disposal.</summary>
    /// <returns>The number of tokens observed as cancelled.</returns>
    [Benchmark]
    public int PrimitivesCancellationDisposableCycle()
    {
        var cancelled = 0;
        for (var i = 0; i < Count; i++)
        {
            using CancellationTokenSource source = new();
            CancellationDisposable disposable = new(source);
            var token = disposable.Token;
            disposable.Dispose();
            disposable.Dispose();
            if (token.IsCancellationRequested && disposable.IsDisposed)
            {
                cancelled++;
            }
        }

        return cancelled;
    }

    /// <summary>Links a token to a cancellation disposable and cancels it by disposal using System.Reactive.</summary>
    /// <returns>The number of tokens observed as cancelled.</returns>
    [Benchmark]
    public int SystemReactiveCancellationDisposableCycle()
    {
        var cancelled = 0;
        for (var i = 0; i < Count; i++)
        {
            using CancellationTokenSource source = new();
            RxCancellationDisposable disposable = new(source);
            var token = disposable.Token;
            disposable.Dispose();
            disposable.Dispose();
            if (token.IsCancellationRequested && disposable.IsDisposed)
            {
                cancelled++;
            }
        }

        return cancelled;
    }
}
