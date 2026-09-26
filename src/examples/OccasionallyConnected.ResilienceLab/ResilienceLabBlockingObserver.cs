// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>An observer that blocks its callback until a gate opens, then records the values it receives.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <param name="gate">The gate that releases blocked callbacks.</param>
/// <param name="gateTimeout">The bound on each blocked wait.</param>
/// <param name="isLatest">Recognizes the value that proves the observer caught up.</param>
[DebuggerDisplay("Entered={Entered.IsCompleted,nq}; Latest={Latest.IsCompleted,nq}")]
internal sealed class ResilienceLabBlockingObserver<T>(ManualResetEventSlim gate, TimeSpan gateTimeout, Func<T, bool> isLatest) : IObserver<T>
{
    /// <summary>Completes when the first callback starts waiting on the gate.</summary>
    private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes with the first value that satisfies the latest-value check.</summary>
    private readonly TaskCompletionSource<T> _latest = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Gets a task that completes when the first callback starts waiting on the gate.</summary>
    public Task Entered => _entered.Task;

    /// <summary>Gets a task that completes with the first value that satisfies the latest-value check.</summary>
    public Task<T> Latest => _latest.Task;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        _ = _entered.TrySetResult();
        _ = gate.Wait(gateTimeout);
        if (isLatest(value))
        {
            _ = _latest.TrySetResult(value);
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => _ = _latest.TrySetException(error);

    /// <inheritdoc/>
    public void OnCompleted()
    {
    }
}
