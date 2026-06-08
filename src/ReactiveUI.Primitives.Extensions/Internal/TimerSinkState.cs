// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Shared lock + timer + done-flag triple used by the synchronous timer-driven operator sinks
/// (Debounce-Until, Detect-Stale, Throttle-Distinct, Buffer-Until-Idle, etc.). Each of those sinks
/// previously hand-rolled three identical OnError / OnCompleted / Dispose method bodies on top of
/// the same fields; this helper centralises the bodies so the per-sink class only carries the
/// operator-specific OnNext logic. Sinks compose one instance and forward to it — no base class.
/// </summary>
/// <typeparam name="T">The element type the downstream observer receives.</typeparam>
/// <param name="downstream">The downstream observer terminal callbacks fan out to.</param>
internal sealed class TimerSinkState<T>(IObserver<T> downstream)
{
    /// <summary>Gets the timer slot used by the operator's OnNext logic to schedule deferred emissions.</summary>
    public SwapDisposable Timer { get; } = new();

    /// <summary>
    /// Gets a value indicating whether the sink has reached a terminal state (OnError, OnCompleted,
    /// or Dispose). Read inside the owning sink's gate by callers that need to short-circuit a deferred operation.
    /// </summary>
    public bool Done { get; private set; }

    /// <summary>Forwards a terminal error to the downstream observer and tears the sink down. The caller must hold the sink's gate.</summary>
    /// <param name="error">The error to forward.</param>
    public void HandleErrorLocked(Exception error)
    {
        if (Done)
        {
            return;
        }

        Done = true;
        Timer.Dispose();
        downstream.OnError(error);
    }

    /// <summary>Forwards completion to the downstream observer and tears the sink down. The caller must hold the sink's gate.</summary>
    public void HandleCompletedLocked()
    {
        if (Done)
        {
            return;
        }

        Done = true;
        Timer.Dispose();
        downstream.OnCompleted();
    }

    /// <summary>Marks the sink terminal and disposes the timer without forwarding a notification. The caller must hold the sink's gate.</summary>
    public void HandleDisposeLocked()
    {
        Done = true;
        Timer.Dispose();
    }
}
