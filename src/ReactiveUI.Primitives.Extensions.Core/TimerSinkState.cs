// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Stores timer ownership, disposal state, and terminal notification state.</summary>
/// <typeparam name="T">The element type the downstream observer receives.</typeparam>
/// <param name="downstream">The downstream observer terminal callbacks fan out to.</param>
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Design",
    "SST2315:A type that owns a disposable should be disposable",
    Justification =
        "The parent operator sink owns the timer's lifetime and releases it under its own gate, so an independent "
        + "disposal path on this state object would race that gate.")]
[System.Diagnostics.DebuggerDisplay("TimerSinkState: Done = {Done}, Timer = {Timer}")]
public sealed class TimerSinkState<T>(IObserver<T> downstream)
{
    /// <summary>Gets the timer slot used by the operator's OnNext logic to schedule deferred emissions.</summary>
    public SwapDisposable Timer { get; } = new();

    /// <summary>Gets a value indicating whether the sink has terminated through error, completion or disposal; read it under the owning sink's gate.</summary>
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
