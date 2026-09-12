// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using Android.OS;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Schedules immediate and delayed work on the Android handler thread.</summary>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class HandlerSequencer : CoalescingDispatchScheduler
{
    /// <summary>JNI drain callback reused across posted batches.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Maintainability",
        "SST1422:Move this field into the method that uses it",
        Justification = "The JNI runnable bridge is built once and reused across every posted batch.")]
    private Java.Lang.IRunnable? _drainRunnable;

    /// <summary>Initializes a new instance of the <see cref="HandlerSequencer"/> class.</summary>
    /// <param name="handler">The handler used to marshal work onto its looper thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> is <see langword="null"/>.</exception>
    public HandlerSequencer(Handler handler) =>
        Handler = handler ?? throw new ArgumentNullException(nameof(handler));

    /// <summary>Gets a sequencer that marshals work onto the application's main (UI) looper.</summary>
    public static HandlerSequencer Main { get; } = new(new(Looper.MainLooper!));

    /// <summary>Gets the handler used to marshal work onto its looper thread.</summary>
    public Handler Handler { get; }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public override string ToString() => $"HandlerSequencer({Handler})";

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        _drainRunnable ??= new Java.Lang.Runnable(drain);
        return PostToHandler(_drainRunnable);
    }

    /// <inheritdoc/>
    protected override IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime)
    {
        var runnable = new Java.Lang.Runnable(work);
        return ScheduleOnHandler(runnable, dueTime);
    }

    /// <summary>Posts the runnable through the native handler.</summary>
    /// <param name="runnable">The runnable to post.</param>
    /// <returns>Whether the handler accepted the runnable.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool PostToHandler(Java.Lang.IRunnable runnable) => Handler.Post(runnable);

    /// <summary>Schedules cancellable work through the native handler.</summary>
    /// <param name="runnable">The callback to run.</param>
    /// <param name="dueTime">The requested delay.</param>
    /// <returns>The callback cancellation handle.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private IDisposable ScheduleOnHandler(Java.Lang.IRunnable runnable, TimeSpan dueTime)
    {
        _ = Handler.PostDelayed(runnable, (long)dueTime.TotalMilliseconds);
        return Disposable.Create((Handler, runnable), static state => state.Handler.RemoveCallbacks(state.runnable));
    }
}
