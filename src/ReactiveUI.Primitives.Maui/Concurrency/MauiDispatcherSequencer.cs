// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Maui;
using Microsoft.Maui.Dispatching;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>MAUI dispatcher sequencer that coalesces scheduled work through an <see cref="IDispatcher"/>.</summary>
/// <remarks>
/// Callbacks run in posted dispatcher batches without inline reentrancy; cancellation suppresses delayed actions without cancelling the
/// underlying delay.
/// </remarks>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class MauiDispatcherSequencer : ISequencer
{
    /// <summary>The shared main-thread sequencer, set once the application's dispatcher is available.</summary>
    private static MauiDispatcherSequencer? _main;

    /// <summary>The sequencer for the calling thread's dispatcher, cached per thread.</summary>
    [ThreadStatic]
    private static MauiDispatcherSequencer? _current;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Initializes a new instance of the <see cref="MauiDispatcherSequencer"/> class.</summary>
    /// <param name="dispatcher">The dispatcher used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public MauiDispatcherSequencer(IDispatcher dispatcher)
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        _state = new(this, Post, RunDrain, ScheduleDelayed);
    }

    /// <summary>Gets the shared sequencer for the MAUI main (UI) thread.</summary>
    /// <exception cref="InvalidOperationException">No application exists yet and the calling thread has no dispatcher.</exception>
    /// <remarks>
    /// Bound once to the running application's dispatcher, from any thread. Until an application exists nothing is
    /// cached, and the calling thread's existing dispatcher is used instead. Never falls back to the thread pool.
    /// </remarks>
    public static MauiDispatcherSequencer Main =>
        Volatile.Read(ref _main) ?? BindMain(ref _main, ResolveApplicationDispatcher(IPlatformApplication.Current)) ?? Current;

    /// <summary>Gets the sequencer for the calling thread's dispatcher.</summary>
    /// <exception cref="InvalidOperationException">The calling thread has no dispatcher.</exception>
    /// <remarks>
    /// Cached per thread, for applications that run UI on more than one thread. Never creates a dispatcher: a thread
    /// that has none cannot run the scheduled work.
    /// </remarks>
    public static MauiDispatcherSequencer Current => _current ??= new(ResolveCurrentDispatcher());

    /// <summary>Gets the dispatcher used to marshal work to the UI thread.</summary>
    public IDispatcher Dispatcher { get; }

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Caches the shared main-thread sequencer for the application's dispatcher, keeping the first one bound.</summary>
    /// <param name="slot">The field that holds the shared sequencer.</param>
    /// <param name="applicationDispatcher">The application's dispatcher, or <see langword="null"/> when no application exists.</param>
    /// <returns>The shared sequencer, or <see langword="null"/> when <paramref name="applicationDispatcher"/> is <see langword="null"/>.</returns>
    internal static MauiDispatcherSequencer? BindMain(ref MauiDispatcherSequencer? slot, IDispatcher? applicationDispatcher)
    {
        if (applicationDispatcher is null)
        {
            return null;
        }

        MauiDispatcherSequencer created = new(applicationDispatcher);
        return Interlocked.CompareExchange(ref slot, created, null) ?? created;
    }

    /// <summary>Returns the application's dispatcher, which MAUI resolves on the UI thread while it starts the application.</summary>
    /// <param name="application">The running platform application, or <see langword="null"/> before it exists.</param>
    /// <returns>The application's dispatcher, or <see langword="null"/> when no application or dispatcher exists yet.</returns>
    internal static IDispatcher? ResolveApplicationDispatcher(IPlatformApplication? application) =>
        application?.Services?.GetService(typeof(IDispatcher)) as IDispatcher;

    /// <summary>Returns the calling thread's existing dispatcher without creating one.</summary>
    /// <returns>The calling thread's dispatcher.</returns>
    /// <exception cref="InvalidOperationException">The calling thread has no dispatcher.</exception>
    internal static IDispatcher ResolveCurrentDispatcher() =>
        Microsoft.Maui.Dispatching.Dispatcher.GetForCurrentThread()
        ?? throw new InvalidOperationException(
            "The calling thread has no MAUI dispatcher. Use MauiDispatcherSequencer.Main or Current from a UI thread, or after the application is created.");

    /// <summary>Marshals the cached drain callback through the dispatcher.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/> when the dispatcher accepted the work.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Post(Action drain) => Dispatcher.Dispatch(drain);

    /// <summary>Runs delayed work through the dispatcher's native delayed dispatch.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    private void ScheduleDelayed(IWorkItem item, long dueTimestamp) =>
        _ = Dispatcher.DispatchDelayed(
            DispatchSequencerState.DelayUntil(dueTimestamp),
            () => DispatchSequencerState.RunIfActive(item));

    /// <summary>Runs one queued batch on the coalescing engine.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _state.RunDrain();
}
