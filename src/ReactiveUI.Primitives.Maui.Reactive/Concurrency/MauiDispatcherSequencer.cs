// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using Microsoft.Maui;
using Microsoft.Maui.Dispatching;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>MAUI dispatcher scheduler that coalesces scheduled work through an <see cref="IDispatcher"/>.</summary>
/// <remarks>Callbacks run on the dispatcher thread; cancellation suppresses delayed actions without cancelling the underlying delay.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("MauiDispatcherSequencer: Dispatcher = {Dispatcher}")]
public sealed class MauiDispatcherSequencer : LocalScheduler
{
    /// <summary>The shared main-thread scheduler, set once the application's dispatcher is available.</summary>
    private static MauiDispatcherSequencer? _main;

    /// <summary>The scheduler for the calling thread's dispatcher, cached per thread.</summary>
    [ThreadStatic]
    private static MauiDispatcherSequencer? _current;

    /// <summary>Queues work and coalesces dispatcher drains.</summary>
    private CoalescingDispatchState _dispatch;

    /// <summary>Initializes a new instance of the <see cref="MauiDispatcherSequencer"/> class.</summary>
    /// <param name="dispatcher">The dispatcher used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public MauiDispatcherSequencer(IDispatcher dispatcher)
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        _dispatch = new(RunDrain, DefaultScheduler.Instance);
    }

    /// <summary>Gets the shared scheduler for the MAUI main (UI) thread.</summary>
    /// <exception cref="InvalidOperationException">No application exists yet and the calling thread has no dispatcher.</exception>
    /// <remarks>
    /// Bound once to the running application's dispatcher, from any thread. Until an application exists nothing is
    /// cached, and the calling thread's existing dispatcher is used instead. Never falls back to the thread pool.
    /// </remarks>
    public static MauiDispatcherSequencer Main =>
        Volatile.Read(ref _main) ?? BindMain(ref _main, ResolveApplicationDispatcher(IPlatformApplication.Current)) ?? Current;

    /// <summary>Gets the scheduler for the calling thread's dispatcher.</summary>
    /// <exception cref="InvalidOperationException">The calling thread has no dispatcher.</exception>
    /// <remarks>
    /// Cached per thread, for applications that run UI on more than one thread. Never creates a dispatcher: a thread
    /// that has none cannot run the scheduled work.
    /// </remarks>
    public static MauiDispatcherSequencer Current => _current ??= new(ResolveCurrentDispatcher());

    /// <summary>Gets the dispatcher used to marshal work to the UI thread.</summary>
    public IDispatcher Dispatcher { get; }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, action);

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, dueTime, action);

    /// <summary>Caches the shared main-thread scheduler for the application's dispatcher, keeping the first one bound.</summary>
    /// <param name="slot">The field that holds the shared scheduler.</param>
    /// <param name="applicationDispatcher">The application's dispatcher, or <see langword="null"/> when no application exists.</param>
    /// <returns>The shared scheduler, or <see langword="null"/> when <paramref name="applicationDispatcher"/> is <see langword="null"/>.</returns>
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

    /// <summary>Schedules delayed work through the dispatcher.</summary>
    /// <param name="work">The callback to run.</param>
    /// <param name="dueTime">The requested delay.</param>
    /// <returns>An empty handle; cancelling suppresses delivery while the requested delay remains scheduled.</returns>
    private IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime)
    {
        _ = Dispatcher.DispatchDelayed(dueTime, work);
        return Disposable.Empty;
    }

    /// <summary>Runs one dispatcher batch.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _dispatch.RunDrain(new DispatchHost(this));

    /// <summary>Reaches this scheduler's dispatcher for its dispatch state.</summary>
    /// <param name="Owner">The scheduler.</param>
    private readonly record struct DispatchHost(MauiDispatcherSequencer Owner) : IDispatchHost
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Post(Action drain) => Owner.Dispatcher.Dispatch(drain);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) => Owner.ScheduleOnDispatcher(work, dueTime);
    }
}
