// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using Microsoft.Maui.Dispatching;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>MAUI dispatcher scheduler that coalesces scheduled work through an <see cref="IDispatcher"/>.</summary>
/// <remarks>Callbacks run on the dispatcher thread; cancellation suppresses delayed actions without cancelling the underlying delay.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("MauiDispatcherSequencer: Dispatcher = {Dispatcher}")]
public sealed class MauiDispatcherSequencer : CoalescingDispatchScheduler
{
    /// <summary>Initializes a new instance of the <see cref="MauiDispatcherSequencer"/> class.</summary>
    /// <param name="dispatcher">The dispatcher used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public MauiDispatcherSequencer(IDispatcher dispatcher) =>
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));

    /// <summary>Gets the dispatcher used to marshal work to the UI thread.</summary>
    public IDispatcher Dispatcher { get; }

    /// <inheritdoc/>
    protected override bool Post(Action drain) => Dispatcher.Dispatch(drain);

    /// <inheritdoc/>
    protected override IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime)
    {
        _ = Dispatcher.DispatchDelayed(dueTime, work);

        // Cancelling suppresses delivery while the requested delay remains scheduled.
        return Disposable.Empty;
    }
}
