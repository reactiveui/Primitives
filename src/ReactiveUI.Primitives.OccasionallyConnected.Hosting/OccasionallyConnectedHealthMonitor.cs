// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting;

/// <summary>Tracks the latest synchronization state of a context so health checks can read it without waiting.</summary>
[DebuggerDisplay("Latest = {_latest}")]
public sealed class OccasionallyConnectedHealthMonitor : IObserver<SyncState>, IDisposable
{
    /// <summary>The state reported before the context publishes its first state.</summary>
    private readonly SyncState _initialState;

    /// <summary>The clock used to compute state ages.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>The subscription to the context states.</summary>
    private readonly IDisposable _subscription;

    /// <summary>The latest observed state.</summary>
    private SyncState? _latest;

    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedHealthMonitor"/> class.</summary>
    /// <param name="context">The monitored context.</param>
    /// <param name="timeProvider">The clock used to compute state ages.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    public OccasionallyConnectedHealthMonitor(IOccasionallyConnectedContext context, TimeProvider timeProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(context);
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
        _initialState = new(SyncLifecycleStatus.Created, NetworkAvailable: false, 0, 0, timeProvider.GetUtcNow(), null, null, null);
        _subscription = context.SyncStates.Subscribe(this);
    }

    /// <summary>Gets the health report for the latest observed state.</summary>
    public OccasionallyConnectedHealthReport Current =>
        OccasionallyConnectedHealth.Evaluate(Volatile.Read(ref _latest) ?? _initialState, _timeProvider.GetUtcNow());

    /// <inheritdoc/>
    public void OnNext(SyncState value)
    {
        ArgumentExceptionHelper.ThrowIfNull(value);
        Volatile.Write(ref _latest, value);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => ArgumentExceptionHelper.ThrowIfNull(error);

    /// <inheritdoc/>
    public void OnCompleted()
    {
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _subscription.Dispose();
}
