// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting;

/// <summary>Tracks the latest synchronization state of a context so health checks can read it without waiting.</summary>
[DebuggerDisplay("Latest = {_latest}")]
public sealed class OccasionallyConnectedHealthMonitor : IDisposable
{
    /// <summary>The state reported before the context publishes its first state.</summary>
    private readonly SyncState _initialState;

    /// <summary>The clock used to compute state ages.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>The subscription to the context states.</summary>
    private readonly IDisposable _subscription;

    /// <summary>Serializes state updates with terminal error handling.</summary>
    private readonly Lock _gate = new();

    /// <summary>The latest observed state.</summary>
    private SyncState? _latest;

    /// <summary>Indicates that the observable terminated with an error.</summary>
    private bool _faulted;

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
        _subscription = context.SyncStates.Subscribe(RecordState, RecordError);
    }

    /// <summary>Gets the health report for the latest observed state.</summary>
    public OccasionallyConnectedHealthReport Current =>
        OccasionallyConnectedHealth.Evaluate(Volatile.Read(ref _latest) ?? _initialState, _timeProvider.GetUtcNow());

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _subscription.Dispose();

    /// <summary>Records a state from the subscribed context until that sequence faults.</summary>
    /// <param name="value">The context state.</param>
    private void RecordState(SyncState value)
    {
        ArgumentExceptionHelper.ThrowIfNull(value);
        lock (_gate)
        {
            if (!_faulted)
            {
                Volatile.Write(ref _latest, value);
            }
        }
    }

    /// <summary>Latches terminal context failures without exposing exception details.</summary>
    /// <param name="error">The terminal failure.</param>
    private void RecordError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);
        lock (_gate)
        {
            if (_faulted)
            {
                return;
            }

            var latest = Volatile.Read(ref _latest) ?? _initialState;
            Volatile.Write(ref _latest, latest with
            {
                Status = SyncLifecycleStatus.Faulted,
                ChangedAtUtc = _timeProvider.GetUtcNow(),
                ReasonCode = OccasionallyConnectedHealth.FaultedReasonCode,
            });
            _faulted = true;
        }
    }
}
