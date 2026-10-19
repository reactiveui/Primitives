// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Threading;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>Creates bounded diagnostic activities without attaching operation payloads or tags.</summary>
internal sealed class OccasionallyConnectedActivities : IDisposable
{
    /// <summary>The shared activity source name defined by the diagnostics design.</summary>
    internal const string DiagnosticSourceName = "ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>Provides the diagnostic source owned by this recorder.</summary>
    private readonly ActivitySource _source;

    /// <summary>Indicates whether activity recording is enabled.</summary>
    private readonly bool _enabled;

    /// <summary>Tracks whether the recorder has disposed its source.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedActivities"/> class.</summary>
    /// <param name="enabled">Whether activity recording is enabled.</param>
    internal OccasionallyConnectedActivities(bool enabled = true)
    {
        _enabled = enabled;
        _source = new(DiagnosticSourceName);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _source.Dispose();
    }

    /// <summary>Starts the requested bounded activity when recording and sampling permit it.</summary>
    /// <param name="activityName">The predefined activity to start.</param>
    /// <returns>The caller-owned started activity, or <see langword="null"/> when no activity is recorded.</returns>
    internal Activity? Start(OccasionallyConnectedActivityName activityName)
    {
        var operationName = GetOperationName(activityName);
        if (!_enabled || Volatile.Read(ref _disposed) != 0)
        {
            return null;
        }

        return _source.HasListeners() ? _source.StartActivity(operationName, GetKind(activityName)) : null;
    }

    /// <summary>Gets the required operation name for an activity.</summary>
    /// <param name="activityName">The predefined activity identifier.</param>
    /// <returns>The required operation name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="activityName"/> is not defined.</exception>
    private static string GetOperationName(OccasionallyConnectedActivityName activityName) => activityName switch
    {
        OccasionallyConnectedActivityName.ContextStart => "oc.context.start",
        OccasionallyConnectedActivityName.TransportConnect => "oc.transport.connect",
        OccasionallyConnectedActivityName.SyncPush => "oc.sync.push",
        OccasionallyConnectedActivityName.SyncReceive => "oc.sync.receive",
        OccasionallyConnectedActivityName.StoreCommit => "oc.store.commit",
        OccasionallyConnectedActivityName.ConflictResolve => "oc.conflict.resolve",
        OccasionallyConnectedActivityName.StoreCompact => "oc.store.compact",
        _ => throw new ArgumentOutOfRangeException(nameof(activityName)),
    };

    /// <summary>Gets the activity kind appropriate to a predefined operation.</summary>
    /// <param name="activityName">The predefined activity identifier.</param>
    /// <returns>The activity kind.</returns>
    private static ActivityKind GetKind(OccasionallyConnectedActivityName activityName) => activityName is
        OccasionallyConnectedActivityName.TransportConnect or
        OccasionallyConnectedActivityName.SyncPush or
        OccasionallyConnectedActivityName.SyncReceive ? ActivityKind.Client : ActivityKind.Internal;
}
