// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Threading;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Creates bounded diagnostic activities without attaching operation payloads or tags.</summary>
internal sealed class OccasionallyConnectedActivities : IDisposable
{
    /// <summary>The shared activity source name defined by the diagnostics design.</summary>
    internal const string DiagnosticSourceName = "ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>The FNV-1a offset basis used for deterministic trace selection.</summary>
    private const ulong FnvOffsetBasis = 14_695_981_039_346_656_037UL;

    /// <summary>The FNV-1a multiplication prime used for deterministic trace selection.</summary>
    private const ulong FnvPrime = 1_099_511_628_211UL;

    /// <summary>Provides the diagnostic source owned by this recorder.</summary>
    private readonly ActivitySource _source;

    /// <summary>Indicates whether activity recording is enabled.</summary>
    private readonly bool _enabled;

    /// <summary>Determines whether an activity invocation is selected for sampling.</summary>
    private readonly Func<ActivityTraceId, double, bool> _sampler;

    /// <summary>Defines the fraction of activity starts selected by the sampler.</summary>
    private readonly double _samplingRatio;

    /// <summary>Tracks whether the recorder has disposed its source.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedActivities"/> class.</summary>
    /// <param name="enabled">Whether activity recording is enabled.</param>
    /// <param name="samplingRatio">The fraction of activity starts to sample.</param>
    /// <param name="sampler">An optional deterministic sampling function.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="samplingRatio"/> is not finite and within zero through one.</exception>
    internal OccasionallyConnectedActivities(
        bool enabled = true,
        double samplingRatio = 1D,
        Func<ActivityTraceId, double, bool>? sampler = null)
    {
        if (double.IsNaN(samplingRatio) || double.IsInfinity(samplingRatio) || samplingRatio < 0D || samplingRatio > 1D)
        {
            throw new ArgumentOutOfRangeException(nameof(samplingRatio));
        }

        _enabled = enabled;
        _samplingRatio = samplingRatio;
        _sampler = sampler ?? ShouldSample;
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
        if (!_enabled || Volatile.Read(ref _disposed) != 0)
        {
            return null;
        }

        var operationName = GetOperationName(activityName);
        var kind = GetKind(activityName);
        var traceId = Activity.Current?.TraceId ?? default;
        if (traceId == default)
        {
            traceId = ActivityTraceId.CreateRandom();
        }

        return _sampler(traceId, _samplingRatio) ? _source.StartActivity(operationName, kind) : null;
    }

    /// <summary>Determines whether the supplied trace is included by the deterministic ratio.</summary>
    /// <param name="traceId">The parent trace identifier or a fresh random sampling seed when no W3C parent exists.</param>
    /// <param name="samplingRatio">The fraction of traces to select.</param>
    /// <returns><see langword="true"/> when the trace is selected; otherwise, <see langword="false"/>.</returns>
    private static bool ShouldSample(ActivityTraceId traceId, double samplingRatio)
    {
        if (samplingRatio <= 0D)
        {
            return false;
        }

        if (samplingRatio >= 1D)
        {
            return true;
        }

        var hash = FnvOffsetBasis;
        foreach (var character in traceId.ToHexString())
        {
            hash ^= character;
            hash *= FnvPrime;
        }

        return hash / (double)ulong.MaxValue < samplingRatio;
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
