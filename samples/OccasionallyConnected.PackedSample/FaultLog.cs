// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.PackedSample;

/// <summary>Counts the faults a client reports, by code, so the sample can print them.</summary>
internal sealed class FaultLog : IObserver<OccasionallyConnectedFault>
{
    private readonly SampleCounterLog _counts = new();

    /// <summary>Describes the recorded faults.</summary>
    /// <returns>"none", or each fault code with its count.</returns>
    internal string Describe() =>
        _counts.Describe("none");

    /// <inheritdoc/>
    public void OnNext(OccasionallyConnectedFault value) =>
        _counts.Record($"{value.Code} ({value.Message} {value.Exception?.GetType().Name}: {value.Exception?.Message})");

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
    }
}
