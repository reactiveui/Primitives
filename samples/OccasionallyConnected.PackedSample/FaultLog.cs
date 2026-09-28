// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.PackedSample;

/// <summary>Counts the faults a client reports, by code, so the sample can print them.</summary>
internal sealed class FaultLog : IObserver<OccasionallyConnectedFault>
{
    private readonly ConcurrentDictionary<string, int> _counts = new(StringComparer.Ordinal);

    /// <summary>Describes the recorded faults.</summary>
    /// <returns>"none", or each fault code with its count.</returns>
    internal string Describe() =>
        _counts.IsEmpty ? "none" : string.Join(", ", _counts.OrderBy(pair => pair.Key, StringComparer.Ordinal).Select(pair => $"{pair.Key} x{pair.Value}"));

    /// <inheritdoc/>
    public void OnNext(OccasionallyConnectedFault value) =>
        _counts.AddOrUpdate($"{value.Code} ({value.Message} {value.Exception?.GetType().Name}: {value.Exception?.Message})", 1, static (_, count) => count + 1);

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
    }
}
