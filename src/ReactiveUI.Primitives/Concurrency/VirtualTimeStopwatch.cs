// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Stopwatch backed by a virtual-time clock reader.</summary>
internal sealed class VirtualTimeStopwatch : IStopwatch
{
    /// <summary>Reads the owner's current virtual time.</summary>
    private readonly Func<DateTimeOffset> _readNow;

    /// <summary>Start time captured when the stopwatch was created.</summary>
    private readonly DateTimeOffset _start;

    /// <summary>Initializes a new instance of the <see cref="VirtualTimeStopwatch"/> class.</summary>
    /// <param name="readNow">Reads the owner's current virtual time.</param>
    /// <param name="start">Start time for elapsed calculations.</param>
    public VirtualTimeStopwatch(Func<DateTimeOffset> readNow, DateTimeOffset start)
    {
        _readNow = readNow;
        _start = start;
    }

    /// <summary>Gets the elapsed virtual time.</summary>
    public TimeSpan Elapsed => _readNow() - _start;
}
