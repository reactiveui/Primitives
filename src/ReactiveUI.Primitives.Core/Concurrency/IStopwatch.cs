// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Abstraction for a stopwatch to compute time relative to a starting point.</summary>
public interface IStopwatch
{
    /// <summary>Gets the time elapsed since the stopwatch object was obtained.</summary>
    TimeSpan Elapsed { get; }
}
