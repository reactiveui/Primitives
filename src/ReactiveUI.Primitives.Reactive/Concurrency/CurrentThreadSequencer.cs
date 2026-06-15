// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Maps the current-thread sequencer onto System.Reactive's <see cref="CurrentThreadScheduler"/>.</summary>
internal static class CurrentThreadSequencer
{
    /// <summary>Gets the singleton current-thread scheduler.</summary>
    public static IScheduler Instance => CurrentThreadScheduler.Instance;

    /// <summary>Gets a value indicating whether the caller must call a Schedule method.</summary>
    public static bool IsScheduleRequired => CurrentThreadScheduler.IsScheduleRequired;
}
