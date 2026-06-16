// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>A scheduled, cancellable unit of work drained by a <see cref="CoalescingDispatchScheduler"/>.</summary>
internal interface IDispatchWorkItem : IDisposable
{
    /// <summary>Executes the work unless it has already been cancelled.</summary>
    void Run();
}
