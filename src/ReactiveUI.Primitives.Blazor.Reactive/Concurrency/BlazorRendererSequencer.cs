// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Blazor.Reactive.Concurrency;

/// <summary>Scheduler that coalesces scheduled work through a Blazor renderer dispatcher delegate.</summary>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
public sealed class BlazorRendererSequencer : CoalescingDispatchScheduler
{
    /// <summary>Delegate used to marshal work through Blazor's renderer.</summary>
    private readonly Func<Action, Task> _invokeAsync;

    /// <summary>Initializes a new instance of the <see cref="BlazorRendererSequencer"/> class.</summary>
    /// <param name="invokeAsync">A delegate such as <c>ComponentBase.InvokeAsync</c> that runs work through the renderer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="invokeAsync"/> is <see langword="null"/>.</exception>
    public BlazorRendererSequencer(Func<Action, Task> invokeAsync) =>
        _invokeAsync = invokeAsync ?? throw new ArgumentNullException(nameof(invokeAsync));

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        _ = _invokeAsync(drain);
        return true;
    }
}
