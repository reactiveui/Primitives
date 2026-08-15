// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Task-source <c>Chain</c>/<c>Concat</c> signal.</summary>
/// <typeparam name="T">The task result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Sources = {_sources}")]
public sealed class TaskChainSignal<T> : IObservable<T>
{
    /// <summary>The outer task source.</summary>
    private readonly IObservable<Task<T>> _sources;

    /// <summary>Initializes a new instance of the <see cref="TaskChainSignal{T}"/> class.</summary>
    /// <param name="sources">The outer task source.</param>
    public TaskChainSignal(IObservable<Task<T>> sources) => _sources = sources;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new TaskChainCoordinator<T>(observer).Run(_sources);
    }
}
