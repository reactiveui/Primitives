// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Cold task-backed signal that forwards an external cancellation token as an observer error.</summary>
/// <typeparam name="T">The task result type.</typeparam>
public sealed class FromAsyncExternalCancellationSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="FromAsyncExternalCancellationSignal{T}"/> class.</summary>
    /// <param name="taskFactory">The factory invoked once for each subscription.</param>
    /// <param name="cancellationToken">The external cancellation token linked into each subscription.</param>
    public FromAsyncExternalCancellationSignal(Func<CancellationToken, Task<T>> taskFactory, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(taskFactory);

        TaskFactory = taskFactory;
        CancellationToken = cancellationToken;
    }

    /// <summary>Gets the per-subscription task factory.</summary>
    private Func<CancellationToken, Task<T>> TaskFactory { get; }

    /// <summary>Gets the external cancellation token linked into each subscription.</summary>
    private CancellationToken CancellationToken { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        FromAsyncSubscription<T> subscription = new(observer, TaskFactory, CancellationToken);
        return subscription.Start();
    }
}
