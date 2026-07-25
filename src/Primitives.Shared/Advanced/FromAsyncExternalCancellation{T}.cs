// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Forwards an external cancellation token as the terminal error for a task-backed signal subscription.</summary>
/// <typeparam name="T">The task result type.</typeparam>
internal sealed class FromAsyncExternalCancellation<T> : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="FromAsyncExternalCancellation{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="lifetime">The subscription lifetime.</param>
    /// <param name="cancellationToken">The external cancellation token.</param>
    public FromAsyncExternalCancellation(
        IObserver<T> observer,
        AsyncSubscriptionLifetime lifetime,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ArgumentExceptionHelper.ThrowIfNull(lifetime);

        Observer = observer;
        Lifetime = lifetime;
        CancellationToken = cancellationToken;
    }

    /// <summary>Gets a value indicating whether the external token can cancel.</summary>
    internal bool CanBeCanceled => CancellationToken.CanBeCanceled;

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets the subscription lifetime.</summary>
    private AsyncSubscriptionLifetime Lifetime { get; }

    /// <summary>Gets the external cancellation token.</summary>
    private CancellationToken CancellationToken { get; }

    /// <summary>Gets or sets the external cancellation registration.</summary>
    private CancellationTokenRegistration Registration { get; set; }

    /// <inheritdoc/>
    public void Dispose() => Registration.Dispose();

    /// <summary>Creates a linked source for subscription disposal and external cancellation.</summary>
    /// <param name="subscriptionToken">The subscription-owned cancellation token.</param>
    /// <returns>A linked cancellation token source.</returns>
    internal CancellationTokenSource CreateLinkedSource(CancellationToken subscriptionToken) =>
        CancellationTokenSource.CreateLinkedTokenSource(CancellationToken, subscriptionToken);

    /// <summary>Registers external cancellation.</summary>
    /// <returns><see langword="true"/> when the subscription should continue starting.</returns>
    internal bool Start()
    {
        if (!CancellationToken.CanBeCanceled)
        {
            return true;
        }

        Registration =
            CancellationToken.Register(
                static state => ((FromAsyncExternalCancellation<T>)(
                    state ?? throw new InvalidOperationException("The cancellation state is missing."))).Cancel(),
                this);
        return !Lifetime.IsCompleted;
    }

    /// <summary>Attempts to forward external cancellation as a terminal error.</summary>
    /// <returns><see langword="true"/> when external cancellation was forwarded.</returns>
    internal bool TryForwardCancellation()
    {
        if (!CancellationToken.IsCancellationRequested || Lifetime.IsCancellationRequested)
        {
            return false;
        }

        if (!Lifetime.TryComplete())
        {
            return false;
        }

        Observer.OnError(new TaskCanceledException());
        return true;
    }

    /// <summary>Forwards cancellation from the external token registration.</summary>
    private void Cancel() => _ = TryForwardCancellation();
}
