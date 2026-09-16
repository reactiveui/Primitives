// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Subscribes an operator's per-subscription witness to its source and links the two teardowns together.</summary>
/// <remarks>
/// An operator that wraps one source in one witness subscribes the same way every time: link the downstream witness's
/// disposal to this one, subscribe the witness to the source, hand the source subscription to the witness, and return the
/// witness as the subscription handle. Disposing the returned handle therefore tears down the source subscription too.
/// </remarks>
public static class WitnessSubscription
{
    /// <summary>Subscribes a witness to its source and returns the witness as the subscription handle.</summary>
    /// <typeparam name="TSource">The element type the witness observes.</typeparam>
    /// <typeparam name="TResult">The element type the downstream observer receives.</typeparam>
    /// <param name="source">The source the witness observes.</param>
    /// <param name="witness">The per-subscription witness.</param>
    /// <param name="observer">The downstream observer, whose disposal is linked when it is itself a witness.</param>
    /// <param name="cancellationToken">A token that cancels establishing the subscription.</param>
    /// <returns>The witness, whose disposal also disposes the source subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="witness"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
    public static async ValueTask<IAsyncDisposable> SubscribeAsync<TSource, TResult>(
        IObservableAsync<TSource> source,
        IWitnessAsync<TSource> witness,
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(witness);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (observer is IWitnessAsync<TResult> downstream)
        {
            downstream.LinkUpstreamCancellation(witness.InternalDisposedToken);
        }

        var subscription = await source.SubscribeAsync(witness, cancellationToken).ConfigureAwait(false);
        await witness.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
        return witness;
    }
}
