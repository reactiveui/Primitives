// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Filtering operator that forwards only <c>true</c> values from a boolean source.
/// Replaces the <c>source.Where(b =&gt; b)</c> pattern with a dedicated forwarding
/// observer, avoiding the per-subscription closure allocation that the predicate
/// lambda would otherwise capture.
/// </summary>
/// <param name="source">The boolean source observable.</param>
public sealed class WhereTrueObservable(IObservable<bool> source) : IObservable<bool>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new WhereTrueWitness(observer));
    }

    /// <summary>Forwarding observer that drops <c>false</c> values and passes <c>true</c> values, terminal error and completion through unchanged.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class WhereTrueWitness(IObserver<bool> downstream) : IObserver<bool>
    {
        /// <inheritdoc/>
        public void OnNext(bool value)
        {
            if (!value)
            {
                return;
            }

            downstream.OnNext(true);
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
