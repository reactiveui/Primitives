// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Filtering operator that forwards only <c>false</c> values from a boolean source.
/// Replaces the <c>source.Where(b =&gt; !b)</c> pattern with a dedicated forwarding
/// observer, avoiding the per-subscription closure allocation that the predicate
/// lambda would otherwise capture.
/// </summary>
/// <param name="source">The boolean source observable.</param>
internal sealed class WhereFalseObservable(IObservable<bool> source) : IObservable<bool>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new WhereFalseObserver(observer));
    }

    /// <summary>Forwarding observer that drops <c>true</c> values and passes <c>false</c> values, terminal error and completion through unchanged.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class WhereFalseObserver(IObserver<bool> downstream) : IObserver<bool>
    {
        /// <inheritdoc/>
        public void OnNext(bool value)
        {
            if (value)
            {
                return;
            }

            downstream.OnNext(false);
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
