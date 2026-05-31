// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Executes an action when the subscription is disposed.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="disposeAction">The action to execute when the subscription is disposed.</param>
internal sealed class DoOnDisposeObservable<T>(
    IObservable<T> source,
    Action disposeAction) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(disposeAction);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new DoOnDisposeSubscription(source.Subscribe(observer), disposeAction);
    }

    /// <summary>
    /// Per-subscribe disposal handle that forwards <see cref="IDisposable.Dispose"/> to the source
    /// subscription and then to the caller-supplied action. Dedicated class instead of the
    /// previous <c>ActionDisposable(() =&gt; …)</c> form so no closure is allocated per subscribe.
    /// </summary>
    /// <param name="subscription">The upstream subscription disposed before the action fires.</param>
    /// <param name="disposeAction">The action executed once after the upstream is disposed.</param>
    private sealed class DoOnDisposeSubscription(IDisposable subscription, Action disposeAction) : IDisposable
    {
        /// <summary>Latches to <c>1</c> on the first dispose so the action fires exactly once.</summary>
        private int _disposed;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                subscription.Dispose();
            }
            finally
            {
                disposeAction();
            }
        }
    }
}
