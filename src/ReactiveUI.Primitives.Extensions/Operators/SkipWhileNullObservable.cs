// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Filtering operator that drops leading <see langword="null"/> values, then forwards every value
/// (including subsequent <see langword="null"/>s) once the latch has opened. Replaces the previous
/// <c>source.SkipWhile(x =&gt; x == null)</c> composition that delegated to System.Reactive's
/// <c>SkipWhile</c>, eliminating the per-subscription closure allocation and the extra observer
/// layer the chain introduced.
/// </summary>
/// <typeparam name="T">The element type of the source observable; must be a reference type so the
/// null check is meaningful.</typeparam>
/// <param name="source">The source observable.</param>
internal sealed class SkipWhileNullObservable<T>(IObservable<T> source) : IObservable<T>
    where T : class
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new SkipWhileNullWitness(observer));
    }

    /// <summary>
    /// Forwarding observer that swallows leading <see langword="null"/> values until the first
    /// non-null value, then becomes a transparent forwarder for the remainder of the sequence.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class SkipWhileNullWitness(IObserver<T> downstream) : IObserver<T>
    {
        /// <summary>Latches to <see langword="true"/> after the first non-null value has been forwarded.</summary>
        private bool _gateOpen;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (!_gateOpen)
            {
                if (value is null)
                {
                    return;
                }

                _gateOpen = true;
            }

            downstream.OnNext(value);
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
