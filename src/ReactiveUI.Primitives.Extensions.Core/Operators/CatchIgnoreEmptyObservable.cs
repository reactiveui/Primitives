// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Forwards source values; on any source error, swallows it and completes silently.</summary>
/// <typeparam name="T">Element type.</typeparam>
/// <param name="source">Upstream source.</param>
public sealed class CatchIgnoreEmptyObservable<T>(IObservable<T> source) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new CatchIgnoreEmptyWitness(observer));
    }

    /// <summary>Forwarding observer that swaps <see cref="OnError"/> for terminal completion.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class CatchIgnoreEmptyWitness(IObserver<T> downstream) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value) => downstream.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnCompleted();

        /// <inheritdoc/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "SST2318:Members should not have identical bodies",
            Justification =
                "This witness deliberately swaps a terminal error for downstream completion, so OnError and OnCompleted "
                + "both forward to downstream.OnCompleted by design. They are distinct IObserver<T> channels, not a copy "
                + "that was meant to differ.")]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
