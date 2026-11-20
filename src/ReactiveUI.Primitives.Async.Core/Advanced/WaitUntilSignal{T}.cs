// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Forwards the first value matching the predicate, then completes and tears down the source subscription.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("WaitUntilSignal: Source = {_source}")]
public sealed class WaitUntilSignal<T> : IObservableAsync<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The predicate matched against each value.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="WaitUntilSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate matched against each value.</param>
    public WaitUntilSignal(IObservableAsync<T> source, Func<T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);
        _source = source;
        _predicate = predicate;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new WaitUntilWitness<T>(observer, _predicate, cancellationToken),
            observer,
            cancellationToken);
}
