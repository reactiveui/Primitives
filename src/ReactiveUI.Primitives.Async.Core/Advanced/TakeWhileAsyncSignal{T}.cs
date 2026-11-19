// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Forwards values while the asynchronous predicate holds; the first miss completes the sequence.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TakeWhileAsyncSignal: Source = {_source}")]
public sealed class TakeWhileAsyncSignal<T> : IObservableAsync<T>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The async take-while predicate.</summary>
    private readonly Func<T, CancellationToken, ValueTask<bool>> _predicate;

    /// <summary>Initializes a new instance of the <see cref="TakeWhileAsyncSignal{T}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="predicate">The async take-while predicate.</param>
    public TakeWhileAsyncSignal(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate)
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
            new TakeWhileAsyncWitness<T>(observer, _predicate, cancellationToken),
            observer,
            cancellationToken);
}
