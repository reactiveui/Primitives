// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Skips values while the asynchronous predicate holds, then forwards every later value without awaiting it.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SkipWhileAsyncSignal: Source = {_source}")]
public sealed class SkipWhileAsyncSignal<T> : IObservableAsync<T>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The async skip-while predicate.</summary>
    private readonly Func<T, CancellationToken, ValueTask<bool>> _predicate;

    /// <summary>Initializes a new instance of the <see cref="SkipWhileAsyncSignal{T}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="predicate">The async skip-while predicate.</param>
    public SkipWhileAsyncSignal(
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
            new SkipWhileAsyncWitness<T>(observer, _predicate, cancellationToken),
            observer,
            cancellationToken);
}
