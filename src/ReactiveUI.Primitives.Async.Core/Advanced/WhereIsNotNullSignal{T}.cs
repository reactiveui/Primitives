// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Forwards the non-null source values as non-nullable through a single observer layer.</summary>
/// <typeparam name="T">The non-nullable element type seen downstream.</typeparam>
[System.Diagnostics.DebuggerDisplay("WhereIsNotNullSignal: Source = {_source}")]
public sealed class WhereIsNotNullSignal<T> : IObservableAsync<T>
    where T : class
{
    /// <summary>The nullable source observable.</summary>
    private readonly IObservableAsync<T?> _source;

    /// <summary>Initializes a new instance of the <see cref="WhereIsNotNullSignal{T}"/> class.</summary>
    /// <param name="source">The nullable source observable.</param>
    public WhereIsNotNullSignal(IObservableAsync<T?> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        _source = source;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new WhereIsNotNullWitness<T>(observer, cancellationToken),
            observer,
            cancellationToken);
}
