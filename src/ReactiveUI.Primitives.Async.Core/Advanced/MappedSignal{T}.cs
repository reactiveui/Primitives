// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A signal that applies a transformation to the observable values of the source signal.</summary>
/// <typeparam name="T">The type of elements processed by the signal.</typeparam>
[System.Diagnostics.DebuggerDisplay("MappedSignal: Values = {Values}")]
public sealed class MappedSignal<T> : ISignalAsync<T>
{
    /// <summary>The source signal.</summary>
    private readonly ISignalAsync<T> _original;

    /// <summary>Initializes a new instance of the <see cref="MappedSignal{T}"/> class.</summary>
    /// <param name="original">The source signal.</param>
    /// <param name="mapper">Transforms the source's value sequence once during construction.</param>
    public MappedSignal(
        ISignalAsync<T> original,
        Func<IObservableAsync<T>, IObservableAsync<T>> mapper)
    {
        ArgumentExceptionHelper.ThrowIfNull(original);
        ArgumentExceptionHelper.ThrowIfNull(mapper);
        _original = original;
        Values = mapper(original.Values);
    }

    /// <inheritdoc/>
    public IObservableAsync<T> Values { get; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        Values.SubscribeAsync(observer, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
        _original.OnNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        _original.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) => _original.OnCompletedAsync(result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => _original.DisposeAsync();
}
