// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Runs the action for one value at a time and drops the values that arrive while it is running.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("DropIfBusySignal: Source = {_source}")]
public sealed class DropIfBusySignal<T> : IObservableAsync<T>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The async side-effect invoked for accepted values.</summary>
    private readonly Func<T, CancellationToken, ValueTask> _asyncAction;

    /// <summary>Initializes a new instance of the <see cref="DropIfBusySignal{T}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="asyncAction">The async side-effect invoked for accepted values.</param>
    public DropIfBusySignal(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask> asyncAction)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(asyncAction);
        _source = source;
        _asyncAction = asyncAction;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new DropIfBusyWitness<T>(observer, _asyncAction, cancellationToken),
            observer,
            cancellationToken);
}
