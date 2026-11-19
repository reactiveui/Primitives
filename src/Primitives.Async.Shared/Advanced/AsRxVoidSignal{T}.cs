// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>
/// Fuses <c>source.Select(static _ =&gt; RxVoid.Default)</c> into a single observer layer; every
/// upstream emission is forwarded as <see cref = "RxVoid.Default"/> with no closure capture.
/// </summary>
/// <typeparam name = "T">The upstream element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AsRxVoidSignal: Source = {_source}")]
public sealed class AsRxVoidSignal<T> : IObservableAsync<RxVoid>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>Initializes a new instance of the <see cref="AsRxVoidSignal{T}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    public AsRxVoidSignal(IObservableAsync<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        _source = source;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<RxVoid>.SubscribeAsync(
        IObserverAsync<RxVoid> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new AsSignalWitness<T>(observer, cancellationToken),
            observer,
            cancellationToken);
}
