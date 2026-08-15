// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Switches source notifications onto an async context before forwarding them downstream.</summary>
/// <typeparam name="T">The source element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Source = {Source}, ForceYielding = {ForceYielding}")]
public sealed class WitnessOnSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="asyncContext">The target async context.</param>
    /// <param name="forceYielding">true to force yielding before every forwarded notification; otherwise, false.</param>
    public WitnessOnSignal(IObservableAsync<T> source, AsyncContext asyncContext, bool forceYielding)
    {
        Source = source;
        AsyncContext = asyncContext;
        ForceYielding = forceYielding;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservableAsync<T> Source { get; }

    /// <summary>Gets the target async context.</summary>
    private AsyncContext AsyncContext { get; }

    /// <summary>Gets a value indicating whether every notification should yield.</summary>
    private bool ForceYielding { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        WitnessOnWitness<T> contextSwitchObserver = new(observer, AsyncContext, ForceYielding);
        return Source.SubscribeAsync(contextSwitchObserver, cancellationToken);
    }
}
