// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Switches each notification onto the configured async context before forwarding it.</summary>
/// <typeparam name="T">The observed element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AsyncContext = {AsyncContext}, ForceYielding = {ForceYielding}")]
public sealed class WitnessOnWitness<T> : WitnessAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="WitnessOnWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="asyncContext">The target async context.</param>
    /// <param name="forceYielding">true to force yielding before every forwarded notification; otherwise, false.</param>
    public WitnessOnWitness(IObserverAsync<T> observer, AsyncContext asyncContext, bool forceYielding)
    {
        Observer = observer;
        AsyncContext = asyncContext;
        ForceYielding = forceYielding;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserverAsync<T> Observer { get; }

    /// <summary>Gets the target async context.</summary>
    private AsyncContext AsyncContext { get; }

    /// <summary>Gets a value indicating whether every notification should yield.</summary>
    private bool ForceYielding { get; }

    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        !ForceYielding && AsyncContext.IsSameAsCurrentAsyncContext()
            ? Observer.OnNextAsync(value, cancellationToken)
            : ForwardAfterContextSwitchAsync(value, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        !ForceYielding && AsyncContext.IsSameAsCurrentAsyncContext()
            ? Observer.OnErrorResumeAsync(error, cancellationToken)
            : ForwardErrorAfterContextSwitchAsync(error, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) =>
        !ForceYielding && AsyncContext.IsSameAsCurrentAsyncContext()
            ? Observer.OnCompletedAsync(result)
            : ForwardCompletionAfterContextSwitchAsync(result);

    /// <summary>Switches context and forwards a value notification.</summary>
    /// <param name="value">The value to forward.</param>
    /// <param name="cancellationToken">The cancellation token for the context switch and notification.</param>
    /// <returns>A task that completes after the notification has been forwarded.</returns>
    private async ValueTask ForwardAfterContextSwitchAsync(T value, CancellationToken cancellationToken)
    {
        await AsyncContext.SwitchContextAsync(ForceYielding, cancellationToken);
        await Observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Switches context and forwards an error notification.</summary>
    /// <param name="error">The error to forward.</param>
    /// <param name="cancellationToken">The cancellation token for the context switch and notification.</param>
    /// <returns>A task that completes after the notification has been forwarded.</returns>
    private async ValueTask ForwardErrorAfterContextSwitchAsync(Exception error, CancellationToken cancellationToken)
    {
        await AsyncContext.SwitchContextAsync(ForceYielding, cancellationToken);
        await Observer.OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Switches context and forwards a completion notification.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task that completes after the notification has been forwarded.</returns>
    private async ValueTask ForwardCompletionAfterContextSwitchAsync(Result result)
    {
        await AsyncContext.SwitchContextAsync(ForceYielding, CancellationToken.None);
        await Observer.OnCompletedAsync(result).ConfigureAwait(false);
    }
}
