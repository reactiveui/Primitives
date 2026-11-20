// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that emits items from the source until another async observable emits or completes.</summary>
/// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
/// <typeparam name="TOther">The type of the elements in the signal sequence.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="other">The signal observable whose emission triggers completion.</param>
/// <param name="options">Options controlling the take-until behavior.</param>
/// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
[System.Diagnostics.DebuggerDisplay("TakeUntilAsyncSignal: Source = {Source}")]
public sealed class TakeUntilAsyncSignal<T, TOther>(
    IObservableAsync<T> source,
    IObservableAsync<TOther> other,
    TakeUntilOptions options,
    CancellationToken stopToken) : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="TakeUntilAsyncSignal{T, TOther}"/> class.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="other">The signal observable whose emission triggers completion.</param>
    /// <param name="options">Options controlling the take-until behavior.</param>
    public TakeUntilAsyncSignal(
        IObservableAsync<T> source,
        IObservableAsync<TOther> other,
        TakeUntilOptions options)
        : this(source, other, options, default)
    {
    }

    /// <summary>Gets the source observable sequence.</summary>
    internal IObservableAsync<T> Source { get; } = source;

    /// <summary>Gets the signal observable whose emission triggers completion.</summary>
    internal IObservableAsync<TOther> Other { get; } = other;

    /// <summary>Gets options controlling the take-until behavior.</summary>
    internal TakeUntilOptions Options { get; } = options;

    /// <summary>Gets a token whose cancellation also completes the sequence.</summary>
    internal CancellationToken StopToken { get; } = stopToken;

    /// <summary>Creates a <see cref="TakeUntilAsyncSignal{T, TOther}"/> with default options.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="other">The signal observable whose emission triggers completion.</param>
    /// <returns>A configured <see cref="TakeUntilAsyncSignal{T, TOther}"/>.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static TakeUntilAsyncSignal<T, TOther> Create(
        IObservableAsync<T> source,
        IObservableAsync<TOther> other) =>
        Create(source, other, null, default);

    /// <summary>Creates a <see cref="TakeUntilAsyncSignal{T, TOther}"/> with optional options.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="other">The signal observable whose emission triggers completion.</param>
    /// <param name="options">Optional options; <see langword="null"/> uses <see cref="TakeUntilOptions.Default"/>.</param>
    /// <returns>A configured <see cref="TakeUntilAsyncSignal{T, TOther}"/>.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static TakeUntilAsyncSignal<T, TOther> Create(
        IObservableAsync<T> source,
        IObservableAsync<TOther> other,
        TakeUntilOptions? options) =>
        Create(source, other, options, default);

    /// <summary>Creates a <see cref="TakeUntilAsyncSignal{T, TOther}"/> with default options and a stop token.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="other">The signal observable whose emission triggers completion.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    /// <returns>A configured <see cref="TakeUntilAsyncSignal{T, TOther}"/>.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static TakeUntilAsyncSignal<T, TOther> Create(
        IObservableAsync<T> source,
        IObservableAsync<TOther> other,
        CancellationToken stopToken) =>
        Create(source, other, null, stopToken);

    /// <summary>Creates a <see cref="TakeUntilAsyncSignal{T, TOther}"/> with optional options and a stop token.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="other">The signal observable whose emission triggers completion.</param>
    /// <param name="options">Optional options; <see langword="null"/> uses <see cref="TakeUntilOptions.Default"/>.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    /// <returns>A configured <see cref="TakeUntilAsyncSignal{T, TOther}"/>.</returns>
    public static TakeUntilAsyncSignal<T, TOther> Create(
        IObservableAsync<T> source,
        IObservableAsync<TOther> other,
        TakeUntilOptions? options,
        CancellationToken stopToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(other);

        return new(source, other, options ?? TakeUntilOptions.Default, stopToken);
    }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        AsyncStopCoordinator<T, TOther> subscription = new(this, observer);
        subscription.LinkExternalCancellation(cancellationToken);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            async () => await subscription.SubscribeSourcesAsync(cancellationToken).ConfigureAwait(false));
    }
}
