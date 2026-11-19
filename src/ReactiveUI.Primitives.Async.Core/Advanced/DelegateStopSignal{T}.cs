// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that emits items from the source until a raw completion signal fires.</summary>
/// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="stopSignal">The delegate that provides the stop signal.</param>
/// <param name="options">Options controlling the take-until behavior.</param>
/// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
[System.Diagnostics.DebuggerDisplay("DelegateStopSignal: Source = {Source}")]
public sealed class DelegateStopSignal<T>(
    IObservableAsync<T> source,
    CompletionSignalDelegate stopSignal,
    TakeUntilOptions options,
    CancellationToken stopToken) : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="DelegateStopSignal{T}"/> class.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="stopSignal">The delegate that provides the stop signal.</param>
    /// <param name="options">Options controlling the take-until behavior.</param>
    public DelegateStopSignal(
        IObservableAsync<T> source,
        CompletionSignalDelegate stopSignal,
        TakeUntilOptions options)
        : this(source, stopSignal, options, default)
    {
    }

    /// <summary>Gets the source observable sequence.</summary>
    internal IObservableAsync<T> Source { get; } = source;

    /// <summary>Gets the delegate that provides the stop signal.</summary>
    internal CompletionSignalDelegate StopSignal { get; } = stopSignal;

    /// <summary>Gets options controlling the take-until behavior.</summary>
    internal TakeUntilOptions Options { get; } = options;

    /// <summary>Gets a token whose cancellation also completes the sequence.</summary>
    internal CancellationToken StopToken { get; } = stopToken;

    /// <summary>Creates a <see cref="DelegateStopSignal{T}"/> with default options.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="stopSignal">The delegate that provides the stop signal.</param>
    /// <returns>A configured <see cref="DelegateStopSignal{T}"/>.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static DelegateStopSignal<T> Create(
        IObservableAsync<T> source,
        CompletionSignalDelegate stopSignal) =>
        Create(source, stopSignal, null, default);

    /// <summary>Creates a <see cref="DelegateStopSignal{T}"/> with optional options.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="stopSignal">The delegate that provides the stop signal.</param>
    /// <param name="options">Optional options; <see langword="null"/> uses <see cref="TakeUntilOptions.Default"/>.</param>
    /// <returns>A configured <see cref="DelegateStopSignal{T}"/>.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static DelegateStopSignal<T> Create(
        IObservableAsync<T> source,
        CompletionSignalDelegate stopSignal,
        TakeUntilOptions? options) =>
        Create(source, stopSignal, options, default);

    /// <summary>Creates a <see cref="DelegateStopSignal{T}"/> with default options and a stop token.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="stopSignal">The delegate that provides the stop signal.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    /// <returns>A configured <see cref="DelegateStopSignal{T}"/>.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static DelegateStopSignal<T> Create(
        IObservableAsync<T> source,
        CompletionSignalDelegate stopSignal,
        CancellationToken stopToken) =>
        Create(source, stopSignal, null, stopToken);

    /// <summary>Creates a <see cref="DelegateStopSignal{T}"/> with optional options and a stop token.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="stopSignal">The delegate that provides the stop signal.</param>
    /// <param name="options">Optional options; <see langword="null"/> uses <see cref="TakeUntilOptions.Default"/>.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    /// <returns>A configured <see cref="DelegateStopSignal{T}"/>.</returns>
    public static DelegateStopSignal<T> Create(
        IObservableAsync<T> source,
        CompletionSignalDelegate stopSignal,
        TakeUntilOptions? options,
        CancellationToken stopToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(stopSignal);

        return new(source, stopSignal, options ?? TakeUntilOptions.Default, stopToken);
    }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        DelegateStopCoordinator<T> subscription = new(this, observer);
        subscription.LinkExternalCancellation(cancellationToken);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeSourcesAsync(cancellationToken));
    }
}
