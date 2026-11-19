// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Routes notifications to callbacks and unhandled failures to the exception handler.</summary>
/// <typeparam name="T">The type of the elements received by the witness.</typeparam>
/// <param name="onNextAsync">The asynchronous function invoked for each element.</param>
/// <param name="onErrorResumeAsync">An optional asynchronous function invoked when a resumable error occurs.</param>
/// <param name="onCompletedAsync">An optional asynchronous function invoked when the sequence completes.</param>
[DebuggerDisplay("CallbackWitnessAsync: {_witness}")]
public sealed class CallbackWitnessAsync<T>(
    Func<T, CancellationToken, ValueTask> onNextAsync,
    Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync = null,
    Func<Result, ValueTask>? onCompletedAsync = null) : IWitnessAsync<T>
{
    /// <summary>The asynchronous function invoked when a resumable error occurs.</summary>
    private readonly Func<Exception, CancellationToken, ValueTask> _onErrorResumeAsync =
        onErrorResumeAsync ?? ReportUnhandledError;

    /// <summary>The asynchronous function invoked when the sequence completes.</summary>
    private readonly Func<Result, ValueTask> _onCompletedAsync =
        onCompletedAsync ?? ReportUnhandledCompletion;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Subscribes to <paramref name="source"/> ignoring all emitted values.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A value task containing the subscription disposable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<IAsyncDisposable> SubscribeAsync(IObservableAsync<T> source) =>
        SubscribeAsync(source, static (_, _) => default, CancellationToken.None);

    /// <summary>Subscribes to <paramref name="source"/> with an asynchronous item callback.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNextAsync">Asynchronous callback invoked for each item.</param>
    /// <returns>A value task containing the subscription disposable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<IAsyncDisposable> SubscribeAsync(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask> onNextAsync) =>
        SubscribeAsync(source, onNextAsync, CancellationToken.None);

    /// <summary>Subscribes to <paramref name="source"/> with an asynchronous item callback and cancellation token.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNextAsync">Asynchronous callback invoked for each item.</param>
    /// <param name="cancellationToken">Token that cancels the subscription.</param>
    /// <returns>A value task containing the subscription disposable.</returns>
    public static ValueTask<IAsyncDisposable> SubscribeAsync(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask> onNextAsync,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(onNextAsync);

        CallbackWitnessAsync<T> observer = new(onNextAsync);
        return source.SubscribeAsync(observer, cancellationToken);
    }

    /// <summary>Subscribes to <paramref name="source"/> with asynchronous item and error callbacks.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNextAsync">Asynchronous callback invoked for each item.</param>
    /// <param name="onErrorResumeAsync">Optional asynchronous error callback.</param>
    /// <returns>A value task containing the subscription disposable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<IAsyncDisposable> SubscribeAsync(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask> onNextAsync,
        Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync) =>
        SubscribeAsync(source, onNextAsync, onErrorResumeAsync, null, CancellationToken.None);

    /// <summary>Subscribes to <paramref name="source"/> with asynchronous item, error, and completion callbacks.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNextAsync">Asynchronous callback invoked for each item.</param>
    /// <param name="onErrorResumeAsync">Optional asynchronous error callback.</param>
    /// <param name="onCompletedAsync">Optional asynchronous completion callback.</param>
    /// <returns>A value task containing the subscription disposable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<IAsyncDisposable> SubscribeAsync(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask> onNextAsync,
        Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync,
        Func<Result, ValueTask>? onCompletedAsync) =>
        SubscribeAsync(source, onNextAsync, onErrorResumeAsync, onCompletedAsync, CancellationToken.None);

    /// <summary>Subscribes to <paramref name="source"/> with asynchronous item, error, and completion callbacks and cancellation token.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNextAsync">Asynchronous callback invoked for each item.</param>
    /// <param name="onErrorResumeAsync">Optional asynchronous error callback.</param>
    /// <param name="onCompletedAsync">Optional asynchronous completion callback.</param>
    /// <param name="cancellationToken">Token that cancels the subscription.</param>
    /// <returns>A value task containing the subscription disposable.</returns>
    public static ValueTask<IAsyncDisposable> SubscribeAsync(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask> onNextAsync,
        Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync,
        Func<Result, ValueTask>? onCompletedAsync,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(onNextAsync);

        CallbackWitnessAsync<T> observer = new(onNextAsync, onErrorResumeAsync, onCompletedAsync);
        return source.SubscribeAsync(observer, cancellationToken);
    }

    /// <summary>Subscribes to <paramref name="source"/> with a synchronous item action.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">Action invoked for each item.</param>
    /// <returns>A value task containing the subscription disposable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<IAsyncDisposable> SubscribeAsync(IObservableAsync<T> source, Action<T> onNext) =>
        SubscribeAsync(source, onNext, CancellationToken.None);

    /// <summary>Subscribes to <paramref name="source"/> with a synchronous item action and cancellation token.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">Action invoked for each item.</param>
    /// <param name="cancellationToken">Token that cancels the subscription.</param>
    /// <returns>A value task containing the subscription disposable.</returns>
    public static ValueTask<IAsyncDisposable> SubscribeAsync(
        IObservableAsync<T> source,
        Action<T> onNext,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        CallbackWitnessAsync<T> observer = new((x, _) =>
        {
            onNext(x);
            return default;
        });

        return source.SubscribeAsync(observer, cancellationToken);
    }

    /// <summary>Subscribes to <paramref name="source"/> with synchronous item, error, and completion actions.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">Action invoked for each item.</param>
    /// <param name="onErrorResume">Optional action invoked on resumable error.</param>
    /// <param name="onCompleted">Optional action invoked on completion.</param>
    /// <param name="cancellationToken">Token that cancels the subscription.</param>
    /// <returns>A value task containing the subscription disposable.</returns>
    public static ValueTask<IAsyncDisposable> SubscribeAsync(
        IObservableAsync<T> source,
        Action<T> onNext,
        Action<Exception>? onErrorResume,
        Action<Result>? onCompleted,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);
        ArgumentExceptionHelper.ThrowIfNull(source);

        static ValueTask OnErrorResumeCallback(Exception e, Action<Exception> handler)
        {
            handler(e);
            return default;
        }

        static ValueTask OnCompletedCallback(Result x, Action<Result> handler)
        {
            handler(x);
            return default;
        }

        CallbackWitnessAsync<T> observer = new(
            (x, _) =>
            {
                onNext(x);
                return default;
            },
            onErrorResume is null ? null : (e, _) => OnErrorResumeCallback(e, onErrorResume),
            onCompleted is null ? null : x => OnCompletedCallback(x, onCompleted));

        return source.SubscribeAsync(observer, cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
        WitnessAsync.OnNextAsync(this, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

    /// <summary>Reports an unhandled resumable error when no error callback was supplied.</summary>
    /// <param name="error">The unhandled exception.</param>
    /// <param name="cancellationToken">The cancellation token supplied by the source.</param>
    /// <returns>A completed value task.</returns>
    private static ValueTask ReportUnhandledError(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        UnhandledExceptionHandler.ReportUnhandledException(error);
        return default;
    }

    /// <summary>Reports a terminal failure when no completion callback was supplied.</summary>
    /// <param name="result">The terminal result.</param>
    /// <returns>A completed value task.</returns>
    private static ValueTask ReportUnhandledCompletion(Result result)
    {
        var exception = result.Exception;
        if (exception is null)
        {
            return default;
        }

        UnhandledExceptionHandler.ReportUnhandledException(exception);
        return default;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        onNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _onErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        _onCompletedAsync(result);
}
