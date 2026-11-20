// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Subscribes the handler-produced fallback observable when the source completes with a failure.</summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
[System.Diagnostics.DebuggerDisplay("CatchSignal: Source = {_source}")]
public sealed class CatchSignal<T> : IObservableAsync<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The fallback handler invoked with the source exception when the source completes with a failure.</summary>
    private readonly Func<Exception, IObservableAsync<T>> _handler;

    /// <summary>Optional asynchronous error-resume callback.</summary>
    private readonly Func<Exception, CancellationToken, ValueTask>? _onErrorResume;

    /// <summary>Initializes a new instance of the <see cref="CatchSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="handler">The fallback handler invoked with the source exception when the source completes with a failure.</param>
    public CatchSignal(IObservableAsync<T> source, Func<Exception, IObservableAsync<T>> handler)
        : this(source, handler, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CatchSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="handler">The fallback handler invoked with the source exception when the source completes with a failure.</param>
    /// <param name="onErrorResume">Optional asynchronous error-resume callback. When <see langword="null"/>, error-resume notifications are
    /// forwarded straight to the downstream observer.</param>
    public CatchSignal(
        IObservableAsync<T> source,
        Func<Exception, IObservableAsync<T>> handler,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(handler);

        _source = source;
        _handler = handler;
        _onErrorResume = onErrorResume;
    }

    /// <summary>Creates a <see cref="CatchSignal{T}"/> that switches to <paramref name="fallback"/> on failure.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="fallback">The fallback sequence used after a failure.</param>
    /// <returns>A configured <see cref="CatchSignal{T}"/>.</returns>
    public static CatchSignal<T> CreateResume(IObservableAsync<T> source, IObservableAsync<T> fallback)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new(source, _ => fallback, null);
    }

    /// <summary>Creates a <see cref="CatchSignal{T}"/> that reports resumable errors to the global unhandled-exception handler and switches on terminal failure.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="handler">The fallback handler invoked on terminal failure.</param>
    /// <returns>A configured <see cref="CatchSignal{T}"/>.</returns>
    public static CatchSignal<T> CreateIgnoreErrorResume(
        IObservableAsync<T> source,
        Func<Exception, IObservableAsync<T>> handler) =>
        new(
            source,
            handler,
            static (error, _) =>
            {
                UnhandledExceptionHandler.ReportUnhandledException(error);
                return default;
            });

    /// <summary>Creates a <see cref="CatchSignal{T}"/> that suppresses terminal failures by continuing with an empty sequence.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A configured <see cref="CatchSignal{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CatchSignal<T> CreateIgnore(IObservableAsync<T> source) =>
        new(source, static _ => SignalAsync.EmptySignalAsync<T>.Instance, null);

    /// <summary>Creates a <see cref="CatchSignal{T}"/> that suppresses terminal failures matching <typeparamref name="TException"/> after invoking <paramref name="errorAction"/>.</summary>
    /// <typeparam name="TException">The exception type to intercept.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="errorAction">The side effect invoked when a matching exception is observed.</param>
    /// <returns>A configured <see cref="CatchSignal{T}"/>.</returns>
    public static CatchSignal<T> CreateIgnore<TException>(IObservableAsync<T> source, Action<TException> errorAction)
        where TException : Exception
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(errorAction);

        return new(
            source,
            ex =>
            {
                if (ex is not TException typedException)
                {
                    return new SignalAsync.ThrowSignalAsync<T>(ex);
                }

                errorAction(typedException);
                return SignalAsync.EmptySignalAsync<T>.Instance;
            },
            null);
    }

    /// <summary>Creates a <see cref="CatchSignal{T}"/> that emits <paramref name="fallback"/> when <paramref name="source"/> fails.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="fallback">The fallback value to emit.</param>
    /// <returns>A configured <see cref="CatchSignal{T}"/>.</returns>
    public static CatchSignal<T> CreateReturn(IObservableAsync<T> source, T fallback)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new(source, _ => new SignalAsync.ReturnSignalAsync<T>(fallback), null);
    }

    /// <summary>
    /// Creates a <see cref="CatchSignal{T}"/> that emits a value produced by <paramref name="fallbackFactory"/> when
    /// <paramref name="source"/> fails with <typeparamref name="TException"/>.
    /// </summary>
    /// <typeparam name="TException">The exception type to intercept.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="fallbackFactory">The fallback value factory.</param>
    /// <returns>A configured <see cref="CatchSignal{T}"/>.</returns>
    public static CatchSignal<T> CreateReturn<TException>(
        IObservableAsync<T> source,
        Func<TException, T> fallbackFactory)
        where TException : Exception
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(fallbackFactory);

        return new(
            source,
            ex =>
                ex is TException typedException
                    ? new SignalAsync.ReturnSignalAsync<T>(fallbackFactory(typedException))
                    : new SignalAsync.ThrowSignalAsync<T>(ex),
            null);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new CatchWitness<T>(observer, _handler, _onErrorResume, cancellationToken),
            observer,
            cancellationToken);
}
