// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that logs resumable errors without changing the source sequence.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Source = {Source}, Logger = {Logger}")]
public sealed class LogErrorsSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="LogErrorsSignal{T}"/> class.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="logger">The error logger.</param>
    public LogErrorsSignal(IObservableAsync<T> source, Action<Exception> logger)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(logger);

        Source = source;
        Logger = logger;
    }

    /// <summary>Gets the error logger.</summary>
    private Action<Exception> Logger { get; }

    /// <summary>Gets the source observable sequence.</summary>
    private IObservableAsync<T> Source { get; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        Source.SubscribeAsync(new LogErrorsWitness<T>(observer, Logger), cancellationToken);
}
