// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observer that logs resumable errors before forwarding them downstream.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Downstream = {Downstream}, Logger = {Logger}")]
public sealed class LogErrorsWitness<T> : WitnessAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="LogErrorsWitness{T}"/> class.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="logger">The error logger.</param>
    public LogErrorsWitness(IObserverAsync<T> downstream, Action<Exception> logger)
    {
        ArgumentExceptionHelper.ThrowIfNull(downstream);
        ArgumentExceptionHelper.ThrowIfNull(logger);

        Downstream = downstream;
        Logger = logger;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserverAsync<T> Downstream { get; }

    /// <summary>Gets the error logger.</summary>
    private Action<Exception> Logger { get; }

    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        Downstream.OnNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        Logger(error);
        return Downstream.OnErrorResumeAsync(error, cancellationToken);
    }

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) =>
        Downstream.OnCompletedAsync(result);
}
