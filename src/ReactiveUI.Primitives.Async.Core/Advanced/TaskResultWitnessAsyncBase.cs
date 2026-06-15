// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Base class for witnesses that produce a single task-based result value when the observed sequence completes.</summary>
/// <typeparam name="T">The type of elements received from the observable sequence.</typeparam>
/// <typeparam name="TTaskValue">The type of the result value produced by this witness.</typeparam>
/// <param name="cancellationToken">A cancellation token used to cancel the waiting operation.</param>
public abstract class TaskResultWitnessAsyncBase<T, TTaskValue>(CancellationToken cancellationToken) : WitnessAsync<T>
{
    /// <summary>The completion helper used to produce and cancel the observer's single result value.</summary>
    private readonly TaskResultCompletionSource<TTaskValue> _completion = new(cancellationToken);

    /// <summary>Asynchronously waits for the observer to produce its result value.</summary>
    /// <returns>A task representing the asynchronous operation, containing the result value.</returns>
    public ValueTask<TTaskValue> AwaitResultAsync() =>
        _completion.AwaitResultAsync(this);

    /// <summary>Attempts to set the result value and complete the task.</summary>
    /// <param name="value">The result value to set.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [DebuggerStepThrough]
    protected ValueTask SetResultAndDisposeAsync(TTaskValue value) =>
        _completion.SetResultAndDisposeAsync(value, this);

    /// <summary>Attempts to set the task to a faulted state with the specified exception.</summary>
    /// <param name="e">The exception that caused the fault.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected ValueTask SetExceptionAndDisposeAsync(Exception e) =>
        _completion.SetExceptionAndDisposeAsync(e, this);
}
