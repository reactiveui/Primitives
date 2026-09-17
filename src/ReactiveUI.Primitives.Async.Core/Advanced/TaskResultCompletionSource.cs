// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates task completion for terminal observers and disposes the owning subscription when complete.</summary>
/// <typeparam name="T">The result value type.</typeparam>
/// <param name="cancellationToken">The cancellation token that can cancel the pending result.</param>
[System.Diagnostics.DebuggerDisplay("TaskResultCompletionSource: IsCompleted = {_taskSource.Task.IsCompleted}")]
public sealed class TaskResultCompletionSource<T>(CancellationToken cancellationToken)
{
    /// <summary>The task completion source that publishes the terminal result.</summary>
    private readonly TaskCompletionSource<T> _taskSource = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The cancellation token that cancels the terminal wait.</summary>
    private readonly CancellationToken _cancellationToken = cancellationToken;

    /// <summary>Waits for the terminal result and disposes <paramref name="owner"/> when the wait exits.</summary>
    /// <param name="owner">The owner to dispose when the result wait completes, faults, or is cancelled.</param>
    /// <returns>The terminal result value.</returns>
    public async ValueTask<T> AwaitResultAsync(IAsyncDisposable owner)
    {
        var cancellationRegistration = RegisterCancellation();
        ExceptionDispatchInfo? failure = null;
        T result;
        try
        {
            result = await _taskSource.Task.ConfigureAwait(false);
        }
        catch (Exception e)
        {
            failure = ExceptionDispatchInfo.Capture(e);
            result = default!;
        }

#if NET8_0_OR_GREATER
        await cancellationRegistration.DisposeAsync().ConfigureAwait(false);
#else
        cancellationRegistration.Dispose();
#endif
        await owner.DisposeAsync().ConfigureAwait(false);
        failure?.Throw();
        return result;
    }

    /// <summary>Completes the result successfully and disposes <paramref name="owner"/>.</summary>
    /// <param name="value">The result value.</param>
    /// <param name="owner">The owner to dispose after publishing the result. Disposed via the reentrant path
    /// because this runs from within the owner's own in-flight notification.</param>
    /// <returns>A task that completes when the owner has been disposed.</returns>
    public ValueTask SetResultAndDisposeAsync(T value, IWitnessState owner)
    {
        _ = _taskSource.TrySetResult(value);
        return WitnessAsync.DisposeFromNotificationAsync(owner);
    }

    /// <summary>Completes the result with an exception and disposes <paramref name="owner"/>.</summary>
    /// <param name="exception">The exception that faults the result.</param>
    /// <param name="owner">The owner to dispose after publishing the exception. Disposed via the reentrant path
    /// because this runs from within the owner's own in-flight notification.</param>
    /// <returns>A task that completes when the owner has been disposed.</returns>
    public ValueTask SetExceptionAndDisposeAsync(Exception exception, IWitnessState owner)
    {
        _ = _taskSource.TrySetException(exception);
        return WitnessAsync.DisposeFromNotificationAsync(owner);
    }

    /// <summary>Publishes <paramref name="result"/> as the terminal result and disposes <paramref name="owner"/>.</summary>
    /// <param name="result">The terminal result; a success publishes <paramref name="value"/>, a failure publishes its exception.</param>
    /// <param name="value">The result value published when <paramref name="result"/> is a success.</param>
    /// <param name="owner">The owner to dispose after publishing.</param>
    /// <returns>A task that completes when the owner has been disposed.</returns>
    public ValueTask CompleteAndDisposeAsync(Result result, T value, IWitnessState owner) =>
        result.IsSuccess
            ? SetResultAndDisposeAsync(value, owner)
            : SetExceptionAndDisposeAsync(result.Exception, owner);

    /// <summary>Registers cancellation for the pending result task.</summary>
    /// <returns>The cancellation registration.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private CancellationTokenRegistration RegisterCancellation() =>
        _cancellationToken.UnsafeRegister(
            static state =>
            {
                var source = (TaskResultCompletionSource<T>)state!;
                _ = source._taskSource.TrySetException(new OperationCanceledException(source._cancellationToken));
            },
            this);
}
