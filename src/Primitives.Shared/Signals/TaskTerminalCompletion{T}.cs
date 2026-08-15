// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>
/// Shared completion plumbing for task-returning terminals with optional cancellation: owns the task source,
/// the source subscription, and the cancellation registration. Callers wire their observer callbacks to
/// <see cref="Resolve"/>/<see cref="Fail"/> and hand the subscription to <see cref="Attach"/>, which registers
/// for cancellation only while the task is still pending — synchronous sources never pay for a registration,
/// and registering after the subscription write publishes it to the thread that runs the cancellation callback.
/// </summary>
/// <typeparam name="T">The task result type.</typeparam>
internal sealed class TaskTerminalCompletion<T>
{
    /// <summary>The task source completed by the observer callbacks or by cancellation.</summary>
    private readonly TaskCompletionSource<T> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The source subscription; released on completion or cancellation.</summary>
    private IDisposable? _subscription;

    /// <summary>The cancellation registration; released on completion.</summary>
    private CancellationTokenRegistration _cancellationRegistration;

    /// <summary>The token observed by the terminal, captured for the canceled-task transition.</summary>
    private CancellationToken _cancellationToken;

    /// <summary>Gets the task completed by the terminal.</summary>
    internal Task<T> Task => _completion.Task;

    /// <summary>Completes the task with a value and releases the subscription and registration.</summary>
    /// <param name="value">The result value.</param>
    internal void Resolve(T value)
    {
        Release();
        _ = _completion.TrySetResult(value);
    }

    /// <summary>Faults the task and releases the subscription and registration.</summary>
    /// <param name="error">The error.</param>
    internal void Fail(Exception error)
    {
        Release();
        _ = _completion.TrySetException(error);
    }

    /// <summary>Faults the task with the shared empty-source error and releases the subscription and registration.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void FailEmpty() =>
        Fail(new InvalidOperationException("The source completed without producing a value."));

    /// <summary>
    /// Adopts the subscription created after the observer callbacks were wired, then registers for cancellation
    /// only when the task is still pending.
    /// </summary>
    /// <param name="subscription">The source subscription.</param>
    /// <param name="cancellationToken">The token that cancels the task and disposes the subscription.</param>
    /// <returns>The terminal task.</returns>
    internal Task<T> Attach(IDisposable subscription, CancellationToken cancellationToken)
    {
        _subscription = subscription;
        if (_completion.Task.IsCompleted)
        {
            subscription.Dispose();
            return _completion.Task;
        }

        if (cancellationToken.CanBeCanceled)
        {
            _cancellationToken = cancellationToken;
            _cancellationRegistration = cancellationToken.UnsafeRegister(
                static state => ((TaskTerminalCompletion<T>)state!).Cancel(),
                this);

            // The source may have completed while the registration was being created; the observer callbacks
            // saw a default registration then, so release the real one here (without waiting on an in-flight
            // callback, whose effects are already race-safe).
            if (_completion.Task.IsCompleted)
            {
                _ = _cancellationRegistration.Unregister();
            }
        }

        return _completion.Task;
    }

    /// <summary>Cancels the task and disposes the subscription; runs on the canceling thread.</summary>
    private void Cancel()
    {
        _subscription?.Dispose();
        _ = _completion.TrySetCanceled(_cancellationToken);
    }

    /// <summary>Releases the subscription and cancellation registration.</summary>
    private void Release()
    {
        _cancellationRegistration.Dispose();
        _subscription?.Dispose();
    }
}
