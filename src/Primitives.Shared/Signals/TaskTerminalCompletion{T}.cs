// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Owns terminal task completion, the source subscription, and cancellation registration while pending.</summary>
/// <typeparam name="T">The task result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TaskTerminalCompletion: Task = {Task}")]
public sealed class TaskTerminalCompletion<T>
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
    public Task<T> Task => _completion.Task;

    /// <summary>Awaits the first value emitted by <paramref name="source"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A task that completes with the first source value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> FirstAsync(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return FirstCoreAsync(source, false, default!, CancellationToken.None);
    }

    /// <summary>Awaits the first value emitted by <paramref name="source"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the first source value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> FirstAsync(IObservable<T> source, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return FirstCoreAsync(source, false, default!, cancellationToken);
    }

    /// <summary>Awaits the first value emitted by <paramref name="source"/>, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A task that completes with the first source value or <see langword="default"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> FirstOrDefaultAsync(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return FirstCoreAsync(source, true, default!, CancellationToken.None);
    }

    /// <summary>Awaits the first value emitted by <paramref name="source"/>, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">The value to return when the source is empty.</param>
    /// <returns>A task that completes with the first source value or <paramref name="defaultValue"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> FirstOrDefaultAsync(IObservable<T> source, T defaultValue)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return FirstCoreAsync(source, true, defaultValue, CancellationToken.None);
    }

    /// <summary>Awaits the first value emitted by <paramref name="source"/>, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the first source value or <see langword="default"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> FirstOrDefaultAsync(IObservable<T> source, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return FirstCoreAsync(source, true, default!, cancellationToken);
    }

    /// <summary>Awaits the first value emitted by <paramref name="source"/>, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">The value to return when the source is empty.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the first source value or <paramref name="defaultValue"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> FirstOrDefaultAsync(
        IObservable<T> source,
        T defaultValue,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return FirstCoreAsync(source, true, defaultValue, cancellationToken);
    }

    /// <summary>Awaits completion of <paramref name="source"/> and returns its final value.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the final source value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> ToTask(IObservable<T> source, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return Signal.ToTask(source, cancellationToken);
    }

    /// <summary>Awaits completion of <paramref name="source"/> and returns its final value, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A task that completes with the final source value or <see langword="default"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<T> LastOrDefaultAsync(IObservable<T> source) =>
        LastOrDefaultAsync(source, default!, CancellationToken.None);

    /// <summary>Awaits completion of <paramref name="source"/> and returns its final value, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">The fallback value when the source is empty.</param>
    /// <returns>A task that completes with the final source value or <paramref name="defaultValue"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<T> LastOrDefaultAsync(IObservable<T> source, T defaultValue) =>
        LastOrDefaultAsync(source, defaultValue, CancellationToken.None);

    /// <summary>Awaits completion of <paramref name="source"/> and returns its final value, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the final source value or <see langword="default"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<T> LastOrDefaultAsync(IObservable<T> source, CancellationToken cancellationToken) =>
        LastOrDefaultAsync(source, default!, cancellationToken);

    /// <summary>Awaits completion of <paramref name="source"/> and returns its final value, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">The fallback value when the source is empty.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the final source value or <paramref name="defaultValue"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> LastOrDefaultAsync(
        IObservable<T> source,
        T defaultValue,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return Signal.ToTaskOrDefault(source, defaultValue, cancellationToken);
    }

    /// <summary>Completes the task with a value and releases the subscription and registration.</summary>
    /// <param name="value">The result value.</param>
    public void Resolve(T value)
    {
        Release();
        _ = _completion.TrySetResult(value);
    }

    /// <summary>Faults the task and releases the subscription and registration.</summary>
    /// <param name="error">The error.</param>
    public void Fail(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        Release();
        _ = _completion.TrySetException(error);
    }

    /// <summary>Faults the task with the shared empty-source error and releases the subscription and registration.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void FailEmpty() =>
        Fail(new InvalidOperationException("The source completed without producing a value."));

    /// <summary>Adopts the source subscription and registers for cancellation only while the task is pending.</summary>
    /// <param name="subscription">The source subscription.</param>
    /// <param name="cancellationToken">The token that cancels the task and disposes the subscription.</param>
    /// <returns>The terminal task.</returns>
    public Task<T> Attach(IDisposable subscription, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscription);

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

            if (_completion.Task.IsCompleted)
            {
                _ = _cancellationRegistration.Unregister();
            }
        }

        return _completion.Task;
    }

    /// <summary>Awaits the first source value, applying the configured empty-source behavior and optional cancellation.</summary>
    /// <param name="source">The validated source observable.</param>
    /// <param name="hasDefault">Whether to use <paramref name="defaultValue"/> when the source is empty.</param>
    /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task representing the awaited first value.</returns>
    private static Task<T> FirstCoreAsync(
        IObservable<T> source,
        bool hasDefault,
        T defaultValue,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return System.Threading.Tasks.Task.FromCanceled<T>(cancellationToken);
        }

        if (source is RangeSignal range && typeof(T) == typeof(int))
        {
            return System.Threading.Tasks.Task.FromResult((T)(object)range.Start);
        }

        TaskTerminalCompletion<T> completion = new();
        var seen = false;
        var subscription = source.Subscribe(
            value =>
            {
                if (seen)
                {
                    return;
                }

                seen = true;
                completion.Resolve(value);
            },
            completion.Fail,
            () =>
            {
                if (seen)
                {
                    return;
                }

                if (hasDefault)
                {
                    completion.Resolve(defaultValue);
                }
                else
                {
                    completion.FailEmpty();
                }
            });

        return completion.Attach(subscription, cancellationToken);
    }

    /// <summary>Cancels the task and disposes the subscription; runs on the canceling thread.</summary>
    private void Cancel()
    {
        _subscription!.Dispose();
        _ = _completion.TrySetCanceled(_cancellationToken);
    }

    /// <summary>Releases the subscription and cancellation registration.</summary>
    private void Release()
    {
        _cancellationRegistration.Dispose();
        _subscription?.Dispose();
    }
}
