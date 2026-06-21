// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Task-producing terminal sink that completes with whether a source produced a matching value.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class TaskAnySink<T> : IObserver<T>, IDisposable
{
    /// <summary>The task completed by this sink.</summary>
    private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The cancellation token used by this sink.</summary>
    private readonly CancellationToken _cancellationToken;

    /// <summary>The optional predicate.</summary>
    private readonly Func<T, bool>? _predicate;

    /// <summary>The cancellation registration.</summary>
    private CancellationTokenRegistration _registration;

    /// <summary>The source subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Non-zero after disposal or terminal result.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="TaskAnySink{T}"/> class.</summary>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    public TaskAnySink(CancellationToken cancellationToken) => _cancellationToken = cancellationToken;

    /// <summary>Initializes a new instance of the <see cref="TaskAnySink{T}"/> class.</summary>
    /// <param name="predicate">The predicate.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    public TaskAnySink(Func<T, bool> predicate, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        _cancellationToken = cancellationToken;
        _predicate = predicate;
    }

    /// <summary>Gets the task completed by this sink.</summary>
    public Task<bool> Task => _completion.Task;

    /// <summary>Registers cancellation after construction.</summary>
    public void RegisterCancellation() =>
        TaskTerminalSinkHelper.RegisterCancellation(this, static state => ((TaskAnySink<T>)state!).Cancel(), ref _registration, _cancellationToken);

    /// <summary>Assigns the source subscription.</summary>
    /// <param name="subscription">The source subscription.</param>
    public void SetSubscription(IDisposable subscription) =>
        TaskTerminalSinkHelper.SetSubscription(ref _subscription, ref _stopped, subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        if (!TaskTerminalSinkHelper.TryStop(ref _stopped))
        {
            return;
        }

        _registration.Dispose();
        TaskTerminalSinkHelper.DisposeSubscription(ref _subscription);
    }

    /// <inheritdoc/>
    public void OnCompleted() => Complete(false);

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        if (!TaskTerminalSinkHelper.TryStop(ref _stopped))
        {
            return;
        }

        _registration.Dispose();
        _completion.TrySetException(error);
        TaskTerminalSinkHelper.DisposeSubscription(ref _subscription);
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (TaskTerminalSinkHelper.IsStopped(ref _stopped))
        {
            return;
        }

        try
        {
            if (_predicate is null || _predicate(value))
            {
                Complete(result: true);
            }
        }
        catch (Exception error)
        {
            OnError(error);
        }
    }

    /// <summary>Completes the sink with cancellation.</summary>
    private void Cancel()
    {
        if (!TaskTerminalSinkHelper.TryStop(ref _stopped))
        {
            return;
        }

        TaskTerminalSinkHelper.DisposeSubscription(ref _subscription);
        _completion.TrySetCanceled(_cancellationToken);
    }

    /// <summary>Completes the sink with a result.</summary>
    /// <param name="result">The terminal result.</param>
    private void Complete(bool result)
    {
        if (!TaskTerminalSinkHelper.TryStop(ref _stopped))
        {
            return;
        }

        _registration.Dispose();
        _completion.TrySetResult(result);
        TaskTerminalSinkHelper.DisposeSubscription(ref _subscription);
    }
}
