// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Task-producing terminal sink that completes with the number of matching source values.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TaskCountWitness: Count = {_count}, Stopped = {_stopped}")]
public sealed class TaskCountWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The task completed by this sink.</summary>
    private readonly TaskCompletionSource<int> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The cancellation token used by this sink.</summary>
    private readonly CancellationToken _cancellationToken;

    /// <summary>The optional predicate.</summary>
    private readonly Func<T, bool>? _predicate;

    /// <summary>The cancellation registration.</summary>
    private CancellationTokenRegistration _registration;

    /// <summary>The source subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>The observed count.</summary>
    private int _count;

    /// <summary>Non-zero after disposal or terminal result.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="TaskCountWitness{T}"/> class.</summary>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    public TaskCountWitness(CancellationToken cancellationToken) => _cancellationToken = cancellationToken;

    /// <summary>Initializes a new instance of the <see cref="TaskCountWitness{T}"/> class.</summary>
    /// <param name="predicate">The predicate.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    public TaskCountWitness(Func<T, bool> predicate, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        _cancellationToken = cancellationToken;
        _predicate = predicate;
    }

    /// <summary>Gets the task completed by this sink.</summary>
    public Task<int> Task => _completion.Task;

    /// <summary>Registers cancellation after construction.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RegisterCancellation() =>
        TaskTerminalWitnessHelper.RegisterCancellation(
            this,
            static state => ((TaskCountWitness<T>)state!).Cancel(),
            ref _registration,
            _cancellationToken);

    /// <summary>Assigns the source subscription.</summary>
    /// <param name="subscription">The source subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) =>
        TaskTerminalWitnessHelper.SetSubscription(ref _subscription, ref _stopped, subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        if (!TaskTerminalWitnessHelper.TryStop(ref _stopped))
        {
            return;
        }

        _registration.Dispose();
        TaskTerminalWitnessHelper.DisposeSubscription(ref _subscription);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Complete(_count);

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        if (!TaskTerminalWitnessHelper.TryStop(ref _stopped))
        {
            return;
        }

        _registration.Dispose();
        _ = _completion.TrySetException(error);
        TaskTerminalWitnessHelper.DisposeSubscription(ref _subscription);
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (TaskTerminalWitnessHelper.IsStopped(ref _stopped))
        {
            return;
        }

        try
        {
            if (_predicate is null || _predicate(value))
            {
                _count = checked(_count + 1);
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
        if (!TaskTerminalWitnessHelper.TryStop(ref _stopped))
        {
            return;
        }

        TaskTerminalWitnessHelper.DisposeSubscription(ref _subscription);
        _ = _completion.TrySetCanceled(_cancellationToken);
    }

    /// <summary>Completes the sink with a result.</summary>
    /// <param name="result">The terminal result.</param>
    private void Complete(int result)
    {
        if (!TaskTerminalWitnessHelper.TryStop(ref _stopped))
        {
            return;
        }

        _registration.Dispose();
        _ = _completion.TrySetResult(result);
        TaskTerminalWitnessHelper.DisposeSubscription(ref _subscription);
    }
}
