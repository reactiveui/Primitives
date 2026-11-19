// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Task-producing terminal sink that completes with whether a source produced a matching value.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TaskAnyWitness: Stopped = {_stopped}, Task = {Task}")]
public sealed class TaskAnyWitness<T> : IObserver<T>, IDisposable
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

    /// <summary>Initializes a new instance of the <see cref="TaskAnyWitness{T}"/> class.</summary>
    /// <param name="cancellationToken">The token that cancels the task and disposes the subscription.</param>
    public TaskAnyWitness(CancellationToken cancellationToken) => _cancellationToken = cancellationToken;

    /// <summary>Initializes a new instance of the <see cref="TaskAnyWitness{T}"/> class.</summary>
    /// <param name="predicate">The predicate.</param>
    /// <param name="cancellationToken">The token that cancels the task and disposes the subscription.</param>
    public TaskAnyWitness(Func<T, bool> predicate, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        _cancellationToken = cancellationToken;
        _predicate = predicate;
    }

    /// <summary>Gets the task completed by this sink.</summary>
    public Task<bool> Task => _completion.Task;

    /// <summary>Subscribes to <paramref name="source"/> and completes with whether any value was observed.</summary>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with whether any value was observed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<bool> AnyAsync(IObservable<T> source) =>
        AnyAsync(source, CancellationToken.None);

    /// <summary>Subscribes to <paramref name="source"/> and completes with whether any value was observed.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with whether any value was observed.</returns>
    public static Task<bool> AnyAsync(IObservable<T> source, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (cancellationToken.IsCancellationRequested)
        {
            return System.Threading.Tasks.Task.FromCanceled<bool>(cancellationToken);
        }

        if (source is RangeSignal range)
        {
            return System.Threading.Tasks.Task.FromResult(range.Count > 0);
        }

        TaskAnyWitness<T> sink = new(cancellationToken);
        sink.RegisterCancellation();
        sink.SetSubscription(source.Subscribe(sink));
        return sink.Task;
    }

    /// <summary>Subscribes to <paramref name="source"/> and completes with whether any value matched <paramref name="predicate"/>.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The predicate.</param>
    /// <returns>A task that completes with whether any value matched the predicate.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<bool> AnyAsync(IObservable<T> source, Func<T, bool> predicate) =>
        AnyAsync(source, predicate, CancellationToken.None);

    /// <summary>Subscribes to <paramref name="source"/> and completes with whether any value matched <paramref name="predicate"/>.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The predicate.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with whether any value matched the predicate.</returns>
    public static Task<bool> AnyAsync(
        IObservable<T> source,
        Func<T, bool> predicate,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        if (cancellationToken.IsCancellationRequested)
        {
            return System.Threading.Tasks.Task.FromCanceled<bool>(cancellationToken);
        }

        if (source is RangeSignal range && typeof(T) == typeof(int))
        {
            return AnyRangeTask(range, predicate);
        }

        TaskAnyWitness<T> sink = new(predicate, cancellationToken);
        sink.RegisterCancellation();
        sink.SetSubscription(source.Subscribe(sink));
        return sink.Task;
    }

    /// <summary>Registers cancellation after construction.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RegisterCancellation() =>
        TaskTerminalWitnessHelper.RegisterCancellation(
            this,
            static state => ((TaskAnyWitness<T>)state!).Cancel(),
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
    public void OnCompleted() => Complete(false);

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
                Complete(true);
            }
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            OnError(error);
        }
    }

    /// <summary>Evaluates any directly over an integer range.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="predicate">The predicate.</param>
    /// <returns>A completed task for the range result.</returns>
    private static Task<bool> AnyRangeTask(RangeSignal range, Func<T, bool> predicate)
    {
        try
        {
            var typedPredicate = (Func<int, bool>)(object)predicate;
            for (var i = 0; i < range.Count; i++)
            {
                if (typedPredicate(range.Start + i))
                {
                    return System.Threading.Tasks.Task.FromResult(true);
                }
            }

            return System.Threading.Tasks.Task.FromResult(false);
        }
        catch (Exception error)
        {
            return System.Threading.Tasks.Task.FromException<bool>(error);
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
    private void Complete(bool result)
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
