// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Task-producing terminal sinks for parity operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Subscribes to the source and completes with whether any value was observed.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with whether any value was observed.</returns>
    internal static Task<bool> AnyTaskAsync<T>(IObservable<T> source, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<bool>(cancellationToken);
        }

        if (source is RangeSignal range)
        {
            return Task.FromResult(range.Count > 0);
        }

        TaskAnyWitness<T> sink = new(cancellationToken);
        sink.RegisterCancellation();
        sink.SetSubscription(source.Subscribe(sink));
        return sink.Task;
    }

    /// <summary>Subscribes to the source and completes with whether any value matched the predicate.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The predicate.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with whether any value matched the predicate.</returns>
    internal static Task<bool> AnyTaskAsync<T>(
        IObservable<T> source,
        Func<T, bool> predicate,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(predicate);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<bool>(cancellationToken);
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

    /// <summary>Subscribes to the source and completes with the observed value count.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the value count.</returns>
    internal static Task<int> CountTaskAsync<T>(IObservable<T> source, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<int>(cancellationToken);
        }

        if (source is RangeSignal range)
        {
            return Task.FromResult(range.Count);
        }

        TaskCountWitness<T> sink = new(cancellationToken);
        sink.RegisterCancellation();
        sink.SetSubscription(source.Subscribe(sink));
        return sink.Task;
    }

    /// <summary>Subscribes to the source and completes with the matching value count.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The predicate.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the matching value count.</returns>
    internal static Task<int> CountTaskAsync<T>(
        IObservable<T> source,
        Func<T, bool> predicate,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(predicate);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<int>(cancellationToken);
        }

        if (source is RangeSignal range && typeof(T) == typeof(int))
        {
            return CountRangeTask(range, predicate);
        }

        TaskCountWitness<T> sink = new(predicate, cancellationToken);
        sink.RegisterCancellation();
        sink.SetSubscription(source.Subscribe(sink));
        return sink.Task;
    }

    /// <summary>Evaluates any directly over an integer range.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="range">The range source.</param>
    /// <param name="predicate">The predicate.</param>
    /// <returns>A completed task for the range result.</returns>
    private static Task<bool> AnyRangeTask<T>(RangeSignal range, Func<T, bool> predicate)
    {
        try
        {
            var typedPredicate = (Func<int, bool>)(object)predicate;
            for (var i = 0; i < range.Count; i++)
            {
                if (typedPredicate(range.Start + i))
                {
                    return Task.FromResult(true);
                }
            }

            return Task.FromResult(false);
        }
        catch (Exception error)
        {
            return Task.FromException<bool>(error);
        }
    }

    /// <summary>Counts matching values directly over an integer range.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="range">The range source.</param>
    /// <param name="predicate">The predicate.</param>
    /// <returns>A completed task for the range result.</returns>
    private static Task<int> CountRangeTask<T>(RangeSignal range, Func<T, bool> predicate)
    {
        try
        {
            var typedPredicate = (Func<int, bool>)(object)predicate;
            var count = 0;
            for (var i = 0; i < range.Count; i++)
            {
                if (typedPredicate(range.Start + i))
                {
                    count = checked(count + 1);
                }
            }

            return Task.FromResult(count);
        }
        catch (Exception error)
        {
            return Task.FromException<int>(error);
        }
    }
}
