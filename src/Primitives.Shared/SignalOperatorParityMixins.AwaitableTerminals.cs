// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Terminal operators that await a source and hand back its result as a task or a single-value signal.</summary>
public static partial class LinqExtensions
{
    /// <summary>Terminal operators that reduce a source to one awaited result.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Awaits the first source value.</summary>
        /// <returns>A task that completes with the first source value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
        public Task<T> FirstAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.FirstCoreAsync(false, default!, CancellationToken.None);
        }

        /// <summary>Awaits the first source value.</summary>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the first source value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
        public Task<T> FirstAsync(CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.FirstCoreAsync(false, default!, cancellationToken);
        }

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <returns>A task that completes with the first source value, or <see langword="default"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<T> FirstOrDefaultAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.FirstCoreAsync(true, default!, CancellationToken.None);
        }

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <param name="defaultValue">The value to return when the source is empty.</param>
        /// <returns>A task that completes with the first source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<T> FirstOrDefaultAsync(T defaultValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.FirstCoreAsync(true, defaultValue, CancellationToken.None);
        }

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the first source value, or <see langword="default"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <remarks>Deprioritized so calls like <c>FirstOrDefaultAsync(default!)</c> keep binding to the
        /// <c>FirstOrDefaultAsync(T)</c> overload they compiled against before this overload existed.</remarks>
        [OverloadResolutionPriority(-1)]
        public Task<T> FirstOrDefaultAsync(CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.FirstCoreAsync(true, default!, cancellationToken);
        }

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <param name="defaultValue">The value to return when the source is empty.</param>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the first source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<T> FirstOrDefaultAsync(T defaultValue, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.FirstCoreAsync(true, defaultValue, cancellationToken);
        }

        /// <summary>Awaits source completion and returns the last value produced by the source.</summary>
        /// <returns>A task that completes with the final source value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> ToTask() => Signal.ToTask(source);

        /// <summary>Awaits source completion and returns the last value produced by the source.</summary>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the final source value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
        public Task<T> ToTask(CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return Signal.ToTask(source, cancellationToken);
        }

        /// <summary>Awaits source completion and returns the last value produced by the source.</summary>
        /// <returns>A task that completes with the final source value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> LastAsync() => Signal.ToTask(source);

        /// <summary>Awaits source completion and returns the last value produced by the source.</summary>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the final source value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> LastAsync(CancellationToken cancellationToken) =>
            source.ToTask(cancellationToken);

        /// <summary>Awaits source completion and returns the last value produced by the source, or <see langword="default"/> when the source is empty.</summary>
        /// <returns>A task that completes with the final source value, or <see langword="default"/> when the source is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> LastOrDefaultAsync() =>
            source.LastOrDefaultAsync(default!);

        /// <summary>Awaits source completion and returns the last value produced by the source, or <paramref name="defaultValue"/> when the source is empty.</summary>
        /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
        /// <returns>A task that completes with the final source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        public Task<T> LastOrDefaultAsync(T defaultValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.DefaultIfEmpty(defaultValue).ToTask();
        }

        /// <summary>Awaits source completion and returns the last value produced by the source, or <see langword="default"/> when the source is empty.</summary>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the final source value, or <see langword="default"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <remarks>Deprioritized so calls like <c>LastOrDefaultAsync(default!)</c> keep binding to the
        /// <c>LastOrDefaultAsync(T)</c> overload they compiled against before this overload existed.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public Task<T> LastOrDefaultAsync(CancellationToken cancellationToken) =>
            source.LastOrDefaultAsync(default!, cancellationToken);

        /// <summary>Awaits source completion and returns the last value produced by the source, or <paramref name="defaultValue"/> when the source is empty.</summary>
        /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the final source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<T> LastOrDefaultAsync(T defaultValue, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.DefaultIfEmpty(defaultValue).ToTask(cancellationToken);
        }

        /// <summary>Awaits the source count as a task.</summary>
        /// <returns>A task that completes with the number of source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<int> CountAsync() => CountTaskAsync(source, CancellationToken.None);

        /// <summary>Awaits the source count as a task.</summary>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with the number of source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<int> CountAsync(CancellationToken cancellationToken) => CountTaskAsync(source, cancellationToken);

        /// <summary>Awaits the source predicate count as a task.</summary>
        /// <param name="predicate">The function that identifies values to count.</param>
        /// <returns>A task that completes with the matching value count.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<int> CountAsync(Func<T, bool> predicate) =>
            CountTaskAsync(source, predicate, CancellationToken.None);

        /// <summary>Awaits the source predicate count as a task.</summary>
        /// <param name="predicate">The function that identifies values to count.</param>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with the matching value count.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<int> CountAsync(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            CountTaskAsync(source, predicate, cancellationToken);

        /// <summary>Awaits whether any value is present.</summary>
        /// <returns>A task that completes with whether the source produced any values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<bool> AnyAsync() => AnyTaskAsync(source, CancellationToken.None);

        /// <summary>Awaits whether any value is present.</summary>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with whether the source produced any values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<bool> AnyAsync(CancellationToken cancellationToken) => AnyTaskAsync(source, cancellationToken);

        /// <summary>Awaits whether any value matches a predicate.</summary>
        /// <param name="predicate">The function that tests each value.</param>
        /// <returns>A task that completes with whether any source value satisfies <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<bool> AnyAsync(Func<T, bool> predicate) => AnyTaskAsync(source, predicate, CancellationToken.None);

        /// <summary>Awaits whether any value matches a predicate.</summary>
        /// <param name="predicate">The function that tests each value.</param>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with whether any source value satisfies <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<bool> AnyAsync(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            AnyTaskAsync(source, predicate, cancellationToken);

        /// <summary>Collects all values into an array task.</summary>
        /// <returns>A task that completes with all source values in an array.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<T[]> CollectArrayAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (source is RangeSignal range && typeof(T) == typeof(int))
            {
                var integers = new int[range.Count];
                for (var i = 0; i < integers.Length; i++)
                {
                    integers[i] = range.Start + i;
                }

                return Task.FromResult((T[])(object)integers);
            }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            if (TryCollectArrayFromAsyncEnumerable(source, out var asyncEnumerableTask))
            {
                return asyncEnumerableTask;
            }
#endif

            TaskCompletionSource<T[]> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            List<T> values = [];
            _ = source.Subscribe(
                values.Add,
                error => completion.TrySetException(error),
                () => completion.TrySetResult([.. values]));
            return completion.Task;
        }

        /// <summary>Collects all values into an array.</summary>
        /// <returns>A sequence that emits a single array containing all source values.</returns>
        public IObservable<T[]> ToArray()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && typeof(T) == typeof(int)
                ? new RangeArraySignal<T>(range)
                : new CollectArraySignal<T>(source);
        }

        /// <summary>Collects all values into an array task.</summary>
        /// <returns>A task that completes with all source values in an array.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T[]> ToArrayAsync() => source.CollectArrayAsync();

        /// <summary>Collects all values into a list task.</summary>
        /// <returns>A task that completes with all source values in a list.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<IList<T>> CollectListAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (source is RangeSignal range && typeof(T) == typeof(int))
            {
                List<int> integers = new(range.Count);
                for (var i = 0; i < range.Count; i++)
                {
                    integers.Add(range.Start + i);
                }

                return Task.FromResult((IList<T>)(object)integers);
            }

            TaskCompletionSource<IList<T>> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            List<T> values = [];
            _ = source.Subscribe(
                values.Add,
                error => completion.TrySetException(error),
                () => completion.TrySetResult(values));
            return completion.Task;
        }

        /// <summary>Collects all values into a list.</summary>
        /// <returns>A sequence that emits a single list containing all source values.</returns>
        public IObservable<IList<T>> ToList()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && typeof(T) == typeof(int)
                ? new RangeListSignal<T>(range)
                : new CollectListSignal<T>(source);
        }

        /// <summary>Collects all values into a list task.</summary>
        /// <returns>A task that completes with all source values in a list.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<IList<T>> ToListAsync() => source.CollectListAsync();

        /// <summary>Awaits the first source value, applying the configured empty-source behavior and optional cancellation.</summary>
        /// <param name="hasDefault">A value indicating whether to use <paramref name="defaultValue"/> when the source is empty.</param>
        /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
        private Task<T> FirstCoreAsync(bool hasDefault, T defaultValue, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled<T>(cancellationToken);
            }

            if (source is RangeSignal range && typeof(T) == typeof(int))
            {
                return Task.FromResult((T)(object)range.Start);
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
    }
}
