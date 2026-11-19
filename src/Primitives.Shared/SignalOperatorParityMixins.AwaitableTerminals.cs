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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> FirstAsync() => TaskTerminalCompletion<T>.FirstAsync(source);

        /// <summary>Awaits the first source value.</summary>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the first source value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> FirstAsync(CancellationToken cancellationToken) =>
            TaskTerminalCompletion<T>.FirstAsync(source, cancellationToken);

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <returns>A task that completes with the first source value, or <see langword="default"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> FirstOrDefaultAsync() => TaskTerminalCompletion<T>.FirstOrDefaultAsync(source);

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <param name="defaultValue">The value to return when the source is empty.</param>
        /// <returns>A task that completes with the first source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> FirstOrDefaultAsync(T defaultValue) =>
            TaskTerminalCompletion<T>.FirstOrDefaultAsync(source, defaultValue);

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the first source value, or <see langword="default"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <remarks>A call to FirstOrDefaultAsync(default!) selects the default-value overload.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public Task<T> FirstOrDefaultAsync(CancellationToken cancellationToken) =>
            TaskTerminalCompletion<T>.FirstOrDefaultAsync(source, cancellationToken);

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <param name="defaultValue">The value to return when the source is empty.</param>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the first source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> FirstOrDefaultAsync(T defaultValue, CancellationToken cancellationToken) =>
            TaskTerminalCompletion<T>.FirstOrDefaultAsync(source, defaultValue, cancellationToken);

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> ToTask(CancellationToken cancellationToken) =>
            TaskTerminalCompletion<T>.ToTask(source, cancellationToken);

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
            TaskTerminalCompletion<T>.ToTask(source, cancellationToken);

        /// <summary>Awaits source completion and returns the last value produced by the source, or <see langword="default"/> when the source is empty.</summary>
        /// <returns>A task that completes with the final source value, or <see langword="default"/> when the source is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> LastOrDefaultAsync() => TaskTerminalCompletion<T>.LastOrDefaultAsync(source);

        /// <summary>Awaits source completion and returns the last value produced by the source, or <paramref name="defaultValue"/> when the source is empty.</summary>
        /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
        /// <returns>A task that completes with the final source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> LastOrDefaultAsync(T defaultValue) =>
            TaskTerminalCompletion<T>.LastOrDefaultAsync(source, defaultValue);

        /// <summary>Awaits source completion and returns the last value produced by the source, or <see langword="default"/> when the source is empty.</summary>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the final source value, or <see langword="default"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <remarks>A call to LastOrDefaultAsync(default!) selects the default-value overload.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public Task<T> LastOrDefaultAsync(CancellationToken cancellationToken) =>
            TaskTerminalCompletion<T>.LastOrDefaultAsync(source, cancellationToken);

        /// <summary>Awaits source completion and returns the last value produced by the source, or <paramref name="defaultValue"/> when the source is empty.</summary>
        /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the final source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T> LastOrDefaultAsync(T defaultValue, CancellationToken cancellationToken) =>
            TaskTerminalCompletion<T>.LastOrDefaultAsync(source, defaultValue, cancellationToken);

        /// <summary>Awaits the source count as a task.</summary>
        /// <returns>A task that completes with the number of source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<int> CountAsync() => TaskCountWitness<T>.CountAsync(source);

        /// <summary>Awaits the source count as a task.</summary>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with the number of source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<int> CountAsync(CancellationToken cancellationToken) =>
            TaskCountWitness<T>.CountAsync(source, cancellationToken);

        /// <summary>Awaits the source predicate count as a task.</summary>
        /// <param name="predicate">The function that identifies values to count.</param>
        /// <returns>A task that completes with the matching value count.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<int> CountAsync(Func<T, bool> predicate) =>
            TaskCountWitness<T>.CountAsync(source, predicate);

        /// <summary>Awaits the source predicate count as a task.</summary>
        /// <param name="predicate">The function that identifies values to count.</param>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with the matching value count.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<int> CountAsync(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            TaskCountWitness<T>.CountAsync(source, predicate, cancellationToken);

        /// <summary>Awaits whether any value is present.</summary>
        /// <returns>A task that completes with whether the source produced any values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<bool> AnyAsync() => TaskAnyWitness<T>.AnyAsync(source);

        /// <summary>Awaits whether any value is present.</summary>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with whether the source produced any values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<bool> AnyAsync(CancellationToken cancellationToken) =>
            TaskAnyWitness<T>.AnyAsync(source, cancellationToken);

        /// <summary>Awaits whether any value matches a predicate.</summary>
        /// <param name="predicate">The function that tests each value.</param>
        /// <returns>A task that completes with whether any source value satisfies <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<bool> AnyAsync(Func<T, bool> predicate) =>
            TaskAnyWitness<T>.AnyAsync(source, predicate);

        /// <summary>Awaits whether any value matches a predicate.</summary>
        /// <param name="predicate">The function that tests each value.</param>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with whether any source value satisfies <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<bool> AnyAsync(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            TaskAnyWitness<T>.AnyAsync(source, predicate, cancellationToken);

        /// <summary>Collects all values into an array task.</summary>
        /// <returns>A task that completes with all source values in an array.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T[]> CollectArrayAsync() => CollectArraySignal<T>.CollectAsync(source);

        /// <summary>Collects all values into an array.</summary>
        /// <returns>A sequence that emits a single array containing all source values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T[]> ToArray() => CollectArraySignal<T>.Create(source);

        /// <summary>Collects all values into an array task.</summary>
        /// <returns>A task that completes with all source values in an array.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<T[]> ToArrayAsync() => CollectArraySignal<T>.CollectAsync(source);

        /// <summary>Collects all values into a list task.</summary>
        /// <returns>A task that completes with all source values in a list.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<IList<T>> CollectListAsync() => CollectListSignal<T>.CollectAsync(source);

        /// <summary>Collects all values into a list.</summary>
        /// <returns>A sequence that emits a single list containing all source values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IList<T>> ToList() => CollectListSignal<T>.Create(source);

        /// <summary>Collects all values into a list task.</summary>
        /// <returns>A task that completes with all source values in a list.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<IList<T>> ToListAsync() => CollectListSignal<T>.CollectAsync(source);
    }
}
