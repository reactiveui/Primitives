// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Observer used by the value-returning <c>WaitFor</c> path: captures the last value,
/// signals the gate on terminal, swallows errors (caller's timeout / default reflects outcome).
/// </summary>
/// <typeparam name="T">The element type of the source.</typeparam>
/// <param name="done">The gate signalled on terminal.</param>
[System.Diagnostics.DebuggerDisplay("BlockingValueWitness: Result = {Result}")]
public sealed class BlockingValueWitness<T>(ManualResetEventSlim done) : IObserver<T>
{
    /// <summary>Gets the most recent value seen.</summary>
    public T? Result { get; private set; }

    /// <summary>Blocks until <paramref name="source"/> completes and returns the observed values.</summary>
    /// <param name="source">The source sequence.</param>
    /// <returns>The values observed before completion.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IEnumerable<T> ToEnumerable(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        List<T> values = [];
        Exception? error = null;
        using ManualResetEventSlim completed = new();
        using var subscription = source.Subscribe(
            values.Add,
            ex =>
            {
                error = ex;
                completed.Set();
            },
            completed.Set);

        WaitForCompletion(completed);

        return error is null ? values : Rethrow(error, values);
    }

    /// <inheritdoc/>
    public void OnNext(T value) => Result = value;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => done.Set();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "Completion and error are distinct IObserver<T> channels that deliberately share one action: "
            + "releasing the gate.")]
    public void OnCompleted() => done.Set();

    /// <summary>Rethrows <paramref name="error"/> with its original stack, typed by <paramref name="fallback"/> so a value-returning caller can return the call.</summary>
    /// <typeparam name="TResult">The caller's return type.</typeparam>
    /// <param name="error">The failure to rethrow.</param>
    /// <param name="fallback">The value the caller would otherwise return; never returned.</param>
    /// <returns>Never returns; the return after the throw is unreachable.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TResult Rethrow<TResult>(Exception error, TResult fallback)
    {
        ExceptionDispatchInfo.Capture(error).Throw();
        return fallback;
    }

    /// <summary>Blocks until the source signals completion.</summary>
    /// <param name="completed">The source's completion signal.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WaitForCompletion(ManualResetEventSlim completed) => completed.Wait();
}
