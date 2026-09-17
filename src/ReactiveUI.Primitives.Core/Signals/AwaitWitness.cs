// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Observer that discards values and runs a continuation on the first terminal notification.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AwaitWitness: Callback = {_callback}, Context = {_context}")]
public sealed class AwaitWitness<T> : IObserver<T>
{
    /// <summary>The context captured at construction, or <see langword="null"/> to run the continuation inline.</summary>
    private readonly SynchronizationContext? _context;

    /// <summary>The continuation run on completion or failure.</summary>
    private readonly Action _callback;

    /// <summary>Initializes a new instance of the <see cref="AwaitWitness{T}"/> class.</summary>
    /// <param name="callback">The continuation to run on the terminal notification.</param>
    /// <param name="originalContext">Whether to capture the current synchronization context and post the continuation to it.</param>
    public AwaitWitness(Action callback, bool originalContext)
    {
        if (originalContext)
        {
            _context = SynchronizationContext.Current;
        }

        _callback = callback;
    }

    /// <summary>Resumes the awaiting continuation on completion.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => InvokeOnOriginalContext();

    /// <summary>Resumes the awaiting continuation when the source fails.</summary>
    /// <param name="error">The terminal error, which the continuation does not receive.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "Both terminal notifications resume the same continuation, and neither may delegate to the other.")]
    public void OnError(Exception error) => InvokeOnOriginalContext();

    /// <summary>Ignores values; only terminal notifications resume the continuation.</summary>
    /// <param name="value">The ignored value.</param>
    public void OnNext(T value)
    {
    }

    /// <summary>Posts a continuation to the captured context.</summary>
    /// <param name="context">The context receiving the callback.</param>
    /// <param name="callback">The callback to post.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Post(SynchronizationContext context, Action callback) =>
        context.Post(static state => ((Action?)state)?.Invoke(), callback);

    /// <summary>Posts the continuation to its captured context, or invokes it directly when none was captured.</summary>
    private void InvokeOnOriginalContext()
    {
        if (_context is not null)
        {
            Post(_context, _callback);
        }
        else
        {
            _callback();
        }
    }
}
