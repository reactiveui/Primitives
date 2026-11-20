// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer used by the completion / error <c>WaitFor</c> paths: captures any terminal error and signals the gate on terminal.</summary>
/// <typeparam name="T">The element type of the source.</typeparam>
/// <param name="done">The gate signalled on terminal.</param>
[System.Diagnostics.DebuggerDisplay("BlockingTerminalWitness: Error = {Error}")]
public sealed class BlockingTerminalWitness<T>(ManualResetEventSlim done) : IObserver<T>
{
    /// <summary>Gets the captured terminal error, or <see langword="null"/> if completion was normal.</summary>
    public Exception? Error { get; private set; }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        Error = error;
        done.Set();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => done.Set();
}
