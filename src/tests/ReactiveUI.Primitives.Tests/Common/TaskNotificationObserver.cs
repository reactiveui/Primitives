// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Records notifications and signals terminal delivery.</summary>
internal sealed class TaskNotificationObserver : IObserver<int>
{
    /// <summary>Gets the received values.</summary>
    public List<int> Values { get; } = [];

    /// <summary>Gets the received failure.</summary>
    public Exception? Error { get; private set; }

    /// <summary>Gets the number of completion notifications.</summary>
    public int Completions { get; private set; }

    /// <summary>Gets the terminal delivery signal.</summary>
    public TaskCompletionSource Terminal { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(int value) => Values.Add(value);

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        Error = error;
        _ = Terminal.TrySetResult();
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        Completions++;
        _ = Terminal.TrySetResult();
    }
}
