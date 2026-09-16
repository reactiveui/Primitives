// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Tests;

/// <summary>A dedicated thread that runs posted work in order, standing in for a UI dispatcher.</summary>
internal sealed class MarshallingThread : IDisposable
{
    /// <summary>How long disposal waits for the thread to finish its queued work, in seconds.</summary>
    private const int ShutdownSeconds = 5;

    /// <summary>The work waiting to run.</summary>
    private readonly BlockingCollection<Action> _work = [];

    /// <summary>The thread that runs the work.</summary>
    private readonly Thread _thread;

    /// <summary>Initializes a new instance of the <see cref="MarshallingThread"/> class and starts its thread.</summary>
    internal MarshallingThread()
    {
        _thread = new(Run) { IsBackground = true, Name = nameof(MarshallingThread) };
        _thread.Start();
    }

    /// <summary>Gets the managed thread id of the dispatcher thread.</summary>
    internal int ManagedThreadId => _thread.ManagedThreadId;

    /// <inheritdoc/>
    public void Dispose()
    {
        _work.CompleteAdding();
        _ = _thread.Join(TimeSpan.FromSeconds(ShutdownSeconds));
        _work.Dispose();
    }

    /// <summary>Queues work to run on the dispatcher thread.</summary>
    /// <param name="action">The work to run.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Post(Action action) => _work.Add(action);

    /// <summary>Runs work on the dispatcher thread and blocks until it has run, as a synchronous marshal does.</summary>
    /// <param name="action">The work to run.</param>
    internal void Invoke(Action action)
    {
        using ManualResetEventSlim done = new(false);
        Post(() =>
        {
            action();
            done.Set();
        });
        done.Wait();
    }

    /// <summary>Runs posted work until adding completes.</summary>
    private void Run()
    {
        foreach (var action in _work.GetConsumingEnumerable())
        {
            action();
        }
    }
}
