// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ExtensionsResult = ReactiveUI.Extensions.Async.Result;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>ReactiveUI.Extensions async observer that tallies notifications and exposes the next value and the completion as tasks.</summary>
/// <typeparam name="T">The observed element type.</typeparam>
[DebuggerDisplay("ExtensionsAsyncTallyWitness: Count = {_count}, Errors = {_errorCount}")]
internal sealed class ExtensionsAsyncTallyWitness<T> : ReactiveUI.Extensions.Async.ObserverAsync<T>
{
    /// <summary>Completes with the value count once the terminal notification arrives.</summary>
    private readonly TaskCompletionSource<int> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes with the running count when the next value arrives.</summary>
    private TaskCompletionSource<int> _nextValue = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The number of values observed.</summary>
    private int _count;

    /// <summary>The number of resumable errors observed.</summary>
    private int _errorCount;

    /// <summary>Gets the number of values observed.</summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>Gets the number of resumable errors observed.</summary>
    public int ErrorCount => Volatile.Read(ref _errorCount);

    /// <summary>Gets a value indicating whether the sequence completed with a failure.</summary>
    public bool Failed { get; private set; }

    /// <summary>Gets a task that completes with the value count when the sequence terminates.</summary>
    public Task<int> Completion => _completion.Task;

    /// <summary>Gets a task that completes when the next value arrives; read it before triggering that value.</summary>
    public Task<int> NextValue => Volatile.Read(ref _nextValue).Task;

    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        var count = Interlocked.Increment(ref _count);
        var reached = Interlocked.Exchange(
            ref _nextValue,
            new(TaskCreationOptions.RunContinuationsAsynchronously));
        _ = reached.TrySetResult(count);
        return default;
    }

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _errorCount);
        return default;
    }

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(ExtensionsResult result)
    {
        Failed = !result.IsSuccess;
        _ = _completion.TrySetResult(Count);
        return default;
    }
}
