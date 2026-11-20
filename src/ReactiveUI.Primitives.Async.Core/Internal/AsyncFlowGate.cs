// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Internal;

/// <summary>Serializes asynchronous downstream deliveries, letting the holding async flow re-enter while it holds the gate.</summary>
/// <remarks>
/// <para>
/// Ownership follows the logical async flow, not the thread. Two independent flows that share a thread (such as a UI
/// dispatcher, or a pool thread freed by an awaiting holder) still wait for each other, while an observer that feeds back
/// into its source from inside a delivery re-enters, including after an await.
/// </para>
/// <para>
/// Work started inside a hold (a <c>Task.Run</c>, an unawaited async call, or a background producer started by a source
/// subscribed under the gate) inherits the holder's async context. If it reaches this gate while the hold is still
/// active, it re-enters instead of waiting. .NET gives no way to tell that work apart from a genuine feedback call, so
/// this is a deliberate trade-off. Code that starts such work from inside a hold and needs it serialized should call
/// <see cref="DetachCurrentFlow"/> at the start of that work.
/// </para>
/// </remarks>
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Design",
    "CA1001:Types that own disposable fields should be disposable",
    Justification = "SemaphoreSlim holds an OS handle only once AvailableWaitHandle is read, which this gate never does. "
        + "Leaving it undisposed lets a delivery that is still waiting finish after its owner is torn down.")]
internal sealed class AsyncFlowGate
{
    /// <summary>Admits one holder at a time.</summary>
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    /// <summary>The owner token visible to the holding flow and the work it awaits.</summary>
    private readonly AsyncLocal<object?> _flowOwner = new();

    /// <summary>The token of the current holder, or <see langword="null"/> when the gate is free.</summary>
    private object? _owner;

    /// <summary>Waits for the gate without a cancellation token.</summary>
    /// <returns>An awaitable entry; dispose the lease it produces to leave the gate.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal Entry EnterAsync() => EnterAsync(CancellationToken.None);

    /// <summary>Waits for the gate, or enters straight away when the current flow already holds it.</summary>
    /// <param name="cancellationToken">Cancels waiting for the gate.</param>
    /// <returns>An awaitable entry; dispose the lease it produces to leave the gate.</returns>
    internal Entry EnterAsync(CancellationToken cancellationToken)
    {
        var owner = Volatile.Read(ref _owner);
        return owner is not null && ReferenceEquals(_flowOwner.Value, owner)
            ? new(this, null)
            : new(this, _semaphore.WaitAsync(cancellationToken));
    }

    /// <summary>Runs <paramref name="work"/> while holding the gate.</summary>
    /// <typeparam name="TState">The state passed to <paramref name="work"/>.</typeparam>
    /// <param name="state">The state forwarded to <paramref name="work"/>.</param>
    /// <param name="work">The delivery to serialize.</param>
    /// <param name="cancellationToken">Cancels waiting for the gate.</param>
    /// <returns>A task that completes when <paramref name="work"/> completes.</returns>
    internal async ValueTask RunAsync<TState>(
        TState state,
        Func<TState, ValueTask> work,
        CancellationToken cancellationToken)
    {
        using (await EnterAsync(cancellationToken).ConfigureAwait(false))
        {
            await work(state).ConfigureAwait(false);
        }
    }

    /// <summary>Stops the current async flow counting as the holder.</summary>
    /// <remarks>Call this at the start of work that runs on its own schedule, such as a timer, but was started from inside a hold.</remarks>
    internal void DetachCurrentFlow() => _flowOwner.Value = null;

    /// <summary>Records the awaiting flow as the holder.</summary>
    /// <returns>The lease that releases the gate.</returns>
    /// <remarks>
    /// Runs inside the awaiter's <c>GetResult</c>, which executes in the caller's own async method. The AsyncLocal set here
    /// therefore stays visible for the rest of the caller's held region and the work it awaits, and is dropped when the
    /// caller returns. A fresh token per hold stops a flow that captured an older token from passing as the holder.
    /// </remarks>
    private Lease Acquire()
    {
        object token = new();
        Volatile.Write(ref _owner, token);
        _flowOwner.Value = token;
        return new(this);
    }

    /// <summary>Clears the holder and admits the next waiter.</summary>
    private void Release()
    {
        Volatile.Write(ref _owner, null);
        _ = _semaphore.Release();
    }

    /// <summary>One hold on an <see cref="AsyncFlowGate"/>; a re-entrant hold releases nothing.</summary>
    /// <param name="Gate">The gate to release, or <see langword="null"/> for a re-entrant hold.</param>
    internal readonly record struct Lease(AsyncFlowGate? Gate) : IDisposable
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Gate?.Release();
    }

    /// <summary>A pending entry into an <see cref="AsyncFlowGate"/>.</summary>
    /// <param name="Gate">The gate being entered.</param>
    /// <param name="Wait">The semaphore wait, or <see langword="null"/> for a re-entrant entry.</param>
    internal readonly record struct Entry(AsyncFlowGate Gate, Task? Wait)
    {
        /// <summary>Configures how the continuation resumes after waiting.</summary>
        /// <param name="continueOnCapturedContext">Whether to resume on the captured context.</param>
        /// <returns>The awaitable entry.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ConfiguredEntry ConfigureAwait(bool continueOnCapturedContext) =>
            new(Gate, (Wait ?? Task.CompletedTask).ConfigureAwait(continueOnCapturedContext).GetAwaiter(), Wait is null);
    }

    /// <summary>An awaitable entry whose result is the <see cref="Lease"/>.</summary>
    /// <param name="Gate">The gate being entered.</param>
    /// <param name="Waiter">The awaiter for the semaphore wait.</param>
    /// <param name="Reentrant">Whether the current flow already holds the gate.</param>
    internal readonly record struct ConfiguredEntry(
        AsyncFlowGate Gate,
        ConfiguredTaskAwaitable.ConfiguredTaskAwaiter Waiter,
        bool Reentrant) : ICriticalNotifyCompletion
    {
        /// <summary>Gets a value indicating whether the gate has been entered.</summary>
        internal bool IsCompleted => Waiter.IsCompleted;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action continuation) => Waiter.OnCompleted(continuation);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeOnCompleted(Action continuation) => Waiter.UnsafeOnCompleted(continuation);

        /// <summary>Gets the awaiter for this entry.</summary>
        /// <returns>This entry.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ConfiguredEntry GetAwaiter() => this;

        /// <summary>Completes the entry, recording the caller's flow as the holder.</summary>
        /// <returns>The lease that leaves the gate when disposed.</returns>
        internal Lease GetResult()
        {
            Waiter.GetResult();
            return Reentrant ? default : Gate.Acquire();
        }
    }
}
