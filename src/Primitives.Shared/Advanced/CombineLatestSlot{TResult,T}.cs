// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Holds the latest value of one source in a field of that source's own type and observes it directly.</summary>
/// <typeparam name="TResult">The projected result type.</typeparam>
/// <typeparam name="T">The source element type.</typeparam>
/// <param name="coordinator">The coordinator that serializes this slot against its siblings.</param>
/// <param name="source">The source observable.</param>
[System.Diagnostics.DebuggerDisplay("CombineLatestSlot: Value = {Value}, HasValue = {HasValue}")]
internal sealed class CombineLatestSlot<TResult, T>(
    CombineLatestCoordinator<TResult> coordinator,
    IObservable<T> source) : ICombineLatestSlot, IObserver<T>
{
    /// <summary>Values this source produced while another thread was delivering, in order; created on first contention.</summary>
    private ConcurrentQueue<T>? _queued;

    /// <summary>Whether this source has completed, as 0 or 1.</summary>
    private int _completed;

    /// <summary>Gets the latest value this source produced, valid once every slot has one.</summary>
    internal T Value { get; private set; } = default!;

    /// <summary>Gets or sets a value indicating whether the source has produced a value; touched only while the delivery gate is held.</summary>
    internal bool HasValue { get; set; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => coordinator.OnNext(this, value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => coordinator.OnError(error);

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
        {
            return;
        }

        coordinator.OnCompleted();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe() => source.Subscribe(this);

    /// <inheritdoc/>
    public void ApplyQueued()
    {
        _ = Volatile.Read(ref _queued)!.TryDequeue(out var value);
        coordinator.Apply(this, value!);
    }

    /// <summary>Queues a value for the delivering thread; the coordinator records the slot's turn separately.</summary>
    /// <param name="value">The value the source produced.</param>
    internal void Queue(T value)
    {
        _ = Interlocked.CompareExchange(ref _queued, new(), null);
        Volatile.Read(ref _queued)!.Enqueue(value);
    }

    /// <summary>Records the latest value while the coordinator's delivery gate is held.</summary>
    /// <param name="value">The value the source produced.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Accept(T value) => Value = value;
}
