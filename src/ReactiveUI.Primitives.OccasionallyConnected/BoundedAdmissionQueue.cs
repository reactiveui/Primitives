// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Maintains a count-and-byte bounded FIFO admission queue.</summary>
/// <typeparam name="T">The queued value type.</typeparam>
internal sealed class BoundedAdmissionQueue<T> : IDisposable
{
    /// <summary>Stores an empty eviction result.</summary>
    private static readonly BoundedAdmissionItem<T>[] NoEvictions = [];

    /// <summary>Protects queue, waiter, and lifecycle state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores admitted items in FIFO order.</summary>
    private readonly List<BoundedAdmissionItem<T>> _items = [];

    /// <summary>Stores blocked producers in FIFO order.</summary>
    private readonly LinkedList<BlockedProducer> _blockedProducers = [];

    /// <summary>Stores queue capacity and reserve settings.</summary>
    private readonly BoundedAdmissionQueueOptions _options;

    /// <summary>Stores the optional custom overflow policy.</summary>
    private readonly BoundedAdmissionPolicy<T>? _customPolicy;

    /// <summary>Tracks the total estimated bytes currently admitted.</summary>
    private long _bytes;

    /// <summary>Tracks the number of admitted data items.</summary>
    private int _dataCount;

    /// <summary>Tracks the total estimated bytes currently admitted for data items.</summary>
    private long _dataBytes;

    /// <summary>Tracks structural changes used to revalidate custom decisions.</summary>
    private long _version;

    /// <summary>Tracks whether the queue has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="BoundedAdmissionQueue{T}"/> class.</summary>
    /// <param name="options">The queue capacity settings.</param>
    /// <param name="customPolicy">The optional custom overflow policy.</param>
    internal BoundedAdmissionQueue(in BoundedAdmissionQueueOptions options, BoundedAdmissionPolicy<T>? customPolicy = null)
    {
        options.Validate();
        _options = options;
        _customPolicy = customPolicy;
    }

    /// <summary>Gets the number of currently admitted items.</summary>
    internal int Count
    {
        get
        {
            lock (_gate)
            {
                return _items.Count;
            }
        }
    }

    /// <summary>Gets the estimated number of currently admitted bytes.</summary>
    internal long Bytes
    {
        get
        {
            lock (_gate)
            {
                return _bytes;
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        BlockedProducer[] blocked;

        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            blocked = CopyBlockedProducers();
            _blockedProducers.Clear();
            _items.Clear();
            _bytes = 0;
            _dataBytes = 0;
            _dataCount = 0;
        }

        var exception = new ObjectDisposedException(nameof(BoundedAdmissionQueue<>));
        for (var i = 0; i < blocked.Length; i++)
        {
            blocked[i].ReleaseException(exception);
            blocked[i].DisposeRegistration();
        }
    }

    /// <summary>Attempts to enqueue an item using the selected strategy.</summary>
    /// <param name="value">The value to enqueue.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <param name="durable">Whether the item is protected by durable delivery semantics.</param>
    /// <param name="control">Whether the item carries control traffic.</param>
    /// <param name="strategy">The admission strategy.</param>
    /// <param name="cancellationToken">The cancellation token observed while waiting to be admitted.</param>
    /// <returns>The committed admission result.</returns>
    /// <exception cref="BoundedAdmissionRejectedException">The selected strategy cannot admit the item.</exception>
    /// <exception cref="ObjectDisposedException">The queue is disposed.</exception>
    internal Task<BoundedAdmissionResult<T>> EnqueueAsync(
        T value,
        long sizeBytes,
        bool durable,
        bool control,
        BufferStrategy strategy = BufferStrategy.Reject,
        CancellationToken cancellationToken = default)
    {
        ValidateStrategy(strategy);

        cancellationToken.ThrowIfCancellationRequested();

        var item = new BoundedAdmissionItem<T>(value, sizeBytes, durable, control);
        ValidateItem(item);

        return strategy == BufferStrategy.Custom
            ? EnqueueWithCustomPolicy(item, cancellationToken)
            : EnqueueWithBuiltInPolicy(item, strategy, cancellationToken);
    }

    /// <summary>Attempts to remove the oldest admitted item.</summary>
    /// <param name="item">The removed item when one is available.</param>
    /// <returns><see langword="true"/> when an item was removed; otherwise, <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">The queue is disposed.</exception>
    internal bool TryDequeue([NotNullWhen(true)] out BoundedAdmissionItem<T> item)
    {
        List<BlockedProducerCompletion>? completions = null;

        lock (_gate)
        {
            ThrowIfDisposed();

            if (_items.Count == 0)
            {
                item = default;
                return false;
            }

            item = _items[0];
            RemoveAt(0);
            DrainBlockedProducers(ref completions);
        }

        CompleteBlockedProducers(completions);
        return true;
    }

    /// <summary>Creates an admitted result.</summary>
    /// <param name="item">The admitted item.</param>
    /// <param name="evictedItems">The committed evictions.</param>
    /// <returns>The admitted result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static BoundedAdmissionResult<T> Admitted(BoundedAdmissionItem<T> item, IReadOnlyList<BoundedAdmissionItem<T>> evictedItems) =>
        new(BoundedAdmissionResultKind.Admitted, item, evictedItems);

    /// <summary>Completes blocked producers outside the queue lock.</summary>
    /// <param name="completions">The blocked completions to signal.</param>
    private static void CompleteBlockedProducers(List<BlockedProducerCompletion>? completions)
    {
        if (completions is null)
        {
            return;
        }

        for (var i = 0; i < completions.Count; i++)
        {
            completions[i].Producer.ReleaseResult(completions[i].Result);
            completions[i].Producer.DisposeRegistration();
        }
    }

    /// <summary>Drops the incoming item if it is eligible.</summary>
    /// <param name="item">The incoming item.</param>
    /// <returns>The committed drop result.</returns>
    private static Task<BoundedAdmissionResult<T>> DropNewest(BoundedAdmissionItem<T> item)
    {
        if (!item.IsDropEligible)
        {
            throw Rejected("The incoming item is protected and cannot be dropped.");
        }

        return Task.FromResult(DroppedIncoming(item));
    }

    /// <summary>Creates a dropped incoming result.</summary>
    /// <param name="item">The incoming item.</param>
    /// <returns>The dropped incoming result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static BoundedAdmissionResult<T> DroppedIncoming(BoundedAdmissionItem<T> item) =>
        new(BoundedAdmissionResultKind.DroppedIncoming, item, NoEvictions);

    /// <summary>Creates a typed rejection exception.</summary>
    /// <param name="message">The rejection message.</param>
    /// <returns>The typed rejection exception.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static BoundedAdmissionRejectedException Rejected(string message) => new(message);

    /// <summary>Validates an incoming item.</summary>
    /// <param name="item">The incoming item.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="item"/> has a non-positive byte size.</exception>
    private static void ValidateItem(BoundedAdmissionItem<T> item)
    {
        if (item.SizeBytes > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(item), "The item byte size must be positive.");
    }

    /// <summary>Validates the selected admission strategy.</summary>
    /// <param name="strategy">The selected strategy.</param>
    /// <exception cref="BoundedAdmissionRejectedException">The selected strategy is not supported.</exception>
    private static void ValidateStrategy(BufferStrategy strategy)
    {
        if (strategy is BufferStrategy.DropOldest or BufferStrategy.DropNewest or BufferStrategy.Block or BufferStrategy.Reject or BufferStrategy.Custom)
        {
            return;
        }

        throw Rejected("The selected admission strategy is not supported by this queue.");
    }

    /// <summary>Adds an item to the admitted queue.</summary>
    /// <param name="item">The item to admit.</param>
    private void Admit(BoundedAdmissionItem<T> item)
    {
        _items.Add(item);
        _bytes += item.SizeBytes;

        if (!item.Control)
        {
            _dataCount++;
            _dataBytes += item.SizeBytes;
        }

        _version++;
    }

    /// <summary>Blocks a producer until capacity is available.</summary>
    /// <param name="item">The incoming item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The producer waiting for admission.</returns>
    private BlockedProducer BlockProducer(BoundedAdmissionItem<T> item, CancellationToken cancellationToken)
    {
        if (!CanEverFit(item))
        {
            throw Rejected("The incoming item cannot fit within the configured queue capacity.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        if (_blockedProducers.Count >= _options.MaximumBlockedProducers)
        {
            throw Rejected("The bounded admission queue has reached its blocked producer limit.");
        }

        var producer = new BlockedProducer(this, item, cancellationToken);
        producer.Node = _blockedProducers.AddLast(producer);
        return producer;
    }

    /// <summary>Determines whether the item can ever fit an empty queue.</summary>
    /// <param name="item">The incoming item.</param>
    /// <returns><see langword="true"/> when the item can fit; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CanEverFit(BoundedAdmissionItem<T> item) => CanFitProjected(item, 0, 0, 0, 0);

    /// <summary>Determines whether the item fits the current queue state.</summary>
    /// <param name="item">The incoming item.</param>
    /// <returns><see langword="true"/> when the item fits; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CanFit(BoundedAdmissionItem<T> item) => CanFitProjected(item, _items.Count, _bytes, _dataCount, _dataBytes);

    /// <summary>Determines whether the item fits a projected queue state.</summary>
    /// <param name="item">The incoming item.</param>
    /// <param name="projectedCount">The projected admitted count.</param>
    /// <param name="projectedBytes">The projected admitted bytes.</param>
    /// <param name="projectedDataCount">The projected admitted data count.</param>
    /// <param name="projectedDataBytes">The projected admitted data bytes.</param>
    /// <returns><see langword="true"/> when the item fits; otherwise, <see langword="false"/>.</returns>
    private bool CanFitProjected(BoundedAdmissionItem<T> item, int projectedCount, long projectedBytes, int projectedDataCount, long projectedDataBytes) =>
        projectedCount < _options.Capacity
            && item.SizeBytes <= _options.CapacityBytes - projectedBytes
            && (item.Control || (projectedDataCount < _options.DataCapacity && item.SizeBytes <= _options.DataCapacityBytes - projectedDataBytes));

    /// <summary>Copies blocked producers for completion outside the lock.</summary>
    /// <returns>The blocked producer array.</returns>
    private BlockedProducer[] CopyBlockedProducers()
    {
        var blocked = new BlockedProducer[_blockedProducers.Count];
        var index = 0;

        for (var node = _blockedProducers.First; node is not null; node = node.Next)
        {
            blocked[index] = node.Value;
            blocked[index].Node = null;
            index++;
        }

        return blocked;
    }

    /// <summary>Creates a lock-protected queue snapshot.</summary>
    /// <returns>The immutable queue snapshot.</returns>
    private BoundedAdmissionSnapshot<T> CreateSnapshot()
    {
        var items = new BoundedAdmissionItem<T>[_items.Count];
        _items.CopyTo(items);

        return new(
            items,
            _items.Count,
            _bytes,
            _options.Capacity,
            _options.CapacityBytes,
            _options.ReservedControlCapacity,
            _options.ReservedControlBytes,
            _version);
    }

    /// <summary>Admits blocked producers that now fit.</summary>
    /// <param name="completions">The blocked completions to signal after releasing the lock.</param>
    private void DrainBlockedProducers(ref List<BlockedProducerCompletion>? completions)
    {
        var node = _blockedProducers.First;
        var dataBlockedAhead = false;
        var controlBlockedAhead = false;

        while (node is not null)
        {
            var next = node.Next;
            var producer = node.Value;
            var hasBlockedPredecessor = producer.Item.Control ? controlBlockedAhead : dataBlockedAhead;

            if (hasBlockedPredecessor || !CanFit(producer.Item))
            {
                if (producer.Item.Control)
                {
                    controlBlockedAhead = true;
                }
                else
                {
                    dataBlockedAhead = true;
                }

                node = next;
                continue;
            }

            _blockedProducers.Remove(node);
            producer.Node = null;
            Admit(producer.Item);
            completions ??= [];
            completions.Add(new(producer, Admitted(producer.Item, NoEvictions)));
            node = next;
        }
    }

    /// <summary>Drops oldest eligible items until the incoming item fits.</summary>
    /// <param name="item">The incoming item.</param>
    /// <param name="maximumDropCount">The maximum number of eligible items to drop.</param>
    /// <returns>The committed admission result.</returns>
    private Task<BoundedAdmissionResult<T>> DropOldestAndAdmit(BoundedAdmissionItem<T> item, int maximumDropCount = int.MaxValue)
    {
        var evictions = SelectOldestEligibleEvictions(item, maximumDropCount);

        if (evictions.Items.Length == 0)
        {
            throw Rejected("The bounded admission queue cannot drop enough eligible items to admit the incoming item.");
        }

        RemoveEvictions(evictions.Indexes);

        Admit(item);
        return Task.FromResult(Admitted(item, evictions.Items));
    }

    /// <summary>Applies a revalidated custom decision.</summary>
    /// <param name="item">The incoming item.</param>
    /// <param name="decision">The custom policy decision.</param>
    /// <returns>The committed admission result.</returns>
    private Task<BoundedAdmissionResult<T>> ApplyCustomDecision(BoundedAdmissionItem<T> item, BoundedAdmissionDecision decision) =>
        decision.Kind switch
        {
            BoundedAdmissionDecisionKind.Reject => throw Rejected("The custom admission policy rejected the item."),
            BoundedAdmissionDecisionKind.DropNewest => DropNewest(item),
            BoundedAdmissionDecisionKind.DropOldest => DropOldestAndAdmit(item, decision.DropOldestCount),
            _ => throw Rejected("The custom admission policy returned an unknown decision."),
        };

    /// <summary>Attempts built-in queue admission.</summary>
    /// <param name="item">The incoming item.</param>
    /// <param name="strategy">The admission strategy.</param>
    /// <param name="cancellationToken">The cancellation token observed by block strategy.</param>
    /// <returns>The committed admission result.</returns>
    private Task<BoundedAdmissionResult<T>> EnqueueWithBuiltInPolicy(
        BoundedAdmissionItem<T> item,
        BufferStrategy strategy,
        CancellationToken cancellationToken)
    {
        Task<BoundedAdmissionResult<T>> result;
        BlockedProducer? blockedProducer = null;

        lock (_gate)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (CanFit(item) && !HasBlockedPredecessor(item))
            {
                Admit(item);
                result = Task.FromResult(Admitted(item, NoEvictions));
            }
            else if (HasBlockedPredecessor(item) && strategy != BufferStrategy.Block)
            {
                throw Rejected("A compatible producer is already waiting for bounded queue capacity.");
            }
            else if (strategy == BufferStrategy.Block)
            {
                blockedProducer = BlockProducer(item, cancellationToken);
                result = blockedProducer.Task;
            }
            else if (strategy == BufferStrategy.DropOldest)
            {
                result = DropOldestAndAdmit(item);
            }
            else if (strategy == BufferStrategy.DropNewest)
            {
                result = DropNewest(item);
            }
            else
            {
                throw Rejected("The bounded admission queue is full.");
            }
        }

        blockedProducer?.RegisterCancellation();
        return result;
    }

    /// <summary>Attempts custom policy admission using an unlocked snapshot and locked revalidation.</summary>
    /// <param name="item">The incoming item.</param>
    /// <param name="cancellationToken">The token observed before the admission commit.</param>
    /// <returns>The committed admission result.</returns>
    private Task<BoundedAdmissionResult<T>> EnqueueWithCustomPolicy(BoundedAdmissionItem<T> item, CancellationToken cancellationToken)
    {
        if (_customPolicy is null)
        {
            throw Rejected("A custom admission policy was requested, but no policy is registered.");
        }

        BoundedAdmissionSnapshot<T> snapshot;

        lock (_gate)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (CanFit(item) && !HasBlockedPredecessor(item))
            {
                Admit(item);
                return Task.FromResult(Admitted(item, NoEvictions));
            }

            snapshot = CreateSnapshot();
        }

        var decision = _customPolicy(snapshot, item);
        Task<BoundedAdmissionResult<T>> result;
        BlockedProducer? blockedProducer = null;

        lock (_gate)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (snapshot.Version != _version)
            {
                throw Rejected("The custom admission decision was based on a stale queue snapshot.");
            }

            if (decision.Kind == BoundedAdmissionDecisionKind.Block)
            {
                blockedProducer = BlockProducer(item, cancellationToken);
                result = blockedProducer.Task;
            }
            else if (HasBlockedPredecessor(item))
            {
                throw Rejected("A compatible producer is already waiting for bounded queue capacity.");
            }
            else
            {
                result = ApplyCustomDecision(item, decision);
            }
        }

        blockedProducer?.RegisterCancellation();
        return result;
    }

    /// <summary>Removes the selected evictions from the queue.</summary>
    /// <param name="indexes">The selected eviction indexes.</param>
    private void RemoveEvictions(int[] indexes)
    {
        for (var i = indexes.Length - 1; i >= 0; i--)
        {
            RemoveAt(indexes[i]);
        }
    }

    /// <summary>Removes a queued item at the supplied index.</summary>
    /// <param name="index">The index to remove.</param>
    private void RemoveAt(int index)
    {
        var item = _items[index];
        _bytes -= item.SizeBytes;

        if (!item.Control)
        {
            _dataCount--;
            _dataBytes -= item.SizeBytes;
        }

        _items.RemoveAt(index);
        _version++;
    }

    /// <summary>Selects oldest eligible evictions that make the incoming item fit.</summary>
    /// <param name="item">The incoming item.</param>
    /// <param name="maximumDropCount">The maximum number of eligible items to drop.</param>
    /// <returns>The selected evictions.</returns>
    private EvictionSelection SelectOldestEligibleEvictions(BoundedAdmissionItem<T> item, int maximumDropCount)
    {
        if (maximumDropCount <= 0)
        {
            return new([], []);
        }

        var selected = new List<BoundedAdmissionItem<T>>();
        var indexes = new List<int>();
        var projectedCount = _items.Count;
        var projectedBytes = _bytes;
        var projectedDataCount = _dataCount;
        var projectedDataBytes = _dataBytes;

        for (var i = 0; i < _items.Count && selected.Count < maximumDropCount; i++)
        {
            var candidate = _items[i];

            if (!candidate.IsDropEligible)
            {
                continue;
            }

            selected.Add(candidate);
            indexes.Add(i);
            projectedCount--;
            projectedBytes -= candidate.SizeBytes;

            if (!candidate.Control)
            {
                projectedDataCount--;
                projectedDataBytes -= candidate.SizeBytes;
            }

            if (CanFitProjected(item, projectedCount, projectedBytes, projectedDataCount, projectedDataBytes))
            {
                break;
            }
        }

        return selected.Count > 0 && CanFitProjected(item, projectedCount, projectedBytes, projectedDataCount, projectedDataBytes)
            ? new(selected.ToArray(), indexes.ToArray())
            : new([], []);
    }

    /// <summary>Determines whether a compatible producer is already waiting ahead of an incoming item.</summary>
    /// <param name="item">The incoming item.</param>
    /// <returns><see langword="true"/> when the incoming item must not bypass a predecessor; otherwise, <see langword="false"/>.</returns>
    private bool HasBlockedPredecessor(BoundedAdmissionItem<T> item)
    {
        for (var node = _blockedProducers.First; node is not null; node = node.Next)
        {
            if (node.Value.Item.Control == item.Control)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Throws when the queue has been disposed.</summary>
    /// <exception cref="ObjectDisposedException">The queue has been disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);

    /// <summary>Stores a blocked producer completion.</summary>
    /// <param name="Producer">The blocked producer.</param>
    /// <param name="Result">The admission result.</param>
    private readonly record struct BlockedProducerCompletion(BlockedProducer Producer, BoundedAdmissionResult<T> Result);

    /// <summary>Stores selected evictions and their queue indexes.</summary>
    /// <param name="Items">The selected items.</param>
    /// <param name="Indexes">The selected queue indexes.</param>
    private readonly record struct EvictionSelection(BoundedAdmissionItem<T>[] Items, int[] Indexes);

    /// <summary>Stores a blocked producer waiting for capacity.</summary>
    internal sealed class BlockedProducer
    {
        /// <summary>Stores the owning queue.</summary>
        private readonly BoundedAdmissionQueue<T> _owner;

        /// <summary>Stores the completion source for the blocked enqueue call.</summary>
        private readonly TaskCompletionSource<BoundedAdmissionResult<T>> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The caller's cancellation token.</summary>
        private readonly CancellationToken _cancellationToken;

        /// <summary>Initializes a new instance of the <see cref="BlockedProducer"/> class.</summary>
        /// <param name="owner">The owning queue.</param>
        /// <param name="item">The incoming item.</param>
        /// <param name="cancellationToken">The caller's cancellation token.</param>
        internal BlockedProducer(BoundedAdmissionQueue<T> owner, BoundedAdmissionItem<T> item, CancellationToken cancellationToken)
        {
            _owner = owner;
            Item = item;
            _cancellationToken = cancellationToken;
        }

        /// <summary>Gets the item waiting for admission.</summary>
        internal BoundedAdmissionItem<T> Item { get; }

        /// <summary>Gets or sets the cancellation registration.</summary>
        internal CancellationTokenRegistration CancellationRegistration { get; set; }

        /// <summary>Gets or sets the linked-list node for O(1) cancellation removal.</summary>
        internal LinkedListNode<BlockedProducer>? Node { get; set; }

        /// <summary>Gets the task completed when the item is admitted, rejected, or cancelled.</summary>
        internal Task<BoundedAdmissionResult<T>> Task => _completion.Task;

        /// <summary>Registers cancellation outside the queue lock, allowing synchronous callbacks to complete safely.</summary>
        internal void RegisterCancellation()
        {
#if NET8_0_OR_GREATER
            var registration = _cancellationToken.UnsafeRegister(CancelRegisteredProducer, this);
#else
            var registration = _cancellationToken.Register(CancelRegisteredProducer, this);
#endif

            lock (_owner._gate)
            {
                if (Node is not null)
                {
                    CancellationRegistration = registration;
                    return;
                }
            }

            registration.Dispose();
        }

        /// <summary>Cancels the blocked admission if it has not yet been admitted.</summary>
        internal void Cancel()
        {
            List<BlockedProducerCompletion>? completions = null;

            lock (_owner._gate)
            {
                if (Node is null)
                {
                    return;
                }

                _owner._blockedProducers.Remove(Node);
                Node = null;
                _owner.DrainBlockedProducers(ref completions);
            }

            _ = _completion.TrySetCanceled(_cancellationToken);
            CompleteBlockedProducers(completions);
        }

        /// <summary>Disposes the cancellation registration.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRegistration() => CancellationRegistration.Dispose();

        /// <summary>Completes the blocked admission with an exception.</summary>
        /// <param name="exception">The exception.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseException(Exception exception) => _completion.TrySetException(exception);

        /// <summary>Completes the blocked admission with a result.</summary>
        /// <param name="result">The admission result.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseResult(BoundedAdmissionResult<T> result) => _completion.TrySetResult(result);

        /// <summary>Cancels the producer carried by a cancellation registration.</summary>
        /// <param name="state">The producer supplied when the callback was registered.</param>
        private static void CancelRegisteredProducer(object? state)
        {
            ArgumentExceptionHelper.ThrowIfNull(state);
            ((BlockedProducer)state).Cancel();
        }
    }
}
