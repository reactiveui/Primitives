// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Thread-safe wrapper around <see cref="SequencerQueue{TAbsolute}"/> used by the virtual-time sequencers. Bundling
/// the queue with its lock lets the self-removal callback synchronize through a reference without capturing the
/// owning struct's <c>this</c>.
/// </summary>
/// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
internal sealed class SynchronizedSequencerQueue<TAbsolute>
    where TAbsolute : IComparable<TAbsolute>
{
    /// <summary>Priority queue storing scheduled work.</summary>
    private readonly SequencerQueue<TAbsolute> _queue = new();

    /// <summary>Gate guarding the queue.</summary>
    private readonly Lock _gate = new();

    /// <summary>Enqueues a scheduled item.</summary>
    /// <param name="item">The item to enqueue.</param>
    public void Enqueue(ScheduledItem<TAbsolute> item)
    {
        lock (_gate)
        {
            _queue.Enqueue(item);
        }
    }

    /// <summary>Removes a scheduled item.</summary>
    /// <param name="item">The item to remove.</param>
    public void Remove(ScheduledItem<TAbsolute> item)
    {
        lock (_gate)
        {
            _queue.Remove(item);
        }
    }

    /// <summary>Gets the next non-cancelled scheduled item, leaving it on the queue and discarding cancelled items it passes.</summary>
    /// <returns>The next live scheduled item, or <see langword="null"/> when none remain.</returns>
    public IScheduledItem<TAbsolute>? GetNextLive()
    {
        lock (_gate)
        {
            while (_queue.Count > 0)
            {
                var next = _queue.Peek();
                if (next.IsDisposed)
                {
                    _queue.Dequeue();
                }
                else
                {
                    return next;
                }
            }
        }

        return null;
    }
}
