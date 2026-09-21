// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Sink that routes each source value to the group of its key and ends a group when its duration signal emits or completes.
/// </summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TElement">The value type inside each group.</typeparam>
/// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
/// <remarks>
/// A key that appears after its group has ended opens a new group. A failure in the source, a selector or a duration
/// signal faults every open group and the outer sequence. Notifications are delivered outside the sink's lock.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("GroupByUntilWitness: Groups = {_groups.Count}, Done = {_done}")]
public sealed class GroupByUntilWitness<TSource, TKey, TElement, TDuration> : IObserver<TSource>, IDisposable
    where TKey : notnull
{
    /// <summary>Serializes access to the group table and the terminal latch.</summary>
    private readonly Lock _gate = new();

    /// <summary>The selector that extracts the key of a value.</summary>
    private readonly Func<TSource, TKey> _keySelector;

    /// <summary>The selector that extracts the value stored in a group.</summary>
    private readonly Func<TSource, TElement> _elementSelector;

    /// <summary>The selector that supplies the signal that ends a group.</summary>
    private readonly Func<GroupedSignal<TKey, TElement>, IObservable<TDuration>> _durationSelector;

    /// <summary>The open groups by key.</summary>
    private readonly Dictionary<TKey, SliceWindow<TElement>> _groups;

    /// <summary>Delivers groups and their values in the order they were posted.</summary>
    private readonly SliceRouter<GroupedSignal<TKey, TElement>, TElement> _router;

    /// <summary>The source subscription and the live duration subscriptions.</summary>
    private readonly MultipleDisposable _upstream = [];

    /// <summary>The terminal latch; non-zero once the sink has terminated and must ignore further notifications.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="GroupByUntilWitness{TSource, TKey, TElement, TDuration}"/> class.</summary>
    /// <param name="observer">The downstream observer of the groups.</param>
    /// <param name="keySelector">The selector that extracts the key of a value.</param>
    /// <param name="elementSelector">The selector that extracts the value stored in a group.</param>
    /// <param name="durationSelector">The selector that supplies the signal that ends a group.</param>
    /// <param name="comparer">The key comparer, or <see langword="null"/> for the default comparer.</param>
    /// <param name="capacity">The initial capacity of the group table; zero uses the default.</param>
    /// <exception cref="ArgumentNullException">A reference argument other than <paramref name="comparer"/> is <see langword="null"/>.</exception>
    public GroupByUntilWitness(
        IObserver<GroupedSignal<TKey, TElement>> observer,
        Func<TSource, TKey> keySelector,
        Func<TSource, TElement> elementSelector,
        Func<GroupedSignal<TKey, TElement>, IObservable<TDuration>> durationSelector,
        IEqualityComparer<TKey>? comparer,
        int capacity)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
        _elementSelector = elementSelector ?? throw new ArgumentNullException(nameof(elementSelector));
        _durationSelector = durationSelector ?? throw new ArgumentNullException(nameof(durationSelector));
        _groups = [with(capacity, comparer)];
        Subscription = new();
        Subscription.Attach(_upstream);
        _router = new(observer, Subscription);
    }

    /// <summary>Gets the subscription handed to the outer subscriber; the source stays subscribed while any group is.</summary>
    public SharedSubscription Subscription { get; }

    /// <inheritdoc/>
    public void OnNext(TSource value)
    {
        if (Volatile.Read(ref _done) != 0)
        {
            return;
        }

        TKey key;
        SliceWindow<TElement> group;
        GroupedSignal<TKey, TElement>? opened = null;
        try
        {
            key = _keySelector(value);
            lock (_gate)
            {
                if (_done != 0)
                {
                    return;
                }

                if (!_groups.TryGetValue(key, out group!))
                {
                    group = new(Subscription);
                    _groups.Add(key, group);
                    opened = new(key, group);
                }
            }
        }
        catch (Exception error)
        {
            Terminate(error);
            return;
        }

        if (opened is not null && !Open(key, group, opened))
        {
            return;
        }

        TElement element;
        try
        {
            element = _elementSelector(value);
        }
        catch (Exception error)
        {
            Terminate(error);
            return;
        }

        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            _router.Publish(group, element);
        }

        Deliver();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => Terminate(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Terminate(null);

    /// <summary>Assigns the upstream subscription, disposing the incoming one when this sink has been disposed.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => _upstream.Add(subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _done, 1);
        Subscription.Release();
        _upstream.Dispose();
    }

    /// <summary>Delivers a new group to the outer observer and starts watching its duration signal.</summary>
    /// <param name="key">The key of the group.</param>
    /// <param name="group">The window that carries the group's values.</param>
    /// <param name="opened">The group handed to the outer observer.</param>
    /// <returns><see langword="false"/> when the sink terminated before the group was delivered.</returns>
    private bool Open(TKey key, SliceWindow<TElement> group, GroupedSignal<TKey, TElement> opened)
    {
        IObservable<TDuration> duration;
        try
        {
            duration = _durationSelector(opened);
        }
        catch (Exception error)
        {
            Terminate(error);
            return false;
        }

        lock (_gate)
        {
            if (_done != 0)
            {
                return false;
            }

            _router.Open(opened);
        }

        Deliver();

        DurationWitness witness = new(this, key, group);
        _upstream.Add(witness);
        witness.SetSubscription(duration.Subscribe(witness));
        return true;
    }

    /// <summary>Ends a group when its duration signal fires.</summary>
    /// <param name="key">The key of the group.</param>
    /// <param name="group">The window that carries the group's values.</param>
    /// <param name="witness">The duration subscription to release.</param>
    private void Expire(TKey key, SliceWindow<TElement> group, DurationWitness witness)
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            if (_groups.TryGetValue(key, out var current) && ReferenceEquals(current, group))
            {
                _ = _groups.Remove(key);
                _router.Complete(group);
            }
        }

        Deliver();
        _ = _upstream.Remove(witness);
    }

    /// <summary>Delivers the posted notifications, tearing the sink down when a downstream observer throws.</summary>
    private void Deliver()
    {
        try
        {
            _router.Flush();
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>Ends every open group and the outer sequence, then tears the sink down.</summary>
    /// <param name="error">The error, or <see langword="null"/> to complete.</param>
    private void Terminate(Exception? error)
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            Volatile.Write(ref _done, 1);
            _router.Finish(_groups.Values, error);
        }

        try
        {
            _router.Flush();
        }
        finally
        {
            Dispose();
        }
    }

    /// <summary>Observes the duration signal of one group.</summary>
    /// <param name="parent">The owning sink.</param>
    /// <param name="key">The key of the group.</param>
    /// <param name="group">The window that carries the group's values.</param>
    private sealed class DurationWitness(
        GroupByUntilWitness<TSource, TKey, TElement, TDuration> parent,
        TKey key,
        SliceWindow<TElement> group) : IObserver<TDuration>, IDisposable
    {
        /// <summary>The duration subscription.</summary>
        private IDisposable? _subscription;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(TDuration value) => parent.Expire(key, group, this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.Terminate(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => parent.Expire(key, group, this);

        /// <summary>Assigns the duration subscription, disposing the incoming one when this witness has been disposed.</summary>
        /// <param name="subscription">The duration subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => SinkSubscription.Dispose(ref _subscription);
    }
}
