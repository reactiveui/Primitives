// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that routes each source value to the group of its key, opening a group the first time a key appears.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TElement">The value type inside each group.</typeparam>
/// <remarks>
/// Groups end when the source ends. A group is delivered to the outer observer before the value that opened it reaches
/// the group. Source notifications must be serialized, as the observer contract requires.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("GroupByWitness: Groups = {_groups.Count}, Done = {_done}")]
public sealed class GroupByWitness<TSource, TKey, TElement> : IObserver<TSource>, IDisposable
    where TKey : notnull
{
    /// <summary>The selector that extracts the key of a value.</summary>
    private readonly Func<TSource, TKey> _keySelector;

    /// <summary>The selector that extracts the value stored in a group.</summary>
    private readonly Func<TSource, TElement> _elementSelector;

    /// <summary>The open groups by key.</summary>
    private readonly Dictionary<TKey, SliceWindow<TElement>> _groups;

    /// <summary>Delivers groups and their values in the order they were posted.</summary>
    private readonly SliceRouter<GroupedSignal<TKey, TElement>, TElement> _router;

    /// <summary>The terminal latch; non-zero once the sink has terminated and must ignore further notifications.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="GroupByWitness{TSource, TKey, TElement}"/> class.</summary>
    /// <param name="observer">The downstream observer of the groups.</param>
    /// <param name="keySelector">The selector that extracts the key of a value.</param>
    /// <param name="elementSelector">The selector that extracts the value stored in a group.</param>
    /// <param name="comparer">The key comparer, or <see langword="null"/> for the default comparer.</param>
    /// <param name="capacity">The initial capacity of the group table; zero uses the default.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/>, <paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
    public GroupByWitness(
        IObserver<GroupedSignal<TKey, TElement>> observer,
        Func<TSource, TKey> keySelector,
        Func<TSource, TElement> elementSelector,
        IEqualityComparer<TKey>? comparer,
        int capacity)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
        _elementSelector = elementSelector ?? throw new ArgumentNullException(nameof(elementSelector));
        _groups = [with(capacity, comparer)];
        Subscription = new();
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

        try
        {
            var key = _keySelector(value);
            if (!_groups.TryGetValue(key, out var group))
            {
                group = new(Subscription);
                _groups.Add(key, group);
                _router.Open(new(key, group));
            }

            _router.Publish(group, _elementSelector(value));
        }
        catch (Exception error)
        {
            Terminate(error);
            return;
        }

        Deliver();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => Terminate(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Terminate(null);

    /// <summary>Assigns the upstream subscription, disposing the incoming one when this sink holds a subscription or has been disposed.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => Subscription.Attach(subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _done, 1);
        Subscription.Release();
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

    /// <summary>Ends every group and the outer sequence, then tears the sink down.</summary>
    /// <param name="error">The error, or <see langword="null"/> to complete.</param>
    private void Terminate(Exception? error)
    {
        if (Interlocked.Exchange(ref _done, 1) != 0)
        {
            return;
        }

        try
        {
            _router.Finish(_groups.Values, error);
            _router.Flush();
        }
        finally
        {
            Dispose();
        }
    }
}
