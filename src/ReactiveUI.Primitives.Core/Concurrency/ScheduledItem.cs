// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Provides the base implementation for a scheduled unit of work that is ordered by an absolute due time.</summary>
/// <typeparam name="TAbsolute">
/// The type used to represent absolute time. The type must be comparable so scheduled items can be ordered.
/// </typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ScheduledItem<TAbsolute> : IScheduledItem<TAbsolute>, IComparable<ScheduledItem<TAbsolute>>,
    IsDisposed, IComparable
    where TAbsolute : IComparable<TAbsolute>
{
    /// <summary>Due-time comparer.</summary>
    private readonly IComparer<TAbsolute> _comparer;

    /// <summary>The work performed when the item is invoked; receives this item so callers can self-deregister.</summary>
    private readonly Func<ScheduledItem<TAbsolute>, IDisposable> _invokeCore;

    /// <summary>Invocation disposable.</summary>
    private IDisposable? _disposable;

    /// <summary>Disposal flag.</summary>
    private int _isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ScheduledItem{TAbsolute}"/> class with the due time, comparer,
    /// and invocation callback. The callback supplies the work performed by <see cref="Invoke"/> and replaces what a
    /// derived <c>InvokeCore</c> override used to provide.
    /// </summary>
    /// <param name="dueTime">The absolute time at which this item is due to run.</param>
    /// <param name="comparer">The comparer used to order due-time values.</param>
    /// <param name="invokeCore">
    /// The work performed when the item runs. It receives this scheduled item (so callers can deregister it) and
    /// returns a disposable that cancels any work that outlives the invocation.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="comparer"/> or <paramref name="invokeCore"/> is <see langword="null"/>.</exception>
    public ScheduledItem(
        TAbsolute dueTime,
        IComparer<TAbsolute> comparer,
        Func<ScheduledItem<TAbsolute>, IDisposable> invokeCore)
    {
        DueTime = dueTime;
        _comparer = comparer ?? throw new ArgumentNullException(nameof(comparer));
        _invokeCore = invokeCore ?? throw new ArgumentNullException(nameof(invokeCore));
    }

    /// <summary>Gets the absolute time at which this work item is scheduled to run.</summary>
    public TAbsolute DueTime { get; }

    /// <summary>Gets a value indicating whether this work item has been canceled or disposed.</summary>
    /// <remarks>
    /// Once this property becomes <see langword="true"/>, calls to <see cref="Invoke"/> will not start new work.
    /// If the work has already started and returned a disposable resource, that resource is disposed.
    /// </remarks>
    public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <summary>Gets the Debugger text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Determines whether two scheduled item references are not the same object.</summary>
    /// <param name="left">The first scheduled item to compare.</param>
    /// <param name="right">The second scheduled item to compare.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> and <paramref name="right"/> do not refer to the same
    /// object; otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Equality operators use reference equality. They do not compare <see cref="DueTime"/> values and are therefore
    /// intentionally different from the ordering behavior used by <see cref="CompareTo(ScheduledItem{TAbsolute}?)"/>.
    /// </remarks>
    public static bool operator !=(ScheduledItem<TAbsolute>? left, ScheduledItem<TAbsolute>? right) =>
        !ReferenceEquals(left, right);

    /// <summary>Determines whether the first scheduled item is due before the second scheduled item.</summary>
    /// <param name="left">The scheduled item on the left side of the comparison.</param>
    /// <param name="right">The scheduled item on the right side of the comparison.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> is ordered before <paramref name="right"/> by due time;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// This operator uses the same due-time ordering as <see cref="CompareTo(ScheduledItem{TAbsolute}?)"/>.
    /// </remarks>
    public static bool operator <(ScheduledItem<TAbsolute> left, ScheduledItem<TAbsolute> right) =>
        Comparer<ScheduledItem<TAbsolute>>.Default.Compare(left, right) < 0;

    /// <summary>Determines whether the first scheduled item is due before, or at the same time as, the second scheduled item.</summary>
    /// <param name="left">The scheduled item on the left side of the comparison.</param>
    /// <param name="right">The scheduled item on the right side of the comparison.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> is ordered before or equal to <paramref name="right"/> by
    /// due time; otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// This operator uses the same due-time ordering as <see cref="CompareTo(ScheduledItem{TAbsolute}?)"/>.
    /// </remarks>
    public static bool operator <=(ScheduledItem<TAbsolute> left, ScheduledItem<TAbsolute> right) =>
        Comparer<ScheduledItem<TAbsolute>>.Default.Compare(left, right) <= 0;

    /// <summary>Determines whether two scheduled item references are the same object.</summary>
    /// <param name="left">The first scheduled item to compare.</param>
    /// <param name="right">The second scheduled item to compare.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> and <paramref name="right"/> refer to the same object;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Equality operators use reference equality. They do not compare <see cref="DueTime"/> values and are therefore
    /// intentionally different from the ordering behavior used by <see cref="CompareTo(ScheduledItem{TAbsolute}?)"/>.
    /// </remarks>
    public static bool operator ==(ScheduledItem<TAbsolute>? left, ScheduledItem<TAbsolute>? right) =>
        ReferenceEquals(left, right);

    /// <summary>Determines whether the first scheduled item is due after the second scheduled item.</summary>
    /// <param name="left">The scheduled item on the left side of the comparison.</param>
    /// <param name="right">The scheduled item on the right side of the comparison.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> is ordered after <paramref name="right"/> by due time;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// This operator uses the same due-time ordering as <see cref="CompareTo(ScheduledItem{TAbsolute}?)"/>.
    /// </remarks>
    public static bool operator >(ScheduledItem<TAbsolute> left, ScheduledItem<TAbsolute> right) =>
        Comparer<ScheduledItem<TAbsolute>>.Default.Compare(left, right) > 0;

    /// <summary>Determines whether the first scheduled item is due after, or at the same time as, the second scheduled item.</summary>
    /// <param name="left">The scheduled item on the left side of the comparison.</param>
    /// <param name="right">The scheduled item on the right side of the comparison.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> is ordered after or equal to <paramref name="right"/> by
    /// due time; otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// This operator uses the same due-time ordering as <see cref="CompareTo(ScheduledItem{TAbsolute}?)"/>.
    /// </remarks>
    public static bool operator >=(ScheduledItem<TAbsolute> left, ScheduledItem<TAbsolute> right) =>
        Comparer<ScheduledItem<TAbsolute>>.Default.Compare(left, right) >= 0;

    /// <summary>Cancels this scheduled work item.</summary>
    /// <remarks>
    /// Canceling prevents work that has not started from running. If the work has already started and the invocation
    /// callback returned a disposable resource, that resource is disposed as soon as it is available.
    /// </remarks>
    public void Cancel() => Dispose();

    /// <summary>Compares this scheduled item with another scheduled item by due time.</summary>
    /// <param name="other">The scheduled item to compare with this instance.</param>
    /// <returns>
    /// A value less than zero when this item is due earlier than <paramref name="other"/>, zero when both items have
    /// the same due-time ordering, or a value greater than zero when this item is due later. Any non-null item compares
    /// greater than <see langword="null"/>.
    /// </returns>
    /// <remarks>
    /// This comparison controls scheduling order. It does not imply object equality; <see cref="Equals(object?)"/> and
    /// the equality operators use reference identity.
    /// </remarks>
    public int CompareTo(ScheduledItem<TAbsolute>? other) =>

        // MSDN: By definition, any object compares greater than null, and two null references compare equal to each other.
        other is null ? 1 : _comparer.Compare(DueTime, other.DueTime);

    /// <summary>Compares this scheduled item with another object by due time.</summary>
    /// <param name="obj">The object to compare with this instance.</param>
    /// <returns>
    /// A value less than zero, zero, or greater than zero indicating this item's relative due-time ordering.
    /// Any non-null item compares greater than <see langword="null"/>.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="obj"/> is not a compatible <see cref="ScheduledItem{TAbsolute}"/>.
    /// </exception>
    public int CompareTo(object? obj)
    {
        if (obj is null)
        {
            return 1;
        }

        if (obj is ScheduledItem<TAbsolute> x)
        {
            return CompareTo(x);
        }

        throw new ArgumentException("Object must be a compatible scheduled item.", nameof(obj));
    }

    /// <summary>Releases the resources associated with this scheduled item and prevents future invocation.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
        {
            return;
        }

        Interlocked.Exchange(ref _disposable, EmptyDisposable.Instance)?.Dispose();
    }

    /// <summary>Determines whether the specified object is the same scheduled item instance.</summary>
    /// <param name="obj">The object to compare with this instance.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="obj"/> is the same object instance; otherwise,
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Equality is based on reference identity, not on <see cref="DueTime"/>. Use
    /// <see cref="CompareTo(ScheduledItem{TAbsolute}?)"/> to compare scheduling order.
    /// </remarks>
    public override bool Equals(object? obj) => ReferenceEquals(this, obj);

    /// <summary>Returns a hash code based on this scheduled item's object identity.</summary>
    /// <returns>A hash code for this scheduled item instance.</returns>
    public override int GetHashCode() => RuntimeHelpers.GetHashCode(this);

    /// <summary>Runs this scheduled work item if it has not been canceled.</summary>
    /// <remarks>
    /// The implementation calls the invocation callback once and stores the disposable it returns. If cancellation
    /// happens before, during, or immediately after invocation, the returned disposable is disposed to propagate
    /// cancellation to any work created by the invocation.
    /// </remarks>
    public void Invoke()
    {
        if (IsDisposed)
        {
            return;
        }

        var disposable = _invokeCore(this);
        var previous = Interlocked.CompareExchange(ref _disposable, disposable, null);
        if (previous is not null)
        {
            disposable.Dispose();
            return;
        }

        if (!IsDisposed)
        {
            return;
        }

        disposable.Dispose();
    }
}
