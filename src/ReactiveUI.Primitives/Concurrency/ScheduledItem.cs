// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Abstract base class for scheduled work items.
/// </summary>
/// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
public abstract class ScheduledItem<TAbsolute> : IScheduledItem<TAbsolute>, IComparable<ScheduledItem<TAbsolute>>, IsDisposed, IComparable
    where TAbsolute : IComparable<TAbsolute>
{
    /// <summary>
    /// Compares scheduled items by due time.
    /// </summary>
    private readonly IComparer<TAbsolute> _comparer;

    /// <summary>
    /// Holds the disposable returned by the scheduled action.
    /// </summary>
    private IDisposable? _disposable;

    /// <summary>
    /// Tracks cancellation without taking a lock.
    /// </summary>
    private int _isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ScheduledItem{TAbsolute}"/> class.
    /// Creates a new scheduled work item to run at the specified time.
    /// </summary>
    /// <param name="dueTime">Absolute time at which the work item has to be executed.</param>
    /// <param name="comparer">Comparer used to compare work items based on their scheduled time.</param>
    /// <exception cref="System.ArgumentNullException">comparer.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="comparer" /> is <c>null</c>.</exception>
    protected ScheduledItem(TAbsolute dueTime, IComparer<TAbsolute> comparer)
    {
        DueTime = dueTime;
        _comparer = comparer ?? throw new ArgumentNullException(nameof(comparer));
    }

    /// <summary>
    /// Gets the absolute time at which the item is due for invocation.
    /// </summary>
    public TAbsolute DueTime { get; }

    /// <summary>
    /// Gets a value indicating whether gets whether the work item has received a cancellation request.
    /// </summary>
    public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <summary>
    /// Determines whether two specified <see cref="ScheduledItem{TAbsolute, TValue}" /> objects are inequal.
    /// </summary>
    /// <param name="left">The first object to compare.</param>
    /// <param name="right">The second object to compare.</param>
    /// <returns><c>true</c> if both <see cref="ScheduledItem{TAbsolute, TValue}" /> are inequal; otherwise, <c>false</c>.</returns>
    /// <remarks>This operator does not provide results consistent with the IComparable implementation. Instead, it implements reference equality.</remarks>
    public static bool operator !=(ScheduledItem<TAbsolute>? left, ScheduledItem<TAbsolute>? right) => !(left == right);

    /// <summary>
    /// Determines whether one specified <see cref="ScheduledItem{TAbsolute}" /> object is due before a second specified <see cref="ScheduledItem{TAbsolute}" /> object.
    /// </summary>
    /// <param name="left">The first object to compare.</param>
    /// <param name="right">The second object to compare.</param>
    /// <returns><c>true</c> if the <see cref="DueTime"/> value of left is earlier than the <see cref="DueTime"/> value of right; otherwise, <c>false</c>.</returns>
    /// <remarks>This operator provides results consistent with the <see cref="IComparable"/> implementation.</remarks>
    public static bool operator <(ScheduledItem<TAbsolute> left, ScheduledItem<TAbsolute> right) => Comparer<ScheduledItem<TAbsolute>>.Default.Compare(left, right) < 0;

    /// <summary>
    /// Determines whether one specified <see cref="ScheduledItem{TAbsolute}" /> object is due before or at the same of a second specified <see cref="ScheduledItem{TAbsolute}" /> object.
    /// </summary>
    /// <param name="left">The first object to compare.</param>
    /// <param name="right">The second object to compare.</param>
    /// <returns><c>true</c> if the <see cref="DueTime"/> value of left is earlier than or simultaneous with the <see cref="DueTime"/> value of right; otherwise, <c>false</c>.</returns>
    /// <remarks>This operator provides results consistent with the <see cref="IComparable"/> implementation.</remarks>
    public static bool operator <=(ScheduledItem<TAbsolute> left, ScheduledItem<TAbsolute> right) => Comparer<ScheduledItem<TAbsolute>>.Default.Compare(left, right) <= 0;

    /// <summary>
    /// Determines whether two specified <see cref="ScheduledItem{TAbsolute, TValue}" /> objects are equal.
    /// </summary>
    /// <param name="left">The first object to compare.</param>
    /// <param name="right">The second object to compare.</param>
    /// <returns><c>true</c> if both <see cref="ScheduledItem{TAbsolute, TValue}" /> are equal; otherwise, <c>false</c>.</returns>
    /// <remarks>This operator does not provide results consistent with the IComparable implementation. Instead, it implements reference equality.</remarks>
    public static bool operator ==(ScheduledItem<TAbsolute>? left, ScheduledItem<TAbsolute>? right) => ReferenceEquals(left, right);

    /// <summary>
    /// Determines whether one specified <see cref="ScheduledItem{TAbsolute}" /> object is due after a second specified <see cref="ScheduledItem{TAbsolute}" /> object.
    /// </summary>
    /// <param name="left">The first object to compare.</param>
    /// <param name="right">The second object to compare.</param>
    /// <returns><c>true</c> if the <see cref="DueTime"/> value of left is later than the <see cref="DueTime"/> value of right; otherwise, <c>false</c>.</returns>
    /// <remarks>This operator provides results consistent with the <see cref="IComparable"/> implementation.</remarks>
    public static bool operator >(ScheduledItem<TAbsolute> left, ScheduledItem<TAbsolute> right) => Comparer<ScheduledItem<TAbsolute>>.Default.Compare(left, right) > 0;

    /// <summary>
    /// Determines whether one specified <see cref="ScheduledItem{TAbsolute}" /> object is due after or at the same time of a second specified <see cref="ScheduledItem{TAbsolute}" /> object.
    /// </summary>
    /// <param name="left">The first object to compare.</param>
    /// <param name="right">The second object to compare.</param>
    /// <returns><c>true</c> if the <see cref="DueTime"/> value of left is later than or simultaneous with the <see cref="DueTime"/> value of right; otherwise, <c>false</c>.</returns>
    /// <remarks>This operator provides results consistent with the <see cref="IComparable"/> implementation.</remarks>
    public static bool operator >=(ScheduledItem<TAbsolute> left, ScheduledItem<TAbsolute> right) => Comparer<ScheduledItem<TAbsolute>>.Default.Compare(left, right) >= 0;

    /// <summary>
    /// Cancels the work item by disposing the resource returned by <see cref="InvokeCore"/> as soon as possible.
    /// </summary>
    public void Cancel() => Dispose();

    /// <summary>
    /// Compares the work item with another work item based on absolute time values.
    /// </summary>
    /// <param name="other">Work item to compare the current work item to.</param>
    /// <returns>Relative ordering between this and the specified work item.</returns>
    /// <remarks>
    /// The inequality operators are overloaded to provide results consistent with the
    /// <see cref="IComparable"/> implementation. Equality operators implement traditional
    /// reference equality semantics.
    /// </remarks>
    public int CompareTo(ScheduledItem<TAbsolute>? other)
    {
        // MSDN: By definition, any object compares greater than null, and two null references compare equal to each other.
        if (other is null)
        {
            return 1;
        }

        return _comparer.Compare(DueTime, other.DueTime);
    }

    /// <summary>
    /// Compares the current instance with another object of the same type and returns an integer that indicates relative ordering.
    /// </summary>
    /// <param name="obj">An object to compare with this instance.</param>
    /// <returns>A value that indicates the relative order of the objects being compared.</returns>
    public int CompareTo(object? obj)
    {
        if (obj == null)
        {
            return 1;
        }

        if (obj is ScheduledItem<TAbsolute> x)
        {
            return CompareTo(x);
        }

        throw new ArgumentException("Object must be a compatible scheduled item.", nameof(obj));
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Determines whether a <see cref="ScheduledItem{TAbsolute}" /> object is equal to the specified object.
    /// </summary>
    /// <param name="obj">The object to compare to the current <see cref="ScheduledItem{TAbsolute}" /> object.</param>
    /// <returns>
    /// <c>true</c> if the obj parameter is a <see cref="ScheduledItem{TAbsolute}" />
    /// object and is equal to the current <see cref="ScheduledItem{TAbsolute}" />
    /// object; otherwise, <c>false</c>.
    /// </returns>
    public override bool Equals(object? obj) => ReferenceEquals(this, obj);

    /// <summary>
    /// Returns the hash code for the current <see cref="ScheduledItem{TAbsolute}" /> object.
    /// </summary>
    /// <returns>A 32-bit signed integer hash code.</returns>
    public override int GetHashCode() => RuntimeHelpers.GetHashCode(this);

    /// <summary>
    /// Invokes the work item.
    /// </summary>
    public void Invoke()
    {
        if (IsDisposed)
        {
            return;
        }

        var disposable = InvokeCore() ?? Disposable.Empty;
        var previous = Interlocked.CompareExchange(ref _disposable, disposable, null);
        if (previous != null)
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

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing || Interlocked.Exchange(ref _isDisposed, 1) != 0)
        {
            return;
        }

        Interlocked.Exchange(ref _disposable, Disposable.Empty)?.Dispose();
    }

    /// <summary>
    /// Implement this method to perform the work item invocation, returning a disposable object for deep cancellation.
    /// </summary>
    /// <returns>Disposable object used to cancel the work item and/or derived work items.</returns>
    protected abstract IDisposable InvokeCore();
}
