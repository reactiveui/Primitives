// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>Represents a .NET event notification as a value.</summary>
/// <typeparam name="TEventArgs">The event arguments type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public readonly struct EventPattern<TEventArgs> : IEquatable<EventPattern<TEventArgs>>
    where TEventArgs : EventArgs
{
    /// <summary>Initializes a new instance of the <see cref="EventPattern{TEventArgs}"/> struct.</summary>
    /// <param name="sender">The event sender.</param>
    /// <param name="eventArgs">The event arguments.</param>
    public EventPattern(object? sender, TEventArgs eventArgs)
    {
        Sender = sender;
        EventArgs = eventArgs ?? throw new ArgumentNullException(nameof(eventArgs));
    }

    /// <summary>Gets the event sender.</summary>
    public object? Sender { get; }

    /// <summary>Gets the event arguments.</summary>
    public TEventArgs EventArgs { get; }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString();

    /// <summary>Compares two event pattern values for equality.</summary>
    /// <param name="left">The first value.</param>
    /// <param name="right">The second value.</param>
    /// <returns><see langword="true"/> when the values are equal; otherwise, <see langword="false"/>.</returns>
    public static bool operator ==(EventPattern<TEventArgs> left, EventPattern<TEventArgs> right) => left.Equals(right);

    /// <summary>Compares two event pattern values for inequality.</summary>
    /// <param name="left">The first value.</param>
    /// <param name="right">The second value.</param>
    /// <returns><see langword="true"/> when the values are not equal; otherwise, <see langword="false"/>.</returns>
    public static bool operator !=(EventPattern<TEventArgs> left, EventPattern<TEventArgs> right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(EventPattern<TEventArgs> other) =>
        ReferenceEquals(Sender, other.Sender) && EqualityComparer<TEventArgs>.Default.Equals(EventArgs, other.EventArgs);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is EventPattern<TEventArgs> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var senderHashCode = Sender?.GetHashCode() ?? 0;
        return (senderHashCode * 397) ^ EqualityComparer<TEventArgs>.Default.GetHashCode(EventArgs);
    }

    /// <inheritdoc/>
    public override string ToString() => $"{Sender}: {EventArgs}";
}
