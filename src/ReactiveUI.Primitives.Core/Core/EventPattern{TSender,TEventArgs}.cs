// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Core;

/// <summary>Represents a .NET event notification as a value, keeping the sender's static type.</summary>
/// <typeparam name="TSender">The event sender type.</typeparam>
/// <typeparam name="TEventArgs">The event arguments type.</typeparam>
[System.Diagnostics.DebuggerDisplay("EventPattern: {DebuggerDisplay,nq}")]
public readonly struct EventPattern<TSender, TEventArgs> : IEquatable<EventPattern<TSender, TEventArgs>>
{
    /// <summary>Initializes a new instance of the <see cref="EventPattern{TSender, TEventArgs}"/> struct.</summary>
    /// <param name="sender">The event sender.</param>
    /// <param name="eventArgs">The event arguments.</param>
    public EventPattern(TSender sender, TEventArgs eventArgs)
    {
        Sender = sender;
        EventArgs = eventArgs;
    }

    /// <summary>Gets the event sender.</summary>
    public TSender Sender { get; }

    /// <summary>Gets the event arguments.</summary>
    public TEventArgs EventArgs { get; }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString();

    /// <summary>Compares two event pattern values for equality.</summary>
    /// <param name="left">The first value.</param>
    /// <param name="right">The second value.</param>
    /// <returns><see langword="true"/> when the values are equal; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator ==(EventPattern<TSender, TEventArgs> left, EventPattern<TSender, TEventArgs> right) =>
        left.Equals(right);

    /// <summary>Compares two event pattern values for inequality.</summary>
    /// <param name="left">The first value.</param>
    /// <param name="right">The second value.</param>
    /// <returns><see langword="true"/> when the values are not equal; otherwise, <see langword="false"/>.</returns>
    public static bool operator !=(EventPattern<TSender, TEventArgs> left, EventPattern<TSender, TEventArgs> right) =>
        !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(EventPattern<TSender, TEventArgs> other) =>
        EqualityComparer<TSender>.Default.Equals(Sender, other.Sender)
        && EqualityComparer<TEventArgs>.Default.Equals(EventArgs, other.EventArgs);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is EventPattern<TSender, TEventArgs> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var senderHashCode = Sender is null ? 0 : EqualityComparer<TSender>.Default.GetHashCode(Sender);
        var eventArgsHashCode = EventArgs is null ? 0 : EqualityComparer<TEventArgs>.Default.GetHashCode(EventArgs);
        return (senderHashCode * 397) ^ eventArgsHashCode;
    }

    /// <inheritdoc/>
    public override string ToString() => $"{Sender}: {EventArgs}";
}
