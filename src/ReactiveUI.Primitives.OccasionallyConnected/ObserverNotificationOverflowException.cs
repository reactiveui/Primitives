// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a subscription notification queue overflow.</summary>
internal sealed class ObserverNotificationOverflowException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="ObserverNotificationOverflowException"/> class.</summary>
    internal ObserverNotificationOverflowException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ObserverNotificationOverflowException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    internal ObserverNotificationOverflowException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ObserverNotificationOverflowException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal ObserverNotificationOverflowException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ObserverNotificationOverflowException"/> class.</summary>
    /// <param name="capacity">The configured data item capacity.</param>
    /// <param name="capacityBytes">The configured data byte capacity.</param>
    /// <param name="notificationBytes">The incoming notification size.</param>
    internal ObserverNotificationOverflowException(int capacity, long capacityBytes, long notificationBytes)
        : base("The observer notification queue exceeded its configured capacity.")
    {
        Capacity = capacity;
        CapacityBytes = capacityBytes;
        NotificationBytes = notificationBytes;
    }

    /// <summary>Gets the configured data item capacity.</summary>
    internal int Capacity { get; }

    /// <summary>Gets the configured data byte capacity.</summary>
    internal long CapacityBytes { get; }

    /// <summary>Gets the incoming notification size.</summary>
    internal long NotificationBytes { get; }
}
