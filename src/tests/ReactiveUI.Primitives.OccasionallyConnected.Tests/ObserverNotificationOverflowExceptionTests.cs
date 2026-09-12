// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for ObserverNotificationOverflowException.</summary>
public sealed class ObserverNotificationOverflowExceptionTests
{
    /// <summary>The single-item capacity.</summary>
    private const int OneItem = 1;

    /// <summary>The single-byte capacity.</summary>
    private const long OneByte = 1;

    /// <summary>The oversized notification length.</summary>
    private const long TwoBytes = 2;

    /// <summary>Verifies the internal overflow exception constructors preserve message and inner exception data.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OverflowExceptionConstructorsExposeExpectedState()
    {
        var defaultException = new ObserverNotificationOverflowException();
        var messageException = new ObserverNotificationOverflowException("message");
        var inner = new InvalidOperationException("inner");
        var wrapped = new ObserverNotificationOverflowException("outer", inner);
        var bounded = new ObserverNotificationOverflowException(OneItem, OneByte, TwoBytes);

        await Assert.That(defaultException).IsNotNull();
        await Assert.That(messageException.Message).IsEqualTo("message");
        await Assert.That(wrapped.InnerException).IsSameReferenceAs(inner);
        await Assert.That(bounded.Capacity).IsEqualTo(OneItem);
        await Assert.That(bounded.CapacityBytes).IsEqualTo(OneByte);
        await Assert.That(bounded.NotificationBytes).IsEqualTo(TwoBytes);
    }
}
