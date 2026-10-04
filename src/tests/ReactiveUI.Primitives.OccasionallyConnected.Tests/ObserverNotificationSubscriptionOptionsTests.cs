// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for ObserverNotificationSubscriptionOptions.</summary>
public sealed class ObserverNotificationSubscriptionOptionsTests
{
    /// <summary>The single-item capacity.</summary>
    private const int OneItem = 1;

    /// <summary>The single-byte capacity.</summary>
    private const long OneByte = 1;

    /// <summary>The invalid zero-item capacity.</summary>
    private const int None = 0;

    /// <summary>The invalid zero-byte capacity.</summary>
    private const long NoBytes = 0;

    /// <summary>An undefined overflow mode.</summary>
    private const int SecondValue = 2;

    /// <summary>Verifies all subscription option validation branches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionOptionsValidateEveryInvalidValue()
    {
        await Assert
            .That(static () => new ObserverNotificationSubscriptionOptions(None, OneByte, ObserverNotificationOverflowMode.CoalesceLatest).Validate())
            .ThrowsExactly<InvalidOperationException>();
        await Assert
            .That(static () => new ObserverNotificationSubscriptionOptions(OneItem, NoBytes, ObserverNotificationOverflowMode.CoalesceLatest).Validate())
            .ThrowsExactly<InvalidOperationException>();
        await Assert
            .That(static () => new ObserverNotificationSubscriptionOptions(OneItem, OneByte, (ObserverNotificationOverflowMode)SecondValue).Validate())
            .ThrowsExactly<InvalidOperationException>();
    }
}
