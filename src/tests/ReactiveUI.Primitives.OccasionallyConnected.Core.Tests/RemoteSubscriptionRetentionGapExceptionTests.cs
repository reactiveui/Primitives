// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteSubscriptionRetentionGapException"/>.</summary>
public sealed class RemoteSubscriptionRetentionGapExceptionTests
{
    /// <summary>The exception message used by constructor tests.</summary>
    private const string Message = "retention gap";

    /// <summary>Verifies a null expired cursor is retained as an initial-frontier gap.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorAcceptsNullExpiredCursor()
    {
        var streamId = new StreamId("orders/live");
        var subscriptionId = SubscriptionId.New();
        var exception = new RemoteSubscriptionRetentionGapException(streamId, subscriptionId, null, "OC.Gap");

        await Assert.That(exception.StreamId).IsEqualTo(streamId);
        await Assert.That(exception.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(exception.ExpiredCursor).IsNull();
        await Assert.That(exception.ReasonCode).IsEqualTo("OC.Gap");
    }

    /// <summary>Verifies an empty expired cursor is rejected instead of being treated like null.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsEmptyExpiredCursor() =>
        await Assert.That(static () => new RemoteSubscriptionRetentionGapException(new("orders/live"), SubscriptionId.New(), string.Empty, null))
            .ThrowsExactly<ArgumentException>();

    /// <summary>Verifies the parameterless constructor creates a usable exception.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ParameterlessConstructorCreatesException()
    {
        var exception = new RemoteSubscriptionRetentionGapException();

        await Assert.That(exception.Message).IsNotEmpty();
    }

    /// <summary>Verifies the message constructor preserves the supplied message.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MessageConstructorPreservesMessage()
    {
        var exception = new RemoteSubscriptionRetentionGapException(Message);

        await Assert.That(exception.Message).IsEqualTo(Message);
    }

    /// <summary>Verifies the message and inner-exception constructor preserves both arguments.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MessageAndInnerConstructorPreservesArguments()
    {
        var inner = new InvalidOperationException("inner");
        var exception = new RemoteSubscriptionRetentionGapException(Message, inner);

        await Assert.That(exception.Message).IsEqualTo(Message);
        await Assert.That(exception.InnerException).IsSameReferenceAs(inner);
    }
}
