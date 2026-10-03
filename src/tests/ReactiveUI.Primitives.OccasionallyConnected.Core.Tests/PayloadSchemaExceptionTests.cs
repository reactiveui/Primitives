// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="PayloadSchemaException"/>.</summary>
public sealed class PayloadSchemaExceptionTests
{
    /// <summary>The exception message used by tests.</summary>
    private const string ExceptionMessage = "Schema failure.";

    /// <summary>Verifies the default constructor keeps the default reason.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultConstructorKeepsUnknownReason()
    {
        PayloadSchemaException exception = new();

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Verifies the message constructor stores the message.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MessageConstructorStoresMessage()
    {
        PayloadSchemaException exception = new(ExceptionMessage);

        await Assert.That(exception.Message).IsEqualTo(ExceptionMessage);
        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Verifies the message and inner exception constructor stores both values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MessageAndInnerConstructorStoresValues()
    {
        InvalidOperationException innerException = new(ExceptionMessage);
        PayloadSchemaException exception = new(ExceptionMessage, innerException);

        await Assert.That(exception.Message).IsEqualTo(ExceptionMessage);
        await Assert.That(exception.InnerException).IsEqualTo(innerException);
    }

    /// <summary>Verifies the reason constructor stores the reason.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReasonConstructorStoresReason()
    {
        PayloadSchemaException exception = new(PayloadSchemaFailureReason.PayloadHashMismatch, ExceptionMessage);

        await Assert.That(exception.Message).IsEqualTo(ExceptionMessage);
        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadHashMismatch);
    }

    /// <summary>Verifies the reason and inner exception constructor stores all values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReasonAndInnerConstructorStoresValues()
    {
        InvalidOperationException innerException = new(ExceptionMessage);
        PayloadSchemaException exception = new(PayloadSchemaFailureReason.UpcasterFailed, ExceptionMessage, innerException);

        await Assert.That(exception.Message).IsEqualTo(ExceptionMessage);
        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.UpcasterFailed);
        await Assert.That(exception.InnerException).IsEqualTo(innerException);
    }
}
