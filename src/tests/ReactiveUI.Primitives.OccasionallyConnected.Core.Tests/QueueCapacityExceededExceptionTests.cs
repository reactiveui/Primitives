// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="QueueCapacityExceededException"/>.</summary>
public sealed class QueueCapacityExceededExceptionTests
{
    /// <summary>Verifies a full queue failure retains its message and recovery hint.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FullQueueFailureRetainsMessageAndCanFitWhenEmpty()
    {
        const string message = "The queue is full.";

        var exception = Assert.ThrowsExactly<QueueCapacityExceededException>(
            static () => throw new QueueCapacityExceededException("The queue is full.", canFitWhenEmpty: true));

        await Assert.That(exception.Message).IsEqualTo(message);
        await Assert.That(exception.CanFitWhenEmpty).IsTrue();
    }

    /// <summary>Verifies an oversized item failure retains its message and permanent rejection hint.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OversizedItemFailureRetainsMessageAndCannotFitWhenEmpty()
    {
        const string message = "The item exceeds the queue's configured capacity.";

        var exception = Assert.ThrowsExactly<QueueCapacityExceededException>(
            static () => throw new QueueCapacityExceededException(
                "The item exceeds the queue's configured capacity.",
                canFitWhenEmpty: false));

        await Assert.That(exception.Message).IsEqualTo(message);
        await Assert.That(exception.CanFitWhenEmpty).IsFalse();
    }

    /// <summary>Verifies a null failure message is rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The expected constructor is unavailable.</exception>
    [Test]
    public async Task NullMessageIsRejected()
    {
        var constructor = typeof(QueueCapacityExceededException).GetConstructor([typeof(string), typeof(bool)])
            ?? throw new InvalidOperationException("The queue exception constructor is unavailable.");
        var exception = await Assert.That(() => constructor.Invoke([null, true])).ThrowsExactly<TargetInvocationException>();

        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
        await Assert.That((exception?.InnerException as ArgumentNullException)?.ParamName).IsEqualTo("message");
    }

    /// <summary>Verifies wrapping an underlying failure does not accept a null message.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The expected constructor is unavailable.</exception>
    [Test]
    public async Task WrappedFailureRejectsNullMessage()
    {
        var constructor = typeof(QueueCapacityExceededException).GetConstructor([typeof(string), typeof(Exception)])
            ?? throw new InvalidOperationException("The queue exception constructor is unavailable.");
        var exception = await Assert.That(() => constructor.Invoke([null, new InvalidOperationException("storage")])).ThrowsExactly<TargetInvocationException>();

        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
        await Assert.That((exception?.InnerException as ArgumentNullException)?.ParamName).IsEqualTo("message");
    }

    /// <summary>Verifies the standard exception constructors preserve the inherited exception state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StandardExceptionConstructorsPreserveInheritedState()
    {
        const string message = "The queue admission failed.";
        InvalidOperationException innerException = new(message);
        QueueCapacityExceededException defaultException = new();
        QueueCapacityExceededException messageException = new(message);
        QueueCapacityExceededException wrappedException = new(message, innerException);

        await Assert.That(defaultException.CanFitWhenEmpty).IsFalse();
        await Assert.That(defaultException.Message).IsEqualTo(string.Empty);
        await Assert.That(messageException.Message).IsEqualTo(message);
        await Assert.That(messageException.CanFitWhenEmpty).IsFalse();
        await Assert.That(wrappedException.InnerException).IsSameReferenceAs(innerException);
        await Assert.That(wrappedException.Message).IsEqualTo(message);
        await Assert.That(wrappedException.CanFitWhenEmpty).IsFalse();
    }
}
