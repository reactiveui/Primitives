// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RetryOptions"/>.</summary>
public sealed class RetryOptionsTests
{
    /// <summary>The default minimum retry delay in milliseconds.</summary>
    private const int DefaultMinimumDelayMilliseconds = 500;

    /// <summary>The default maximum retry delay in seconds.</summary>
    private const int DefaultMaximumDelaySeconds = 30;

    /// <summary>The default maximum retry attempts.</summary>
    private const int DefaultMaximumRetryAttempts = 8;

    /// <summary>The default maximum retry age in minutes.</summary>
    private const int DefaultMaximumRetryAgeMinutes = 15;

    /// <summary>The shorter retry delay in seconds.</summary>
    private const int ShortDelaySeconds = 1;

    /// <summary>The longer retry delay in seconds.</summary>
    private const int LongDelaySeconds = 2;

    /// <summary>An invalid negative retry attempt count.</summary>
    private const int NegativeRetryAttempts = -1;

    /// <summary>Verifies the finite default retry bounds.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultsUseFiniteDecorrelatedJitterBounds()
    {
        var options = RetryOptions.Default;

        options.Validate();

        await Assert.That(options.MinimumDelay).IsEqualTo(TimeSpan.FromMilliseconds(DefaultMinimumDelayMilliseconds));
        await Assert.That(options.MaximumDelay).IsEqualTo(TimeSpan.FromSeconds(DefaultMaximumDelaySeconds));
        await Assert.That(options.MaximumRetryAttempts).IsEqualTo(DefaultMaximumRetryAttempts);
        await Assert.That(options.MaximumRetryAge).IsEqualTo(TimeSpan.FromMinutes(DefaultMaximumRetryAgeMinutes));
    }

    /// <summary>Verifies invalid delay bounds are rejected by validation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenMinimumDelayIsNotPositive_ThenValidateThrowsArgumentOutOfRangeException()
    {
        var options = new RetryOptions { MinimumDelay = TimeSpan.Zero };
        var action = options.Validate;

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies maximum delay validation is independent of initializer order.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenMaximumDelayIsAssignedBeforeLargerMinimumDelay_ThenValidateThrowsArgumentOutOfRangeException()
    {
        var options = new RetryOptions { MaximumDelay = TimeSpan.FromSeconds(ShortDelaySeconds), MinimumDelay = TimeSpan.FromSeconds(LongDelaySeconds) };
        var action = options.Validate;

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies maximum delay validation is independent of initializer order.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenMinimumDelayIsAssignedBeforeSmallerMaximumDelay_ThenValidateThrowsArgumentOutOfRangeException()
    {
        var options = new RetryOptions { MinimumDelay = TimeSpan.FromSeconds(LongDelaySeconds), MaximumDelay = TimeSpan.FromSeconds(ShortDelaySeconds) };
        var action = options.Validate;

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies valid copied delay bounds are accepted when the maximum is assigned first.</summary>
    [Test]
    public void WhenCopiedMaximumDelayIsAssignedBeforeMinimumDelay_ThenValidateSucceeds()
    {
        var options = RetryOptions.Default with { MaximumDelay = TimeSpan.FromSeconds(LongDelaySeconds), MinimumDelay = TimeSpan.FromSeconds(ShortDelaySeconds) };

        options.Validate();
    }

    /// <summary>Verifies valid copied delay bounds are accepted when the minimum is assigned first.</summary>
    [Test]
    public void WhenCopiedMinimumDelayIsAssignedBeforeMaximumDelay_ThenValidateSucceeds()
    {
        var options = RetryOptions.Default with { MinimumDelay = TimeSpan.FromSeconds(ShortDelaySeconds), MaximumDelay = TimeSpan.FromSeconds(LongDelaySeconds) };

        options.Validate();
    }

    /// <summary>Verifies retry attempts are bounded by validation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenMaximumRetryAttemptsIsNegative_ThenValidateThrowsArgumentOutOfRangeException()
    {
        var options = new RetryOptions { MaximumRetryAttempts = NegativeRetryAttempts };
        var action = options.Validate;

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies retry age is bounded by validation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenMaximumRetryAgeIsNotPositive_ThenValidateThrowsArgumentOutOfRangeException()
    {
        var options = new RetryOptions { MaximumRetryAge = TimeSpan.Zero };
        var action = options.Validate;

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
