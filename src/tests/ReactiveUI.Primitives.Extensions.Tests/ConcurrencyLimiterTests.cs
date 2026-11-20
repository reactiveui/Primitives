// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for <see cref="ConcurrencyLimiter{T}"/>.</summary>
public sealed class ConcurrencyLimiterTests
{
    /// <summary>Verifies <see cref="ConcurrencyLimiter{T}.Observable"/> returns the limiter instance itself.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObservablePropertyRead_ThenReturnsLimiterInstance()
    {
        var limiter = ConcurrencyLimiter<int>.Create([Task.FromResult(1)], 1);

        await Assert.That(limiter.Observable).IsSameReferenceAs(limiter);
    }
}
