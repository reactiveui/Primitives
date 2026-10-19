// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalStoreRecordAuthenticationException"/>.</summary>
public sealed class LocalStoreRecordAuthenticationExceptionTests
{
    /// <summary>Verifies authentication failures are found anywhere in an exception chain.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecordAuthenticationFailureIsFoundInExceptionChains()
    {
        var failure = new LocalStoreRecordAuthenticationException();
        var wrapped = new InvalidOperationException("outer", failure);
        var aggregate = new AggregateException(new InvalidOperationException("other"), wrapped);

        await Assert.That(LocalStoreRecordAuthenticationException.IsInChain(failure)).IsTrue();
        await Assert.That(LocalStoreRecordAuthenticationException.IsInChain(wrapped)).IsTrue();
        await Assert.That(LocalStoreRecordAuthenticationException.IsInChain(aggregate)).IsTrue();
        await Assert.That(LocalStoreRecordAuthenticationException.IsInChain(new InvalidOperationException())).IsFalse();
        await Assert.That(LocalStoreRecordAuthenticationException.IsInChain(null)).IsFalse();
    }
}
