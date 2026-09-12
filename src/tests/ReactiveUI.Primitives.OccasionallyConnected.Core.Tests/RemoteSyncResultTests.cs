// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteSyncResult"/>.</summary>
public sealed class RemoteSyncResultTests
{
    /// <summary>Verifies null results are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullResults()
    {
        var constructor = typeof(RemoteSyncResult).GetConstructors().Single();
        var exception = await Assert.That(() => constructor.Invoke([Guid.NewGuid(), null, null, null])).ThrowsExactly<TargetInvocationException>();

        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
    }
}
