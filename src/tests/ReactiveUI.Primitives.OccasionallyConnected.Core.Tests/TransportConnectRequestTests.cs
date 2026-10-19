// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="TransportConnectRequest"/>.</summary>
public sealed class TransportConnectRequestTests
{
    /// <summary>Verifies guarantees are copied from caller-owned collections.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorCopiesRequiredGuarantees()
    {
        var guarantees = new List<DeliveryGuarantee> { DeliveryGuarantee.AtLeastOnce };
        var request = new TransportConnectRequest(CreateRange(), new("client-1", "tenant-1"), guarantees);
        guarantees.Add(DeliveryGuarantee.ExactlyOnce);
        await Assert.That(request.RequiredGuarantees).Count().IsEqualTo(1);
    }

    /// <summary>Verifies null guarantees are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullRequiredGuarantees()
    {
        var constructor = typeof(TransportConnectRequest).GetConstructors().Single();
        var exception = await Assert.That(() => constructor.Invoke([CreateRange(), new ClientIdentity("client-1"), null])).ThrowsExactly<TargetInvocationException>();

        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
    }

    /// <summary>Creates a representative protocol version range.</summary>
    /// <returns>A protocol version range.</returns>
    private static VersionRange CreateRange() => new(new(1, 0), new(1, 1));
}
