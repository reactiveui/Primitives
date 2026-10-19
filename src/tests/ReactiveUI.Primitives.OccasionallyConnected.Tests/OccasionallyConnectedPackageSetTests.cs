// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using OccasionallyConnected.Ci;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests the framework scope shared by neutral validation commands.</summary>
public sealed class OccasionallyConnectedPackageSetTests
{
    /// <summary>Verifies neutral commands clear native source heads without replacing shared framework policy.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task NeutralValidationClearsOnlyNativeHeads()
    {
        var properties = OccasionallyConnectedPackageSet.NeutralFrameworkProperties;
        await Assert.That(properties).Contains("-p:AndroidPrimitivesTargetFrameworks=");
        await Assert.That(properties).Contains("-p:ApplePrimitivesTargetFrameworks=");
        await Assert.That(properties).Contains("-p:MobilePlatformTargetFrameworks=");
        await Assert.That(Array.Exists(properties, static property => property.StartsWith("-p:NetTargetFrameworks=", StringComparison.Ordinal))).IsFalse();
        await Assert.That(Array.Exists(properties, static property => property.StartsWith("-p:MauiTargetFrameworks=", StringComparison.Ordinal))).IsFalse();
    }
}
