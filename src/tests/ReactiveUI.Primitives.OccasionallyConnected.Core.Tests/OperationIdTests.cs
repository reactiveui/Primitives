// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OperationId"/>.</summary>
public sealed class OperationIdTests
{
    /// <summary>Verifies the default GUID version follows the target framework.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task NewUsesTheFrameworkDefaultGuidVersion()
    {
#if NET9_0_OR_GREATER
        const char expectedVersion = '7';
#else
        const char expectedVersion = '4';
#endif
        await Assert.That(OperationId.New().Value.ToString("D")[14]).IsEqualTo(expectedVersion);
    }

    /// <summary>Verifies callers can explicitly request random version 4 identifiers.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task NewSupportsVersion4OptOut() =>
        await Assert.That(OperationId.New(false).Value.ToString("D")[14]).IsEqualTo('4');

    /// <summary>Verifies new operation identifiers are non-empty.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task NewCreatesNonEmptyIdentifier()
    {
        var operationId = OperationId.New();

        await Assert.That(operationId.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies new operation identifiers are unique across calls.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task NewCreatesDistinctIdentifiers()
    {
        var first = OperationId.New();
        var second = OperationId.New();

        await Assert.That(first).IsNotEqualTo(second);
    }
}
