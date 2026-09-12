// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteEventOrigin"/>.</summary>
public sealed class RemoteEventOriginTests
{
    /// <summary>A valid trusted client identity.</summary>
    private const string ClientId = "client";

    /// <summary>The maximum trusted client identity length.</summary>
    private const int MaximumClientIdCharacters = 256;

    /// <summary>The valid supplementary code point used in identity testing.</summary>
    private const int ValidSupplementaryCodePoint = 128_512;

    /// <summary>Verifies origin records use value equality and preserve their constructor values.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConstructorCreatesEqualImmutableOrigins()
    {
        var operationId = OperationId.New();
        var first = new RemoteEventOrigin(ClientId, operationId);
        var second = new RemoteEventOrigin(ClientId, operationId);

        await Assert.That(first).IsEqualTo(second);
        await Assert.That(first.ClientId).IsEqualTo(ClientId);
        await Assert.That(first.OperationId).IsEqualTo(operationId);
    }

    /// <summary>Verifies client and operation identity each participate in origin equality.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task EqualityIsolatedByClientAndOperation()
    {
        var operationId = OperationId.New();
        var differentOperationId = OperationId.New();
        var origin = new RemoteEventOrigin(ClientId, operationId);

        await Assert.That(origin).IsNotEqualTo(new("other-client", operationId));
        await Assert.That(origin).IsNotEqualTo(new(ClientId, differentOperationId));
    }

    /// <summary>Verifies malformed and invalid client identities are rejected.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConstructorRejectsInvalidClientId()
    {
        var operationId = OperationId.New();
        var oversized = new string('x', MaximumClientIdCharacters + 1);
        var malformed = new string('\ud800', 1);
        var oversizedMalformed = $"{oversized}\ud800";

        await Assert.That(() => new RemoteEventOrigin(" ", operationId)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => new RemoteEventOrigin(oversized, operationId)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => new RemoteEventOrigin(oversizedMalformed, operationId)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => new RemoteEventOrigin(malformed, operationId)).ThrowsExactly<EncoderFallbackException>();
    }

    /// <summary>Verifies Unicode client identities with valid surrogate pairs are retained exactly.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConstructorAcceptsUnicodeClientId()
    {
        var clientId = $"{ClientId}-{char.ConvertFromUtf32(ValidSupplementaryCodePoint)}";
        var origin = new RemoteEventOrigin(clientId, OperationId.New());

        await Assert.That(origin.ClientId).IsEqualTo(clientId);
    }

    /// <summary>Verifies the inclusive client identity size bound is accepted.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConstructorAcceptsMaximumLengthClientId()
    {
        var clientId = new string('x', MaximumClientIdCharacters);
        var origin = new RemoteEventOrigin(clientId, OperationId.New());

        await Assert.That(origin.ClientId).IsEqualTo(clientId);
    }

    /// <summary>Verifies client identity preserves ordinally distinct composed and decomposed Unicode text.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConstructorPreservesUnicodeWithoutNormalization()
    {
        const string composed = "caf\u00e9";
        const string decomposed = "cafe\u0301";
        var operationId = OperationId.New();
        var composedOrigin = new RemoteEventOrigin(composed, operationId);
        var decomposedOrigin = new RemoteEventOrigin(decomposed, operationId);

        await Assert.That(composedOrigin.ClientId).IsEqualTo(composed);
        await Assert.That(decomposedOrigin.ClientId).IsEqualTo(decomposed);
        await Assert.That(composedOrigin).IsNotEqualTo(decomposedOrigin);
    }

    /// <summary>Verifies empty operation identifiers are rejected.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConstructorRejectsDefaultOperationId() =>
        await Assert.That(static () => new RemoteEventOrigin(ClientId, default)).ThrowsExactly<ArgumentException>();
}
