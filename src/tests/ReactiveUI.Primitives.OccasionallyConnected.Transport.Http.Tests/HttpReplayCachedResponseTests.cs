// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpReplayCachedResponse"/>.</summary>
public sealed class HttpReplayCachedResponseTests
{
    /// <summary>The retained content type.</summary>
    private const string ContentType = "application/replay";

    /// <summary>The retained header name.</summary>
    private const string HeaderName = "X-ReactiveUI-Replay-Session-Secret";

    /// <summary>The retained header value.</summary>
    private const string HeaderValue = "secret";

    /// <summary>The first retained body byte.</summary>
    private const byte FirstBodyByte = 1;

    /// <summary>The second retained body byte.</summary>
    private const byte SecondBodyByte = 2;

    /// <summary>The third retained body byte.</summary>
    private const byte ThirdBodyByte = 3;

    /// <summary>The original response body.</summary>
    private static readonly byte[] Body = [FirstBodyByte, SecondBodyByte, ThirdBodyByte];

    /// <summary>Verifies caller-visible body and header snapshots are owned copies.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AccessorsReturnOwnedSnapshots()
    {
        HttpReplayCachedResponse response = new(HttpStatusCode.OK, ContentType, Body, CreateHeaders());
        var firstBody = response.Body.ToArray();
        firstBody[0] = 0;
        var secondBody = response.Body.ToArray();
        var firstHeaders = response.Headers;
        var secondHeaders = response.Headers;

        await Assert.That(secondBody[0]).IsEqualTo(FirstBodyByte);
        await Assert.That(ReferenceEquals(firstHeaders, secondHeaders)).IsFalse();
        await Assert.That(firstHeaders[0].Key).IsEqualTo(HeaderName);
        await Assert.That(secondHeaders[0].Value).IsEqualTo(HeaderValue);
    }

    /// <summary>Verifies clearing retained storage does not mutate an already issued replay snapshot.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ClearZerosRetainedStorageWithoutMutatingExistingSnapshot()
    {
        HttpReplayCachedResponse response = new(HttpStatusCode.Created, ContentType, Body, CreateHeaders());
        var snapshot = response.CreateSnapshot();

        response.Clear();

        await AssertClearedBodyAsync(response.Body.ToArray());
        await Assert.That(response.ContentType).IsEqualTo(new('\0', ContentType.Length));
        await Assert.That(response.Headers[0].Key).IsEqualTo(new('\0', HeaderName.Length));
        await Assert.That(response.Headers[0].Value).IsEqualTo(new('\0', HeaderValue.Length));
        await Assert.That(snapshot.StatusCode).IsEqualTo(HttpStatusCode.Created);
        await Assert.That(snapshot.ContentType).IsEqualTo(ContentType);
        await AssertBodyAsync(snapshot.Body.ToArray());
        await Assert.That(snapshot.Headers[0].Key).IsEqualTo(HeaderName);
        await Assert.That(snapshot.Headers[0].Value).IsEqualTo(HeaderValue);
    }

    /// <summary>Creates the retained replay headers.</summary>
    /// <returns>The retained replay headers.</returns>
    private static KeyValuePair<string, string>[] CreateHeaders() => [new(HeaderName, HeaderValue)];

    /// <summary>Asserts the original body bytes.</summary>
    /// <param name="body">The body bytes.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertBodyAsync(byte[] body)
    {
        await Assert.That(body.Length).IsEqualTo(Body.Length);
        await Assert.That(body[0]).IsEqualTo(FirstBodyByte);
        await Assert.That(body[1]).IsEqualTo(SecondBodyByte);
        await Assert.That(body[2]).IsEqualTo(ThirdBodyByte);
    }

    /// <summary>Asserts cleared body bytes.</summary>
    /// <param name="body">The body bytes.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertClearedBodyAsync(byte[] body)
    {
        await Assert.That(body.Length).IsEqualTo(Body.Length);
        for (var index = 0; index < body.Length; index++)
        {
            await Assert.That((int)body[index]).IsEqualTo(0);
        }
    }
}
