// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <summary>Tests client option boundaries before opening any transport or store.</summary>
public sealed class CollaborationClientOptionsTests
{
    /// <summary>Verifies invalid endpoints and resource limits fail before allocation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateRejectsInvalidEndpointAndResourceLimits()
    {
        CollaborationClientOptions valid = new() { ServerUri = new("http://127.0.0.1:5088"), DatabasePath = "client.db", Token = "token-a", ClientId = "client-a" };

        _ = await Assert.ThrowsAsync<ArgumentException>(() => ValidateAsync(valid with
        {
            ServerUri = new("relative", UriKind.Relative),
        }));
        _ = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ValidateAsync(valid with
        {
            WaitTimeout = TimeSpan.Zero,
        }));
        _ = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ValidateAsync(valid with
        {
            MaximumTransportBytes = 0,
        }));
    }

    /// <summary>Validates an option record within an asynchronous test assertion.</summary>
    /// <param name="options">The options to check.</param>
    /// <returns>The validation task.</returns>
    private static Task ValidateAsync(CollaborationClientOptions options)
    {
        options.Validate();
        return Task.CompletedTask;
    }
}
