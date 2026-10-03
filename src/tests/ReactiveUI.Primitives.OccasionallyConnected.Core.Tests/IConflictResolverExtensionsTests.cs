// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IConflictResolverExtensions"/>.</summary>
public sealed class IConflictResolverExtensionsTests
{
    /// <summary>Verifies the convenience overload forwards context and preserves the returned decision.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ResolveAsyncForwardsContextWithoutCancellation()
    {
        var result = new ConflictResolutionResult([], [], [], [], "v1");
        var resolver = new RecordingResolver(result);
        var state = new ServerState(new("counter"), "v1", new("counter", 1, "application/json", ReadOnlyMemory<byte>.Empty, "hash"));
        var context = new ConflictContext(state, [], new("device"));

        var actual = await resolver.ResolveAsync(context);

        await Assert.That(actual).IsSameReferenceAs(result);
        await Assert.That(resolver.Context).IsSameReferenceAs(context);
        await Assert.That(resolver.Token).IsEqualTo(CancellationToken.None);
        await Assert.That(resolver.Calls).IsEqualTo(1);
    }

    /// <summary>Records the invocation of a resolver.</summary>
    /// <param name="result">The decision returned to the caller.</param>
    private sealed class RecordingResolver(ConflictResolutionResult result) : IConflictResolver
    {
        /// <summary>Gets the received context.</summary>
        public ConflictContext? Context { get; private set; }

        /// <summary>Gets the received cancellation token.</summary>
        public CancellationToken Token { get; private set; }

        /// <summary>Gets the number of resolver calls.</summary>
        public int Calls { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ConflictResolutionResult> ResolveAsync(ConflictContext context, CancellationToken cancellationToken)
        {
            Context = context;
            Token = cancellationToken;
            Calls++;
            return new(result);
        }
    }
}
