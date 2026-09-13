// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Verifies conflict write provenance stays bound to the resolved operation.</summary>
public sealed partial class ConflictResolvingServerOperationHandlerTests
{
    /// <summary>Verifies another operation's write stamp cannot authorize the candidate operation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task LastWriterWinsResolverRejectsForeignOperationProvenance()
    {
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = new IncrementingVersionFactory() });
        var operation = Operation(FirstOperationSeed, Alpha, InitialVersion, Start);
        var context = new ConflictContext(
            State(InitialVersion, "initial"),
            [operation],
            ClientIdentity(Alpha),
            new() { CandidateWrite = Stamp(Start, Alpha, OperationId(SecondOperationSeed)) });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.RejectedOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(result.ServerVersion).IsEqualTo(InitialVersion);
    }
}
