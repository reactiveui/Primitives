// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteOperationCompletion"/>.</summary>
public sealed class RemoteOperationCompletionTests
{
    /// <summary>Verifies caller mutations cannot alter complete operation membership.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompletionOwnsItsEventIdentifiers()
    {
        var origin = new RemoteEventOrigin("client", OperationId.New());
        var eventId = Guid.NewGuid();
        var ids = new List<Guid> { eventId };
        var completion = new RemoteOperationCompletion(origin, ids);
        ids.Clear();

        await Assert.That(completion.Origin).IsEqualTo(origin);
        await Assert.That(completion.EventIds.Count).IsEqualTo(1);
        await Assert.That(completion.EventIds[0]).IsEqualTo(eventId);
        Action mutate = () => ((IList<Guid>)completion.EventIds).Clear();
        await Assert.That(mutate).ThrowsExactly<NotSupportedException>();
    }

    /// <summary>Verifies missing constructor arguments fail at the public boundary.</summary>
    /// <param name="missingOrigin">Whether the missing argument is the origin.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task ConstructorRejectsMissingArguments(bool missingOrigin)
    {
        var origin = new RemoteEventOrigin("client", OperationId.New());
        var constructor = typeof(RemoteOperationCompletion).GetConstructors().Single();
        object?[] arguments = [missingOrigin ? null : origin, missingOrigin ? Array.Empty<Guid>() : null];
        var exception = await Assert.That(() => constructor.Invoke(arguments)).ThrowsExactly<TargetInvocationException>();
        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
    }
}
