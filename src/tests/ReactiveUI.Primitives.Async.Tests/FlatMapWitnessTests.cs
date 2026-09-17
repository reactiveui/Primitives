// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the flat-map witnesses relaying resumable errors to their coordinator.</summary>
public sealed class FlatMapWitnessTests
{
    /// <summary>The inner witness relays a resumable error to the coordinator's observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnErrorResumeAsync_InnerWitness_RelaysToCoordinator()
    {
        List<Exception> errors = [];
        FlatMapCoordinator<int> coordinator = new(RecordErrors(errors));
        FlatMapWitness<int> witness = new(coordinator);
        Exception expected = new InvalidOperationException("inner resume");

        await witness.OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).IsCollectionEqualTo([expected]);
    }

    /// <summary>The outer witness relays a resumable error to the coordinator's observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnErrorResumeAsync_OuterWitness_RelaysToCoordinator()
    {
        List<Exception> errors = [];
        FlatMapCoordinator<int> coordinator = new(RecordErrors(errors));
        FlatMapWitness<int, int> witness = new(coordinator, SignalAsync.Return, null);
        Exception expected = new InvalidOperationException("outer resume");

        await witness.OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).IsCollectionEqualTo([expected]);
    }

    /// <summary>Creates an observer that records resumable errors.</summary>
    /// <param name="errors">The list receiving the errors.</param>
    /// <returns>The recording observer.</returns>
    private static CallbackWitnessAsync<int> RecordErrors(List<Exception> errors) =>
        new(
            static (_, _) => default,
            (error, _) =>
            {
                errors.Add(error);
                return default;
            });
}
