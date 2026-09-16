// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests argument validation of the enumerable combine-latest coordinator.</summary>
public sealed class SyncLatestEnumerableCoordinatorTests
{
    /// <summary>A null source array is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSources_ThrowsArgumentNull()
    {
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);

        await Assert.That(() => new SyncLatestEnumerableCoordinator<int, int>(null!, observer, static values => values.Count))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullObserver_ThrowsArgumentNull() =>
        await Assert.That(static () => new SyncLatestEnumerableCoordinator<int, int>([], null!, static values => values.Count))
            .ThrowsExactly<ArgumentNullException>();
}
