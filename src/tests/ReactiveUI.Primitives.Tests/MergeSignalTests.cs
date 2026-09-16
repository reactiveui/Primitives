// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests argument validation of <see cref="MergeSignal{T}"/>.</summary>
public sealed class MergeSignalTests
{
    /// <summary>The concurrency bound used by the bounded constructor.</summary>
    private const int MaxConcurrent = 2;

    /// <summary>A null source collection is rejected by the unbounded constructor.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSources_ThrowsArgumentNull() =>
        await Assert.That(static () => new MergeSignal<int>((IEnumerable<IObservable<int>>)null!))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null source collection is rejected by the bounded constructor.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSourcesWithBound_ThrowsArgumentNull() =>
        await Assert.That(static () => new MergeSignal<int>(null!, MaxConcurrent))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null first source is rejected by the pair constructor.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullFirst_ThrowsArgumentNull() =>
        await Assert.That(static () => new MergeSignal<int>(null!, Signal.None<int>()))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null second source is rejected by the pair constructor.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSecond_ThrowsArgumentNull() =>
        await Assert.That(static () => new MergeSignal<int>(Signal.None<int>(), null!))
            .ThrowsExactly<ArgumentNullException>();
}
