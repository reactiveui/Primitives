// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests argument validation of <see cref="AsyncDeferSignal{T}"/>.</summary>
public sealed class AsyncDeferSignalTests
{
    /// <summary>A null cancellable factory is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullCancellableFactory_ThrowsArgumentNull() =>
        await Assert.That(static () => new AsyncDeferSignal<int>((Func<CancellationToken, Task<IObservable<int>>>)null!))
            .ThrowsExactly<ArgumentNullException>();
}
