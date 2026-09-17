// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests argument validation of <see cref="CancellationDisposable"/>.</summary>
public sealed class CancellationDisposableTests
{
    /// <summary>A null cancellation source is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSource_ThrowsArgumentNull() =>
        await Assert.That(static () => new CancellationDisposable(null!)).ThrowsExactly<ArgumentNullException>();
}
