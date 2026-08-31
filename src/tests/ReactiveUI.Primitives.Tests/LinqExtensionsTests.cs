// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies miscellaneous Primitives extension contracts.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>Verifies DisposeWith preserves the concrete type and tracks the original disposable.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeWithReturnsConcreteDisposableAndTracksIt()
    {
        var disposalCount = 0;
        ActionDisposable disposable = new(() => disposalCount++);
        MultipleDisposable disposables = [];

        var result = disposable.DisposeWith(disposables);

        await Assert.That(result).IsSameReferenceAs(disposable);
        await Assert.That(result.IsDisposed).IsFalse();
        await Assert.That(disposables.Contains(disposable)).IsTrue();
        disposables.Dispose();
        await Assert.That(disposalCount).IsEqualTo(1);
    }

    /// <summary>Verifies DisposeWith rejects a null disposable collection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeWithThrowsForNullMultipleDisposable()
    {
        using ActionDisposable disposable = new(static () => { });

        var exception = Assert.Throws<ArgumentNullException>(() =>
            disposable.DisposeWith((MultipleDisposable)null!));

        await Assert.That(exception.ParamName).IsEqualTo("disposables");
    }
}
