// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables.Fluent;

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.IO;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>
/// Tests disposable extensions.
/// </summary>
public class DisposableExtensionsTests
{
    /// <summary>
    /// Tests DisposeWith returns a disposable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GivenNull_WhenDisposeWith_ThenExceptionThrown()
    {
        // Given
        var sut = Disposable.Create(() => { });

        // When
        var result = Assert.Throws<ArgumentNullException>(() => sut.DisposeWith((CompositeDisposable)null!));

        // Then
        await Assert.That(result).IsTypeOf<ArgumentNullException>();
    }

    /// <summary>
    /// Tests DisposeWith disposes the underlying disposable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GivenDisposable_WhenDisposeWith_ThenDisposed()
    {
        // Given
        var sut = new CompositeDisposable();
        var compositeDisposable = new CompositeDisposable();
        sut.DisposeWith(compositeDisposable);

        // When
        compositeDisposable.Dispose();

        // Then
        await Assert.That(sut.IsDisposed).IsTrue();
    }

    /// <summary>
    /// Tests DisposeWith returns the original disposable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GivenDisposable_WhenDisposeWith_ThenReturnsDisposable()
    {
        // Given, When
        var sut = new CompositeDisposable();
        var compositeDisposable = new CompositeDisposable();
        var result = sut.DisposeWith(compositeDisposable);

        // Then
        await Assert.That(result).IsCollectionEqualTo(sut);
    }
}
