// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// DisposableTests.
/// </summary>
public class DisposableTests
{
    /// <summary>
    /// Called when [dispose once].
    /// </summary>
    [Test]
    public void OnlyDisposeOnce()
    {
        var disposed = 0;
        var disposable = Disposable.Create(() => disposed++);

        disposable.Dispose();

        Assert.Equal(1, disposed);

        disposable.Dispose();

        Assert.Equal(1, disposed);
    }

    /// <summary>
    /// Empties the disposable.
    /// </summary>
    [Test]
    public void EmptyDisposable()
    {
        var disposable = Disposable.Empty;
        disposable.Dispose();
        disposable.Dispose();
        disposable.Dispose();
    }

    /// <summary>
    /// Singles the disposable dispose.
    /// </summary>
    [Test]
    public void SingleDisposableDispose()
    {
        var disposable = new SingleDisposable(Disposable.Empty);
        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
    }

    /// <summary>
    /// Singles the disposable dispose with action.
    /// </summary>
    [Test]
    public void SingleDisposableDisposeWithAction()
    {
        var disposed = 0;
        var disposable = new SingleDisposable(Disposable.Empty, () => disposed++);
        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
        Assert.Equal(1, disposed);

        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
        Assert.Equal(1, disposed);

        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
        Assert.Equal(1, disposed);
    }

    /// <summary>
    /// Multiples the disposable dispose.
    /// </summary>
    [Test]
    public void MultipleDisposableDispose()
    {
        var disposable = new MultipleDisposable();
        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
    }

    /// <summary>
    /// Multiples the disposable with items dispose.
    /// </summary>
    [Test]
    public void MultipleDisposableWithItemsDispose()
    {
        var disposable = new MultipleDisposable();
        disposable.Add(Disposable.Empty);
        var disposed = 0;

        // create a disposable that will be disposed when the MultipleDisposable is disposed
        var singleDisposable = Disposable.Empty.DisposeWith(() => disposed++);

        // add the disposable to the MultipleDisposable
        singleDisposable?.DisposeWith(disposable);

        var singleDisposable2 = Disposable.Empty.DisposeWith();
        singleDisposable2?.DisposeWith(disposable);

        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
        Assert.True(singleDisposable?.IsDisposed);
        Assert.True(singleDisposable2?.IsDisposed);
        Assert.Equal(1, disposed);
    }
}
