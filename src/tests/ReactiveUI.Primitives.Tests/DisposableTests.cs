// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>DisposableTests.</summary>
public class DisposableTests
{
    /// <summary>Called when [dispose once].</summary>
    [Test]
    public void OnlyDisposeOnce()
    {
        var disposed = 0;
        var disposable = new ActionDisposable(() => disposed++);

        disposable.Dispose();

        Assert.Equal(1, disposed);

        disposable.Dispose();

        Assert.Equal(1, disposed);
    }

    /// <summary>Empties the disposable.</summary>
    [Test]
    public void EmptyDisposableInstanceDoesNothing()
    {
        var disposable = EmptyDisposable.Instance;
        disposable.Dispose();
        disposable.Dispose();
        disposable.Dispose();
    }

    /// <summary>Verifies cancellation disposable state transitions with the default owned token source.</summary>
    [Test]
    public void CancellationDisposableDefaultConstructorCancelsOwnedToken()
    {
        var disposable = new CancellationDisposable();

        Assert.False(disposable.Token.IsCancellationRequested);
        Assert.False(disposable.IsDisposed);

        disposable.Dispose();

        Assert.True(disposable.Token.IsCancellationRequested);
        Assert.True(disposable.IsDisposed);

        disposable.Dispose();

        Assert.True(disposable.Token.IsCancellationRequested);
        Assert.True(disposable.IsDisposed);
    }

    /// <summary>Singles the disposable dispose.</summary>
    [Test]
    public void SingleDisposableDispose()
    {
        var disposable = new SingleDisposable(EmptyDisposable.Instance);
        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
    }

    /// <summary>Singles the disposable dispose with action.</summary>
    [Test]
    public void SingleDisposableDisposeWithAction()
    {
        var disposed = 0;
        var disposable = new SingleDisposable(EmptyDisposable.Instance, () => disposed++);
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

    /// <summary>Multiples the disposable dispose.</summary>
    [Test]
    public void MultipleDisposableDispose()
    {
        var disposable = new MultipleDisposable();
        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
    }

    /// <summary>Multiples the disposable with items dispose.</summary>
    [Test]
    public void MultipleDisposableWithItemsDispose()
    {
        var disposable = new MultipleDisposable();
        disposable.Add(EmptyDisposable.Instance);
        var disposed = 0;

        // create a disposable that will be disposed when the MultipleDisposable is disposed
        var singleDisposable = EmptyDisposable.Instance.DisposeWith(() => disposed++);

        // add the disposable to the MultipleDisposable
        singleDisposable?.DisposeWith(disposable);

        var singleDisposable2 = EmptyDisposable.Instance.DisposeWith();
        singleDisposable2?.DisposeWith(disposable);

        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
        Assert.True(singleDisposable?.IsDisposed);
        Assert.True(singleDisposable2?.IsDisposed);
        Assert.Equal(1, disposed);
    }
}
