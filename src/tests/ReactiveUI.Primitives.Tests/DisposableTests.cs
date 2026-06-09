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
        MultipleDisposable disposable = [EmptyDisposable.Instance];
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

    /// <summary>Verifies <see cref="MultipleDisposable.Count"/> tracks the held disposables and resets on dispose.</summary>
    [Test]
    public void MultipleDisposableCountReflectsContents()
    {
        var first = new ActionDisposable(() => { });
        var second = new ActionDisposable(() => { });
        IDisposable[] items = [first, second];
        MultipleDisposable disposable = [.. items];

        Assert.Equal(items.Length, disposable.Count);

        disposable.Remove(first);
        Assert.Equal(items.Length - 1, disposable.Count);

        disposable.Dispose();
        Assert.Equal(0, disposable.Count);
    }

    /// <summary>Verifies a collection expression initializes a <see cref="MultipleDisposable"/>.</summary>
    [Test]
    public void MultipleDisposableSupportsCollectionInitializer()
    {
        var first = new ActionDisposable(() => { });
        var second = new ActionDisposable(() => { });
        IDisposable[] items = [first, second];
        MultipleDisposable disposable = [.. items];

        Assert.Equal(items.Length, disposable.Count);
        Assert.True(disposable.Contains(first));
        Assert.True(disposable.Contains(second));
        Assert.False(disposable.IsReadOnly);
    }

    /// <summary>Verifies <see cref="MultipleDisposable.Contains"/> reports membership.</summary>
    [Test]
    public void MultipleDisposableContainsReportsMembership()
    {
        var tracked = new ActionDisposable(() => { });
        var untracked = new ActionDisposable(() => { });
        MultipleDisposable disposable = [tracked];

        Assert.True(disposable.Contains(tracked));
        Assert.False(disposable.Contains(untracked));
    }

    /// <summary>Verifies <see cref="MultipleDisposable.Clear"/> disposes the contents and stays usable.</summary>
    [Test]
    public void MultipleDisposableClearDisposesContentsAndStaysUsable()
    {
        var disposedCount = 0;
        IDisposable[] items =
        [
            new ActionDisposable(() => disposedCount++),
            new ActionDisposable(() => disposedCount++)
        ];
        MultipleDisposable disposable = [.. items];

        disposable.Clear();

        Assert.Equal(items.Length, disposedCount);
        Assert.Equal(0, disposable.Count);
        Assert.False(disposable.IsDisposed);

        var reused = 0;
        disposable.Add(new ActionDisposable(() => reused++));
        Assert.Equal(1, disposable.Count);

        disposable.Dispose();
        Assert.Equal(1, reused);
    }

    /// <summary>Verifies enumeration and <see cref="MultipleDisposable.CopyTo"/> expose the held disposables.</summary>
    [Test]
    public void MultipleDisposableEnumeratesAndCopies()
    {
        var first = new ActionDisposable(() => { });
        var second = new ActionDisposable(() => { });
        var disposable = new MultipleDisposable(first, second);

        var enumeratedCount = 0;
        var sawFirst = false;
        var sawSecond = false;
        foreach (var item in disposable)
        {
            enumeratedCount++;
            sawFirst |= ReferenceEquals(item, first);
            sawSecond |= ReferenceEquals(item, second);
        }

        Assert.True(sawFirst);
        Assert.True(sawSecond);
        Assert.Equal(disposable.Count, enumeratedCount);

        var array = new IDisposable[disposable.Count];
        disposable.CopyTo(array, 0);
        Assert.True(Array.IndexOf(array, first) >= 0);
        Assert.True(Array.IndexOf(array, second) >= 0);
    }
}
