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
        Assert.False(disposable.Contains(null!));

        // Removing an absent item while there is no overflow store returns false.
        Assert.False(disposable.Remove(untracked));
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

    /// <summary>Exercises the overflow path (more than the two inline slots) across count, contains, remove and dispose.</summary>
    [Test]
    public void MultipleDisposableHandlesOverflow()
    {
        var disposedCount = 0;
        IDisposable[] items =
        [
            new ActionDisposable(() => disposedCount++),
            new ActionDisposable(() => disposedCount++),
            new ActionDisposable(() => disposedCount++),
            new ActionDisposable(() => disposedCount++)
        ];
        MultipleDisposable disposable = [.. items];

        Assert.Equal(items.Length, disposable.Count);

        // Enumerate and copy while the group spills into the overflow store.
        var seen = 0;
        foreach (var _ in disposable)
        {
            seen++;
        }

        Assert.Equal(items.Length, seen);

        var array = new IDisposable[disposable.Count];
        disposable.CopyTo(array, 0);
        Assert.Equal(items.Length, array.Length);

        var missing = new ActionDisposable(() => { });
        Assert.True(disposable.Contains(items[0]));
        Assert.True(disposable.Contains(items[items.Length - 1]));
        Assert.False(disposable.Contains(missing));

        Assert.True(disposable.Remove(items[items.Length - 1]));
        Assert.False(disposable.Remove(missing));
        Assert.Equal(items.Length - 1, disposable.Count);

        // Clear disposes the remaining items, including the overflow store.
        disposable.Clear();
        Assert.Equal(items.Length, disposedCount);
    }

    /// <summary>Verifies <see cref="MultipleDisposable.CopyTo"/> validates its arguments.</summary>
    [Test]
    public void MultipleDisposableCopyToValidatesArguments()
    {
        var disposable = new MultipleDisposable();
        Assert.Throws<ArgumentNullException>(() => disposable.CopyTo(null!, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => disposable.CopyTo([], -1));
    }

    /// <summary>Verifies the non-generic enumerator exposes the held disposables.</summary>
    [Test]
    public void MultipleDisposableNonGenericEnumeration()
    {
        var first = new ActionDisposable(() => { });
        var second = new ActionDisposable(() => { });
        MultipleDisposable disposable = [first, second];

        var count = 0;
        foreach (var _ in (System.Collections.IEnumerable)disposable)
        {
            count++;
        }

        Assert.Equal(disposable.Count, count);
    }

    /// <summary>Verifies behaviour once the group is disposed: queries are empty and further adds dispose immediately.</summary>
    [Test]
    public void MultipleDisposableAfterDisposeIsEmptyAndDisposesNewItems()
    {
        var disposable = new MultipleDisposable();
        disposable.Dispose();

        Assert.Equal(0, disposable.Count);
        Assert.False(disposable.Contains(new ActionDisposable(() => { })));

        var enumeratedAfterDispose = 0;
        foreach (var _ in disposable)
        {
            enumeratedAfterDispose++;
        }

        Assert.Equal(0, enumeratedAfterDispose);

        var lateDisposed = 0;
        disposable.Add(new ActionDisposable(() => lateDisposed++));
        Assert.Equal(1, lateDisposed);

        // Clear and a redundant Dispose are no-ops on an already-disposed group.
        disposable.Clear();
        disposable.Dispose();
        Assert.True(disposable.IsDisposed);
    }
}
