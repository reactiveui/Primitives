// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests for the disposables family of types.</summary>
public class DisposableTests
{
    /// <summary>Called when [dispose once].</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnlyDisposeOnce()
    {
        var disposed = 0;
        ActionDisposable disposable = new(() => disposed++);
        disposable.Dispose();
        await Assert.That(disposed).IsEqualTo(1);
        disposable.Dispose();
        await Assert.That(disposed).IsEqualTo(1);
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
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task CancellationDisposableDefaultConstructorCancelsOwnedToken()
    {
        CancellationDisposable disposable = new();
        await Assert.That(disposable.Token.IsCancellationRequested).IsFalse();
        await Assert.That(disposable.IsDisposed).IsFalse();
        disposable.Dispose();
        await Assert.That(disposable.Token.IsCancellationRequested).IsTrue();
        await Assert.That(disposable.IsDisposed).IsTrue();
        disposable.Dispose();
        await Assert.That(disposable.Token.IsCancellationRequested).IsTrue();
        await Assert.That(disposable.IsDisposed).IsTrue();
    }

    /// <summary>Singles the disposable dispose.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SingleDisposableDispose()
    {
        SingleDisposable disposable = new(EmptyDisposable.Instance);
        disposable.Dispose();
        await Assert.That(disposable.IsDisposed).IsTrue();
    }

    /// <summary>Singles the disposable dispose with action.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SingleDisposableDisposeWithAction()
    {
        var disposed = 0;
        SingleDisposable disposable = new(EmptyDisposable.Instance, () => disposed++);
        disposable.Dispose();
        await Assert.That(disposable.IsDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
        disposable.Dispose();
        await Assert.That(disposable.IsDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
        disposable.Dispose();
        await Assert.That(disposable.IsDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Multiples the disposable dispose.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableDispose()
    {
        MultipleDisposable disposable = [];
        disposable.Dispose();
        await Assert.That(disposable.IsDisposed).IsTrue();
    }

    /// <summary>Multiples the disposable with items dispose.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableWithItemsDispose()
    {
        MultipleDisposable disposable = [EmptyDisposable.Instance];
        var disposed = 0;

        // A child disposable whose action runs when the group is disposed.
        SingleDisposable singleDisposable = new(EmptyDisposable.Instance, () => disposed++);
        disposable.Add(singleDisposable);
        SingleDisposable singleDisposable2 = new(EmptyDisposable.Instance);
        disposable.Add(singleDisposable2);
        disposable.Dispose();
        await Assert.That(disposable.IsDisposed).IsTrue();
        await Assert.That(singleDisposable.IsDisposed).IsTrue();
        await Assert.That(singleDisposable2.IsDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Verifies <see cref = "MultipleDisposable.Count"/> tracks the held disposables and resets on dispose.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableCountReflectsContents()
    {
        ActionDisposable first = new(() => { });
        ActionDisposable second = new(() => { });
        IDisposable[] items = [first, second];
        MultipleDisposable disposable = [.. items];
        await Assert.That(disposable.Count).IsEqualTo(items.Length);
        disposable.Remove(first);
        await Assert.That(disposable.Count).IsEqualTo(items.Length - 1);
        disposable.Dispose();
        await Assert.That(disposable.Count).IsEqualTo(0);
    }

    /// <summary>Verifies a collection expression initializes a <see cref = "MultipleDisposable"/>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableSupportsCollectionInitializer()
    {
        ActionDisposable first = new(() => { });
        ActionDisposable second = new(() => { });
        IDisposable[] items = [first, second];
        MultipleDisposable disposable = [.. items];
        await Assert.That(disposable.Count).IsEqualTo(items.Length);
        await Assert.That(disposable.Contains(first)).IsTrue();
        await Assert.That(disposable.Contains(second)).IsTrue();
        await Assert.That(disposable.IsReadOnly).IsFalse();
    }

    /// <summary>Verifies <see cref = "MultipleDisposable.Contains"/> reports membership.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableContainsReportsMembership()
    {
        ActionDisposable tracked = new(() => { });
        ActionDisposable untracked = new(() => { });
        MultipleDisposable disposable = [tracked];
        await Assert.That(disposable.Contains(tracked)).IsTrue();
        await Assert.That(disposable.Contains(untracked)).IsFalse();
        await Assert.That(disposable.Contains(null!)).IsFalse();

        // Removing an absent item while there is no overflow store returns false.
        await Assert.That(disposable.Remove(untracked)).IsFalse();
    }

    /// <summary>Verifies <see cref = "MultipleDisposable.Clear"/> disposes the contents and stays usable.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableClearDisposesContentsAndStaysUsable()
    {
        var disposedCount = 0;
        IDisposable[] items =
            [new ActionDisposable(() => disposedCount++), new ActionDisposable(() => disposedCount++)];
        MultipleDisposable disposable = [.. items];
        disposable.Clear();
        await Assert.That(disposedCount).IsEqualTo(items.Length);
        await Assert.That(disposable.Count).IsEqualTo(0);
        await Assert.That(disposable.IsDisposed).IsFalse();
        var reused = 0;
        disposable.Add(new ActionDisposable(() => reused++));
        await Assert.That(disposable.Count).IsEqualTo(1);
        disposable.Dispose();
        await Assert.That(reused).IsEqualTo(1);
    }

    /// <summary>Verifies enumeration and <see cref = "MultipleDisposable.CopyTo"/> expose the held disposables.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableEnumeratesAndCopies()
    {
        ActionDisposable first = new(() => { });
        ActionDisposable second = new(() => { });
        MultipleDisposable disposable = new(first, second);
        var enumeratedCount = 0;
        var sawFirst = false;
        var sawSecond = false;
        foreach (var item in disposable)
        {
            enumeratedCount++;
            sawFirst |= ReferenceEquals(item, first);
            sawSecond |= ReferenceEquals(item, second);
        }

        await Assert.That(sawFirst).IsTrue();
        await Assert.That(sawSecond).IsTrue();
        await Assert.That(enumeratedCount).IsEqualTo(disposable.Count);
        var array = new IDisposable[disposable.Count];
        disposable.CopyTo(array, 0);
        await Assert.That(Array.IndexOf(array, first)).IsGreaterThanOrEqualTo(0);
        await Assert.That(Array.IndexOf(array, second)).IsGreaterThanOrEqualTo(0);
    }

    /// <summary>Exercises the overflow path (more than the two inline slots) across count, contains, remove and dispose.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableHandlesOverflow()
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
        await Assert.That(disposable.Count).IsEqualTo(items.Length);

        // Enumerate and copy while the group spills into the overflow store.
        var seen = 0;
        foreach (var _ in disposable)
        {
            seen++;
        }

        await Assert.That(seen).IsEqualTo(items.Length);
        var array = new IDisposable[disposable.Count];
        disposable.CopyTo(array, 0);
        await Assert.That(array.Length).IsEqualTo(items.Length);
        ActionDisposable missing = new(() => { });
        await Assert.That(disposable.Contains(items[0])).IsTrue();
        await Assert.That(disposable.Contains(items[items.Length - 1])).IsTrue();
        await Assert.That(disposable.Contains(missing)).IsFalse();
        await Assert.That(disposable.Remove(items[items.Length - 1])).IsTrue();
        await Assert.That(disposable.Remove(missing)).IsFalse();
        await Assert.That(disposable.Count).IsEqualTo(items.Length - 1);

        // Clear disposes the remaining items, including the overflow store.
        disposable.Clear();
        await Assert.That(disposedCount).IsEqualTo(items.Length);
    }

    /// <summary>Verifies <see cref = "MultipleDisposable.CopyTo"/> validates its arguments.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableCopyToValidatesArguments()
    {
        MultipleDisposable disposable = [];
        await Assert.That(() => disposable.CopyTo(null!, 0)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => disposable.CopyTo([], -1)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies the non-generic enumerator exposes the held disposables.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableNonGenericEnumeration()
    {
        ActionDisposable first = new(() => { });
        ActionDisposable second = new(() => { });
        MultipleDisposable disposable = [first, second];
        var count = 0;
        foreach (var _ in (System.Collections.IEnumerable)disposable)
        {
            count++;
        }

        await Assert.That(count).IsEqualTo(disposable.Count);
    }

    /// <summary>Verifies behaviour once the group is disposed: queries are empty and further adds dispose immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task MultipleDisposableAfterDisposeIsEmptyAndDisposesNewItems()
    {
        MultipleDisposable disposable = [];
        disposable.Dispose();
        await Assert.That(disposable.Count).IsEqualTo(0);
        await Assert.That(disposable.Contains(new ActionDisposable(() => { }))).IsFalse();
        var enumeratedAfterDispose = 0;
        foreach (var _ in disposable)
        {
            enumeratedAfterDispose++;
        }

        await Assert.That(enumeratedAfterDispose).IsEqualTo(0);
        var lateDisposed = 0;
        disposable.Add(new ActionDisposable(() => lateDisposed++));
        await Assert.That(lateDisposed).IsEqualTo(1);

        // Clear and a redundant Dispose are no-ops on an already-disposed group.
        disposable.Clear();
        disposable.Dispose();
        await Assert.That(disposable.IsDisposed).IsTrue();
    }
}
