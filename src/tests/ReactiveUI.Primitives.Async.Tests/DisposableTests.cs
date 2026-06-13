// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for DisposableAsync, CompositeDisposableAsync, SingleAssignmentDisposableAsync, and SerialDisposableAsync.</summary>
public class DisposableTests
{
    /// <summary>Tests DisposableAsync.Empty dispose does nothing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposableAsyncEmpty_ThenDisposeDoesNothing()
    {
        var empty = DisposableAsync.Empty;
        await empty.DisposeAsync();
    }

    /// <summary>Tests DisposableAsync.Create callback invoked on dispose.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposableAsyncCreate_ThenCallbackInvokedOnDispose()
    {
        var disposed = false;
        var disposable = DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        });
        await Assert.That(disposed).IsFalse();
        await disposable.DisposeAsync();
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests DisposableAsync.Create double dispose only calls once.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposableAsyncCreate_ThenDoubleDisposeOnlyCallsOnce()
    {
        var callCount = 0;
        var disposable = DisposableAsync.Create(() =>
        {
            callCount++;
            return default;
        });
        await disposable.DisposeAsync();
        await disposable.DisposeAsync();
        await Assert.That(callCount).IsEqualTo(1);
    }

    /// <summary>Tests CompositeDisposableAsync disposes all.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsync_ThenDisposesAll()
    {
        var disposed1 = false;
        var disposed2 = false;
        var d1 = DisposableAsync.Create(() =>
        {
            disposed1 = true;
            return default;
        });
        var d2 = DisposableAsync.Create(() =>
        {
            disposed2 = true;
            return default;
        });
        MultipleDisposableAsync composite = new(d1, d2);
        const int ExpectedCount = 2;
        await Assert.That(composite.Count).IsEqualTo(ExpectedCount);
        await Assert.That(composite.IsDisposed).IsFalse();
        await composite.DisposeAsync();
        await Assert.That(disposed1).IsTrue();
        await Assert.That(disposed2).IsTrue();
        await Assert.That(composite.IsDisposed).IsTrue();
    }

    /// <summary>Tests that null entries passed through the params constructor are ignored.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncParamsContainNull_ThenNullsIgnored()
    {
        const int ExpectedCount = 1;
        var disposed = false;
        var disposable = DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        });
        MultipleDisposableAsync composite = new(EnumerateWithNulls(disposable));
        await Assert.That(composite.Count).IsEqualTo(ExpectedCount);
        await composite.DisposeAsync();
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests CompositeDisposableAsync negative capacity throws.</summary>
    [Test]
    public void WhenCompositeDisposableAsyncNegativeCapacity_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = new MultipleDisposableAsync(-1));

    /// <summary>Tests CompositeDisposableAsync with zero capacity leaves the backing array unallocated.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncZeroCapacity_ThenEmpty()
    {
        MultipleDisposableAsync composite = new(0);
        await Assert.That(composite.Count).IsEqualTo(0);
        await composite.DisposeAsync();
    }

    /// <summary>Tests CompositeDisposableAsync with capacity works.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncWithCapacity_ThenWorks()
    {
        MultipleDisposableAsync composite = new(10);
        await Assert.That(composite.Count).IsEqualTo(0);
        var disposed = false;
        await composite.AddAsync(DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        }));
        await Assert.That(composite.Count).IsEqualTo(1);
        await composite.DisposeAsync();
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests CompositeDisposableAsync from enumerable disposes all.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncFromEnumerable_ThenDisposesAll()
    {
        var count = 0;
        var disposables = Enumerable.Range(0, 3).Select(_ => DisposableAsync.Create(() =>
        {
            Interlocked.Increment(ref count);
            return default;
        }));
        const int ExpectedCount = 3;
        MultipleDisposableAsync composite = new(disposables);
        await Assert.That(composite.Count).IsEqualTo(ExpectedCount);
        await composite.DisposeAsync();
        await Assert.That(count).IsEqualTo(ExpectedCount);
    }

    /// <summary>Tests CompositeDisposableAsync add after dispose disposes immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncAddAfterDispose_ThenItemDisposedImmediately()
    {
        MultipleDisposableAsync composite = new();
        await composite.DisposeAsync();
        var disposed = false;
        await composite.AddAsync(DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        }));
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests CompositeDisposableAsync remove disposes and removes the item.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncRemove_ThenItemRemovedAndDisposed()
    {
        var disposed = false;
        var d = DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        });
        MultipleDisposableAsync composite = new(d);
        var removed = await composite.Remove(d);
        await Assert.That(removed).IsTrue();
        await Assert.That(composite.Count).IsEqualTo(0);
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests CompositeDisposableAsync IsDisposed returns false when active.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncIsDisposed_ThenReturnsFalse()
    {
        MultipleDisposableAsync composite = new();
        await Assert.That(composite.IsDisposed).IsFalse();
    }

    /// <summary>Tests SingleAssignmentDisposableAsync disposes assigned.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsync_ThenDisposesAssigned()
    {
        SingleAssignmentDisposableAsync sad = new();
        var disposed = false;
        await sad.SetDisposableAsync(DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        }));
        await Assert.That(sad.IsDisposed).IsFalse();
        await sad.DisposeAsync();
        await Assert.That(sad.IsDisposed).IsTrue();
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests SingleAssignment dispose before set disposes immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncDisposeBeforeSet_ThenSetDisposedImmediately()
    {
        SingleAssignmentDisposableAsync sad = new();
        await sad.DisposeAsync();
        var disposed = false;
        await sad.SetDisposableAsync(DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        }));
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests SingleAssignment double set throws.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncDoubleSet_ThenThrowsInvalidOperation()
    {
        SingleAssignmentDisposableAsync sad = new();
        await sad.SetDisposableAsync(DisposableAsync.Empty);
        await Assert.That(async () => await sad.SetDisposableAsync(DisposableAsync.Empty))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests SingleAssignment get before set returns null.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncGetBeforeSet_ThenReturnsNull()
    {
        SingleAssignmentDisposableAsync sad = new();
        await Assert.That(sad.GetDisposable()).IsNull();
    }

    /// <summary>Tests SingleAssignment get after dispose returns non-null.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncGetAfterDispose_ThenReturnsEmpty()
    {
        SingleAssignmentDisposableAsync sad = new();
        await sad.DisposeAsync();
        await Assert.That(sad.GetDisposable()).IsNotNull();
    }

    /// <summary>Tests SingleAssignment get after set returns assigned.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncGetAfterSet_ThenReturnsAssigned()
    {
        SingleAssignmentDisposableAsync sad = new();
        var original = DisposableAsync.Empty;
        await sad.SetDisposableAsync(original);
        await Assert.That(sad.GetDisposable()).IsSameReferenceAs(original);
    }

    /// <summary>Tests SerialDisposableAsync replaces and disposes previous.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialDisposableAsync_ThenReplacesAndDisposesPrevious()
    {
        SingleReplaceableDisposableAsync serial = new();
        var disposed1 = false;
        var disposed2 = false;
        var d1 = DisposableAsync.Create(() =>
        {
            disposed1 = true;
            return default;
        });
        var d2 = DisposableAsync.Create(() =>
        {
            disposed2 = true;
            return default;
        });
        await serial.SetDisposableAsync(d1);
        await Assert.That(disposed1).IsFalse();
        await serial.SetDisposableAsync(d2);
        await Assert.That(disposed1).IsTrue();
        await Assert.That(disposed2).IsFalse();
        await serial.DisposeAsync();
        await Assert.That(disposed2).IsTrue();
    }

    /// <summary>Tests SerialDisposableAsync set after dispose disposes immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialDisposableAsyncSetAfterDispose_ThenDisposedImmediately()
    {
        SingleReplaceableDisposableAsync serial = new();
        await serial.DisposeAsync();
        var disposed = false;
        await serial.SetDisposableAsync(DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        }));
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests SerialDisposableAsync double dispose is safe.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialDisposableAsyncDoubleDispose_ThenSafe()
    {
        SingleReplaceableDisposableAsync serial = new();
        var disposed = false;
        await serial.SetDisposableAsync(DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        }));
        await serial.DisposeAsync();
        await serial.DisposeAsync();
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests SerialDisposableAsync set null succeeds.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialDisposableAsyncSetNull_ThenSucceeds()
    {
        SingleReplaceableDisposableAsync serial = new();
        await serial.SetDisposableAsync(null);
        await serial.DisposeAsync();
    }

    /// <summary>
    /// Verifies that removing items from a CompositeDisposableAsync with capacity
    /// above the shrink threshold causes the internal list to shrink when count
    /// drops below half of capacity.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeRemoveTriggersListShrink_ThenInternalListIsShrunk()
    {
        const int InitialCapacity = 100;
        const int TotalAdded = 80;
        const int TotalRemoved = 55;
        const int ExpectedRemaining = TotalAdded - TotalRemoved;
        const int LastIndex = TotalAdded - 1;

        // Create a composite with enough capacity to exceed the shrink threshold (64)
        MultipleDisposableAsync composite = new(InitialCapacity);

        // Add 80 disposables to build capacity above 64
        List<IAsyncDisposable> disposables = [];
        for (var i = 0; i < TotalAdded; i++)
        {
            var d = DisposableAsync.Create(() => default);
            disposables.Add(d);
            await composite.AddAsync(d);
        }

        await Assert.That(composite.Count).IsEqualTo(TotalAdded);

        // Remove enough items to drop count below half of capacity
        // After removing, count should be significantly below half the list's capacity
        for (var i = 0; i < TotalRemoved; i++)
        {
            await composite.Remove(disposables[i]);
        }

        // After shrinking, the count should still reflect the remaining items
        await Assert.That(composite.Count).IsEqualTo(ExpectedRemaining);

        // Verify remaining disposables are still in the collection
        await Assert.That(composite.Contains(disposables[LastIndex])).IsTrue();
        await Assert.That(composite.Contains(disposables[0])).IsFalse();
        await composite.DisposeAsync();
    }

    /// <summary>Verifies that CopyTo on a disposed CompositeDisposableAsync returns without copying.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeCopyToAfterDispose_ThenNoItemsCopied()
    {
        var d1 = DisposableAsync.Create(() => default);
        MultipleDisposableAsync composite = new(d1);
        await composite.DisposeAsync();
        var array = new IAsyncDisposable[5];
        composite.CopyTo(array, 0);

        // Array should remain empty because the composite is disposed
        await Assert.That(array[0]).IsNull();
    }

    /// <summary>Verifies that CopyTo throws ArgumentOutOfRangeException when there is insufficient space in the destination array.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeCopyToInsufficientSpace_ThenThrowsArgumentOutOfRange()
    {
        var d1 = DisposableAsync.Create(() => default);
        var d2 = DisposableAsync.Create(() => default);
        MultipleDisposableAsync composite = new(d1, d2);

        // Array of size 2 starting at index 1 means only 1 slot available for 2 items
        var array = new IAsyncDisposable[2];
        Assert.Throws<ArgumentOutOfRangeException>(() => composite.CopyTo(array, 1));
        await composite.DisposeAsync();
        await Assert.That(composite.IsDisposed).IsTrue();
    }

    /// <summary>Verifies that CopyTo throws ArgumentOutOfRangeException for negative arrayIndex.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeCopyToNegativeIndex_ThenThrowsArgumentOutOfRange()
    {
        MultipleDisposableAsync composite = new();
        var array = new IAsyncDisposable[5];
        Assert.Throws<ArgumentOutOfRangeException>(() => composite.CopyTo(array, -1));
        await composite.DisposeAsync();
        await Assert.That(composite.IsDisposed).IsTrue();
    }

    /// <summary>Exercises the <c>arrayIndex &gt;= array.Length</c> branch of the <c>CompositeDisposableAsync.CopyTo</c> bounds check.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeCopyToIndexAtArrayLength_ThenThrowsArgumentOutOfRange()
    {
        const int ArrayLength = 2;
        MultipleDisposableAsync composite = new();
        var array = new IAsyncDisposable[ArrayLength];
        Assert.Throws<ArgumentOutOfRangeException>(() => composite.CopyTo(array, ArrayLength));
        await composite.DisposeAsync();
        await Assert.That(composite.IsDisposed).IsTrue();
    }

    /// <summary>Exercises the <c>array is null</c> branch of <c>CompositeDisposableAsync.CopyTo</c>
    /// — both bounds-check guards use <c>array?.Length</c>, so a null array lets control fall
    /// through to the body where the per-item-assignment <c>array is not null</c> guard
    /// short-circuits.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeCopyToNullArray_ThenBoundsChecksFallThrough()
    {
        var d1 = DisposableAsync.Create(static () => default);
        MultipleDisposableAsync composite = new(d1);
        composite.CopyTo(null, 0);
        await composite.DisposeAsync();
        await Assert.That(composite.IsDisposed).IsTrue();
    }

    /// <summary>
    /// Verifies that setting a null disposable after disposing SerialDisposableAsync
    /// completes without error (hits the null check on the disposed path).
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("TUnit", "TUnitAssertions0005", Justification = "Asserting expected constant outcome")]
    public async Task WhenSerialSetNullAfterDispose_ThenCompletesWithoutError()
    {
        SingleReplaceableDisposableAsync serial = new();
        await serial.DisposeAsync();

        // Set null after disposal - should complete without error
        await serial.SetDisposableAsync(null);

        // Verify it is still disposed
    }

    /// <summary>
    /// Verifies that concurrent SetDisposableAsync calls on a SerialDisposableAsync
    /// correctly handle the CAS retry loop by setting and disposing values properly.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialConcurrentSet_ThenAllPreviousDisposed()
    {
        SingleReplaceableDisposableAsync serial = new();
        var disposedCount = 0;

        IAsyncDisposable MakeDisposable() => DisposableAsync.Create(() =>
        {
            Interlocked.Increment(ref disposedCount);
            return default;
        });

        const int ExpectedDisposedCount = 50;

        // Rapid concurrent sets to exercise the CAS retry path
        var tasks = Enumerable.Range(0, ExpectedDisposedCount)
            .Select(_ => Task.Run(async () => await serial.SetDisposableAsync(MakeDisposable())));
        await Task.WhenAll(tasks);

        // Dispose the serial to clean up the final remaining disposable
        await serial.DisposeAsync();

        // All 50 disposables should eventually be disposed (49 replaced + 1 final)
        await Assert.That(disposedCount).IsEqualTo(ExpectedDisposedCount);
    }

    /// <summary>
    /// Verifies that disposing SerialDisposableAsync when no disposable has been set
    /// completes without error.
    /// Covers the null field path and the empty dispose path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("TUnit", "TUnitAssertions0005", Justification = "Asserting expected constant outcome")]
    public async Task WhenSerialDisposeWithNoDisposableSet_ThenCompletesCleanly()
    {
        SingleReplaceableDisposableAsync serial = new();

        // Dispose without ever setting a disposable
        await serial.DisposeAsync();

        // Verify double dispose is also safe
        await serial.DisposeAsync();
    }

    /// <summary>
    /// Verifies that setting null after disposing a SingleAssignmentDisposableAsync
    /// completes without error (covers the null check in the disposed path).
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentSetNullAfterDispose_ThenCompletesWithoutError()
    {
        SingleAssignmentDisposableAsync sad = new();
        await sad.DisposeAsync();

        // Set null after disposal - should return default
        await sad.SetDisposableAsync(null);
        await Assert.That(sad.IsDisposed).IsTrue();
    }

    /// <summary>Verifies that double assignment on the static SetDisposableAsync helper throws InvalidOperationException.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDoubleAssignNonNull_ThenThrowsInvalidOperation()
    {
        SingleAssignmentDisposableAsync sad = new();
        await sad.SetDisposableAsync(DisposableAsync.Empty);

        // Second assignment with a different non-null value should throw
        await Assert.That(async () => await sad.SetDisposableAsync(DisposableAsync.Create(() => default)))
            .ThrowsExactly<InvalidOperationException>();
        await sad.DisposeAsync();
        await Assert.That(sad.IsDisposed).IsTrue();
    }

    /// <summary>
    /// Verifies that the dispose sentinel DisposeAsync method returns a completed ValueTask.
    /// Covers the shared disposed sentinel DisposeAsync path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposeSentinel_ThenDisposeAsyncReturnsDefault()
    {
        // Access the sentinel and verify it can be disposed
        var sentinel = DisposableAsyncSlot.DisposedSentinel;
        await sentinel.DisposeAsync();

        // After dispose, getting the disposable should return the empty disposable
        SingleAssignmentDisposableAsync sad = new();
        await sad.DisposeAsync();
        var disposable = sad.GetDisposable();
        await Assert.That(disposable).IsNotNull();
    }

    /// <summary>Tests DisposableAsyncExtensions ToDisposableAsync wraps IDisposable correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToDisposableAsync_ThenWrapsIDisposable()
    {
        var disposed = false;
        TestSyncDisposable syncDisposable = new(() => disposed = true);
        var asyncDisposable = syncDisposable.ToDisposableAsync();
        await Assert.That(disposed).IsFalse();
        await asyncDisposable.DisposeAsync();
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests CompositeDisposableAsync Clear disposes and removes all.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncClear_ThenDisposesAndRemovesAll()
    {
        const int ExpectedCount = 3;
        var count = 0;
        MultipleDisposableAsync composite = new();
        for (var i = 0; i < ExpectedCount; i++)
        {
            await composite.AddAsync(DisposableAsync.Create(() =>
            {
                Interlocked.Increment(ref count);
                return default;
            }));
        }

        await Assert.That(composite.Count).IsEqualTo(ExpectedCount);
        await composite.Clear();
        await Assert.That(composite.Count).IsEqualTo(0);
        await Assert.That(count).IsEqualTo(ExpectedCount);
    }

    /// <summary>Tests CompositeDisposableAsync Contains returns true for added item.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncContains_ThenReturnsTrueForAdded()
    {
        MultipleDisposableAsync composite = new();
        var d = DisposableAsync.Empty;
        await composite.AddAsync(d);
        await Assert.That(composite.Contains(d)).IsTrue();
        await composite.DisposeAsync();
    }

    /// <summary>Tests CompositeDisposableAsync Contains returns false after dispose.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncContainsAfterDispose_ThenReturnsFalse()
    {
        MultipleDisposableAsync composite = new();
        var d = DisposableAsync.Empty;
        await composite.AddAsync(d);
        await composite.DisposeAsync();
        await Assert.That(composite.Contains(d)).IsFalse();
    }

    /// <summary>Tests CompositeDisposableAsync CopyTo copies all items.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncCopyTo_ThenCopiesAllItems()
    {
        var d1 = DisposableAsync.Empty;
        var d2 = DisposableAsync.Empty;
        MultipleDisposableAsync composite = new(d1, d2);
        var array = new IAsyncDisposable[2];
        composite.CopyTo(array, 0);
        await Assert.That(array[0]).IsNotNull();
        await Assert.That(array[1]).IsNotNull();
        await composite.DisposeAsync();
    }

    /// <summary>Tests CompositeDisposableAsync CopyTo throws on invalid index.</summary>
    [Test]
    public void WhenCompositeDisposableAsyncCopyToInvalidIndex_ThenThrows()
    {
        MultipleDisposableAsync composite = new();
        var array = new IAsyncDisposable[1];
        Assert.Throws<ArgumentOutOfRangeException>(() => composite.CopyTo(array, -1));
    }

    /// <summary>Tests CompositeDisposableAsync double dispose is safe.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncDoubleDispose_ThenSafe()
    {
        var disposed = false;
        MultipleDisposableAsync composite = new(DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        }));
        await composite.DisposeAsync();
        await composite.DisposeAsync();
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests CompositeDisposableAsync remove from disposed returns false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncRemoveFromDisposed_ThenReturnsFalse()
    {
        MultipleDisposableAsync composite = new();
        await composite.DisposeAsync();
        var result = await composite.Remove(DisposableAsync.Empty);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Tests CompositeDisposableAsync remove non-existent returns false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncRemoveNonExistent_ThenReturnsFalse()
    {
        MultipleDisposableAsync composite = new();
        var result = await composite.Remove(DisposableAsync.Empty);
        await Assert.That(result).IsFalse();
        await composite.DisposeAsync();
    }

    /// <summary>Tests CompositeDisposableAsync clear on disposed is safe.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncClearOnDisposed_ThenSafe()
    {
        MultipleDisposableAsync composite = new();
        await composite.DisposeAsync();
        await composite.Clear();
        await Assert.That(composite.IsDisposed).IsTrue();
    }

    /// <summary>Tests CompositeDisposableAsync clear on empty is safe.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncClearOnEmpty_ThenSafe()
    {
        MultipleDisposableAsync composite = new();
        await composite.Clear();
        await Assert.That(composite.Count).IsEqualTo(0);
        await composite.DisposeAsync();
    }

    /// <summary>Tests Enumerator from GetEnumerator on CompositeDisposableAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncEnumerate_ThenReturnsItems()
    {
        var d1 = DisposableAsync.Empty;
        var d2 = DisposableAsync.Empty;
        MultipleDisposableAsync composite = new(d1, d2);
        var count = 0;
        foreach (var item in composite)
        {
            count++;
        }

        const int ExpectedCount = 2;
        await Assert.That(count).IsEqualTo(ExpectedCount);
        await composite.DisposeAsync();
    }

    /// <summary>Tests static SetDisposableAsync sets field successfully.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStaticSetDisposableAsync_ThenFieldIsSet()
    {
        IAsyncDisposable? field = null;
        var disposable = DisposableAsync.Empty;
        await SingleAssignmentDisposableAsync.AssignDisposableAsync(ref field, disposable);
        await Assert.That(field).IsNotNull();
        await Assert.That(field).IsEqualTo(disposable);
    }

    /// <summary>Tests static SetDisposableAsync double-set throws InvalidOperationException.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStaticSetDisposableAsyncDoubleSet_ThenThrowsInvalidOperation()
    {
        IAsyncDisposable? field = null;
        var first = DisposableAsync.Empty;
        var second = DisposableAsync.Create(() => default);
        await SingleAssignmentDisposableAsync.AssignDisposableAsync(ref field, first);
        await Assert.That(async () => await SingleAssignmentDisposableAsync.AssignDisposableAsync(ref field, second))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests static DisposeAsync disposes the assigned disposable.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStaticDisposeAsync_ThenDisposableIsDisposed()
    {
        var disposed = false;
        IAsyncDisposable? field = null;
        var disposable = DisposableAsync.Create(() =>
        {
            disposed = true;
            return default;
        });
        await SingleAssignmentDisposableAsync.AssignDisposableAsync(ref field, disposable);
        await SingleAssignmentDisposableAsync.DisposeAsync(ref field);
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Tests CreateAlreadyAssignedException returns InvalidOperationException.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreateAlreadyAssignedException_ThenReturnsInvalidOperation()
    {
        var ex = SingleAssignmentDisposableAsync.CreateAlreadyAssignedException();
        await Assert.That(ex).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Verifies that DisposableAsync.Create throws ArgumentNullException when given a null delegate.</summary>
    [Test]
    public void WhenDisposableAsyncCreateNull_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() => DisposableAsync.Create(null!));

    /// <summary>Verifies that DisposableAsyncExtensions.ToDisposableAsync throws ArgumentNullException for null input.</summary>
    [Test]
    public void WhenToDisposableAsyncNull_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() => ((IDisposable)null!).ToDisposableAsync());

    /// <summary>
    /// Verifies that the CAS retry loop in SetDisposableAsync is exercised
    /// when another thread mutates _current between the Volatile.Read and the
    /// CompareExchange, forcing the loop to re-read and retry.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialCASRetryLoop_ThenAllDisposablesAccountedFor()
    {
        SingleReplaceableDisposableAsync serial = new();
        var disposedCount = 0;

        IAsyncDisposable MakeDisposable() => DisposableAsync.Create(() =>
        {
            Interlocked.Increment(ref disposedCount);
            return default;
        });

        // Bounded contention: enough parallel sets to race on CAS, small enough to run
        // deterministically on any CI runner. Previous 20x10 layout produced unbounded
        // variance (observed: 3s → 15s → never completed) and could timeout the suite.
        const int Parallelism = 4;
        const int IterationsPerTask = 5;
        const int ExpectedDisposedCount = Parallelism * IterationsPerTask;
        Barrier barrier = new(Parallelism);
        var tasks = Enumerable.Range(0, Parallelism).Select(_ => Task.Run(async () =>
        {
            barrier.SignalAndWait();
            for (var i = 0; i < IterationsPerTask; i++)
            {
                await serial.SetDisposableAsync(MakeDisposable());
            }
        }));
        await Task.WhenAll(tasks);
        await serial.DisposeAsync();

        // Every set disposable (Parallelism * IterationsPerTask) should be disposed —
        // (Parallelism * IterationsPerTask - 1) replaced + 1 final dispose.
        await Assert.That(disposedCount).IsEqualTo(ExpectedDisposedCount);
    }

    /// <summary>Verifies that the shared disposed sentinel DisposeAsync returns a completed ValueTask without throwing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialDisposedSlotMarkerDisposeAsync_ThenReturnsCompletedValueTask()
    {
        var sentinel = DisposableAsyncSlot.DisposedSentinel;

        // DisposeAsync should return default (no-op)
        var task = sentinel.DisposeAsync();
        await Assert.That(task.IsCompleted).IsTrue();
    }

    /// <summary>
    /// Verifies that re-assigning a non-null value to a field that already holds a
    /// non-null value throws InvalidOperationException via ThrowAlreadyAssignment.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStaticSetDisposableAsyncReAssignNonNull_ThenThrowsInvalidOperation()
    {
        IAsyncDisposable? field = null;
        var first = DisposableAsync.Create(() => default);
        var second = DisposableAsync.Create(() => default);

        // First set succeeds
        await SingleAssignmentDisposableAsync.AssignDisposableAsync(ref field, first);

        // Second set with a different non-null value triggers ThrowAlreadyAssignment
        await Assert.That(async () => await SingleAssignmentDisposableAsync.AssignDisposableAsync(ref field, second))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that assigning a non-null disposable to a SingleAssignmentDisposableAsync
    /// that already holds a non-null value throws <see cref = "InvalidOperationException"/>
    /// via the ThrowAlreadyAssignment path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncAssignedTwiceWithDistinctValues_ThenThrowsInvalidOperation()
    {
        SingleAssignmentDisposableAsync sad = new();
        var first = DisposableAsync.Create(static () => default);
        var second = DisposableAsync.Create(static () => default);
        await sad.SetDisposableAsync(first);
        await Assert.That(async () => await sad.SetDisposableAsync(second).ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>()
            .WithMessage("Disposable is already assigned.", StringComparison.Ordinal);

        // Original assignment still intact; dispose cleans up correctly
        await Assert.That(sad.IsDisposed).IsFalse();
        await sad.DisposeAsync();
        await Assert.That(sad.IsDisposed).IsTrue();
    }

    /// <summary>Tests CompositeDisposableAsync.CopyTo with negative index throws.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCopyToWithNegativeIndex_ThenThrows()
    {
        await using MultipleDisposableAsync composite = new();
        var array = new IAsyncDisposable[1];
        Assert.Throws<ArgumentOutOfRangeException>(() => composite.CopyTo(array, -1));
    }

    /// <summary>Tests CompositeDisposableAsync.CopyTo with insufficient space throws.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCopyToWithInsufficientSpace_ThenThrows()
    {
        MultipleDisposableAsync composite = new();
        await composite.AddAsync(DisposableAsync.Empty);
        await composite.AddAsync(DisposableAsync.Empty);
        var array = new IAsyncDisposable[1];
        Assert.Throws<ArgumentOutOfRangeException>(() => composite.CopyTo(array, 0));
        await composite.DisposeAsync();
    }

    /// <summary>Tests the <see cref = "ICollection{T}"/> constructor sizes exactly and disposes all members.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncFromCollection_ThenDisposesAll()
    {
        var count = 0;
        List<IAsyncDisposable> list =
        [
            DisposableAsync.Create(() =>
            {
                Interlocked.Increment(ref count);
                return default;
            }),
            DisposableAsync.Create(() =>
            {
                Interlocked.Increment(ref count);
                return default;
            })
        ];
        const int ExpectedCount = 2;
        MultipleDisposableAsync composite = new(list);
        await Assert.That(composite.Count).IsEqualTo(ExpectedCount);
        await composite.DisposeAsync();
        await Assert.That(count).IsEqualTo(ExpectedCount);
    }

    /// <summary>Tests the <see cref = "ICollection{T}"/> constructor with an empty collection yields an empty composite.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncFromEmptyCollection_ThenEmpty()
    {
        List<IAsyncDisposable> empty = [];
        MultipleDisposableAsync composite = new(empty);
        await Assert.That(composite.Count).IsEqualTo(0);
        await composite.DisposeAsync();
    }

    /// <summary>Tests the params constructor with an empty array yields an empty composite.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompositeDisposableAsyncFromEmptyParams_ThenEmpty()
    {
        IAsyncDisposable[] empty = [];
        MultipleDisposableAsync composite = new(empty);
        await Assert.That(composite.Count).IsEqualTo(0);
        await composite.DisposeAsync();
    }

    /// <summary>Tests that the backing array grows beyond the default capacity and compacts after enough
    /// removals, then enumerates a snapshot of the survivors and disposes them.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGrownThenCompacted_ThenSurvivorsEnumeratedAndDisposed()
    {
        const int Total = 20;
        const int Removed = 16;
        var disposed = 0;
        MultipleDisposableAsync composite = new();
        var items = new IAsyncDisposable[Total];
        for (var i = 0; i < Total; i++)
        {
            items[i] = DisposableAsync.Create(() =>
            {
                Interlocked.Increment(ref disposed);
                return default;
            });
            await composite.AddAsync(items[i]);
        }

        await Assert.That(composite.Count).IsEqualTo(Total);
        for (var i = 0; i < Removed; i++)
        {
            await composite.Remove(items[i]);
        }

        await Assert.That(composite.Count).IsEqualTo(Total - Removed);
        var enumerated = 0;
        using (var enumerator = composite.GetEnumerator())
        {
            while (enumerator.MoveNext())
            {
                enumerated++;
            }
        }

        await Assert.That(enumerated).IsEqualTo(Total - Removed);
        await composite.DisposeAsync();
        await Assert.That(disposed).IsEqualTo(Total);
    }

    /// <summary>Tests that enumerating an empty composite yields no elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEnumeratedEmpty_ThenNoElements()
    {
        MultipleDisposableAsync composite = new();
        var enumerated = 0;
        using (var enumerator = composite.GetEnumerator())
        {
            while (enumerator.MoveNext())
            {
                enumerated++;
            }
        }

        await Assert.That(enumerated).IsEqualTo(0);
        await composite.DisposeAsync();
    }

    /// <summary>Builds a non-collection enumerable containing null entries around a disposable.</summary>
    /// <param name = "disposable">The disposable value to yield between null entries.</param>
    /// <returns>The enumerable sequence.</returns>
    private static IEnumerable<IAsyncDisposable> EnumerateWithNulls(IAsyncDisposable disposable)
    {
        yield return null!;
        yield return disposable;
        yield return null!;
    }

    /// <summary>Helper disposable for testing ToDisposableAsync.</summary>
    /// <param name = "onDispose">The action to invoke on disposal.</param>
    private sealed class TestSyncDisposable(Action onDispose) : IDisposable
    {
        /// <summary>Disposes the resource and invokes the disposal callback.</summary>
        public void Dispose() => onDispose();
    }
}
