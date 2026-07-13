// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Linq;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Tests ForEach flattens enumerables.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ForEach_FlattensEnumerables()
    {
        var source = Observable.Return(ExpectedSequence123);
        List<int> results = [];
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests FastForEach with List source emits all items via OnNext.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithList_ThenEmitsAllItems()
    {
        List<int> items = [1, SampleValue2, SampleValue3];
        var source = Observable.Return<IEnumerable<int>>(items);
        List<int> results = [];
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests FastForEach with array source emits all items via OnNext.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithArray_ThenEmitsAllItems()
    {
        int[] arr = [1, SampleValue2, SampleValue3];
        List<int> results = [];
        using var sub = arr.FromArray().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests FastForEach with generic IEnumerable source emits all items.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithEnumerable_ThenEmitsAllItems()
    {
        var source = Observable.Return(Enumerate());
        List<int> results = [];
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);

        static IEnumerable<int> Enumerate()
        {
            yield return 1;
            yield return SampleValue2;
            yield return SampleValue3;
        }
    }

    /// <summary>Tests FastForEach with IList branch using ArraySegment which implements IList but not List or T[].</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithIList_ThenEmitsAllItems()
    {
        IList<int> ilist = new ArraySegment<int>([SampleValue10, SampleValue20, SampleValue30]);
        var source = Observable.Return<IEnumerable<int>>(ilist);
        List<int> results = [];
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([SampleValue10, SampleValue20, SampleValue30]);
    }

    /// <summary>Tests FastForEach with an array passed through ForEach (Observable of IEnumerable).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithArrayViaForEach_ThenEmitsAllItems()
    {
        int[] arr = [SampleValue10, SampleValue20, SampleValue30];
        var source = Observable.Return<IEnumerable<int>>(arr);
        List<int> results = [];
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([SampleValue10, SampleValue20, SampleValue30]);
    }

    /// <summary>Tests FastForEach with an IList source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEach_GivenIList_ThenAllItemsEmitted()
    {
        // Given
        List<int> source = [1, SampleValue2, SampleValue3];
        List<int> received = [];
        var observer = Observer.Create<int>(received.Add);

        // When
        observer.FastForEach(source);

        // Then
        await Assert.That(received).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests FastForEach with an array source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEach_GivenArray_ThenAllItemsEmitted()
    {
        // Given
        int[] source = [SampleValue10, SampleValue20, SampleValue30];
        List<int> received = [];
        var observer = Observer.Create<int>(received.Add);

        // When
        observer.FastForEach(source);

        // Then
        await Assert.That(received).IsCollectionEqualTo([SampleValue10, SampleValue20, SampleValue30]);
    }

    /// <summary>Tests FastForEach with a plain IEnumerable source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEach_GivenIEnumerable_ThenAllItemsEmitted()
    {
        // Given
        static IEnumerable<int> Generate()
        {
            yield return SchedulerWindowTicks;
            yield return SettleDelayMilliseconds;
        }

        List<int> received = [];
        var observer = Observer.Create<int>(received.Add);

        // When
        observer.FastForEach(Generate());

        // Then
        await Assert.That(received).IsCollectionEqualTo([SchedulerWindowTicks, SettleDelayMilliseconds]);
    }

    /// <summary>
    /// Verifies that FastForEach dispatches a T[] array through the dedicated array branch,
    /// which is checked before the IList{T} branch.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithArrayAsIEnumerable_ThenHandledByArrayBranch()
    {
        int[] arr = [SampleValue5, SampleValue10, SampleValue15];
        List<int> received = [];
        var observer = Observer.Create<int>(received.Add);

        observer.FastForEach(arr);

        await Assert.That(received).IsCollectionEqualTo([SampleValue5, SampleValue10, SampleValue15]);
    }

    /// <summary>
    /// Verifies that FastForEach iterates a T[] array correctly when the array contains a
    /// single element, exercising the array branch with a minimal collection.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEach_GivenSingleElementArray_ThenEmitsSingleItem()
    {
        // Given
        int[] source = [SampleValue99];
        List<int> received = [];
        var observer = Observer.Create<int>(received.Add);

        // When
        observer.FastForEach(source);

        // Then
        await Assert.That(received).IsCollectionEqualTo([SampleValue99]);
    }

    /// <summary>Verifies FastForEach handles an <see cref="IReadOnlyList{T}"/> that is not an <see cref="IList{T}"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEach_GivenReadOnlyListOnly_ThenAllItemsEmitted()
    {
        ReadOnlyListOnly<int> source = new([SampleValue5, SampleValue10, SampleValue15]);
        List<int> received = [];
        var observer = Observer.Create<int>(received.Add);

        observer.FastForEach(source);

        await Assert.That(received).IsCollectionEqualTo([SampleValue5, SampleValue10, SampleValue15]);
    }

    /// <summary>Read-only list implementation that deliberately does not implement <see cref="IList{T}"/>.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="items">The backing items.</param>
    private sealed class ReadOnlyListOnly<T>(T[] items) : IReadOnlyList<T>
    {
        /// <inheritdoc/>
        public int Count => items.Length;

        /// <inheritdoc/>
        public T this[int index] => items[index];

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)items).GetEnumerator();

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
