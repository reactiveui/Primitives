// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>
    /// Tests ForEach flattens enumerables.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ForEach_FlattensEnumerables()
    {
        var source = Observable.Return(ExpectedSequence123);
        var results = new List<int>();
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Tests FastForEach with List source emits all items via OnNext.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithList_ThenEmitsAllItems()
    {
        var source = Observable.Return(new List<int> { 1, 2, 3 } as IEnumerable<int>);
        var results = new List<int>();
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Tests FastForEach with array source emits all items via OnNext.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithArray_ThenEmitsAllItems()
    {
        var arr = new[] { 1, 2, 3 };
        var results = new List<int>();
        using var sub = arr.FromArray().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Tests FastForEach with generic IEnumerable source emits all items.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithEnumerable_ThenEmitsAllItems()
    {
        var source = Observable.Return(Enumerate());
        var results = new List<int>();
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);

        static IEnumerable<int> Enumerate()
        {
            yield return 1;
            yield return SampleValue2;
            yield return SampleValue3;
        }
    }

    /// <summary>
    /// Tests FastForEach with IList branch using ArraySegment which implements IList but not List or T[].
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithIList_ThenEmitsAllItems()
    {
        IList<int> ilist = new ArraySegment<int>([10, 20, 30]);
        var source = Observable.Return((IEnumerable<int>)ilist);
        var results = new List<int>();
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([SampleValue10, SampleValue20, SampleValue30]);
    }

    /// <summary>
    /// Tests FastForEach with an array passed through ForEach (Observable of IEnumerable).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEachWithArrayViaForEach_ThenEmitsAllItems()
    {
        var arr = new[] { 10, 20, 30 };
        var source = Observable.Return(arr as IEnumerable<int>);
        var results = new List<int>();
        using var sub = source.ForEach().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([SampleValue10, SampleValue20, SampleValue30]);
    }

    /// <summary>
    /// Tests FastForEach with an IList source.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEach_GivenIList_ThenAllItemsEmitted()
    {
        // Given
        List<int> source = [1, 2, 3];
        var received = new List<int>();
        var observer = Observer.Create<int>(received.Add);

        // When
        observer.FastForEach(source);

        // Then
        await Assert.That(received).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Tests FastForEach with an array source.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFastForEach_GivenArray_ThenAllItemsEmitted()
    {
        // Given
        var source = new[] { 10, 20, 30 };
        var received = new List<int>();
        var observer = Observer.Create<int>(received.Add);

        // When
        observer.FastForEach(source);

        // Then
        await Assert.That(received).IsCollectionEqualTo([SampleValue10, SampleValue20, SampleValue30]);
    }

    /// <summary>
    /// Tests FastForEach with a plain IEnumerable source.
    /// </summary>
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

        var received = new List<int>();
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
        var arr = new[] { 5, 10, 15 };
        var received = new List<int>();
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
        var source = new[] { 99 };
        var received = new List<int>();
        var observer = Observer.Create<int>(received.Add);

        // When
        observer.FastForEach(source);

        // Then
        await Assert.That(received).IsCollectionEqualTo([SampleValue99]);
    }
}
