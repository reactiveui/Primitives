// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the boolean-reduce operators backed by
/// <c>BooleanReduceObservable</c> — empty-source short-circuit, partial-value
/// suppression, target match/mismatch, error broadcast.</summary>
public class BooleanReduceObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Verifies that an empty input emits a single <c>true</c> and completes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllTrueWithEmptySources_ThenEmitsTrueAndCompletes()
    {
        var results = new List<bool>();
        var completed = false;

        using var sub = Array.Empty<IObservable<bool>>()
            .CombineLatestValuesAreAllTrue()
            .Subscribe(results.Add, () => completed = true);

        await Assert.That(results).IsCollectionEqualTo([true]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that partial sources do not emit until every source has a value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllTruePartialSources_ThenSuppressesEmission()
    {
        var a = new Subject<bool>();
        var b = new Subject<bool>();
        var results = new List<bool>();

        using var sub = new IObservable<bool>[] { a, b }
            .CombineLatestValuesAreAllTrue()
            .Subscribe(results.Add);

        a.OnNext(true);

        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that the operator emits <c>true</c> only when every latest value is <c>true</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllTrueTransitions_ThenEmitsExpectedSequence()
    {
        var a = new Subject<bool>();
        var b = new Subject<bool>();
        var results = new List<bool>();

        using var sub = new IObservable<bool>[] { a, b }
            .CombineLatestValuesAreAllTrue()
            .Subscribe(results.Add);

        a.OnNext(true);
        b.OnNext(false);
        b.OnNext(true);
        a.OnNext(false);

        await Assert.That(results).IsCollectionEqualTo([false, true, false]);
    }

    /// <summary>Verifies that <c>AllFalse</c> emits <c>true</c> only when every latest value is <c>false</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllFalseTransitions_ThenEmitsExpectedSequence()
    {
        var a = new Subject<bool>();
        var b = new Subject<bool>();
        var results = new List<bool>();

        using var sub = new IObservable<bool>[] { a, b }
            .CombineLatestValuesAreAllFalse()
            .Subscribe(results.Add);

        a.OnNext(false);
        b.OnNext(false);
        b.OnNext(true);

        await Assert.That(results).IsCollectionEqualTo([true, false]);
    }

    /// <summary>Verifies that a source error propagates downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllTrueSourceErrors_ThenForwardsError()
    {
        var a = new Subject<bool>();
        var b = new Subject<bool>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = new IObservable<bool>[] { a, b }
            .CombineLatestValuesAreAllTrue()
            .Subscribe(static _ => { }, ex => caught = ex);

        a.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies collection-only source materialization uses <see cref="ICollection{T}.CopyTo"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourcesAreCollectionOnly_ThenMaterializedByCopyTo()
    {
        using var a = new BehaviorSubject<bool>(true);
        using var b = new BehaviorSubject<bool>(true);
        var sources = new CollectionOnlySources([a.AsObservable(), b.AsObservable()]);
        var results = new List<bool>();

        using var sub = sources.CombineLatestValuesAreAllTrue().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([true]);
    }

    /// <summary>Verifies the non-collection materialization path returns the exact-size buffer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNonCollectionSourcesExactlyFillBuffer_ThenUsesBufferDirectly()
    {
        using var a = new BehaviorSubject<bool>(true);
        using var b = new BehaviorSubject<bool>(true);
        using var c = new BehaviorSubject<bool>(true);
        using var d = new BehaviorSubject<bool>(true);
        var results = new List<bool>();

        using var sub = Enumerate(a, b, c, d)
            .CombineLatestValuesAreAllTrue()
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([true]);

        static IEnumerable<IObservable<bool>> Enumerate(params BehaviorSubject<bool>[] subjects)
        {
            for (var i = 0; i < subjects.Length; i++)
            {
                yield return subjects[i].AsObservable();
            }
        }
    }

    /// <summary>Verifies non-collection materialization grows beyond the initial buffer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNonCollectionSourcesExceedInitialBuffer_ThenGrowsBuffer()
    {
        using var a = new BehaviorSubject<bool>(true);
        using var b = new BehaviorSubject<bool>(true);
        using var c = new BehaviorSubject<bool>(true);
        using var d = new BehaviorSubject<bool>(true);
        using var e = new BehaviorSubject<bool>(true);
        var results = new List<bool>();

        using var sub = Enumerate(a, b, c, d, e)
            .CombineLatestValuesAreAllTrue()
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([true]);

        static IEnumerable<IObservable<bool>> Enumerate(params BehaviorSubject<bool>[] subjects)
        {
            for (var i = 0; i < subjects.Length; i++)
            {
                yield return subjects[i].AsObservable();
            }
        }
    }

    /// <summary>Verifies that when every source completes, the combined sequence completes via
    /// the per-source <c>OnCompleted</c> path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllSourcesComplete_ThenForwardsCompletion()
    {
        var a = new Subject<bool>();
        var b = new Subject<bool>();
        var completed = false;
        IObservable<bool>[] sources = [a, b];

        using var sub = sources.CombineLatestValuesAreAllTrue().Subscribe(static _ => { }, () => completed = true);

        a.OnNext(true);
        b.OnNext(true);
        a.OnCompleted();
        b.OnCompleted();

        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that an <c>OnNext</c> arriving after the combined sequence has terminated
    /// is silently dropped via the <c>_state.IsDone</c> guard.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAfterTerminated_ThenDropped()
    {
        var a = new SyncDirectSource<bool>();
        var b = new SyncDirectSource<bool>();
        var results = new List<bool>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        IObservable<bool>[] sources = [a, b];

        using var sub = sources.CombineLatestValuesAreAllTrue()
            .Subscribe(results.Add, ex => caught = ex);

        a.Observer.OnError(expected);
        b.Observer.OnNext(true);

        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Collection implementation that deliberately avoids <see cref="IReadOnlyList{T}"/>.</summary>
    /// <param name="items">The source items.</param>
    private sealed class CollectionOnlySources(IObservable<bool>[] items) : ICollection<IObservable<bool>>
    {
        /// <inheritdoc/>
        public int Count => items.Length;

        /// <inheritdoc/>
        public bool IsReadOnly => true;

        /// <inheritdoc/>
        public void Add(IObservable<bool> item) => throw new NotSupportedException();

        /// <inheritdoc/>
        public void Clear() => throw new NotSupportedException();

        /// <inheritdoc/>
        public bool Contains(IObservable<bool> item) => Array.IndexOf(items, item) >= 0;

        /// <inheritdoc/>
        public void CopyTo(IObservable<bool>[] array, int arrayIndex) => Array.Copy(items, 0, array, arrayIndex, items.Length);

        /// <inheritdoc/>
        public bool Remove(IObservable<bool> item) => throw new NotSupportedException();

        /// <inheritdoc/>
        public IEnumerator<IObservable<bool>> GetEnumerator() => ((IEnumerable<IObservable<bool>>)items).GetEnumerator();

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
