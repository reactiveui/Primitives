// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Reactive.Disposables;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Tests for the container that presents itself to System.Reactive as a composite.</summary>
public class ContainerDisposableTests
{
    /// <summary>The number of disposables seeded by the two-argument constructor.</summary>
    private const int PairCount = 2;

    /// <summary>The number of disposables seeded by the three-argument constructor.</summary>
    private const int TripleCount = 3;

    /// <summary>The number of disposables seeded by the array constructor.</summary>
    private const int BatchCount = 4;

    /// <summary>The number of disposables seeded across every constructor.</summary>
    private const int SeededCount = PairCount + TripleCount + BatchCount;

    /// <summary>The number of registrations made directly on the container plus through its composite.</summary>
    private const int DirectAndComposedCount = 2;

    /// <summary>Verifies the container hands the same composite to every conversion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConversionYieldsTheSameCompositeEveryTime()
    {
        using ContainerDisposable container = [];

        CompositeDisposable first = container;
        CompositeDisposable second = container;

        await Assert.That(second).IsSameReferenceAs(first);
        await Assert.That(container.ToCompositeDisposable()).IsSameReferenceAs(first);
    }

    /// <summary>Verifies disposing the container disposes everything registered through the composite.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposingTheContainerDisposesTheCompositeContents()
    {
        ContainerDisposable container = [];
        var disposalCount = 0;
        CompositeDisposable composite = container;
        composite.Add(new ActionDisposable(() => disposalCount++));

        await Assert.That(disposalCount).IsEqualTo(0);

        container.Dispose();

        await Assert.That(disposalCount).IsEqualTo(1);
        await Assert.That(composite.IsDisposed).IsTrue();
    }

    /// <summary>Verifies converting a disposed container yields a sink that disposes late arrivals.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConversionAfterDisposalDisposesLateArrivals()
    {
        ContainerDisposable container = [];
        container.Dispose();
        var disposalCount = 0;

        CompositeDisposable composite = container;
        composite.Add(new ActionDisposable(() => disposalCount++));

        await Assert.That(composite.IsDisposed).IsTrue();
        await Assert.That(disposalCount).IsEqualTo(1);
        await Assert.That(container.ToCompositeDisposable()).IsSameReferenceAs(composite);
    }

    /// <summary>Verifies clearing the container replaces the composite it disposed rather than reusing it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ClearReplacesTheCompositeSoTheContainerStaysUsable()
    {
        using ContainerDisposable container = [];
        var disposalCount = 0;
        CompositeDisposable beforeClear = container;
        beforeClear.Add(new ActionDisposable(() => disposalCount++));

        container.Clear();

        await Assert.That(disposalCount).IsEqualTo(1);
        await Assert.That(beforeClear.IsDisposed).IsTrue();

        CompositeDisposable afterClear = container;
        afterClear.Add(new ActionDisposable(() => disposalCount++));

        await Assert.That(afterClear).IsNotSameReferenceAs(beforeClear);
        await Assert.That(afterClear.IsDisposed).IsFalse();
        await Assert.That(disposalCount).IsEqualTo(1);
    }

    /// <summary>Verifies the conversion rejects a null container.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConversionThrowsForNullContainer()
    {
        var exception = Assert.Throws<ArgumentNullException>(static () =>
            _ = (CompositeDisposable)(ContainerDisposable)null!);

        await Assert.That(exception.ParamName).IsEqualTo("container");
    }

    /// <summary>Verifies the container still behaves as a group of disposables in its own right.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ContainerTracksItsOwnRegistrationsAlongsideTheComposite()
    {
        ContainerDisposable container = [];
        var disposalCount = 0;
        ActionDisposable direct = new(() => disposalCount++);
        container.Add(direct);
        CompositeDisposable composite = container;
        composite.Add(new ActionDisposable(() => disposalCount++));

        await Assert.That(container.Contains(direct)).IsTrue();

        container.Dispose();

        await Assert.That(disposalCount).IsEqualTo(DirectAndComposedCount);
    }

    /// <summary>Verifies the constructors seed the container with the supplied disposables.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorsSeedTheSuppliedDisposables()
    {
        var disposalCount = 0;

        ActionDisposable First() => new(() => disposalCount++);

        ContainerDisposable pair = new(First(), First());
        ContainerDisposable triple = new(First(), First(), First());
        ContainerDisposable batch = new([First(), First(), First(), First()]);

        await Assert.That(pair.Count).IsEqualTo(PairCount);
        await Assert.That(triple.Count).IsEqualTo(TripleCount);
        await Assert.That(batch.Count).IsEqualTo(BatchCount);

        pair.Dispose();
        triple.Dispose();
        batch.Dispose();

        await Assert.That(disposalCount).IsEqualTo(SeededCount);
    }
}
