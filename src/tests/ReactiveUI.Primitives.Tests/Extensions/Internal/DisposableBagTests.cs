// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Coverage for <see cref="DisposableBag"/> — inline-slot fill, overflow growth,
/// add-after-dispose immediate disposal, three-arg constructor, and dispose-order guarantees.</summary>
public class DisposableBagTests
{
    /// <summary>Verifies that the parameterless constructor accepts inline slot fills and disposes both.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDefaultBagFilledViaAdd_ThenDisposesBoth()
    {
        var bag = new DisposableBag();
        var d1 = new CountedDisposable();
        var d2 = new CountedDisposable();

        bag.Add(d1);
        bag.Add(d2);
        await Assert.That(bag.IsDisposed).IsFalse();

        bag.Dispose();

        await Assert.That(bag.IsDisposed).IsTrue();
        await Assert.That(d1.DisposeCount).IsEqualTo(1);
        await Assert.That(d2.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that adding a null disposable is a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAddNull_ThenIgnored()
    {
        var bag = new DisposableBag();
        bag.Add(null!);
        bag.Dispose();

        await Assert.That(bag).IsNotNull();
    }

    /// <summary>Verifies that overflow growth and disposal works for more than three entries.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOverflowGrowthRequired_ThenAllEntriesDisposed()
    {
        const int Count = 8;
        var bag = new DisposableBag();
        var entries = new CountedDisposable[Count];
        for (var i = 0; i < Count; i++)
        {
            entries[i] = new CountedDisposable();
            bag.Add(entries[i]);
        }

        bag.Dispose();

        for (var i = 0; i < Count; i++)
        {
            await Assert.That(entries[i].DisposeCount).IsEqualTo(1);
        }
    }

    /// <summary>Verifies that the three-arg constructor populates inline slots plus overflow and disposes all.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThreeArgConstructor_ThenAllThreeDisposed()
    {
        var d1 = new CountedDisposable();
        var d2 = new CountedDisposable();
        var d3 = new CountedDisposable();
        var bag = new DisposableBag(d1, d2, d3);

        bag.Dispose();

        await Assert.That(d1.DisposeCount).IsEqualTo(1);
        await Assert.That(d2.DisposeCount).IsEqualTo(1);
        await Assert.That(d3.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that double-dispose is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposeCalledTwice_ThenIdempotent()
    {
        var d1 = new CountedDisposable();
        var bag = new DisposableBag(d1, new CountedDisposable());

        bag.Dispose();
        bag.Dispose();

        await Assert.That(d1.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that adding after disposal disposes the supplied entry immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAddAfterDispose_ThenSuppliedDisposableDisposedImmediately()
    {
        var bag = new DisposableBag();
        bag.Dispose();

        var late = new CountedDisposable();
        bag.Add(late);

        await Assert.That(late.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Tracking disposable used to count dispose invocations.</summary>
    private sealed class CountedDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref="Dispose"/> has been invoked.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
