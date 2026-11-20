// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <c>WhereTrue</c> and <c>WhereFalse</c> idempotency and empty short-circuits.</summary>
public sealed class WhereTrueObservableTests
{
    /// <summary>WhereTrue on an already-filtered WhereTrue sequence returns the same instance.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhereTrue_AppliedTwice_ReturnsSameInstance()
    {
        var filtered = Signal.FromEnumerable([true, false, true]).WhereTrue();
        await Assert.That(filtered.WhereTrue()).IsSameReferenceAs(filtered);
    }

    /// <summary>WhereFalse on an already-filtered WhereFalse sequence returns the same instance.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhereFalse_AppliedTwice_ReturnsSameInstance()
    {
        var filtered = Signal.FromEnumerable([true, false, true]).WhereFalse();
        await Assert.That(filtered.WhereFalse()).IsSameReferenceAs(filtered);
    }

    /// <summary>Nested direct construction unwraps the inner source and forwards live completion across WhereTrue, WhereFalse, and WhereIsNotNull.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NestedDirectObservables_UnwrapInnerSourceAndForwardCompletion()
    {
        using Signal<bool> boolSource = new();
        List<bool> trues = [];
        var trueCompleted = false;
        using var subTrue = new ReactiveUI.Primitives.Extensions.Operators.WhereTrueObservable(
            new ReactiveUI.Primitives.Extensions.Operators.WhereTrueObservable(boolSource))
            .Subscribe(trues.Add, () => trueCompleted = true);

        List<bool> falses = [];
        var falseCompleted = false;
        using var subFalse = new ReactiveUI.Primitives.Extensions.Operators.WhereFalseObservable(
            new ReactiveUI.Primitives.Extensions.Operators.WhereFalseObservable(boolSource))
            .Subscribe(falses.Add, () => falseCompleted = true);

        boolSource.OnNext(true);
        boolSource.OnNext(false);
        boolSource.OnCompleted();

        await Assert.That(trues).IsCollectionEqualTo([true]);
        await Assert.That(trueCompleted).IsTrue();
        await Assert.That(falses).IsCollectionEqualTo([false]);
        await Assert.That(falseCompleted).IsTrue();

        using Signal<string?> stringSource = new();
        List<string?> nonNulls = [];
        var nonNullCompleted = false;
        using var subNotNull = new ReactiveUI.Primitives.Extensions.Operators.WhereIsNotNullObservable<string?>(
            new ReactiveUI.Primitives.Extensions.Operators.WhereIsNotNullObservable<string?>(stringSource))
            .Subscribe(nonNulls.Add, () => nonNullCompleted = true);

        stringSource.OnNext("alpha");
        stringSource.OnNext(null);
        stringSource.OnCompleted();

        await Assert.That(nonNulls.SequenceEqual(["alpha"])).IsTrue();
        await Assert.That(nonNullCompleted).IsTrue();
    }
}
