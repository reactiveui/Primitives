// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Linq;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <see cref="SelectManyThenCoordinator{TSource, TMid, TResult}"/>.</summary>
public class SelectManyThenCoordinatorTests
{
    /// <summary>Verifies the coordinator rejects a null observer, first selector or second selector.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConstructedWithNullArgument_ThenThrowsArgumentNullException()
    {
        var observer = Observer.Create<int>(static _ => { });
        Func<int, IObservable<int>> first = Observable.Return;
        Func<int, IObservable<int>> second = Observable.Return;

        await Assert.That(() => new SelectManyThenCoordinator<int, int, int>(null!, first, second)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SelectManyThenCoordinator<int, int, int>(observer, null!, second)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SelectManyThenCoordinator<int, int, int>(observer, first, null!)).ThrowsExactly<ArgumentNullException>();
    }
}
