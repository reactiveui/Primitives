// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests grouped resource ownership.</summary>
public class ScopeTests
{
    /// <summary>Both combination overloads release every resource once.</summary>
    /// <param name="useArray">Whether to use the array overload instead of the pair overload.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Combine_DisposeTwice_ReleasesBothResourcesOnce(bool useArray)
    {
        int[] counts = [0, 0];
        using var first = Scope.Create(counts, static state => state[0]++);
        using var second = Scope.Create(counts, static state => state[1]++);
        var combined = useArray ? Scope.Combine([first, second]) : Scope.Combine(first, second);

        combined.Dispose();
        combined.Dispose();

        await Assert.That(counts[0]).IsEqualTo(1);
        await Assert.That(counts[1]).IsEqualTo(1);
    }

    /// <summary>A null dispose action yields the shared empty disposable from both factory overloads.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Create_NullAction_ReturnsEmpty()
    {
        await Assert.That(Scope.Create(null!)).IsSameReferenceAs(Scope.Empty);
        await Assert.That(Scope.Create(0, (Action<int>)null!)).IsSameReferenceAs(Scope.Empty);
    }

    /// <summary>A dispose action yields a disposable of its own rather than the shared empty one.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Create_Action_ReturnsActionDisposable()
    {
        using var disposable = Scope.Create(static () => { });

        await Assert.That(disposable).IsNotSameReferenceAs(Scope.Empty);
    }
}
