// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests repeated disposal of a single-assignment holder.</summary>
public class OnceDisposableTests
{
    /// <summary>The terminal marker tolerates repeated disposal without releasing the resource again.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_Repeated_ReleasesAssignedResourceOnce()
    {
        int[] counts = [0];
        OnceDisposable holder = new() { Disposable = Scope.Create(counts, static state => state[0]++) };

        holder.Dispose();
        holder.Dispose();

        await Assert.That(counts[0]).IsEqualTo(1);
        await Assert.That(holder.IsDisposed).IsTrue();
        await Assert.That(holder.Disposable).IsNull();
    }
}
