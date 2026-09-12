// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests the boolean disposal latch.</summary>
public class BooleanDisposableTests
{
    /// <summary>The debugger can display both live and disposed instances without altering their state.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DebuggerDisplay_PreservesDisposalState()
    {
        BooleanDisposable disposable = new();
        await Assert.That(GetDebuggerDisplay(disposable)).IsEqualTo(disposable.ToString());
        await Assert.That(disposable.IsDisposed).IsFalse();

        disposable.Dispose();

        await Assert.That(GetDebuggerDisplay(disposable)).IsEqualTo(disposable.ToString());
        await Assert.That(disposable.IsDisposed).IsTrue();
    }

    /// <summary>Invokes the getter used by the debugger without reflection.</summary>
    /// <param name="disposable">The instance to display.</param>
    /// <returns>The debugger display text.</returns>
    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_DebuggerDisplay")]
    private static extern string GetDebuggerDisplay(BooleanDisposable disposable);
}
