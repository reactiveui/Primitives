// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests single-assignment ownership and disposal.</summary>
public partial class DisposableTests
{
    /// <summary>Tests SingleAssignmentDisposableAsync disposes assigned.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsync_ThenDisposesAssigned()
    {
        SingleAssignmentDisposableAsync sad = new();
        StrongBox<bool> disposed = new();
        await sad.SetDisposableAsync(DisposableAsync.Create(disposed, static state =>
        {
            state.Value = true;
            return default;
        }));
        await Assert.That(sad.IsDisposed).IsFalse();
        await sad.DisposeAsync();
        await Assert.That(sad.IsDisposed).IsTrue();
        await Assert.That(disposed.Value).IsTrue();
    }

    /// <summary>Tests SingleAssignment dispose before set disposes immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncDisposeBeforeSet_ThenSetDisposedImmediately()
    {
        SingleAssignmentDisposableAsync sad = new();
        await sad.DisposeAsync();
        StrongBox<bool> disposed = new();
        await sad.SetDisposableAsync(DisposableAsync.Create(disposed, static state =>
        {
            state.Value = true;
            return default;
        }));
        await Assert.That(disposed.Value).IsTrue();
    }

    /// <summary>Tests SingleAssignment double set throws.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncDoubleSet_ThenThrowsInvalidOperation()
    {
        SingleAssignmentDisposableAsync sad = new();
        await sad.SetDisposableAsync(DisposableAsync.Empty);
        await Assert.That(async () => await sad.SetDisposableAsync(DisposableAsync.Empty))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests SingleAssignment get before set returns null.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncGetBeforeSet_ThenReturnsNull()
    {
        SingleAssignmentDisposableAsync sad = new();
        await Assert.That(sad.GetDisposable()).IsNull();
    }

    /// <summary>Tests SingleAssignment get after dispose returns non-null.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncGetAfterDispose_ThenReturnsEmpty()
    {
        SingleAssignmentDisposableAsync sad = new();
        await sad.DisposeAsync();
        await Assert.That(sad.GetDisposable()).IsNotNull();
    }

    /// <summary>Tests SingleAssignment get after set returns assigned.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAssignmentDisposableAsyncGetAfterSet_ThenReturnsAssigned()
    {
        SingleAssignmentDisposableAsync sad = new();
        var original = DisposableAsync.Empty;
        await sad.SetDisposableAsync(original);
        await Assert.That(sad.GetDisposable()).IsSameReferenceAs(original);
    }
}
