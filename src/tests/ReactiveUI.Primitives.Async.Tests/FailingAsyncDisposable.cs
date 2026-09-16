// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Asynchronous disposable whose disposal faults with a supplied exception.</summary>
/// <param name="failure">The exception the disposal faults with.</param>
internal sealed class FailingAsyncDisposable(Exception failure) : IAsyncDisposable
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => ValueTask.FromException(failure);
}
