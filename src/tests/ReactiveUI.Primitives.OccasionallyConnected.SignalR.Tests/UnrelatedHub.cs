// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.SignalR;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Echoes a host-owned DTO through the standard JSON protocol.</summary>
public sealed class UnrelatedHub : Hub
{
    /// <summary>Returns the host-owned value.</summary>
    /// <param name="message">The host value.</param>
    /// <returns>The same value.</returns>
    /// <exception cref="OperationCanceledException">The connection was canceled.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public UnrelatedMessage Echo(UnrelatedMessage message) => Context.ConnectionAborted.IsCancellationRequested
        ? throw new OperationCanceledException(Context.ConnectionAborted)
        : message;
}
