// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Command-line entry point for the collaboration server example.</summary>
internal static class Program
{
    /// <summary>Runs the configured collaboration server.</summary>
    /// <param name="args">The command-line arguments.</param>
    /// <returns>The asynchronous process task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task Main(string[] args) =>
        CollaborationServerExample.RunAsync(CollaborationServerOptions.FromEnvironment(args));
}
