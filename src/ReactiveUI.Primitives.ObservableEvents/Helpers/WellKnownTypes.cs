// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>Caches framework symbols used during event extraction.</summary>
/// <param name="Task">The resolved task type, or null when the consumer cannot see it.</param>
/// <param name="ValueTask">The resolved value task type, or null when the consumer cannot see it.</param>
internal readonly record struct WellKnownTypes(INamedTypeSymbol? Task, INamedTypeSymbol? ValueTask)
{
    /// <summary>Resolves the types from a consumer compilation.</summary>
    /// <param name="compilation">The consumer compilation.</param>
    /// <returns>The resolved types.</returns>
    internal static WellKnownTypes From(Compilation compilation) =>
        new(
            compilation.GetTypeByMetadataName(Constants.TaskMetadataName),
            compilation.GetTypeByMetadataName(Constants.ValueTaskMetadataName));
}
