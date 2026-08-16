// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>The framework types an event delegate is allowed to return, resolved once per extraction.</summary>
/// <param name="Task">The resolved task type, or null when the consumer cannot see it.</param>
/// <param name="ValueTask">The resolved value task type, or null when the consumer cannot see it.</param>
/// <remarks>
/// Resolved up front rather than per event, because an async event host declares many events and each one would
/// otherwise repeat the same two metadata lookups.
/// </remarks>
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
