// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>One strongly typed <c>Events()</c> overload, which replaces the placeholder for a given host.</summary>
/// <param name="TypeReference">The fully qualified host reference the overload accepts.</param>
/// <param name="WrapperReference">The fully qualified wrapper reference the overload returns.</param>
/// <param name="TypeParameterList">The overload's type parameter list, or empty for a non-generic host.</param>
/// <param name="Constraints">One <c>where</c> clause per line without indentation, or empty when unconstrained.</param>
/// <param name="DocumentationName">The host reference, escaped for the generated documentation comment.</param>
/// <param name="SupportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
/// <remarks>
/// Kept apart from <see cref="InstanceTargetModel"/> so the one file carrying every overload only re-emits when an
/// overload signature actually moves, rather than whenever any wrapper's events change.
/// </remarks>
internal sealed record ActivationModel(
    string TypeReference,
    string WrapperReference,
    string TypeParameterList,
    string Constraints,
    string DocumentationName,
    bool SupportsNullableAnnotations);
