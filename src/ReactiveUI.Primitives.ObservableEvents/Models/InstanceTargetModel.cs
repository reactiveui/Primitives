// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>One host type reached through an <c>Events()</c> call, ready to emit as a wrapper and an overload.</summary>
/// <param name="Identity">The host's fully qualified name, which is both the dedup key and the hash source.</param>
/// <param name="DisplayName">The host's readable name, as it appears in a diagnostic message.</param>
/// <param name="DocumentationName">The host reference, escaped for the generated documentation comment.</param>
/// <param name="HintName">The generated file name for the wrapper.</param>
/// <param name="Namespace">The namespace to emit into, or empty for the global namespace.</param>
/// <param name="WrapperName">The generated wrapper class name.</param>
/// <param name="WrapperReference">The fully qualified wrapper reference, including type arguments.</param>
/// <param name="TypeReference">The fully qualified host reference, including type arguments.</param>
/// <param name="TypeParameterList">The wrapper's type parameter list, or empty for a non-generic host.</param>
/// <param name="Constraints">One <c>where</c> clause per line without indentation, or empty when unconstrained.</param>
/// <param name="SupportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
/// <param name="Events">The events to expose.</param>
/// <param name="Diagnostics">What extraction found wrong, reported once a provider is known to exist.</param>
/// <param name="Location">The activation call site, for the diagnostics that point at the request itself.</param>
internal sealed record InstanceTargetModel(
    string Identity,
    string DisplayName,
    string DocumentationName,
    string HintName,
    string Namespace,
    string WrapperName,
    string WrapperReference,
    string TypeReference,
    string TypeParameterList,
    string Constraints,
    bool SupportsNullableAnnotations,
    EquatableArray<EventModel> Events,
    EquatableArray<DiagnosticInfo> Diagnostics,
    LocationInfo? Location)
{
    /// <summary>Creates the activation overload this host's wrapper is reached through.</summary>
    /// <returns>The overload model.</returns>
    /// <remarks>
    /// Projected out rather than stored, so the one file carrying every overload compares equal - and stays
    /// uncompiled - when a host's events change but its signature does not.
    /// </remarks>
    internal ActivationModel ToActivation() =>
        new(
            TypeReference,
            WrapperReference,
            TypeParameterList,
            Constraints,
            DocumentationName,
            SupportsNullableAnnotations);
}
