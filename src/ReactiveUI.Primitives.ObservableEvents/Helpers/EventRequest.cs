// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>One host to extract events from, with everything the extraction needs to render them.</summary>
/// <param name="Host">The type whose events are being wrapped.</param>
/// <param name="IsStatic">Whether static rather than instance events are wanted.</param>
/// <param name="TypeParameterNames">The generated type-parameter names, or null for a non-generic host.</param>
/// <param name="SupportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
/// <param name="Location">Where the request was written, for diagnostics.</param>
/// <param name="WellKnownTypes">The task types resolved from the consumer compilation.</param>
/// <remarks>
/// This carries symbols and so never leaves the semantic transform that created it; what comes back out is a model
/// of strings. Bundling the arguments keeps the extraction methods from growing a parameter list each.
/// </remarks>
internal readonly record struct EventRequest(
    INamedTypeSymbol Host,
    bool IsStatic,
    IReadOnlyDictionary<ITypeParameterSymbol, string>? TypeParameterNames,
    bool SupportsNullableAnnotations,
    LocationInfo? Location,
    WellKnownTypes WellKnownTypes);
