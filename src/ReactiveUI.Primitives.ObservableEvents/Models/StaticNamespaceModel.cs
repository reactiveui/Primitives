// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>Groups deduplicated static event hosts by namespace.</summary>
/// <param name="HintName">The generated file name for this namespace.</param>
/// <param name="Namespace">The namespace to emit into, or empty for the global namespace.</param>
/// <param name="SupportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
/// <param name="Events">The static events to expose, in request order.</param>
internal sealed record StaticNamespaceModel(
    string HintName,
    string Namespace,
    bool SupportsNullableAnnotations,
    EquatableArray<EventModel> Events);
