// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>One host named by a <c>GenerateStaticEventObservables</c> attribute.</summary>
/// <param name="Identity">The host's fully qualified name, used to drop repeated requests.</param>
/// <param name="DisplayName">The host's readable name, as it appears in a diagnostic message.</param>
/// <param name="Namespace">The namespace whose <c>RxEvents</c> class receives these properties.</param>
/// <param name="SupportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
/// <param name="Events">The static events to expose.</param>
/// <param name="Diagnostics">What extraction found wrong, reported once a provider is known to exist.</param>
/// <param name="Location">The attribute application, for the diagnostics that point at the request itself.</param>
internal sealed record StaticTargetModel(
    string Identity,
    string DisplayName,
    string Namespace,
    bool SupportsNullableAnnotations,
    EquatableArray<EventModel> Events,
    EquatableArray<DiagnosticInfo> Diagnostics,
    LocationInfo? Location);
