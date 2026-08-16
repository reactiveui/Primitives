// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>A diagnostic held as values, so it can ride along in a model until it is reported.</summary>
/// <param name="Descriptor">The descriptor to report.</param>
/// <param name="Location">Where to point, or <see langword="null"/> when the request has no source location.</param>
/// <param name="FirstArgument">The first message argument.</param>
/// <param name="SecondArgument">The second message argument, when the descriptor takes one.</param>
/// <remarks>
/// A <see cref="Diagnostic"/> carries a <see cref="Microsoft.CodeAnalysis.Location"/> and therefore a syntax tree,
/// which the pipeline can neither compare nor safely cache. Extraction records what to say and where; the source
/// output turns it back into a diagnostic.
/// </remarks>
internal sealed record DiagnosticInfo(
    DiagnosticDescriptor Descriptor,
    LocationInfo? Location,
    string FirstArgument,
    string? SecondArgument)
{
    /// <summary>Creates a single-argument diagnostic record.</summary>
    /// <param name="descriptor">The descriptor to report.</param>
    /// <param name="location">Where to point.</param>
    /// <param name="argument">The only message argument.</param>
    /// <returns>The diagnostic record.</returns>
    internal static DiagnosticInfo Create(
        DiagnosticDescriptor descriptor,
        LocationInfo? location,
        string argument) =>
        new(descriptor, location, argument, null);

    /// <summary>Rebuilds the diagnostic for reporting.</summary>
    /// <returns>The reportable diagnostic.</returns>
    internal Diagnostic ToDiagnostic()
    {
        var location = Location?.ToLocation() ?? Microsoft.CodeAnalysis.Location.None;
        return SecondArgument is null
            ? Diagnostic.Create(Descriptor, location, FirstArgument)
            : Diagnostic.Create(Descriptor, location, FirstArgument, SecondArgument);
    }
}
