// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>Where a diagnostic points, reduced to values the incremental pipeline can compare.</summary>
/// <param name="FilePath">The source file the request was written in.</param>
/// <param name="TextSpan">The span within that file.</param>
/// <param name="LineSpan">The line and character span within that file.</param>
/// <remarks>
/// A <see cref="Location"/> holds onto its syntax tree, which would pin a whole compilation in the pipeline's cache
/// and never compare equal between runs. Keeping the three values it is built from lets the location survive in a
/// model and be rebuilt at the point a diagnostic is actually reported.
/// </remarks>
internal sealed record LocationInfo(string FilePath, TextSpan TextSpan, LinePositionSpan LineSpan)
{
    /// <summary>Reduces a Roslyn location to its comparable values.</summary>
    /// <param name="location">The location to reduce, which may carry no source.</param>
    /// <returns>The reduced location, or <see langword="null"/> when there is no source to point at.</returns>
    internal static LocationInfo? From(Location? location)
    {
        if (location?.SourceTree is null)
        {
            return null;
        }

        var lineSpan = location.GetLineSpan();
        return new(lineSpan.Path, location.SourceSpan, lineSpan.Span);
    }

    /// <summary>Reduces the location of whatever a syntax reference points at.</summary>
    /// <param name="reference">The reference to reduce, which is absent for an attribute read from metadata.</param>
    /// <param name="cancellationToken">A token that cancels resolving the referenced syntax.</param>
    /// <returns>The reduced location, or <see langword="null"/> when there is no source to point at.</returns>
    internal static LocationInfo? From(SyntaxReference? reference, CancellationToken cancellationToken) =>
        reference is null ? null : From(reference.GetSyntax(cancellationToken).GetLocation());

    /// <summary>Rebuilds a Roslyn location for reporting.</summary>
    /// <returns>The rebuilt location.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal Location ToLocation() => Location.Create(FilePath, TextSpan, LineSpan);
}
