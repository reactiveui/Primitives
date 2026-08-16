// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies the diagnostics a model carries out of extraction and back into a report.</summary>
public sealed class DiagnosticInfoTests
{
    /// <summary>The host name the diagnostics under test are reported against.</summary>
    private const string HostDisplayName = "Samples.EventSource";

    /// <summary>The source location the reported diagnostics point into.</summary>
    private const string SamplePath = "Sample.cs";

    /// <summary>Verifies a single-argument diagnostic keeps its message and its location.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DiagnosticInfoRebuildsASingleArgumentDiagnostic()
    {
        var tree = CSharpSyntaxTree.ParseText("class Sample { }", path: SamplePath);
        var location = LocationInfo.From((await tree.GetRootAsync()).GetLocation());

        var diagnostic = DiagnosticInfo
            .Create(DiagnosticWarnings.MissingProvider, location, HostDisplayName)
            .ToDiagnostic();

        await Assert.That(diagnostic.Id).IsEqualTo("RXOE001");
        await Assert.That(diagnostic.GetMessage()).Contains(HostDisplayName);
        await Assert.That(diagnostic.Location.GetLineSpan().Path).IsEqualTo(SamplePath);
    }

    /// <summary>Verifies a two-argument diagnostic fills both placeholders.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DiagnosticInfoRebuildsATwoArgumentDiagnostic()
    {
        var diagnostic = new DiagnosticInfo(
            DiagnosticWarnings.NoEvents,
            null,
            DiagnosticWarnings.StaticHostKind,
            HostDisplayName).ToDiagnostic();

        await Assert.That(diagnostic.Id).IsEqualTo("RXOE002");
        await Assert.That(diagnostic.GetMessage()).Contains("static");
        await Assert.That(diagnostic.GetMessage()).Contains(HostDisplayName);
    }

    /// <summary>Verifies a diagnostic with nowhere to point still reports rather than throwing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DiagnosticInfoReportsWithoutALocation()
    {
        var diagnostic = DiagnosticInfo
            .Create(DiagnosticWarnings.MissingProvider, null, HostDisplayName)
            .ToDiagnostic();

        await Assert.That(diagnostic.Location).IsEqualTo(Location.None);
    }
}
