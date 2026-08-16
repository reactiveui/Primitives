// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ObservableEvents.Helpers;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies the source fragments symbols are rendered into.</summary>
public sealed class SymbolHelpersTests
{
    /// <summary>Verifies an identifier that collides with a keyword is escaped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SymbolHelpersEscapesKeywordIdentifiers()
    {
        await Assert.That(SymbolHelpers.EscapeIdentifier("event")).IsEqualTo("@event");
        await Assert.That(SymbolHelpers.EscapeIdentifier("value")).IsEqualTo("value");
    }

    /// <summary>Verifies text with nothing to escape comes back as the same instance.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SymbolHelpersLeavesPlainDocumentationTextAlone()
    {
        const string Plain = "global::Samples.EventSource";

        await Assert.That(SymbolHelpers.EscapeXml(Plain)).IsSameReferenceAs(Plain);
    }

    /// <summary>Verifies every character that would break a documentation comment is escaped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SymbolHelpersEscapesEveryDocumentationSpecialCharacter() =>
        await Assert.That(SymbolHelpers.EscapeXml("A<T>&B\"C'D"))
            .IsEqualTo("A&lt;T&gt;&amp;B&quot;C&apos;D");

    /// <summary>Verifies each special character is recognised wherever it happens to appear first.</summary>
    /// <param name="value">Text whose leading character is the one under test.</param>
    /// <param name="expected">The escaped rendering.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments("&x", "&amp;x")]
    [Arguments("<x", "&lt;x")]
    [Arguments(">x", "&gt;x")]
    [Arguments("\"x", "&quot;x")]
    [Arguments("'x", "&apos;x")]
    public async Task SymbolHelpersDetectsEverySpecialCharacterAsTheLeadingOne(string value, string expected) =>
        await Assert.That(SymbolHelpers.EscapeXml(value)).IsEqualTo(expected);

    /// <summary>Verifies a generic reference is escaped so the generated comment stays well-formed XML.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SymbolHelpersEscapesGenericReferencesForDocumentation() =>
        await Assert.That(SymbolHelpers.EscapeXml("global::Samples.Outer<T>.Inner<T2>"))
            .IsEqualTo("global::Samples.Outer&lt;T&gt;.Inner&lt;T2&gt;");
}
