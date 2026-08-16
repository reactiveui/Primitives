// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;

namespace ReactiveUI.Primitives.ObservableEvents;

/// <summary>The diagnostics the observable-event generator reports against consumer code.</summary>
internal static class DiagnosticWarnings
{
    /// <summary>The category shared by every RXOE descriptor.</summary>
    internal const string Category = "ReactiveUI.Primitives.ObservableEvents";

    /// <summary>The reason reported for an event whose type is not a delegate.</summary>
    internal const string NotADelegateReason = "the event type is not a delegate";

    /// <summary>The reason reported for a delegate taking a by-reference parameter.</summary>
    internal const string ByReferenceParameterReason = "by-reference delegate parameters are not supported";

    /// <summary>The reason reported for a delegate whose payload cannot cross a lambda boundary.</summary>
    internal const string UnrepresentablePayloadReason =
        "pointer, function-pointer, and ref-like payloads are not supported";

    /// <summary>The reason reported for a delegate returning something other than void, Task, or ValueTask.</summary>
    internal const string UnsupportedReturnReason = "the delegate must return void, Task, or ValueTask";

    /// <summary>The reason reported for a generic static event host.</summary>
    internal const string GenericStaticHostReason = "generic static event hosts are not supported";

    /// <summary>The host kind reported by <see cref="NoEvents"/> for an instance request.</summary>
    internal const string InstanceHostKind = "instance";

    /// <summary>The host kind reported by <see cref="NoEvents"/> for a static request.</summary>
    internal const string StaticHostKind = "static";

    /// <summary>RXOE001: no supported observable factory is visible to the consumer.</summary>
    internal static readonly DiagnosticDescriptor MissingProvider = new(
        "RXOE001",
        "Observable provider not found",
        "Observable events for '{0}' require ReactiveUI.Primitives, ReactiveUI.Primitives.Reactive, or System.Reactive",
        Category,
        DiagnosticSeverity.Warning,
        true);

    /// <summary>RXOE002: a requested host declares no event this generator can expose.</summary>
    internal static readonly DiagnosticDescriptor NoEvents = new(
        "RXOE002",
        "No supported events found",
        "No supported public {0} events were found on '{1}'",
        Category,
        DiagnosticSeverity.Warning,
        true);

    /// <summary>RXOE003: an event's delegate signature cannot be represented as an observable.</summary>
    internal static readonly DiagnosticDescriptor UnsupportedEvent = new(
        "RXOE003",
        "Event signature is not supported",
        "Event '{0}' cannot be exposed as an observable: {1}",
        Category,
        DiagnosticSeverity.Warning,
        true);
}
