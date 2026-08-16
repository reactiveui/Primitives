// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>One event, reduced to the exact fragments its generated observable property is assembled from.</summary>
/// <param name="PropertyName">The generated property name, already escaped.</param>
/// <param name="EventAccess">The subscription target and event name, already escaped.</param>
/// <param name="PayloadType">The observable payload type, or empty when the delegate takes no parameters.</param>
/// <param name="PayloadValue">The value passed to OnNext, or empty when the delegate takes no parameters.</param>
/// <param name="HandlerParameters">The generated handler's parameter list.</param>
/// <param name="HandlerReturnType">The generated handler's return type.</param>
/// <param name="HandlerReturnValue">The handler's return expression, or empty when it returns void.</param>
/// <param name="IsStatic">Whether the generated property is static.</param>
/// <param name="DocumentationName">The event name, escaped for the generated documentation comment.</param>
/// <remarks>
/// Everything the emitter needs is a string by this point: no symbol, syntax node, or compilation survives into the
/// pipeline. A parameterless delegate leaves the payload fields empty rather than naming a void type, because which
/// void type applies is the one thing that depends on the provider - keeping it out here is what lets a model stay
/// cached when only the consumer's references change.
/// </remarks>
internal sealed record EventModel(
    string PropertyName,
    string EventAccess,
    string PayloadType,
    string PayloadValue,
    string HandlerParameters,
    string HandlerReturnType,
    string HandlerReturnValue,
    bool IsStatic,
    string DocumentationName)
{
    /// <summary>Gets a value indicating whether the delegate takes no parameters and signals a void payload.</summary>
    internal bool HasVoidPayload => PayloadType.Length == 0;
}
