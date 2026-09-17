// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>Stores event emission data without compiler objects; parameterless payloads remain provider-independent.</summary>
/// <param name="PropertyName">The generated property name, escaped for use as an identifier.</param>
/// <param name="EventAccess">The subscription target and event name, escaped for use as an identifier.</param>
/// <param name="PayloadType">The observable payload type, or empty when the delegate takes no parameters.</param>
/// <param name="PayloadValue">The value passed to OnNext, or empty when the delegate takes no parameters.</param>
/// <param name="HandlerParameters">The generated handler's parameter list.</param>
/// <param name="HandlerReturnType">The generated handler's return type.</param>
/// <param name="HandlerReturnValue">The handler's return expression, or empty when it returns void.</param>
/// <param name="IsStatic">Whether the generated property is static.</param>
/// <param name="DocumentationName">The event name, escaped for the generated documentation comment.</param>
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
