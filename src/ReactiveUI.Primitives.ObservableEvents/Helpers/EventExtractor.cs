// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;
using ReactiveUI.Primitives.ObservableEvents.CodeGeneration;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>Turns a host's events into the value-only models the emitter works from.</summary>
internal static class EventExtractor
{
    /// <summary>The parameter count of a delegate whose single parameter is the whole payload.</summary>
    private const int SingleParameterCount = 1;

    /// <summary>The parameter count of the conventional sender-and-arguments event delegate.</summary>
    private const int SenderAndArgsParameterCount = 2;

    /// <summary>Extracts inherited events, preferring the most derived declaration for duplicate names.</summary>
    /// <param name="request">Everything about the host and where it was requested from.</param>
    /// <param name="diagnostics">The destination for anything found wrong.</param>
    /// <param name="cancellationToken">A token that cancels the walk.</param>
    /// <returns>The supported events, in declaration order from the host down to its last base type.</returns>
    internal static EquatableArray<EventModel> Collect(
        in EventRequest request,
        List<DiagnosticInfo> diagnostics,
        CancellationToken cancellationToken)
    {
        var events = new List<EventModel>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        for (INamedTypeSymbol? current = request.Host; current is not null; current = current.BaseType)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var member in current.GetMembers())
            {
                if (member is not IEventSymbol eventSymbol
                    || eventSymbol.IsStatic != request.IsStatic
                    || eventSymbol.DeclaredAccessibility != Accessibility.Public
                    || !seen.Add(eventSymbol.Name))
                {
                    continue;
                }

                var reason = SelectUnsupportedReason(eventSymbol, request.WellKnownTypes);
                if (reason is null)
                {
                    events.Add(CreateModel(eventSymbol, request));
                }
                else
                {
                    diagnostics.Add(new(
                        DiagnosticWarnings.UnsupportedEvent,
                        request.Location,
                        eventSymbol.ToDisplayString(),
                        reason));
                }
            }
        }

        return events.Count == 0 ? EquatableArray<EventModel>.Empty : new([.. events]);
    }

    /// <summary>Builds an unambiguous static property name from length-prefixed host segments.</summary>
    /// <param name="host">The requested static host.</param>
    /// <param name="eventName">The event name.</param>
    /// <returns>The generated property name.</returns>
    private static string StaticPropertyName(INamedTypeSymbol host, string eventName)
    {
        var containers = new Stack<INamedTypeSymbol>();
        for (INamedTypeSymbol? current = host; current is not null; current = current.ContainingType)
        {
            containers.Push(current);
        }

        var builder = new PooledStringBuilder();
        _ = builder.Append('T');
        while (containers.Count > 0)
        {
            var name = containers.Pop().Name;
            _ = builder.Append(name.Length).Append(name);
        }

        return builder.Append(eventName.Length).Append(eventName).ToStringAndReturn();
    }

    /// <summary>Builds the model for one supported event.</summary>
    /// <param name="eventSymbol">The event to expose.</param>
    /// <param name="request">The host request the event was reached through.</param>
    /// <returns>The event model.</returns>
    private static EventModel CreateModel(IEventSymbol eventSymbol, in EventRequest request)
    {
        var invokeMethod = ((INamedTypeSymbol)eventSymbol.Type).DelegateInvokeMethod!;
        var (payloadType, payloadValue) = SelectPayload(invokeMethod, in request);

        return new(
            request.IsStatic
                ? StaticPropertyName(request.Host, eventSymbol.Name)
                : SymbolHelpers.EscapeIdentifier(eventSymbol.Name),
            BuildEventAccess(eventSymbol, request),
            payloadType,
            payloadValue,
            JoinParameterDeclarations(invokeMethod, in request),
            SymbolHelpers.Display(invokeMethod.ReturnType, request.TypeParameterNames, request.SupportsNullableAnnotations),
            SelectHandlerReturnValue(invokeMethod, request.WellKnownTypes),
            request.IsStatic,
            SymbolHelpers.EscapeXml(eventSymbol.Name));
    }

    /// <summary>Qualifies event access with its declaring type to resolve hidden inherited members.</summary>
    /// <param name="eventSymbol">The event to subscribe to.</param>
    /// <param name="request">The host request the event was reached through.</param>
    /// <returns>The subscription target expression.</returns>
    private static string BuildEventAccess(IEventSymbol eventSymbol, in EventRequest request)
    {
        var builder = new PooledStringBuilder();
        if (request.IsStatic)
        {
            _ = builder.Append(SymbolHelpers.Display(eventSymbol.ContainingType, null, request.SupportsNullableAnnotations));
        }
        else
        {
            _ = builder.Append("((")
                .Append(SymbolHelpers.Display(eventSymbol.ContainingType, request.TypeParameterNames, request.SupportsNullableAnnotations))
                .Append(")_host)");
        }

        return builder.Append('.').Append(SymbolHelpers.EscapeIdentifier(eventSymbol.Name)).ToStringAndReturn();
    }

    /// <summary>Selects event payloads: arguments for sender/args delegates, otherwise all parameters.</summary>
    /// <param name="invokeMethod">The delegate's invocation method.</param>
    /// <param name="request">The host request the event was reached through.</param>
    /// <returns>The payload type and value, both empty for a parameterless delegate.</returns>
    private static (string PayloadType, string PayloadValue) SelectPayload(
        IMethodSymbol invokeMethod,
        in EventRequest request)
    {
        var parameters = invokeMethod.Parameters;
        if (parameters.IsEmpty)
        {
            return (string.Empty, string.Empty);
        }

        if (parameters.Length == SingleParameterCount)
        {
            return (
                SymbolHelpers.Display(parameters[0].Type, request.TypeParameterNames, request.SupportsNullableAnnotations),
                SymbolHelpers.EscapeIdentifier(parameters[0].Name));
        }

        return parameters.Length == SenderAndArgsParameterCount
            && parameters[0].Type.SpecialType == SpecialType.System_Object
            ? (SymbolHelpers.Display(parameters[1].Type, request.TypeParameterNames, request.SupportsNullableAnnotations),
                SymbolHelpers.EscapeIdentifier(parameters[1].Name))
            : ($"({JoinParameterDeclarations(invokeMethod, in request)})",
                $"({JoinParameterNames(invokeMethod)})");
    }

    /// <summary>Builds handler parameters with the delegate's nullability annotations.</summary>
    /// <param name="invokeMethod">The delegate's invocation method.</param>
    /// <param name="request">The host request the event was reached through.</param>
    /// <returns>The comma-separated parameter declarations.</returns>
    private static string JoinParameterDeclarations(IMethodSymbol invokeMethod, in EventRequest request)
    {
        var builder = new PooledStringBuilder();
        var parameters = invokeMethod.Parameters;
        for (var index = 0; index < parameters.Length; index++)
        {
            if (index > 0)
            {
                _ = builder.Append(", ");
            }

            _ = builder
                .Append(SymbolHelpers.Display(parameters[index].Type, request.TypeParameterNames, request.SupportsNullableAnnotations))
                .Append(' ')
                .Append(SymbolHelpers.EscapeIdentifier(parameters[index].Name));
        }

        return builder.ToStringAndReturn();
    }

    /// <summary>Renders the generated handler's parameters as a value list.</summary>
    /// <param name="invokeMethod">The delegate's invocation method.</param>
    /// <returns>The comma-separated parameter names.</returns>
    private static string JoinParameterNames(IMethodSymbol invokeMethod)
    {
        var builder = new PooledStringBuilder();
        var parameters = invokeMethod.Parameters;
        for (var index = 0; index < parameters.Length; index++)
        {
            if (index > 0)
            {
                _ = builder.Append(", ");
            }

            _ = builder.Append(SymbolHelpers.EscapeIdentifier(parameters[index].Name));
        }

        return builder.ToStringAndReturn();
    }

    /// <summary>Selects what a non-void generated handler returns.</summary>
    /// <param name="invokeMethod">The delegate's invocation method.</param>
    /// <param name="wellKnownTypes">The task types resolved from the consumer compilation.</param>
    /// <returns>The return expression, or an empty string for a void handler.</returns>
    private static string SelectHandlerReturnValue(IMethodSymbol invokeMethod, WellKnownTypes wellKnownTypes)
    {
        if (invokeMethod.ReturnsVoid)
        {
            return string.Empty;
        }

        var returnsTask = SymbolEqualityComparer.Default.Equals(invokeMethod.ReturnType, wellKnownTypes.Task);
        return returnsTask ? Constants.CompletedTask : Constants.DefaultValueTask;
    }

    /// <summary>Returns why an event cannot be captured or exposed to an observer.</summary>
    /// <param name="eventSymbol">The event to validate.</param>
    /// <param name="wellKnownTypes">The task types resolved from the consumer compilation.</param>
    /// <returns>The reason, or <see langword="null"/> when the event is supported.</returns>
    private static string? SelectUnsupportedReason(IEventSymbol eventSymbol, WellKnownTypes wellKnownTypes)
    {
        if (eventSymbol.Type is not INamedTypeSymbol { DelegateInvokeMethod: { } invokeMethod })
        {
            return DiagnosticWarnings.NotADelegateReason;
        }

        foreach (var parameter in invokeMethod.Parameters)
        {
            if (parameter.RefKind != RefKind.None)
            {
                return DiagnosticWarnings.ByReferenceParameterReason;
            }

            if (parameter.Type.TypeKind is TypeKind.Pointer or TypeKind.FunctionPointer
                || parameter.Type.IsRefLikeType)
            {
                return DiagnosticWarnings.UnrepresentablePayloadReason;
            }
        }

        return invokeMethod.ReturnsVoid
            || SymbolEqualityComparer.Default.Equals(invokeMethod.ReturnType, wellKnownTypes.Task)
            || SymbolEqualityComparer.Default.Equals(invokeMethod.ReturnType, wellKnownTypes.ValueTask)
            ? null
            : DiagnosticWarnings.UnsupportedReturnReason;
    }
}
