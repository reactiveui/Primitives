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

    /// <summary>Collects the events a host can expose, recording a diagnostic for each one it cannot.</summary>
    /// <param name="request">Everything about the host and where it was requested from.</param>
    /// <param name="diagnostics">The destination for anything found wrong.</param>
    /// <param name="cancellationToken">A token that cancels the walk.</param>
    /// <returns>The supported events, in declaration order from the host down to its last base type.</returns>
    /// <remarks>
    /// Walking the base chain by hand rather than asking for all members at once is what lets a derived host expose
    /// an event it inherits. A name already seen is skipped so an event redeclared in a derived type wins over the
    /// one it hides, which is the member a consumer's own code would bind to.
    /// </remarks>
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

    /// <summary>Builds the mangled property name a static event gets on its namespace's shared class.</summary>
    /// <param name="host">The requested static host.</param>
    /// <param name="eventName">The event name.</param>
    /// <returns>The generated property name.</returns>
    /// <remarks>
    /// Every namespace's static events share one class, so the host's name has to be part of the property name.
    /// Concatenating names would let distinct hosts collide - <c>A.BC</c> and <c>AB.C</c> both flattening to
    /// <c>ABC</c> - so each segment is length-prefixed, which no pair of different segmentations can produce.
    /// </remarks>
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

    /// <summary>Builds the expression the generated handler is added to and removed from.</summary>
    /// <param name="eventSymbol">The event to subscribe to.</param>
    /// <param name="request">The host request the event was reached through.</param>
    /// <returns>The subscription target expression.</returns>
    /// <remarks>
    /// An instance target is cast to the type that declares the event rather than to the requested host, so an
    /// event the host inherits and hides with a member of the same name still binds to the one being wrapped.
    /// </remarks>
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

    /// <summary>Selects the observable payload type and the value handed to the observer.</summary>
    /// <param name="invokeMethod">The delegate's invocation method.</param>
    /// <param name="request">The host request the event was reached through.</param>
    /// <returns>The payload type and value, both empty for a parameterless delegate.</returns>
    /// <remarks>
    /// The two-parameter sender/args shape is the one nearly every .NET event has, and its sender is the object the
    /// consumer already holds, so only the args are surfaced. Anything else is passed through whole: a single
    /// parameter as itself, several as a tuple.
    /// </remarks>
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

    /// <summary>Renders the generated handler's parameter list.</summary>
    /// <param name="invokeMethod">The delegate's invocation method.</param>
    /// <param name="request">The host request the event was reached through.</param>
    /// <returns>The comma-separated parameter declarations.</returns>
    /// <remarks>
    /// The declared types carry whatever annotations the delegate declared, because a handler is only assignable to
    /// a delegate whose parameter nullability it matches.
    /// </remarks>
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

    /// <summary>Determines why an event cannot be exposed as an observable.</summary>
    /// <param name="eventSymbol">The event to validate.</param>
    /// <param name="wellKnownTypes">The task types resolved from the consumer compilation.</param>
    /// <returns>The reason, or <see langword="null"/> when the event is supported.</returns>
    /// <remarks>
    /// The generated handler hands its parameters to an observer, which outlives the callback, so anything that
    /// cannot leave the stack or be captured has to be refused here rather than emitted and left to fail the
    /// consumer's build.
    /// </remarks>
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
