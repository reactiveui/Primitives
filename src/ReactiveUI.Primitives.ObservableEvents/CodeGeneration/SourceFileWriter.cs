// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ObservableEvents.Helpers;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.CodeGeneration;

/// <summary>Writes the pieces every generated file shares: its header, its namespace, and its event properties.</summary>
internal static class SourceFileWriter
{
    /// <summary>The width of one level of indentation.</summary>
    internal const int IndentWidth = 4;

    /// <summary>Opens a generated file, wrapping it in a namespace unless the host lives in the global one.</summary>
    /// <param name="builder">The destination builder.</param>
    /// <param name="namespaceName">The namespace, or an empty string for the global namespace.</param>
    /// <param name="supportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
    /// <returns>The indentation the file's top-level declaration sits at.</returns>
    internal static int AppendHeader(
        PooledStringBuilder builder,
        string namespaceName,
        bool supportsNullableAnnotations)
    {
        _ = builder.Append(Constants.GeneratedFileHeader);
        if (supportsNullableAnnotations)
        {
            _ = builder.Append(Constants.NullableEnableDirective);
        }

        if (namespaceName.Length == 0)
        {
            return 0;
        }

        _ = builder.Append("namespace ").AppendLine(namespaceName).AppendLine("{");
        return IndentWidth;
    }

    /// <summary>Closes the namespace a generated file was opened with, when there was one.</summary>
    /// <param name="builder">The destination builder.</param>
    /// <param name="namespaceName">The namespace, or an empty string for the global namespace.</param>
    internal static void AppendFooter(PooledStringBuilder builder, string namespaceName)
    {
        if (namespaceName.Length == 0)
        {
            return;
        }

        _ = builder.AppendLine("}");
    }

    /// <summary>Appends the observable property that wraps one event.</summary>
    /// <param name="builder">The destination builder.</param>
    /// <param name="model">The event to wrap.</param>
    /// <param name="provider">The observable implementation to write against.</param>
    /// <param name="indent">The indentation the property sits at.</param>
    /// <remarks>
    /// The handler is a local function rather than a lambda so a delegate returning <c>Task</c> or
    /// <c>ValueTask</c> can satisfy its own signature; the subscription is torn down through the provider's own
    /// disposable factory, so the wrapper never holds the handler alive past the subscription.
    /// </remarks>
    internal static void AppendEventProperty(
        PooledStringBuilder builder,
        EventModel model,
        ObservableProvider provider,
        int indent)
    {
        var payloadType = model.HasVoidPayload ? ProviderResolver.VoidPayloadType(provider) : model.PayloadType;
        var payloadValue = model.HasVoidPayload ? payloadType + Constants.DefaultMember : model.PayloadValue;
        var bodyIndent = indent + IndentWidth;
        var statementIndent = bodyIndent + IndentWidth;

        _ = builder.AppendIndent(indent)
            .Append("/// <summary>Gets an observable that signals when the ")
            .Append(model.DocumentationName).AppendLine(" event is raised.</summary>")
            .AppendIndent(indent).Append("public ").Append(model.IsStatic ? "static " : string.Empty)
            .Append("global::System.IObservable<").Append(payloadType).Append("> ")
            .Append(model.PropertyName).Append(" => ").Append(ProviderResolver.ObservableFactory(provider))
            .Append('<').Append(payloadType).AppendLine(">(observer =>")
            .AppendIndent(indent).AppendLine("{")
            .AppendIndent(bodyIndent).Append(model.HandlerReturnType).Append(" Handler(")
            .Append(model.HandlerParameters).AppendLine(")")
            .AppendIndent(bodyIndent).AppendLine("{")
            .AppendIndent(statementIndent).Append("observer.OnNext(").Append(payloadValue)
            .AppendLine(");");

        if (model.HandlerReturnValue.Length > 0)
        {
            _ = builder.AppendIndent(statementIndent).Append("return ")
                .Append(model.HandlerReturnValue).AppendLine(";");
        }

        _ = builder.AppendIndent(bodyIndent).AppendLine("}")
            .AppendIndent(bodyIndent).Append(model.EventAccess).AppendLine(" += Handler;")
            .AppendIndent(bodyIndent).Append("return ").Append(ProviderResolver.DisposableFactory(provider))
            .Append("(() => ").Append(model.EventAccess).AppendLine(" -= Handler);")
            .AppendIndent(indent).AppendLine("});")
            .AppendLine();
    }
}
