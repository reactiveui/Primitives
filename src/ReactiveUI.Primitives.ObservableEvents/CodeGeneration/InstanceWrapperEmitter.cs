// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.CodeGeneration;

/// <summary>Emits the wrapper class that exposes one host's instance events as observables.</summary>
internal static class InstanceWrapperEmitter
{
    /// <summary>The room to reserve for the file's fixed scaffolding.</summary>
    private const int ScaffoldCapacity = 512;

    /// <summary>The room to reserve per generated observable property.</summary>
    private const int PropertyCapacity = 512;

    /// <summary>Emits the wrapper for one host.</summary>
    /// <param name="model">The host to wrap.</param>
    /// <param name="provider">The observable implementation to write against.</param>
    /// <returns>The generated source.</returns>
    /// <remarks>
    /// The host is held in a field rather than resubscribed from a captured expression, so every property on one
    /// wrapper observes the same instance the consumer handed it.
    /// </remarks>
    internal static string Emit(InstanceTargetModel model, ObservableProvider provider)
    {
        var events = model.Events.AsArray();
        var builder = new PooledStringBuilder(ScaffoldCapacity + (events.Length * PropertyCapacity));
        var indent = SourceFileWriter.AppendHeader(
            builder,
            model.Namespace,
            model.SupportsNullableAnnotations);
        var memberIndent = indent + SourceFileWriter.IndentWidth;

        _ = builder.AppendIndent(indent).Append("internal sealed class ").Append(model.WrapperName)
            .AppendLine(model.TypeParameterList)
            .AppendIndentedLines(model.Constraints, memberIndent)
            .AppendIndent(indent).AppendLine("{")
            .AppendIndent(memberIndent).Append("private readonly ").Append(model.TypeReference)
            .AppendLine(" _host;")
            .AppendLine()
            .AppendIndent(memberIndent).Append("internal ").Append(model.WrapperName).Append('(')
            .Append(model.TypeReference).AppendLine(" host) => _host = host;")
            .AppendLine();

        foreach (var eventModel in events)
        {
            SourceFileWriter.AppendEventProperty(builder, eventModel, provider, memberIndent);
        }

        _ = builder.AppendIndent(indent).AppendLine("}");
        SourceFileWriter.AppendFooter(builder, model.Namespace);
        return builder.ToStringAndReturn();
    }
}
