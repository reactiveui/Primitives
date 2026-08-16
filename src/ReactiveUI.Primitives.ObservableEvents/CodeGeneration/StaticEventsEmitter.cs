// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.CodeGeneration;

/// <summary>Emits one namespace's static events as observables on a shared class.</summary>
internal static class StaticEventsEmitter
{
    /// <summary>The room to reserve for the file's fixed scaffolding.</summary>
    private const int ScaffoldCapacity = 256;

    /// <summary>The room to reserve per generated observable property.</summary>
    private const int PropertyCapacity = 512;

    /// <summary>Emits the static observable properties for one namespace.</summary>
    /// <param name="model">The namespace and its static events.</param>
    /// <param name="provider">The observable implementation to write against.</param>
    /// <returns>The generated source.</returns>
    /// <remarks>
    /// A static event has no instance to hang an extension method off, so it is reached through a class named the
    /// same in every namespace. The class is partial, so a consumer can add to it and so that two requested hosts
    /// in one namespace do not fight over the declaration.
    /// </remarks>
    internal static string Emit(StaticNamespaceModel model, ObservableProvider provider)
    {
        var events = model.Events.AsArray();
        var builder = new PooledStringBuilder(ScaffoldCapacity + (events.Length * PropertyCapacity));
        var indent = SourceFileWriter.AppendHeader(
            builder,
            model.Namespace,
            model.SupportsNullableAnnotations);

        _ = builder.AppendIndent(indent).Append("internal static partial class ")
            .AppendLine(Constants.StaticEventsClassName)
            .AppendIndent(indent).AppendLine("{");

        foreach (var eventModel in events)
        {
            SourceFileWriter.AppendEventProperty(builder, eventModel, provider, indent + SourceFileWriter.IndentWidth);
        }

        _ = builder.AppendIndent(indent).AppendLine("}");
        SourceFileWriter.AppendFooter(builder, model.Namespace);
        return builder.ToStringAndReturn();
    }
}
