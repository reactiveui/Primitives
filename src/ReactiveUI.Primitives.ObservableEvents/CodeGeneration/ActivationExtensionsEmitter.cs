// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.CodeGeneration;

/// <summary>Emits the strongly typed <c>Events()</c> overloads that replace the placeholder at each call site.</summary>
internal static class ActivationExtensionsEmitter
{
    /// <summary>The room to reserve for the file's fixed scaffolding.</summary>
    private const int ScaffoldCapacity = 256;

    /// <summary>The room to reserve per generated overload.</summary>
    private const int OverloadCapacity = 256;

    /// <summary>The indentation the generated overloads sit at, inside a namespace and a class.</summary>
    private const int MethodIndent = SourceFileWriter.IndentWidth + SourceFileWriter.IndentWidth;

    /// <summary>The indentation the generated overloads' bodies and constraints sit at.</summary>
    private const int BodyIndent = MethodIndent + SourceFileWriter.IndentWidth;

    /// <summary>Emits every generated activation overload into one partial-class file.</summary>
    /// <param name="models">The overloads to emit, in request order.</param>
    /// <returns>The generated source.</returns>
    /// <remarks>
    /// The overloads join the same partial class as the placeholder they displace, so a call site resolves to the
    /// concrete overload without the consumer importing anything new: a non-generic candidate beats the generic
    /// placeholder outright.
    /// </remarks>
    internal static string Emit(ImmutableArray<ActivationModel> models)
    {
        var builder = new PooledStringBuilder(ScaffoldCapacity + (models.Length * OverloadCapacity));
        _ = builder.Append(Constants.GeneratedFileHeader);

        // Every overload was extracted from the same compilation, so they agree on what its language allows.
        if (models[0].SupportsNullableAnnotations)
        {
            _ = builder.Append(Constants.NullableEnableDirective);
        }

        _ = builder.Append("namespace ").AppendLine(Constants.GeneratedNamespace)
            .AppendLine("{")
            .AppendIndent(SourceFileWriter.IndentWidth).Append("internal static partial class ")
            .AppendLine(Constants.ActivationExtensionsClassName)
            .AppendIndent(SourceFileWriter.IndentWidth).AppendLine("{");

        foreach (var model in models)
        {
            AppendOverload(builder, model);
        }

        _ = builder.AppendIndent(SourceFileWriter.IndentWidth).AppendLine("}").AppendLine("}");
        return builder.ToStringAndReturn();
    }

    /// <summary>Appends one activation overload.</summary>
    /// <param name="builder">The destination builder.</param>
    /// <param name="model">The overload to append.</param>
    private static void AppendOverload(PooledStringBuilder builder, ActivationModel model) =>
        _ = builder.AppendIndent(MethodIndent)
            .Append("/// <summary>Gets observable wrappers for public events on <c>")
            .Append(model.DocumentationName).AppendLine("</c>.</summary>")
            .AppendIndent(MethodIndent).Append("public static ").Append(model.WrapperReference)
            .Append(" Events").Append(model.TypeParameterList).Append("(this ").Append(model.TypeReference)
            .AppendLine(" eventHost)")
            .AppendIndentedLines(model.Constraints, BodyIndent)
            .AppendIndent(BodyIndent).Append("=> new ")
            .Append(model.WrapperReference).AppendLine("(eventHost);")
            .AppendLine();
}
