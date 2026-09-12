// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using ReactiveUI.Primitives.ObservableEvents.CodeGeneration;
using ReactiveUI.Primitives.ObservableEvents.Helpers;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents;

/// <summary>Generates observable wrappers for the event-bearing types a consumer asks for.</summary>
/// <remarks>Requests come from Events() calls or static-host attributes. Output is cached per host, namespace, and activation signature.</remarks>
[Generator(LanguageNames.CSharp)]
public sealed class EventGenerator : IIncrementalGenerator
{
    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        RegisterActivationOutput(in context);

        // Resolve provider names independently so reference changes do not invalidate event extraction.
        var provider = context.CompilationProvider
            .Select(static (compilation, _) => ProviderResolver.Resolve(compilation))
            .WithTrackingName(GeneratorStepNames.Provider);

        var instanceTargets = context.SyntaxProvider
            .CreateSyntaxProvider(
                InstanceTargetExtractor.IsActivationInvocation,
                InstanceTargetExtractor.Extract)
            .Where(static target => target is not null)
            .Select(static (target, _) => target!)
            .Collect()
            .SelectMany(static (targets, _) => TargetCollections.Deduplicate(targets))
            .WithTrackingName(GeneratorStepNames.InstanceTargets);

        var staticTargets = context.SyntaxProvider
            .CreateSyntaxProvider(
                StaticTargetExtractor.IsStaticRequestAttribute,
                StaticTargetExtractor.Extract)
            .Where(static target => target is not null)
            .Select(static (target, _) => target!)
            .Collect()
            .SelectMany(static (targets, _) => TargetCollections.Deduplicate(targets))
            .WithTrackingName(GeneratorStepNames.StaticTargets);

        RegisterInstanceOutputs(in context, instanceTargets, provider);
        RegisterStaticOutputs(in context, staticTargets, provider);
    }

    /// <summary>Registers activation source without modifying the semantic input compilation.</summary>
    /// <param name="context">The generator initialization context.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RegisterActivationOutput(in IncrementalGeneratorInitializationContext context) =>
        context.RegisterSourceOutput(
            context.ParseOptionsProvider.Select(static (_, _) => ActivationSource.Text),
            static (output, source) => output.AddSource(Constants.ActivationHintName, source));

    /// <summary>Registers the wrapper files and the one file carrying every activation overload.</summary>
    /// <param name="context">The generator initialization context.</param>
    /// <param name="targets">The distinct requested hosts.</param>
    /// <param name="provider">The resolved observable implementation.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RegisterInstanceOutputs(
        in IncrementalGeneratorInitializationContext context,
        IncrementalValuesProvider<InstanceTargetModel> targets,
        IncrementalValueProvider<ObservableProvider> provider)
    {
        context.RegisterSourceOutput(
            targets.Combine(provider),
            static (output, data) => EmitInstanceTarget(in output, data.Left, data.Right));

        // Keyed on the overload signatures alone, so changing what a wrapper exposes leaves this file untouched.
        context.RegisterSourceOutput(
            targets
                .Where(static target => !target.Events.IsEmpty)
                .Select(static (target, _) => target.ToActivation())
                .Collect()
                .WithTrackingName(GeneratorStepNames.ActivationOverloads)
                .Combine(provider),
            static (output, data) => EmitActivationOverloads(in output, data.Left, data.Right));
    }

    /// <summary>Registers per-request diagnostics and per-namespace static event output.</summary>
    /// <param name="context">The generator initialization context.</param>
    /// <param name="targets">The distinct requested static hosts.</param>
    /// <param name="provider">The resolved observable implementation.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RegisterStaticOutputs(
        in IncrementalGeneratorInitializationContext context,
        IncrementalValuesProvider<StaticTargetModel> targets,
        IncrementalValueProvider<ObservableProvider> provider)
    {
        context.RegisterSourceOutput(
            targets.Combine(provider),
            static (output, data) => ReportStaticTargetDiagnostics(in output, data.Left, data.Right));

        context.RegisterSourceOutput(
            targets
                .Collect()
                .SelectMany(static (collected, _) => TargetCollections.GroupByNamespace(collected))
                .WithTrackingName(GeneratorStepNames.StaticNamespaces)
                .Combine(provider),
            static (output, data) => EmitStaticNamespace(in output, data.Left, data.Right));
    }

    /// <summary>Emits one host's wrapper, or says why it cannot be emitted.</summary>
    /// <param name="output">The source-production context.</param>
    /// <param name="target">The requested host.</param>
    /// <param name="provider">The resolved observable implementation.</param>
    private static void EmitInstanceTarget(
        in SourceProductionContext output,
        InstanceTargetModel target,
        ObservableProvider provider)
    {
        if (provider == ObservableProvider.None)
        {
            ReportMissingProvider(in output, target.DisplayName, target.Location);
            return;
        }

        ReportDiagnostics(in output, target.Diagnostics);
        if (target.Events.IsEmpty)
        {
            return;
        }

        output.AddSource(target.HintName, InstanceWrapperEmitter.Emit(target, provider));
    }

    /// <summary>Emits every generated activation overload.</summary>
    /// <param name="output">The source-production context.</param>
    /// <param name="overloads">The overloads to emit.</param>
    /// <param name="provider">The resolved observable implementation.</param>
    private static void EmitActivationOverloads(
        in SourceProductionContext output,
        ImmutableArray<ActivationModel> overloads,
        ObservableProvider provider)
    {
        if (provider == ObservableProvider.None || overloads.IsEmpty)
        {
            return;
        }

        output.AddSource(Constants.ExtensionsHintName, ActivationExtensionsEmitter.Emit(overloads));
    }

    /// <summary>Reports what a static request could not do, once a provider is known to exist.</summary>
    /// <param name="output">The source-production context.</param>
    /// <param name="target">The requested static host.</param>
    /// <param name="provider">The resolved observable implementation.</param>
    private static void ReportStaticTargetDiagnostics(
        in SourceProductionContext output,
        StaticTargetModel target,
        ObservableProvider provider)
    {
        if (provider == ObservableProvider.None)
        {
            ReportMissingProvider(in output, target.DisplayName, target.Location);
            return;
        }

        ReportDiagnostics(in output, target.Diagnostics);
    }

    /// <summary>Emits one namespace's static observable properties.</summary>
    /// <param name="output">The source-production context.</param>
    /// <param name="model">The namespace and its static events.</param>
    /// <param name="provider">The resolved observable implementation.</param>
    private static void EmitStaticNamespace(
        in SourceProductionContext output,
        StaticNamespaceModel model,
        ObservableProvider provider)
    {
        if (provider == ObservableProvider.None)
        {
            return;
        }

        output.AddSource(model.HintName, StaticEventsEmitter.Emit(model, provider));
    }

    /// <summary>Reports a missing provider, or the extraction diagnostics when a provider is available.</summary>
    /// <param name="output">The source-production context.</param>
    /// <param name="displayName">The requested host's readable name.</param>
    /// <param name="location">Where the request was written.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ReportMissingProvider(
        in SourceProductionContext output,
        string displayName,
        LocationInfo? location) =>
        output.ReportDiagnostic(
            DiagnosticInfo.Create(DiagnosticWarnings.MissingProvider, location, displayName).ToDiagnostic());

    /// <summary>Reports every diagnostic a model carried out of extraction.</summary>
    /// <param name="output">The source-production context.</param>
    /// <param name="diagnostics">The diagnostics to report.</param>
    private static void ReportDiagnostics(
        in SourceProductionContext output,
        EquatableArray<DiagnosticInfo> diagnostics)
    {
        foreach (var diagnostic in diagnostics.AsArray())
        {
            output.ReportDiagnostic(diagnostic.ToDiagnostic());
        }
    }
}
