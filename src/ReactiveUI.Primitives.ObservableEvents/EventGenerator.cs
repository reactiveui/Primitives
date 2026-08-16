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
/// <remarks>
/// <para>
/// Two things ask for generation. An <c>Events()</c> call names its receiver, which is what makes the API
/// discoverable from the call site; an assembly attribute names a static host, which has no receiver to call
/// through. Both converge on the same extraction and the same emitter.
/// </para>
/// <para>
/// Everything that leaves a semantic transform is a model of strings that compares by value, and every output is
/// keyed on the smallest model that decides it: one wrapper per host, one file per namespace of static events, one
/// file of activation overloads. An edit to one host's events therefore re-emits that host's file and nothing else,
/// and an edit anywhere else re-emits nothing at all.
/// </para>
/// </remarks>
[Generator(LanguageNames.CSharp)]
public sealed class EventGenerator : IIncrementalGenerator
{
    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        RegisterActivationOutput(in context);

        // Which observable library is referenced decides every type name in the generated source, but nothing about
        // which events exist. Resolving it here, into a value the pipeline can compare, keeps the far more
        // expensive extraction from re-running when only the reference set moves.
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

    /// <summary>Registers the activation API a consumer writes against.</summary>
    /// <param name="context">The generator initialization context.</param>
    /// <remarks>
    /// Deliberately an ordinary source output rather than post-initialization output, even though it depends on
    /// nothing and could be produced before anything is scanned. Post-initialization source is added to the
    /// compilation the pipeline then runs against, which makes that compilation new on every single run and throws
    /// away every semantic result cached against the previous one - so a driver that has already generated
    /// re-binds every call site from scratch, whether or not anything changed. One inert post-initialization file
    /// is enough to cost that, so this generator emits none.
    /// </remarks>
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

    /// <summary>Registers the per-namespace static wrapper files and their request diagnostics.</summary>
    /// <param name="context">The generator initialization context.</param>
    /// <param name="targets">The distinct requested static hosts.</param>
    /// <param name="provider">The resolved observable implementation.</param>
    /// <remarks>
    /// Diagnostics hang off the individual requests while source hangs off the namespace groups, because a request
    /// that produced no events still has something to say about itself but contributes nothing to a file.
    /// </remarks>
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

    /// <summary>Reports that nothing can be generated for a request because no provider is referenced.</summary>
    /// <param name="output">The source-production context.</param>
    /// <param name="displayName">The requested host's readable name.</param>
    /// <param name="location">Where the request was written.</param>
    /// <remarks>
    /// Reported instead of, not alongside, whatever else extraction found: without a provider nothing would compile
    /// anyway, and the one actionable thing to say is which package to reference.
    /// </remarks>
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
