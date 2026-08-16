// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents;

/// <summary>The names the pipeline's steps are tracked under.</summary>
/// <remarks>
/// Tracking is what makes the caching testable: a driver told to track steps records, for every run, whether each
/// step recomputed its value and whether that value differed from last time. Without names on the steps a
/// regression that quietly reintroduces a symbol into a model - and so defeats the caching entirely - still
/// produces correct output and would go unnoticed.
/// </remarks>
internal static class GeneratorStepNames
{
    /// <summary>The step that resolves which observable implementation is referenced.</summary>
    internal const string Provider = "ObservableEvents.Provider";

    /// <summary>The step that yields the distinct hosts requested through an <c>Events()</c> call.</summary>
    internal const string InstanceTargets = "ObservableEvents.InstanceTargets";

    /// <summary>The step that yields the distinct hosts requested through an assembly attribute.</summary>
    internal const string StaticTargets = "ObservableEvents.StaticTargets";

    /// <summary>The step that yields the generated activation overloads.</summary>
    internal const string ActivationOverloads = "ObservableEvents.ActivationOverloads";

    /// <summary>The step that yields the static events grouped by namespace.</summary>
    internal const string StaticNamespaces = "ObservableEvents.StaticNamespaces";
}
