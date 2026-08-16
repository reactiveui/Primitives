// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using ReactiveUI.Primitives.ObservableEvents.CodeGeneration;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>Reconciles the requests found across a compilation into the set of files to generate.</summary>
/// <remarks>
/// Requests arrive one per call site or attribute, but generated files are keyed on the host or the namespace, so
/// this is where the two are brought back into line. Both passes keep the order the requests were found in, so the
/// generated output does not shuffle when an unrelated file is edited.
/// </remarks>
internal static class TargetCollections
{
    /// <summary>The largest request count that cannot contain a duplicate.</summary>
    private const int SingleTarget = 1;

    /// <summary>Drops repeated requests for one host, keeping the first.</summary>
    /// <param name="targets">The requested hosts, in source order.</param>
    /// <returns>The distinct hosts.</returns>
    internal static InstanceTargetModel[] Deduplicate(ImmutableArray<InstanceTargetModel> targets)
    {
        if (targets.Length <= SingleTarget)
        {
            return [.. targets];
        }

        var result = new List<InstanceTargetModel>(targets.Length);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var target in targets)
        {
            if (seen.Add(target.Identity))
            {
                result.Add(target);
            }
        }

        return [.. result];
    }

    /// <summary>Drops repeated requests for one static host, keeping the first.</summary>
    /// <param name="targets">The requested hosts, in source order.</param>
    /// <returns>The distinct hosts.</returns>
    internal static StaticTargetModel[] Deduplicate(ImmutableArray<StaticTargetModel> targets)
    {
        if (targets.Length <= SingleTarget)
        {
            return [.. targets];
        }

        var result = new List<StaticTargetModel>(targets.Length);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var target in targets)
        {
            if (seen.Add(target.Identity))
            {
                result.Add(target);
            }
        }

        return [.. result];
    }

    /// <summary>Collects the static events of every host in a namespace into that namespace's one file.</summary>
    /// <param name="targets">The distinct static hosts, in source order.</param>
    /// <returns>One model per namespace that has at least one event to expose.</returns>
    internal static StaticNamespaceModel[] GroupByNamespace(ImmutableArray<StaticTargetModel> targets)
    {
        if (targets.IsEmpty)
        {
            return [];
        }

        var order = new List<string>();
        var byNamespace = new Dictionary<string, List<EventModel>>(StringComparer.Ordinal);
        var supportsNullableAnnotations = false;
        foreach (var target in targets)
        {
            if (target.Events.IsEmpty)
            {
                continue;
            }

            // Every request came out of the same compilation, so they agree on what its language allows.
            supportsNullableAnnotations = target.SupportsNullableAnnotations;
            if (!byNamespace.TryGetValue(target.Namespace, out var events))
            {
                events = [];
                byNamespace.Add(target.Namespace, events);
                order.Add(target.Namespace);
            }

            events.AddRange(target.Events.AsArray());
        }

        var result = new StaticNamespaceModel[order.Count];
        for (var index = 0; index < order.Count; index++)
        {
            var namespaceName = order[index];
            result[index] = new(
                GeneratedNames.StaticHintName(namespaceName),
                namespaceName,
                supportsNullableAnnotations,
                new([.. byNamespace[namespaceName]]));
        }

        return result;
    }
}
