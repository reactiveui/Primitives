// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>Chooses which observable implementation the generated code is written against.</summary>
internal static class ProviderResolver
{
    /// <summary>Selects a fully referenced provider, preferring Primitives over System.Reactive.</summary>
    /// <param name="compilation">The consumer compilation.</param>
    /// <returns>The selected provider, or <see cref="ObservableProvider.None"/> when none is referenced.</returns>
    internal static ObservableProvider Resolve(Compilation compilation)
    {
        if (HasTypes(
            compilation,
            Constants.LeanSignalMetadataName,
            Constants.ScopeMetadataName,
            Constants.RxVoidMetadataName))
        {
            return ObservableProvider.Lean;
        }

        if (HasTypes(
            compilation,
            Constants.ReactiveSignalMetadataName,
            Constants.ScopeMetadataName,
            Constants.ReactiveUnitMetadataName))
        {
            return ObservableProvider.Reactive;
        }

        return HasTypes(
            compilation,
            Constants.ReactiveObservableMetadataName,
            Constants.ReactiveDisposableMetadataName,
            Constants.ReactiveUnitMetadataName)
            ? ObservableProvider.SystemReactive
            : ObservableProvider.None;
    }

    /// <summary>Gets the observable factory the provider's generated properties call.</summary>
    /// <param name="provider">The selected provider.</param>
    /// <returns>The fully qualified factory method.</returns>
    internal static string ObservableFactory(ObservableProvider provider) => provider switch
    {
        ObservableProvider.Lean => Constants.LeanSignalCreate,
        ObservableProvider.Reactive => Constants.ReactiveSignalCreate,
        _ => Constants.ObservableCreate,
    };

    /// <summary>Gets the disposable factory the provider's generated unsubscription uses.</summary>
    /// <param name="provider">The selected provider.</param>
    /// <returns>The fully qualified factory method.</returns>
    internal static string DisposableFactory(ObservableProvider provider) =>
        provider == ObservableProvider.SystemReactive ? Constants.DisposableCreate : Constants.ScopeCreate;

    /// <summary>Gets the payload type standing in for a parameterless event delegate.</summary>
    /// <param name="provider">The selected provider.</param>
    /// <returns>The fully qualified void payload type.</returns>
    internal static string VoidPayloadType(ObservableProvider provider) =>
        provider == ObservableProvider.Lean ? Constants.RxVoidType : Constants.ReactiveUnitType;

    /// <summary>Determines whether every named type is visible to a consumer.</summary>
    /// <param name="compilation">The consumer compilation.</param>
    /// <param name="first">The first required metadata name.</param>
    /// <param name="second">The second required metadata name.</param>
    /// <param name="third">The third required metadata name.</param>
    /// <returns><see langword="true"/> when all three types resolve.</returns>
    private static bool HasTypes(Compilation compilation, string first, string second, string third) =>
        compilation.GetTypeByMetadataName(first) is not null
        && compilation.GetTypeByMetadataName(second) is not null
        && compilation.GetTypeByMetadataName(third) is not null;
}
