// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Resolves named occasionally connected streams registered through dependency injection.</summary>
public interface IOccasionallyConnectedStreamRegistry
{
    /// <summary>Gets the required named stream.</summary>
    /// <typeparam name="TState">The stream state type.</typeparam>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="key">The registered stream key.</param>
    /// <returns>The registered stream instance.</returns>
    /// <remarks>The returned stream is owned by the singleton context and follows context disposal behavior.</remarks>
    IOccasionallyConnectedStream<TState, TInput> GetRequiredStream<TState, TInput>(
        OccasionallyConnectedStreamKey<TState, TInput> key);
}
