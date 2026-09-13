// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Owns the synchronous public input observer composed into a stream facade.</summary>
/// <typeparam name="TInput">The input value type.</typeparam>
internal interface IOccasionallyConnectedInputProducer<TInput> : IAsyncDisposable
{
    /// <summary>Gets the public observer exposed by the stream facade.</summary>
    IObserver<TInput> Observer { get; }
}
