// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Exposes the embedded <see cref="WitnessAsyncState"/> of an asynchronous witness.</summary>
/// <remarks>Implement it explicitly by returning a reference to a non-readonly field.</remarks>
public interface IWitnessState : IAsyncDisposable
{
    /// <summary>Gets the witness's notification gate, cancellation link and disposal state.</summary>
    ref WitnessAsyncState Witness { get; }
}
