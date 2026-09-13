// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="ILocalSnapshotRecoveryStore"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class ILocalSnapshotRecoveryStoreExtensions
{
    /// <summary>Convenience overloads for a local snapshot recovery store.</summary>
    /// <param name="store">The local snapshot recovery store.</param>
    extension(ILocalSnapshotRecoveryStore store)
    {
        /// <summary>Atomically applies a snapshot checkpoint and rebuilt optimistic state after a retained-history gap.</summary>
        /// <param name="mutation">The validated local snapshot recovery mutation.</param>
        /// <returns>The durable local snapshot recovery result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshotRecoveryResult> ApplySnapshotRecoveryAsync(LocalSnapshotRecoveryMutation mutation) =>
            store.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
    }
}
