// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides explicit cancellation-free conflict resolution.</summary>
public static class IConflictResolverExtensions
{
    /// <summary>Convenience overloads for a conflict resolver.</summary>
    /// <param name="resolver">The conflict resolver.</param>
    extension(IConflictResolver resolver)
    {
        /// <summary>Resolves an immutable conflict context without a cancellation token.</summary>
        /// <param name="context">The immutable transaction input.</param>
        /// <returns>The canonical conflict decision.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ConflictResolutionResult> ResolveAsync(ConflictContext context) =>
            resolver.ResolveAsync(context, CancellationToken.None);
    }
}
