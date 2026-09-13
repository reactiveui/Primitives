// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides a retry classification for a typed remote transport exception.</summary>
public interface IRemoteTransportFailure
{
    /// <summary>Gets the retry classification exposed by the typed transport exception.</summary>
    RetryFailure RetryFailure { get; }
}
