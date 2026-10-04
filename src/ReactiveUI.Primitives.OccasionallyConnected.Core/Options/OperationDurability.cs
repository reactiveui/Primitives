// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies whether an admitted operation must survive process restart before it is synchronized.</summary>
public enum OperationDurability
{
    /// <summary>The operation is written to durable local storage before admission succeeds.</summary>
    Durable = 0,

    /// <summary>The operation may be retained only in process memory.</summary>
    Volatile = 1,
}
