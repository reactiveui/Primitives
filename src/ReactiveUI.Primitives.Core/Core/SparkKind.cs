// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>Indicates the type of a spark.</summary>
public enum SparkKind
{
    /// <summary>Represents an OnNext spark.</summary>
    OnNext,

    /// <summary>Represents an OnError spark.</summary>
    OnError,

    /// <summary>Represents an OnCompleted spark.</summary>
    OnCompleted
}
