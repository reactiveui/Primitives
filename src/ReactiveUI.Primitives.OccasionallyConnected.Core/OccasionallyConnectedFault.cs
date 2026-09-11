// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a typed operational fault without exposing payload contents by default.</summary>
/// <param name="Code">The stable fault code.</param>
/// <param name="Message">The diagnostic message.</param>
/// <param name="OccurredAtUtc">The time the fault was observed.</param>
/// <param name="StreamId">The optional related stream identifier.</param>
/// <param name="OperationId">The optional related operation identifier.</param>
/// <param name="Exception">The optional local exception.</param>
[System.Diagnostics.DebuggerDisplay("{Code,nq}")]
public sealed record OccasionallyConnectedFault(
    string Code,
    string Message,
    DateTimeOffset OccurredAtUtc,
    StreamId? StreamId,
    OperationId? OperationId,
    Exception? Exception);
