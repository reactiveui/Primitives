// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Captures a command result in a form tests and the CLI can both consume.</summary>
/// <param name="ExitCode">The process-style exit code.</param>
/// <param name="StandardOutput">The text written to standard output.</param>
/// <param name="StandardError">The text written to standard error.</param>
internal sealed record OutboxCommandResult(
    int ExitCode,
    string StandardOutput,
    string StandardError)
{
    /// <summary>Gets the operation id produced or selected by a command.</summary>
    internal OperationId OperationId { get; init; }
}
