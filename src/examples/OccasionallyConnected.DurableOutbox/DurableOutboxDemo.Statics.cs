// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Static helpers for <see cref="DurableOutboxDemo"/>.</summary>
internal sealed partial class DurableOutboxDemo
{
    /// <summary>Appends a stage result to the demo output buffers.</summary>
    /// <param name="output">The standard output buffer.</param>
    /// <param name="error">The standard error buffer.</param>
    /// <param name="result">The stage result.</param>
    /// <returns>The stage exit code.</returns>
    private static int AppendStage(StringBuilder output, StringBuilder error, OutboxCommandResult result)
    {
        _ = output.Append(result.StandardOutput);
        _ = error.Append(result.StandardError);
        return result.ExitCode;
    }
}
