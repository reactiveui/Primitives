// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Process-exit helpers for coverage CLI tests.</summary>
public sealed partial class CoverageTests
{
    /// <summary>Reads a redirected text stream without linking it to the process timeout.</summary>
    /// <param name="reader">The text reader to drain.</param>
    /// <returns>The drained text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<string> ReadToEndAsync(TextReader reader) =>
        reader.ReadToEndAsync();

    /// <summary>Waits for the CLI process and returns its exit code.</summary>
    /// <param name="process">The owned CLI process.</param>
    /// <param name="cancellationToken">The cancellation token used for the timeout.</param>
    /// <returns>The CLI process exit code.</returns>
    /// <exception cref="OperationCanceledException">Thrown when the process wait is canceled.</exception>
    /// <exception cref="InvalidOperationException">The CLI process was terminated by a signal.</exception>
    private static async Task<int> WaitForScriptExitAsync(Process process, CancellationToken cancellationToken)
    {
#if NET11_0_OR_GREATER
        var exitStatus = await process.WaitForExitStatusAsync(cancellationToken).ConfigureAwait(false);
        if (exitStatus.Canceled)
        {
            throw new OperationCanceledException(cancellationToken);
        }

        if (exitStatus.Signal is not null)
        {
            throw new InvalidOperationException("The coverage CLI was terminated by a signal.");
        }

        return exitStatus.ExitCode;
#else
        await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
        return process.ExitCode;
#endif
    }
}
