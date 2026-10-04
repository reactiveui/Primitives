// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>The command-line entry point for the collaboration client example.</summary>
internal static class Program
{
    /// <summary>Runs the command-line client.</summary>
    /// <param name="args">The command-line arguments.</param>
    /// <returns>The process exit code.</returns>
    /// <exception cref="AggregateException">The command and console cancellation cleanup both fail.</exception>
    internal static async Task<int> Main(string[] args)
    {
        var cancellation = new ConsoleCancellationScope();
        Exception? commandFailure = null;
        var exitCode = 0;
        try
        {
            exitCode = await ProgramRunner.RunAsync(args, Console.Out, cancellation.Token).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            commandFailure = exception;
        }

        await cancellation.DisposeAsync().ConfigureAwait(false);
        if (commandFailure is not null && cancellation.Failure is not null)
        {
            throw new AggregateException("The collaboration command and console cleanup both failed.", commandFailure, cancellation.Failure);
        }

        if (commandFailure is not null)
        {
            ExceptionDispatchInfo.Capture(commandFailure).Throw();
        }

        if (cancellation.Failure is not null)
        {
            ExceptionDispatchInfo.Capture(cancellation.Failure).Throw();
        }

        return exitCode;
    }
}
