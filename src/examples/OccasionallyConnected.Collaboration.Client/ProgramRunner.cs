// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Parses command-line arguments and runs the client example.</summary>
internal static class ProgramRunner
{
    /// <summary>The validation error exit code.</summary>
    private const int ValidationErrorExitCode = 1;

    /// <summary>The cancellation exit code.</summary>
    private const int CancellationExitCode = 2;

    /// <summary>The default server endpoint.</summary>
    private static readonly Uri DefaultServerUri = new("http://127.0.0.1:5088");

    /// <summary>Runs the client example.</summary>
    /// <param name="args">The command-line arguments.</param>
    /// <param name="output">The output writer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The process exit code.</returns>
    internal static async Task<int> RunAsync(string[] args, TextWriter output, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        try
        {
            var command = Parse(args);
            return await CollaborationClientApplication.RunAsync(command, output, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await output.WriteLineAsync("The command timed out or was canceled.").ConfigureAwait(false);
            return CancellationExitCode;
        }
        catch (HttpRemoteTransportException exception)
        {
            await WriteTransportFailureAsync(output, exception).ConfigureAwait(false);
            return ValidationErrorExitCode;
        }
        catch (Exception exception) when (exception is ArgumentException or FormatException or InvalidOperationException)
        {
            await WriteCommandFailureAsync(output, exception).ConfigureAwait(false);
            await WriteUsageAsync(output).ConfigureAwait(false);
            return ValidationErrorExitCode;
        }
    }

    /// <summary>Writes a redacted command failure summary.</summary>
    /// <param name="output">The output writer.</param>
    /// <param name="exception">The command exception.</param>
    /// <returns>The write task.</returns>
    private static Task WriteCommandFailureAsync(TextWriter output, Exception exception)
    {
        var code = exception is ArgumentException or FormatException ? "InvalidArguments" : "CommandFailed";
        return output.WriteLineAsync($"error: {code}");
    }

    /// <summary>Writes a redacted transport failure summary.</summary>
    /// <param name="output">The output writer.</param>
    /// <param name="exception">The transport exception.</param>
    /// <returns>The write task.</returns>
    private static async Task WriteTransportFailureAsync(TextWriter output, HttpRemoteTransportException exception)
    {
        var status = exception.StatusCode.HasValue
            ? ((int)exception.StatusCode.Value).ToString(CultureInfo.InvariantCulture)
            : "none";
        await output.WriteLineAsync("faults: 1").ConfigureAwait(false);
        await output.WriteLineAsync($"fault: {exception.Kind} status={status}").ConfigureAwait(false);
    }

    /// <summary>Parses command-line arguments.</summary>
    /// <param name="args">The command-line arguments.</param>
    /// <returns>The parsed command.</returns>
    /// <exception cref="ArgumentException">The arguments do not describe a supported command.</exception>
    private static CollaborationClientCommand Parse(string[] args)
    {
        if (args.Length == 0 || IsHelp(args[0]))
        {
            throw new ArgumentException("A command is required.");
        }

        var reader = new ArgumentReader(args);
        var commandText = reader.ReadCommand();
        var kind = commandText switch
        {
            "publish" => CollaborationClientCommandKind.Publish,
            "watch" => CollaborationClientCommandKind.Watch,
            _ => throw new ArgumentException("Unknown collaboration client command.", nameof(args)),
        };
        var server = reader.ReadUri("--server", DefaultServerUri);
        var database = reader.ReadRequired("--database");
        var token = reader.ReadRequired("--token");
        var client = reader.ReadRequired("--client");
        var autoStart = !reader.ReadSwitch("--offline");
        var status = reader.Read("--status", "active") ?? "active";
        var title = reader.Read("--title", null);
        var details = reader.Read("--details", null);
        reader.ThrowIfUnused();
        return new()
        {
            Kind = kind,
            Options = new() { ServerUri = server, DatabasePath = database, Token = token, ClientId = client, AutoStart = autoStart },
            Update = new() { Status = status, Title = title, TitleSpecified = title is not null, Details = details, DetailsSpecified = details is not null },
        };
    }

    /// <summary>Determines whether an argument asks for help.</summary>
    /// <param name="value">The argument.</param>
    /// <returns>Whether the argument asks for help.</returns>
    private static bool IsHelp(string value) =>
        string.Equals(value, "--help", StringComparison.Ordinal) || string.Equals(value, "-h", StringComparison.Ordinal);

    /// <summary>Writes usage text.</summary>
    /// <param name="output">The output writer.</param>
    /// <returns>The write task.</returns>
    private static async Task WriteUsageAsync(TextWriter output)
    {
        await output.WriteLineAsync("Usage:").ConfigureAwait(false);
        await output.WriteLineAsync(
                "  publish --server http://127.0.0.1:5088 --database client-a.db --token token-a --client client-a --status active --title \"Activity\" --details \"Ready\"")
            .ConfigureAwait(false);
        await output.WriteLineAsync(
                "  publish --offline --server http://127.0.0.1:5088 --database client-a.db --token token-a --client client-a --status draft")
            .ConfigureAwait(false);
        await output.WriteLineAsync(
                "  watch --server http://127.0.0.1:5088 --database client-b.db --token token-b --client client-b")
            .ConfigureAwait(false);
    }

    /// <summary>Reads named command-line arguments.</summary>
    private sealed class ArgumentReader
    {
        /// <summary>The first named argument index.</summary>
        private const int FirstNamedArgumentIndex = 1;

        /// <summary>The arguments.</summary>
        private readonly string[] _args;

        /// <summary>Tracks consumed arguments.</summary>
        private readonly bool[] _used;

        /// <summary>Initializes a new instance of the <see cref="ArgumentReader"/> class.</summary>
        /// <param name="args">The arguments.</param>
        internal ArgumentReader(string[] args)
        {
            _args = args;
            _used = new bool[args.Length];
        }

        /// <summary>Reads the command name.</summary>
        /// <returns>The command name.</returns>
        internal string ReadCommand()
        {
            _used[0] = true;
            return _args[0];
        }

        /// <summary>Reads a required value.</summary>
        /// <param name="name">The argument name.</param>
        /// <returns>The argument value.</returns>
        /// <exception cref="ArgumentException">The required argument is missing.</exception>
        internal string ReadRequired(string name) =>
            Read(name, null) ?? throw new ArgumentException($"Missing required argument {name}.");

        /// <summary>Reads a value with a default.</summary>
        /// <param name="name">The argument name.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns>The argument value.</returns>
        internal string? Read(string name, string? defaultValue)
        {
            for (var index = FirstNamedArgumentIndex; index < _args.Length - 1; index++)
            {
                if (_used[index] || !string.Equals(_args[index], name, StringComparison.Ordinal))
                {
                    continue;
                }

                _used[index] = true;
                _used[index + 1] = true;
                return _args[index + 1];
            }

            return defaultValue;
        }

        /// <summary>Reads a URI value.</summary>
        /// <param name="name">The argument name.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns>The URI value.</returns>
        internal Uri ReadUri(string name, Uri defaultValue)
        {
            var value = Read(name, null);
            return value is null ? defaultValue : new(value, UriKind.Absolute);
        }

        /// <summary>Reads a boolean switch.</summary>
        /// <param name="name">The switch name.</param>
        /// <returns>Whether the switch was present.</returns>
        internal bool ReadSwitch(string name)
        {
            for (var index = FirstNamedArgumentIndex; index < _args.Length; index++)
            {
                if (_used[index] || !string.Equals(_args[index], name, StringComparison.Ordinal))
                {
                    continue;
                }

                _used[index] = true;
                return true;
            }

            return false;
        }

        /// <summary>Throws when unknown arguments remain.</summary>
        /// <exception cref="ArgumentException">An unknown argument remains.</exception>
        internal void ThrowIfUnused()
        {
            for (var index = 0; index < _used.Length; index++)
            {
                if (!_used[index])
                {
                    throw new ArgumentException($"Unknown argument {_args[index]}.");
                }
            }
        }
    }
}
