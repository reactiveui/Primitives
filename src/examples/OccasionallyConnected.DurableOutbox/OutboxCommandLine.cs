// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Parses and executes the durable outbox example command line.</summary>
internal static class OutboxCommandLine
{
    /// <summary>The default delivery guarantee used by append commands.</summary>
    private const string DefaultGuarantee = "at-least-once";

    /// <summary>The exit code returned for invalid command input or expected command failures.</summary>
    private const int InvalidCommandExitCode = 2;

    /// <summary>The number of raw arguments consumed by one named option and value pair.</summary>
    private const int OptionStride = 2;

    /// <summary>The prefix used by named command-line options.</summary>
    private const string OptionPrefix = "--";

    /// <summary>The database option name.</summary>
    private const string DatabaseOption = "--database";

    /// <summary>The device option name.</summary>
    private const string DeviceOption = "--device";

    /// <summary>The value option name.</summary>
    private const string ValueOption = "--value";

    /// <summary>The guarantee option name.</summary>
    private const string GuaranteeOption = "--guarantee";

    /// <summary>The operation option name.</summary>
    private const string OperationOption = "--operation";

    /// <summary>The outcome option name.</summary>
    private const string OutcomeOption = "--outcome";

    /// <summary>The take option name.</summary>
    private const string TakeOption = "--take";

    /// <summary>The shared dotnet run prefix used in usage examples.</summary>
    private const string RunPrefix = "  dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- ";

    /// <summary>Runs the command line and writes process-style output.</summary>
    /// <param name="args">The command-line arguments.</param>
    /// <param name="output">The standard output writer.</param>
    /// <param name="error">The standard error writer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The process-style exit code.</returns>
    internal static async Task<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);

        try
        {
            var command = Parse(args);
            var result = await DurableOutboxApplication.RunAsync(command, cancellationToken).ConfigureAwait(false);
            if (result.StandardOutput.Length > 0)
            {
                await output.WriteAsync(result.StandardOutput.AsMemory(), cancellationToken).ConfigureAwait(false);
            }

            if (result.StandardError.Length > 0)
            {
                await error.WriteAsync(result.StandardError.AsMemory(), cancellationToken).ConfigureAwait(false);
            }

            return result.ExitCode;
        }
        catch (ArgumentException exception)
        {
            await error.WriteLineAsync(exception.Message.AsMemory(), cancellationToken).ConfigureAwait(false);
            await error.WriteLineAsync(Usage().AsMemory(), cancellationToken).ConfigureAwait(false);
            return InvalidCommandExitCode;
        }
        catch (FormatException exception)
        {
            await error.WriteLineAsync(exception.Message.AsMemory(), cancellationToken).ConfigureAwait(false);
            await error.WriteLineAsync(Usage().AsMemory(), cancellationToken).ConfigureAwait(false);
            return InvalidCommandExitCode;
        }
    }

    /// <summary>Parses raw arguments into a command object.</summary>
    /// <param name="args">The raw command-line arguments.</param>
    /// <returns>The parsed command.</returns>
    /// <exception cref="ArgumentException">Thrown when the command is missing or unknown.</exception>
    private static IOutboxCommand Parse(string[] args)
    {
        if (args.Length == 0)
        {
            throw new ArgumentException("A command is required.");
        }

        if (args.Length == 1 && string.Equals(args[0], "--demo", StringComparison.Ordinal))
        {
            return new DemoCommand();
        }

        var command = args[0];
        if (string.Equals(command, "append-reading", StringComparison.Ordinal))
        {
            return ParseAppend(args);
        }

        if (string.Equals(command, "pending", StringComparison.Ordinal))
        {
            return ParseInspect(args, InspectView.Pending);
        }

        if (string.Equals(command, "status", StringComparison.Ordinal))
        {
            return ParseInspect(args, InspectView.Status);
        }

        return string.Equals(command, "snapshot", StringComparison.Ordinal)
            ? ParseInspect(args, InspectView.Snapshot)
            : ParseSecondaryCommand(args, command);
    }

    /// <summary>Parses less common durable outbox commands.</summary>
    /// <param name="args">The raw command-line arguments.</param>
    /// <param name="command">The command name.</param>
    /// <returns>The parsed command.</returns>
    /// <exception cref="ArgumentException">Thrown when the command is unknown or help was requested.</exception>
    private static IOutboxCommand ParseSecondaryCommand(string[] args, string command)
    {
        if (string.Equals(command, "subscription", StringComparison.Ordinal))
        {
            return ParseInspect(args, InspectView.Subscription);
        }

        if (string.Equals(command, "subscribe", StringComparison.Ordinal))
        {
            return ParseSubscribe(args);
        }

        if (string.Equals(command, "simulate-attempt", StringComparison.Ordinal))
        {
            return ParseAttempt(args);
        }

        if (string.Equals(command, "guarantees", StringComparison.Ordinal))
        {
            return ParseGuarantees(args);
        }

        if (string.Equals(command, "--help", StringComparison.Ordinal))
        {
            throw new ArgumentException(Usage());
        }

        throw new ArgumentException($"Unknown command: {command}");
    }

    /// <summary>Parses the append-reading command.</summary>
    /// <param name="args">The raw command-line arguments.</param>
    /// <returns>The parsed append command.</returns>
    /// <exception cref="ArgumentException">Thrown when an option is missing, duplicated, or unknown.</exception>
    /// <exception cref="FormatException">Thrown when the reading value is not a finite number.</exception>
    private static AppendReadingCommand ParseAppend(string[] args)
    {
        var options = ParseOptions(args, DatabaseOption, DeviceOption, ValueOption, GuaranteeOption);
        var database = RequiredOption(options, DatabaseOption);
        var device = RequiredOption(options, DeviceOption);
        var valueText = RequiredOption(options, ValueOption);
        var guaranteeText = OptionalOption(options, GuaranteeOption, DefaultGuarantee);
        if (!double.TryParse(valueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
        {
            throw new FormatException("The reading value must be a number.");
        }

        if (!double.IsFinite(value))
        {
            throw new FormatException("The reading value must be finite.");
        }

        return new(database, device, value, ParseGuarantee(guaranteeText));
    }

    /// <summary>Parses the simulate-attempt command.</summary>
    /// <param name="args">The raw command-line arguments.</param>
    /// <returns>The parsed simulation command.</returns>
    /// <exception cref="ArgumentException">Thrown when an option is missing, duplicated, or unknown.</exception>
    /// <exception cref="FormatException">Thrown when the operation id is not a GUID.</exception>
    private static SimulateAttemptCommand ParseAttempt(string[] args)
    {
        var options = ParseOptions(args, DatabaseOption, OperationOption, OutcomeOption);
        var database = RequiredOption(options, DatabaseOption);
        var operationText = OptionalOption(options, OperationOption, string.Empty);
        var outcomeText = OptionalOption(options, OutcomeOption, "lost-response");
        var operationId = string.IsNullOrEmpty(operationText)
            ? default
            : new OperationId(Guid.Parse(operationText));
        return new(database, operationId, ParseOutcome(outcomeText));
    }

    /// <summary>Parses a durable state inspection command.</summary>
    /// <param name="args">The raw command-line arguments.</param>
    /// <param name="view">The requested inspection view.</param>
    /// <returns>The parsed inspect command.</returns>
    /// <exception cref="ArgumentException">Thrown when an option is missing, duplicated, or unknown.</exception>
    /// <exception cref="FormatException">Thrown when the operation id is not a GUID.</exception>
    private static InspectCommand ParseInspect(string[] args, InspectView view)
    {
        var options = view == InspectView.Status
            ? ParseOptions(args, DatabaseOption, OperationOption)
            : ParseOptions(args, DatabaseOption);
        var database = RequiredOption(options, DatabaseOption);
        var operationText = OptionalOption(options, OperationOption, string.Empty);
        var operationId = string.IsNullOrEmpty(operationText)
            ? (OperationId?)null
            : new OperationId(Guid.Parse(operationText));
        return new(database, view, OperationId: operationId);
    }

    /// <summary>Parses the bounded subscribe command.</summary>
    /// <param name="args">The raw command-line arguments.</param>
    /// <returns>The parsed subscription inspection command.</returns>
    /// <exception cref="ArgumentException">Thrown when an option is missing, duplicated, or unknown.</exception>
    /// <exception cref="FormatException">Thrown when the take count is not positive.</exception>
    private static InspectCommand ParseSubscribe(string[] args)
    {
        var options = ParseOptions(args, DatabaseOption, TakeOption);
        var database = RequiredOption(options, DatabaseOption);
        var takeText = RequiredOption(options, TakeOption);
        if (!int.TryParse(takeText, NumberStyles.None, CultureInfo.InvariantCulture, out var take) || take <= 0)
        {
            throw new FormatException("The subscription take count must be a positive integer.");
        }

        return new(database, InspectView.Subscription, take);
    }

    /// <summary>Parses the guarantees command.</summary>
    /// <param name="args">The raw command-line arguments.</param>
    /// <returns>The parsed guarantees command.</returns>
    /// <exception cref="ArgumentException">Thrown when the command receives unsupported options.</exception>
    private static GuaranteesCommand ParseGuarantees(string[] args)
    {
        if (args.Length != 1)
        {
            throw new ArgumentException("The guarantees command does not accept options.");
        }

        return new();
    }

    /// <summary>Parses a delivery guarantee name.</summary>
    /// <param name="value">The raw guarantee name.</param>
    /// <returns>The parsed delivery guarantee.</returns>
    /// <exception cref="ArgumentException">Thrown when the guarantee is unknown.</exception>
    private static DeliveryGuarantee ParseGuarantee(string value) =>
        value switch
        {
            "at-most-once" => DeliveryGuarantee.AtMostOnce,
            "at-least-once" => DeliveryGuarantee.AtLeastOnce,
            "effectively-once" or "exactly-once" => DeliveryGuarantee.ExactlyOnce,
            _ => throw new ArgumentException($"Unknown delivery guarantee: {value}"),
        };

    /// <summary>Parses a simulated remote outcome name.</summary>
    /// <param name="value">The raw outcome name.</param>
    /// <returns>The parsed simulated outcome.</returns>
    /// <exception cref="ArgumentException">Thrown when the outcome is unknown.</exception>
    private static SimulatedAttemptOutcome ParseOutcome(string value) =>
        value switch
        {
            "lost-response" => SimulatedAttemptOutcome.LostResponse,
            "accepted" => SimulatedAttemptOutcome.Accepted,
            "rejected" => SimulatedAttemptOutcome.Rejected,
            _ => throw new ArgumentException($"Unknown simulated outcome: {value}"),
        };

    /// <summary>Parses closed named options for a command.</summary>
    /// <param name="args">The raw command-line arguments.</param>
    /// <param name="allowedOptions">The allowed option names.</param>
    /// <returns>The parsed option values.</returns>
    /// <exception cref="ArgumentException">Thrown when an option is missing, duplicated, or unknown.</exception>
    private static Dictionary<string, string> ParseOptions(string[] args, params string[] allowedOptions)
    {
        Dictionary<string, string> parsed = [];
        for (var index = 1; index < args.Length; index += OptionStride)
        {
            var option = args[index];
            if (!option.StartsWith(OptionPrefix, StringComparison.Ordinal))
            {
                throw new ArgumentException($"Unexpected value: {option}");
            }

            if (!IsAllowedOption(option, allowedOptions))
            {
                throw new ArgumentException($"Unknown option: {option}");
            }

            if (parsed.ContainsKey(option))
            {
                throw new ArgumentException($"Duplicate option: {option}");
            }

            var valueIndex = index + 1;
            if (valueIndex >= args.Length || args[valueIndex].StartsWith(OptionPrefix, StringComparison.Ordinal))
            {
                throw new ArgumentException($"Missing value for option: {option}");
            }

            parsed.Add(option, args[valueIndex]);
        }

        return parsed;
    }

    /// <summary>Checks whether an option is allowed for a command.</summary>
    /// <param name="option">The option to check.</param>
    /// <param name="allowedOptions">The allowed option names.</param>
    /// <returns><see langword="true"/> when the option is allowed.</returns>
    private static bool IsAllowedOption(string option, string[] allowedOptions)
    {
        for (var index = 0; index < allowedOptions.Length; index++)
        {
            if (string.Equals(option, allowedOptions[index], StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Reads a required option value.</summary>
    /// <param name="options">The parsed options.</param>
    /// <param name="name">The required option name.</param>
    /// <returns>The required option value.</returns>
    /// <exception cref="ArgumentException">Thrown when the option is missing.</exception>
    private static string RequiredOption(Dictionary<string, string> options, string name)
    {
        var value = OptionalOption(options, name, string.Empty);
        if (value.Length == 0)
        {
            throw new ArgumentException($"Missing required option: {name}");
        }

        return value;
    }

    /// <summary>Reads an optional option value.</summary>
    /// <param name="options">The parsed options.</param>
    /// <param name="name">The option name.</param>
    /// <param name="fallback">The fallback value.</param>
    /// <returns>The option value or fallback.</returns>
    private static string OptionalOption(Dictionary<string, string> options, string name, string fallback) =>
        options.TryGetValue(name, out var value) ? value : fallback;

    /// <summary>Builds command-line usage text.</summary>
    /// <returns>The usage text.</returns>
    private static string Usage()
    {
        var appendReading = string.Join(
            ' ',
            [
                $"{RunPrefix}append-reading {DatabaseOption} <path> {DeviceOption} <id> {ValueOption} <number>",
                $"[{GuaranteeOption} at-most-once|at-least-once|effectively-once]",
            ]);
        var simulateAttempt = string.Join(
            ' ',
            [
                $"{RunPrefix}simulate-attempt {DatabaseOption} <path>",
                $"[{OperationOption} <guid>] [{OutcomeOption} lost-response|accepted|rejected]",
            ]);

        return string.Join(
            Environment.NewLine,
            [
                "Usage:",
                appendReading,
                $"{RunPrefix}pending {DatabaseOption} <path>",
                $"{RunPrefix}status {DatabaseOption} <path> [{OperationOption} <guid>]",
                $"{RunPrefix}snapshot {DatabaseOption} <path>",
                $"{RunPrefix}subscription {DatabaseOption} <path>",
                $"{RunPrefix}subscribe {DatabaseOption} <path> {TakeOption} <n>",
                simulateAttempt,
                $"{RunPrefix}guarantees",
                $"{RunPrefix}--demo",
            ]);
    }
}
