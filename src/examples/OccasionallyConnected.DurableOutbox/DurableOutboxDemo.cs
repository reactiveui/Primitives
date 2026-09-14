// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Runs the deterministic durable outbox demonstration in an owned temporary directory.</summary>
internal sealed partial class DurableOutboxDemo
{
    /// <summary>The maximum number of stages allowed in the bounded demo workflow.</summary>
    private const int MaximumStageCount = 16;

    /// <summary>The parent directory used for generated demo directories.</summary>
    private readonly string _root;

    /// <summary>The owned immutable snapshot of stages to run.</summary>
    private readonly DemoStage[] _stages;

    /// <summary>Initializes a new instance of the <see cref="DurableOutboxDemo"/> class.</summary>
    /// <param name="root">The parent directory for owned demo directories.</param>
    /// <param name="stages">The ordered demo stages.</param>
    /// <exception cref="ArgumentException">Thrown when the stage list is empty or too large.</exception>
    internal DurableOutboxDemo(string root, IReadOnlyList<DemoStage> stages)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(root);
        ArgumentNullException.ThrowIfNull(stages);

        var stageCount = stages.Count;
        if (stageCount == 0)
        {
            throw new ArgumentException("At least one demo stage is required.", nameof(stages));
        }

        if (stageCount > MaximumStageCount)
        {
            throw new ArgumentException("The demo has too many configured stages.", nameof(stages));
        }

        var ownedStages = new DemoStage[stageCount];
        for (var index = 0; index < stageCount; index++)
        {
            ownedStages[index] = stages[index];
        }

        _root = root;
        _stages = ownedStages;
    }

    /// <summary>Runs the configured demo stages and cleans the owned directory.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    internal async ValueTask<OutboxCommandResult> RunAsync(CancellationToken cancellationToken)
    {
        OwnedDemoDirectory directory = new(_root);
        StringBuilder output = new();
        StringBuilder error = new();
        var exitCode = 0;
        try
        {
            directory.CreateDirectory();
            cancellationToken.ThrowIfCancellationRequested();
            await directory.MarkOwnedAsync(cancellationToken).ConfigureAwait(false);

            for (var index = 0; index < _stages.Length && exitCode == 0; index++)
            {
                var result = await _stages[index](directory.DatabasePath, cancellationToken).ConfigureAwait(false);
                exitCode = AppendStage(output, error, result);
            }

            _ = output.AppendLine(string.Create(CultureInfo.InvariantCulture, $"demo database: {directory.DatabasePath}"));
        }
        finally
        {
            directory.Cleanup();
        }

        _ = output
            .AppendLine(string.Create(CultureInfo.InvariantCulture, $"demo database cleaned: {directory.DatabasePath}"))
            .AppendLine(string.Create(
                CultureInfo.InvariantCulture,
                $"owned directory removed: {!Directory.Exists(directory.DirectoryPath)}"));
        return new(exitCode, output.ToString(), error.ToString());
    }
}
