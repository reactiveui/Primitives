// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Child process diagnostics for SQLite adapter tests.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Serializes parent tests that boot an MTP host so profiled child startups do not compete.</summary>
    private const string SqliteChildProcessParallelKey = "sqlite-child-process";

    /// <summary>Verifies a child failure reports the SQLite stage and exits without requiring a parent kill.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [NotInParallel(SqliteChildProcessParallelKey)]
    public async Task WhenCrashRecoveryChildFailsBeforeBoundary_ThenParentReportsFailureAndExit()
    {
        using var database = TempDatabase.Create();
        var signalPath = CreateCrashRecoverySignalPath(database.Path, "unknown");
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        }

        var exception = await Assert.That(async () => await RunCrashRecoveryChildUntilSignalAsync(
            new(database.Path, signalPath, "unknown", OperationId.New(), SubscriptionId.New(), DeliveryGuarantee.AtLeastOnce)))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(exception!.Message).Contains("ExitedBeforeStop: True");
        await Assert.That(exception.Message).Contains("KilledByParent: False");
        await Assert.That(exception.Message).Contains("Child progress: MTP child test entered");
        await Assert.That(exception.Message).Contains("SQLite initialized; committing unknown");
        await Assert.That(exception.Message).Contains("System.InvalidOperationException: The child crash recovery boundary is unknown.");
        await Assert.That(File.Exists(signalPath)).IsFalse();
    }

    /// <summary>Publishes child progress outside MTP's buffered test output.</summary>
    /// <param name="signalPath">The boundary signal path.</param>
    /// <param name="progress">The last completed stage or failure.</param>
    private static void WriteCrashRecoveryChildProgress(string signalPath, string progress)
    {
        var progressPath = $"{signalPath}.progress";
        var pendingPath = $"{progressPath}.pending";
        var previousProgress = File.Exists(progressPath) ? File.ReadAllText(progressPath) + Environment.NewLine : string.Empty;
        File.WriteAllText(pendingPath, previousProgress + progress);
        File.Move(pendingPath, progressPath, overwrite: true);
    }

    /// <summary>Reads the child stage after the owned process has stopped.</summary>
    /// <param name="signalPath">The boundary signal path.</param>
    /// <returns>The last child stage.</returns>
    private static string ReadCrashRecoveryChildProgress(string signalPath) =>
        File.Exists($"{signalPath}.progress")
            ? File.ReadAllText($"{signalPath}.progress")
            : "MTP child test was not entered.";
}
