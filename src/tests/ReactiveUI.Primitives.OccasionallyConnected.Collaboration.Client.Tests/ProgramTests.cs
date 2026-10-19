// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <summary>Tests the executable entry point.</summary>
public sealed class ProgramTests
{
    /// <summary>Verifies the executable entry invokes command parsing.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MainRunsCommandParser()
    {
        var exitCode = await Program.Main([]).ConfigureAwait(false);

        await Assert.That(exitCode).IsEqualTo(1);
    }
}
