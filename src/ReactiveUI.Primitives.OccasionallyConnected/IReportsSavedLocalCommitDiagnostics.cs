// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies participants that report their exact saved receipt through a particular coordinator.</summary>
internal interface IReportsSavedLocalCommitDiagnostics
{
    /// <summary>Checks whether the participant reports a saved receipt to the specified engine.</summary>
    /// <param name="engine">The engine receiving the direct enqueue.</param>
    /// <returns>Whether the participant reports the same receipt through this engine's coordinator hook.</returns>
    bool ReportsSavedLocalCommitTo(SyncEngine engine);
}
