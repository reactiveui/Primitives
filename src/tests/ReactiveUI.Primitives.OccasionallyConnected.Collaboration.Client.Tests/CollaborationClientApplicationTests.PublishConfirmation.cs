// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <content>Publish confirmation tests for <see cref="CollaborationClientApplication"/>.</content>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>Verifies an armed publish confirmation survives a newer remote view before terminal operation observation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishConfirmationWaitRetainsAcceptedViewBeforeNewerRemoteView()
    {
        var operationId = OperationId.New();
        var observer = new LatestActivityObserver();
        var confirmation = CollaborationClientApplication.BeginPublishConfirmationWait(
            observer,
            operationId,
            CancellationToken.None);
        var accepted = ActivityView.Empty() with
        {
            AcceptedOperationId = operationId.Value.ToString("N"),
            AcceptedClientId = ClientA,
            AcceptedVersion = "activity-confirmed",
        };
        var newerRemote = ActivityView.Empty() with
        {
            AcceptedOperationId = OperationId.New().Value.ToString("N"),
            AcceptedClientId = ClientB,
            AcceptedVersion = "activity-newer",
        };

        observer.OnNext(accepted);
        observer.OnNext(newerRemote);
        var confirmed = await confirmation.WaitAsync(WaitTimeout).ConfigureAwait(false);

        await Assert.That(confirmed.AcceptedOperationId).IsEqualTo(accepted.AcceptedOperationId);
        await Assert.That(confirmed.AcceptedClientId).IsEqualTo(ClientA);
    }
}
