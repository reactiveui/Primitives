// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Exercises decoded replay admission for the lab HTTP host.</summary>
public sealed partial class DurableHttpLostAckHostTests
{
    /// <summary>The durable lab tenant.</summary>
    private const string LabTenant = "tenant-resilience-lab";

    /// <summary>The writer principal.</summary>
    private const string WriterClient = "client-lost-ack-writer";

    /// <summary>The observer principal.</summary>
    private const string ObserverClient = "client-lost-ack-observer";

    /// <summary>The replay connect operation.</summary>
    private const string ConnectOperation = "Connect";

    /// <summary>The replay subscribe operation.</summary>
    private const string SubscribeOperation = "Subscribe";

    /// <summary>The durable counter stream.</summary>
    private static readonly StreamId LabStream = new("resilience/lost-ack/gcounter");

    /// <summary>The writer's stable subscription.</summary>
    private static readonly SubscriptionId WriterSubscription = new(new("D2939DC6-565C-4EE5-9B71-CF15C809210B"));

    /// <summary>The observer's stable subscription.</summary>
    private static readonly SubscriptionId ObserverSubscription = new(new("CC82AE70-132E-4232-8809-678D9D6DF36B"));

    /// <summary>Replay admission allows only the lab principals, operation, stream, and subscription combinations.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReplayAdmissionEnforcesAuthenticatedStreamAndSubscriptionContract()
    {
        var wrongStream = new StreamId("resilience/other/gcounter");
        var cases = new (string Tenant, string Client, string Operation, IReadOnlyList<StreamId> Streams, SubscriptionId? Subscription, bool Allowed)[]
        {
            (LabTenant, WriterClient, ConnectOperation, [], null, true),
            ("other-tenant", WriterClient, ConnectOperation, [], null, false),
            (LabTenant, "unknown-client", ConnectOperation, [], null, false),
            (LabTenant, WriterClient, ConnectOperation, [LabStream], null, false),
            (LabTenant, WriterClient, ConnectOperation, [], WriterSubscription, false),
            (LabTenant, WriterClient, "Push", [LabStream], null, true),
            (LabTenant, ObserverClient, "Push", [LabStream], null, false),
            (LabTenant, WriterClient, "Push", [], null, false),
            (LabTenant, WriterClient, "Push", [wrongStream], null, false),
            (LabTenant, WriterClient, "Push", [LabStream, wrongStream], null, false),
            (LabTenant, WriterClient, "Push", [LabStream], WriterSubscription, false),
            (LabTenant, WriterClient, SubscribeOperation, [LabStream], WriterSubscription, true),
            (LabTenant, ObserverClient, SubscribeOperation, [LabStream], ObserverSubscription, true),
            (LabTenant, ObserverClient, "Acknowledge", [LabStream], ObserverSubscription, true),
            (LabTenant, WriterClient, SubscribeOperation, [LabStream], ObserverSubscription, false),
            (LabTenant, ObserverClient, SubscribeOperation, [LabStream], WriterSubscription, false),
            (LabTenant, WriterClient, "Acknowledge", [LabStream], null, false),
            (LabTenant, WriterClient, "Unknown", [LabStream], WriterSubscription, false),
        };

        foreach (var testCase in cases)
        {
            var context = new HttpReplayAuthorizationContext
            {
                Client = new(testCase.Tenant, testCase.Client),
                Operation = testCase.Operation,
                StreamIds = testCase.Streams,
                SubscriptionId = testCase.Subscription,
            };
            var allowed = await DurableHttpLostAckScenario.DurableHttpLostAckHost.LabReplayAuthorizer.Instance.AuthorizeReplayAsync(context, CancellationToken.None);
            await Assert.That(allowed).IsEqualTo(testCase.Allowed);
        }
    }

    /// <summary>Replay admission propagates a caller's cancellation before evaluating policy.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReplayAdmissionHonorsCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var context = new HttpReplayAuthorizationContext { Client = new(LabTenant, WriterClient), Operation = ConnectOperation };
        var authorizer = DurableHttpLostAckScenario.DurableHttpLostAckHost.LabReplayAuthorizer.Instance;
        await Assert.That(async () => await authorizer.AuthorizeReplayAsync(context, cancellation.Token)).Throws<OperationCanceledException>();
    }
}
