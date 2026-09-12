// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Client identity binding tests for <see cref="SqliteLocalCommitStore"/>.</summary>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The first client identity used for binding races.</summary>
    private const string FirstBindingClientId = "client-a";

    /// <summary>The second client identity used for binding races.</summary>
    private const string SecondBindingClientId = "client-b";

    /// <summary>The timeout used while coordinating the binding race.</summary>
    private static readonly TimeSpan BindingRaceTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies competing first client bindings serialize to one durable winner.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConcurrentFirstClientBindingsAllowOnlyOneWinner()
    {
        using var database = TempDatabase.Create();
        using var ready = new ManualResetEventSlim();
        var first = Task.Run(() => InitializeClientWhenReady(database.Path, FirstBindingClientId, ready));
        var second = Task.Run(() => InitializeClientWhenReady(database.Path, SecondBindingClientId, ready));

        ready.Set();
        var attempts = await Task.WhenAll(first, second);
        var successes = attempts.Where(static attempt => attempt.Exception is null).ToArray();
        var failures = attempts.Where(static attempt => attempt.Exception is not null).ToArray();
        var winner = successes[0].ClientId;
        var loser = failures[0].ClientId;

        using var accepted = new SqliteLocalCommitStore(database.Path);
        accepted.Initialize(new(StoreIdentity, SchemaVersion, false) { ClientId = winner }, CancellationToken.None);
        using var rejected = new SqliteLocalCommitStore(database.Path);
        Action conflict = () => rejected.Initialize(new(StoreIdentity, SchemaVersion, false) { ClientId = loser }, CancellationToken.None);

        await Assert.That(successes.Length).IsEqualTo(1);
        await Assert.That(failures.Length).IsEqualTo(1);
        await Assert.That(failures[0].Exception).IsTypeOf<InvalidOperationException>();
        await Assert.That(conflict).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Initializes a store after the race gate opens.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="clientId">The client identity.</param>
    /// <param name="ready">The race gate.</param>
    /// <returns>The initialization attempt.</returns>
    /// <exception cref="TimeoutException">The race gate does not open.</exception>
    private static BindingAttempt InitializeClientWhenReady(string path, string clientId, ManualResetEventSlim ready)
    {
        if (!ready.Wait(BindingRaceTimeout))
        {
            throw new TimeoutException("The binding race gate did not open.");
        }

        using var store = new SqliteLocalCommitStore(path);
        try
        {
            store.Initialize(new(StoreIdentity, SchemaVersion, false) { ClientId = clientId }, CancellationToken.None);
            return new(clientId, null);
        }
        catch (Exception exception)
        {
            return new(clientId, exception);
        }
    }

    /// <summary>A captured binding attempt.</summary>
    /// <param name="ClientId">The requested client identity.</param>
    /// <param name="Exception">The thrown exception, if any.</param>
    private sealed record BindingAttempt(string ClientId, Exception? Exception);
}
