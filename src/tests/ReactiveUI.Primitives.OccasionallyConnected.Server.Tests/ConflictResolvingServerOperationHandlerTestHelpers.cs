// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ConflictResolvingServerOperationHandler"/>.</summary>
/// <content>Helper types for conflict resolving server operation handler tests.</content>
public sealed partial class ConflictResolvingServerOperationHandlerTests
{
    /// <summary>Creates initial state for empty streams.</summary>
    private sealed class InitialStateFactory : IServerInitialStateFactory
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerState> CreateInitialStateAsync(
            StreamId streamId,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerState(streamId, InitialVersion, Payload("initial")));
    }

    /// <summary>Creates deterministic incrementing versions.</summary>
    private sealed class IncrementingVersionFactory : IServerConflictVersionFactory
    {
        /// <inheritdoc/>
        public string CreateNextVersion(ConflictContext context, SyncOperation operation) =>
            context.Current.Version == InitialVersion ? FirstVersion : SecondVersion;
    }

    /// <summary>Applies accepted operations by storing operation payloads as canonical state.</summary>
    private sealed class PayloadDomainHandler : IServerDomainHandler
    {
        /// <summary>Whether the test domain handler should produce an operation event.</summary>
        private readonly bool _produceEvents;

        /// <summary>Initializes a new instance of the <see cref="PayloadDomainHandler"/> class.</summary>
        /// <param name="produceEvents">Whether operation events should be produced.</param>
        internal PayloadDomainHandler(bool produceEvents = true) => _produceEvents = produceEvents;

        /// <summary>Gets the number of domain apply calls.</summary>
        internal int CallCount { get; private set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerDomainApplyResult> ApplyAsync(
            ServerDomainApplyContext context,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return ValueTask.FromResult(new ServerDomainApplyResult
            {
                NewState = new(context.Operation.StreamId, context.Resolution.ServerVersion, context.Operation.Payload),
                Events = _produceEvents ? [new() { EventId = context.Operation.OperationId.Value, Payload = context.Operation.Payload }] : [],
            });
        }
    }

    /// <summary>Applies domain results through a supplied callback.</summary>
    /// <param name="apply">The apply callback.</param>
    private sealed class DelegatingDomainHandler(Func<ServerDomainApplyContext, ServerDomainApplyResult> apply) : IServerDomainHandler
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerDomainApplyResult> ApplyAsync(
            ServerDomainApplyContext context,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(apply(context));
    }

    /// <summary>Creates an initial state for the wrong stream.</summary>
    private sealed class ForeignInitialStateFactory : IServerInitialStateFactory
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerState> CreateInitialStateAsync(
            StreamId streamId,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerState(new(ForeignText), InitialVersion, Payload(ForeignText)));
    }

    /// <summary>Records resolver calls and delegates resolution to a callback.</summary>
    /// <param name="resolve">The resolution callback.</param>
    private sealed class RecordingResolver(Func<ConflictContext, ConflictResolutionResult> resolve) : IConflictResolver
    {
        /// <summary>Gets the number of resolver calls.</summary>
        internal int CallCount { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ConflictResolutionResult> ResolveAsync(
            ConflictContext context,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return ValueTask.FromResult(resolve(context));
        }
    }

    /// <summary>Authorizes operations for the authenticated client and tenant.</summary>
    private sealed class RecordingAuthorizer : IServerOperationAuthorizer
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ServerOperationScope Authorize(ClientIdentity client, SyncOperation operation) =>
            new(client.TenantHint ?? Tenant, client.ClientId);
    }

    /// <summary>Provides a fixed server clock.</summary>
    /// <param name="utcNow">The fixed UTC timestamp.</param>
    private sealed class ManualClock(DateTimeOffset utcNow) : TimeProvider
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override DateTimeOffset GetUtcNow() => utcNow;
    }

    /// <summary>Owns a temporary SQLite journal path.</summary>
    private sealed class SqliteLease : IDisposable
    {
        /// <summary>The temporary SQLite directory.</summary>
        private readonly string _directory;

        /// <summary>The temporary SQLite database path.</summary>
        private readonly string _path;

        /// <summary>Initializes a new instance of the <see cref="SqliteLease"/> class.</summary>
        internal SqliteLease()
        {
            _directory = Path.Combine(Path.GetTempPath(), $"rxui-conflict-handler-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(_directory);
            _path = Path.Combine(_directory, "journal.db");
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Directory.Delete(_directory, recursive: true);

        /// <summary>Opens the SQLite journal.</summary>
        /// <param name="clock">The journal clock.</param>
        /// <returns>The SQLite journal.</returns>
        internal SqliteServerCommitJournal Open(TimeProvider clock) => new(_path, JournalOptions(clock));
    }

    /// <summary>Exposes a negative count before any registration can be copied.</summary>
    /// <param name="count">The reported collection count.</param>
    private sealed class NegativeRegistrationList(int count) : IReadOnlyList<ServerConflictStreamRegistration>
    {
        /// <inheritdoc/>
        public int Count => count;

        /// <inheritdoc/>
        public ServerConflictStreamRegistration this[int index] => throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<ServerConflictStreamRegistration> GetEnumerator() => throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Exposes an oversized metadata count before entries can be copied.</summary>
    /// <param name="count">The reported dictionary count.</param>
    private sealed class OversizedStringDictionary(int count) : IReadOnlyDictionary<string, string>
    {
        /// <inheritdoc/>
        public int Count => count;

        /// <inheritdoc/>
        public IEnumerable<string> Keys => throw new NotSupportedException();

        /// <inheritdoc/>
        public IEnumerable<string> Values => throw new NotSupportedException();

        /// <inheritdoc/>
        public string this[string key] => throw new NotSupportedException();

        /// <inheritdoc/>
        public bool ContainsKey(string key) => throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => throw new NotSupportedException();

        /// <inheritdoc/>
        public bool TryGetValue(string key, out string value)
        {
            value = string.Empty;
            return false;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Injects a competing commit before the first wrapped commit attempt.</summary>
    /// <param name="inner">The wrapped journal.</param>
    /// <param name="injected">The injected commit plan.</param>
    private sealed class InjectingJournal(IServerCommitJournal inner, ServerCommitPlan injected) : IServerCommitJournal
    {
        /// <summary>Whether the competing commit has been injected.</summary>
        private int _injected;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ServerCommitSnapshot Read(
            ServerStreamKey streamKey,
            IReadOnlyList<ServerOperationKey> operationKeys) =>
            inner.Read(streamKey, operationKeys);

        /// <inheritdoc/>
        public ServerCommitResult TryCommit(ServerCommitPlan plan)
        {
            if (Interlocked.Exchange(ref _injected, 1) != 0)
            {
                return inner.TryCommit(plan);
            }

            _ = inner.TryCommit(injected);
            return inner.TryCommit(plan);
        }
    }
}
