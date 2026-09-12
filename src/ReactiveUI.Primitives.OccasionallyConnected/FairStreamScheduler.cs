// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Schedules one ready head per registered stream with weighted fairness and bounded aging.</summary>
internal sealed class FairStreamScheduler
{
    /// <summary>Defines deterministic encoded bytes retained for one stream registration excluding the stream identifier.</summary>
    private const long StreamRegistrationFixedBytes = sizeof(int) + sizeof(int) + sizeof(long) + 16;

    /// <summary>Defines deterministic encoded bytes retained for one stream head using canonical UTC ticks for timestamps.</summary>
    private const long HeadFixedBytes = sizeof(int) + sizeof(long) + sizeof(long) + sizeof(long) + sizeof(byte);

    /// <summary>Protects registered stream and head metadata.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores streams in stable registration order for deterministic tie-breaking.</summary>
    private readonly List<StreamState> _streams = [];

    /// <summary>Finds registered stream state by stream identity.</summary>
    private readonly Dictionary<StreamId, StreamState> _streamsById = [];

    /// <summary>Stores validated scheduler options.</summary>
    private readonly FairStreamSchedulerOptions _options;

    /// <summary>Stores the clock used for head aging decisions.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>Tracks the retained encoded bytes for stream registrations and ready heads.</summary>
    private long _encodedDescriptorBytes;

    /// <summary>Initializes a new instance of the <see cref="FairStreamScheduler"/> class.</summary>
    /// <param name="options">The scheduler bounds and fairness settings.</param>
    /// <param name="timeProvider">The clock used to age ready heads.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is null.</exception>
    internal FairStreamScheduler(in FairStreamSchedulerOptions options, TimeProvider timeProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
        _options = options.Validated;
        _timeProvider = timeProvider;
    }

    /// <summary>Gets the number of registered streams.</summary>
    internal int RegisteredStreamCount
    {
        get
        {
            lock (_gate)
            {
                return _streams.Count;
            }
        }
    }

    /// <summary>Gets the retained encoded descriptor byte count.</summary>
    internal long EncodedDescriptorBytes
    {
        get
        {
            lock (_gate)
            {
                return _encodedDescriptorBytes;
            }
        }
    }

    /// <summary>Registers a stream with bounded retained metadata.</summary>
    /// <param name="registration">The stream registration metadata.</param>
    /// <exception cref="InvalidOperationException">The registration is invalid, duplicate, or exceeds scheduler bounds.</exception>
    internal void Register(in FairStreamRegistration registration)
    {
        ValidateRegistration(registration);
        var registrationBytes = CalculateRegistrationBytes(registration.StreamId);

        lock (_gate)
        {
            if (_streamsById.ContainsKey(registration.StreamId))
            {
                throw new InvalidOperationException("The stream is already registered.");
            }

            if (_streams.Count >= _options.MaximumStreams)
            {
                throw new InvalidOperationException("The scheduler has reached its stream registration limit.");
            }

            EnsureDescriptorCapacity(registrationBytes);

            var state = new StreamState(registration.StreamId, registration.Weight, registrationBytes, _streams.Count);
            _streams.Add(state);
            _streamsById.Add(registration.StreamId, state);
            _encodedDescriptorBytes += registrationBytes;
        }
    }

    /// <summary>Makes a stream head eligible once its due time is reached.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="priority">The bounded priority for this specific head.</param>
    /// <param name="dueUtc">The UTC due time for the head.</param>
    /// <exception cref="InvalidOperationException">The stream is missing, already has a head, priority is invalid, or capacity is exceeded.</exception>
    internal void Ready(StreamId streamId, int priority, DateTimeOffset dueUtc)
    {
        ValidatePriority(priority);
        var readySinceUtc = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            var state = GetStream(streamId);
            if (state.Head is not null)
            {
                throw new InvalidOperationException("The stream already has a scheduled head.");
            }

            EnsureDescriptorCapacity(HeadFixedBytes);
            state.Head = new(priority, dueUtc, readySinceUtc);
            _encodedDescriptorBytes += HeadFixedBytes;
        }
    }

    /// <summary>Updates the pending head for a non-inflight stream while preserving its original waiting age.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="priority">The replacement bounded priority for this specific head.</param>
    /// <param name="dueUtc">The replacement UTC due time.</param>
    /// <exception cref="InvalidOperationException">The stream is missing, has no head, is inflight, or priority is invalid.</exception>
    internal void Update(StreamId streamId, int priority, DateTimeOffset dueUtc)
    {
        ValidatePriority(priority);

        lock (_gate)
        {
            var state = GetStream(streamId);
            var head = state.Head ?? throw new InvalidOperationException("The stream does not have a scheduled head.");

            if (head.Inflight)
            {
                throw new InvalidOperationException("An inflight stream head cannot be updated.");
            }

            state.Head = new(priority, dueUtc, head.ReadySinceUtc);
        }
    }

    /// <summary>Attempts to acquire the next eligible stream head.</summary>
    /// <param name="acquisition">The acquired stream when an eligible head is available.</param>
    /// <returns><see langword="true"/> when a head was acquired; otherwise, <see langword="false"/>.</returns>
    internal bool TryAcquire([NotNullWhen(true)] out FairStreamAcquisition? acquisition)
    {
        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            var selected = SelectEligibleStream(now);
            if (!selected.HasValue)
            {
                acquisition = null;
                return false;
            }

            var selectedValue = selected.GetValueOrDefault();
            acquisition = new(selectedValue.Stream.StreamId);
            selectedValue.Head.MarkInflight(acquisition);
            return true;
        }
    }

    /// <summary>Completes an acquired head and releases its retained metadata.</summary>
    /// <param name="acquisition">The acquisition returned by <see cref="TryAcquire(out FairStreamAcquisition?)"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="acquisition"/> is null.</exception>
    /// <exception cref="InvalidOperationException">The acquisition does not match the current inflight head.</exception>
    internal void Complete(FairStreamAcquisition acquisition)
    {
        ArgumentExceptionHelper.ThrowIfNull(acquisition);

        lock (_gate)
        {
            var state = GetStream(acquisition.StreamId);
            var head = state.Head;
            if (head is null || !head.Matches(acquisition))
            {
                throw new InvalidOperationException("The acquisition does not match the inflight stream head.");
            }

            _encodedDescriptorBytes -= HeadFixedBytes;
            state.Head = null;
        }
    }

    /// <summary>Removes a stream and releases all retained scheduler metadata for it.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns><see langword="true"/> when a stream was removed; otherwise, <see langword="false"/>.</returns>
    internal bool Remove(StreamId streamId)
    {
        lock (_gate)
        {
            if (!_streamsById.TryGetValue(streamId, out var state))
            {
                return false;
            }

            _ = _streamsById.Remove(streamId);
            _ = _streams.Remove(state);
            _encodedDescriptorBytes -= state.RegistrationDescriptorBytes;

            if (state.Head is not null)
            {
                _encodedDescriptorBytes -= HeadFixedBytes;
            }

            ReindexStreams();
            return true;
        }
    }

    /// <summary>Calculates scheduler-owned registration bytes from the actual stream identifier.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The retained encoded registration byte count.</returns>
    private static long CalculateRegistrationBytes(StreamId streamId) =>
        StreamRegistrationFixedBytes + Encoding.UTF8.GetByteCount(streamId.Value);

    /// <summary>Validates a stream registration against configured bounds.</summary>
    /// <param name="registration">The stream registration to validate.</param>
    /// <exception cref="InvalidOperationException"><paramref name="registration"/> violates configured stream bounds.</exception>
    private void ValidateRegistration(FairStreamRegistration registration)
    {
        if (string.IsNullOrEmpty(registration.StreamId.Value))
        {
            throw new InvalidOperationException("StreamId must be non-empty.");
        }

        if (registration.Weight > 0 && registration.Weight <= _options.MaximumWeight)
        {
            return;
        }

        throw new InvalidOperationException("Weight must be within the configured scheduler bounds.");
    }

    /// <summary>Validates a head priority against configured bounds.</summary>
    /// <param name="priority">The head priority to validate.</param>
    /// <exception cref="InvalidOperationException"><paramref name="priority"/> violates configured priority bounds.</exception>
    private void ValidatePriority(int priority)
    {
        if (priority >= _options.MinimumPriority && priority <= _options.MaximumPriority)
        {
            return;
        }

        throw new InvalidOperationException("Priority must be within the configured scheduler bounds.");
    }

    /// <summary>Ensures a descriptor-byte change remains within capacity.</summary>
    /// <param name="additionalBytes">The signed retained-byte delta.</param>
    /// <exception cref="InvalidOperationException">The byte change would exceed configured scheduler capacity.</exception>
    private void EnsureDescriptorCapacity(long additionalBytes)
    {
        if (additionalBytes <= _options.MaximumEncodedDescriptorBytes - _encodedDescriptorBytes)
        {
            return;
        }

        throw new InvalidOperationException("The scheduler has reached its encoded descriptor byte limit.");
    }

    /// <summary>Gets registered state for a stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The registered stream state.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="streamId"/> is not registered.</exception>
    private StreamState GetStream(StreamId streamId)
    {
        if (_streamsById.TryGetValue(streamId, out var state))
        {
            return state;
        }

        throw new InvalidOperationException("The stream is not registered.");
    }

    /// <summary>Selects the eligible stream with the highest smooth weighted fair score.</summary>
    /// <param name="now">The current UTC time sampled outside the lock.</param>
    /// <returns>The selected stream state, or null when no stream is eligible.</returns>
    private SelectedStream? SelectEligibleStream(DateTimeOffset now)
    {
        SelectedStream? selected = null;
        decimal totalWeight = 0;

        for (var i = 0; i < _streams.Count; i++)
        {
            var state = _streams[i];
            if (!state.TryGetEligibleHead(now, out var head))
            {
                continue;
            }

            var effectiveWeight = state.CalculateEffectiveWeight(now, head, in _options);
            state.FairCredit += effectiveWeight;
            totalWeight += effectiveWeight;

            if (!selected.HasValue || state.HasHigherScoreThan(head, selected.GetValueOrDefault()))
            {
                selected = new(state, head);
            }
        }

        if (selected.HasValue)
        {
            var selectedValue = selected.GetValueOrDefault();
            selectedValue.Stream.FairCredit -= totalWeight;
        }

        return selected;
    }

    /// <summary>Rebuilds stable stream indexes after a removal.</summary>
    private void ReindexStreams()
    {
        for (var i = 0; i < _streams.Count; i++)
        {
            _streams[i].RegistrationIndex = i;
        }
    }

    /// <summary>Stores a selected stream and its proven non-null head.</summary>
    /// <param name="Stream">The selected stream.</param>
    /// <param name="Head">The selected head.</param>
    private readonly record struct SelectedStream(StreamState Stream, HeadState Head);

    /// <summary>Stores mutable state for one registered stream.</summary>
    private sealed class StreamState
    {
        /// <summary>Bounds age contribution so extreme clocks cannot dominate arithmetic.</summary>
        private const long MaximumAgingWeight = 1_000_000;

        /// <summary>Initializes a new instance of the <see cref="StreamState"/> class.</summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="weight">The relative scheduling weight.</param>
        /// <param name="registrationDescriptorBytes">The retained registration descriptor bytes.</param>
        /// <param name="registrationIndex">The stable registration index.</param>
        internal StreamState(StreamId streamId, int weight, long registrationDescriptorBytes, int registrationIndex)
        {
            StreamId = streamId;
            Weight = weight;
            RegistrationDescriptorBytes = registrationDescriptorBytes;
            RegistrationIndex = registrationIndex;
        }

        /// <summary>Gets the stream identifier.</summary>
        internal StreamId StreamId { get; }

        /// <summary>Gets the relative scheduling weight.</summary>
        internal int Weight { get; }

        /// <summary>Gets the retained registration descriptor bytes.</summary>
        internal long RegistrationDescriptorBytes { get; }

        /// <summary>Gets or sets the pending stream head.</summary>
        internal HeadState? Head { get; set; }

        /// <summary>Gets or sets the smooth weighted fair credit.</summary>
        internal decimal FairCredit { get; set; }

        /// <summary>Gets or sets the deterministic registration index.</summary>
        internal int RegistrationIndex { get; set; }

        /// <summary>Determines whether the stream has a ready, due and non-inflight head.</summary>
        /// <param name="now">The current UTC time.</param>
        /// <param name="head">The eligible head when one is available.</param>
        /// <returns><see langword="true"/> when the stream is eligible; otherwise, <see langword="false"/>.</returns>
        internal bool TryGetEligibleHead(DateTimeOffset now, [NotNullWhen(true)] out HeadState? head)
        {
            head = Head;
            return head is { Inflight: false } && head.DueUtc <= now;
        }

        /// <summary>Calculates the stream's effective scheduling weight for this acquisition pass.</summary>
        /// <param name="now">The current UTC time.</param>
        /// <param name="head">The proven eligible stream head.</param>
        /// <param name="options">The validated scheduler options.</param>
        /// <returns>The bounded effective weight.</returns>
        internal decimal CalculateEffectiveWeight(DateTimeOffset now, HeadState head, in FairStreamSchedulerOptions options)
        {
            var agingTicks = options.AgingInterval.Ticks;
            var waitedTicks = now <= head.ReadySinceUtc ? 0 : (now - head.ReadySinceUtc).Ticks;
            var agingWeight = waitedTicks / agingTicks;
            if (agingWeight > MaximumAgingWeight)
            {
                agingWeight = MaximumAgingWeight;
            }

            // With int-bounded stream weights and priorities, one pass is far below decimal capacity even when every
            // possible stream is eligible, while decimal keeps exact credit for repeated smooth-fair rounds.
            return (decimal)Weight + ((long)head.Priority - options.MinimumPriority) + agingWeight;
        }

        /// <summary>Compares this stream with another selected stream.</summary>
        /// <param name="head">The current stream head.</param>
        /// <param name="selected">The currently selected stream.</param>
        /// <returns><see langword="true"/> when this stream should replace the current selection.</returns>
        internal bool HasHigherScoreThan(HeadState head, SelectedStream selected)
        {
            if (FairCredit > selected.Stream.FairCredit)
            {
                return true;
            }

            if (FairCredit < selected.Stream.FairCredit)
            {
                return false;
            }

            if (head.Priority > selected.Head.Priority)
            {
                return true;
            }

            if (head.Priority < selected.Head.Priority)
            {
                return false;
            }

            return RegistrationIndex < selected.Stream.RegistrationIndex;
        }
    }

    /// <summary>Stores mutable metadata for one scheduled stream head.</summary>
    private sealed class HeadState
    {
        /// <summary>Stores the active acquisition object when the head is inflight.</summary>
        private FairStreamAcquisition? _acquisition;

        /// <summary>Initializes a new instance of the <see cref="HeadState"/> class.</summary>
        /// <param name="priority">The bounded head priority.</param>
        /// <param name="dueUtc">The UTC due time.</param>
        /// <param name="readySinceUtc">The UTC time the head became pending.</param>
        internal HeadState(int priority, DateTimeOffset dueUtc, DateTimeOffset readySinceUtc)
        {
            Priority = priority;
            DueUtc = dueUtc.ToUniversalTime();
            ReadySinceUtc = readySinceUtc.ToUniversalTime();
        }

        /// <summary>Gets the bounded head priority.</summary>
        internal int Priority { get; }

        /// <summary>Gets the UTC due time.</summary>
        internal DateTimeOffset DueUtc { get; }

        /// <summary>Gets the UTC time the head became pending.</summary>
        internal DateTimeOffset ReadySinceUtc { get; }

        /// <summary>Gets whether the head is currently acquired.</summary>
        internal bool Inflight => _acquisition is not null;

        /// <summary>Marks the head as acquired.</summary>
        /// <param name="acquisition">The scheduler-owned acquisition object.</param>
        internal void MarkInflight(FairStreamAcquisition acquisition) => _acquisition = acquisition;

        /// <summary>Determines whether an acquisition belongs to this head.</summary>
        /// <param name="acquisition">The acquisition to compare.</param>
        /// <returns><see langword="true"/> when the acquisition is this head's live acquisition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Matches(FairStreamAcquisition acquisition) => ReferenceEquals(_acquisition, acquisition);
    }
}
