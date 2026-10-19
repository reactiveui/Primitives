// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Payload serializer and model helpers.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>Decodes an empty counter command as a zero delta.</summary>
    /// <param name="inner">The serializer for other commands and state.</param>
    private sealed class EmptyCommandPayloadSerializer(IPayloadSerializer inner) : IPayloadSerializer
    {
        /// <inheritdoc />
        public string ContentType => inner.ContentType;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(string contractId, int schemaVersion, T value, CancellationToken cancellationToken) =>
            inner.SerializeAsync(contractId, schemaVersion, value, cancellationToken);

        /// <inheritdoc />
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return targetType == typeof(CounterInput) && envelope.Payload.IsEmpty
                ? ValueTask.FromResult<object>(new CounterInput(0))
                : inner.DeserializeAsync(envelope, targetType, cancellationToken);
        }
    }

    /// <summary>Composes another serializer while enlarging one valid counter command payload.</summary>
    /// <param name="inner">The serializer used for normal payloads.</param>
    /// <param name="oversizedValue">The command value to enlarge.</param>
    /// <param name="paddedByteCount">The minimum encoded payload length.</param>
    private sealed class OversizedCounterInputPayloadSerializer(IPayloadSerializer inner, int oversizedValue, int paddedByteCount) : IPayloadSerializer
    {
        /// <inheritdoc />
        public string ContentType => inner.ContentType;

        /// <inheritdoc />
        public async ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            var envelope = await inner.SerializeAsync(contractId, schemaVersion, value, cancellationToken).ConfigureAwait(false);
            if (value is not CounterInput { Delta: var delta } || delta != oversizedValue || envelope.Payload.Length >= paddedByteCount)
            {
                return envelope;
            }

            var text = System.Text.Encoding.UTF8.GetString(envelope.Payload.Span).PadRight(paddedByteCount);
            return envelope with
            {
                Payload = System.Text.Encoding.UTF8.GetBytes(text),
                PayloadHash = $"hash-{text}",
            };
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken) =>
            inner.DeserializeAsync(envelope, targetType, cancellationToken);
    }

    /// <summary>Serializes immutable counter payloads as invariant text.</summary>
    private class ScriptedPayloadSerializer : IPayloadSerializer
    {
        /// <inheritdoc />
        public string ContentType => PayloadContentType;

        /// <summary>Gets the optional signal set when input serialization starts.</summary>
        public TaskCompletionSource? InputSerializeEntered { get; init; }

        /// <summary>Gets the optional signal that releases input serialization.</summary>
        public TaskCompletionSource? ReleaseInputSerialize { get; init; }

        /// <summary>Creates an immutable counter state snapshot from a payload.</summary>
        /// <param name="envelope">The payload envelope.</param>
        /// <returns>The state snapshot.</returns>
        public static CounterState CreateCounterStateSnapshot(PayloadEnvelope envelope) =>
            new(ParsePayloadValue(envelope));

        /// <summary>Creates an immutable counter input snapshot from a payload.</summary>
        /// <param name="envelope">The payload envelope.</param>
        /// <returns>The input snapshot.</returns>
        public static CounterInput CreateCounterInputSnapshot(PayloadEnvelope envelope) =>
            new(CreateCounterStateSnapshot(envelope).Sum);

        /// <inheritdoc />
        public virtual async ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int numeric;
            if (value is CounterInput input)
            {
                await WaitForInputSerializeAsync(cancellationToken).ConfigureAwait(false);
                numeric = input.Delta;
            }
            else if (value is CounterState state)
            {
                numeric = state.Sum;
            }
            else
            {
                throw new InvalidOperationException(UnexpectedPayloadTypeMessage);
            }

            var text = numeric.ToString(CultureInfo.InvariantCulture);
            var payload = System.Text.Encoding.UTF8.GetBytes(text);
            return new(contractId, schemaVersion, ContentType, payload, $"hash-{text}");
        }

        /// <inheritdoc />
        public virtual ValueTask<object> DeserializeAsync(
            PayloadEnvelope envelope,
            Type targetType,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var value = ParsePayloadValue(envelope);
            if (targetType == typeof(CounterInput))
            {
                return ValueTask.FromResult<object>(new CounterInput(value));
            }

            if (targetType == typeof(CounterState))
            {
                return ValueTask.FromResult<object>(new CounterState(value));
            }

            throw new InvalidOperationException(UnexpectedTargetTypeMessage);
        }

        /// <summary>Applies optional input serialization back-pressure used by disposal tests.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task that completes when serialization may continue.</returns>
        private async ValueTask WaitForInputSerializeAsync(CancellationToken cancellationToken)
        {
            _ = InputSerializeEntered?.TrySetResult();
            if (ReleaseInputSerialize is not { } release)
            {
                return;
            }

            await release.Task
                .WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds), cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Serializes mutable counter state as invariant text.</summary>
    private sealed class MutableCounterPayloadSerializer : IPayloadSerializer
    {
        /// <inheritdoc />
        public string ContentType => PayloadContentType;

        /// <summary>Creates a mutable counter state snapshot from a payload.</summary>
        /// <param name="envelope">The payload envelope.</param>
        /// <returns>The state snapshot.</returns>
        public static MutableCounterState CreateMutableCounterStateSnapshot(PayloadEnvelope envelope) =>
            new(ParsePayloadValue(envelope));

        /// <summary>Creates an immutable counter input snapshot from a payload.</summary>
        /// <param name="envelope">The payload envelope.</param>
        /// <returns>The input snapshot.</returns>
        public static CounterInput CreateCounterInputSnapshot(PayloadEnvelope envelope) =>
            new(CreateMutableCounterStateSnapshot(envelope).Sum);

        /// <inheritdoc />
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var numeric = value switch
            {
                CounterInput input => input.Delta,
                MutableCounterState state => state.Sum,
                _ => throw new InvalidOperationException(UnexpectedPayloadTypeMessage),
            };
            var text = numeric.ToString(CultureInfo.InvariantCulture);
            var payload = System.Text.Encoding.UTF8.GetBytes(text);
            return ValueTask.FromResult(new PayloadEnvelope(
                contractId,
                schemaVersion,
                ContentType,
                payload,
                $"hash-{text}"));
        }

        /// <inheritdoc />
        public ValueTask<object> DeserializeAsync(
            PayloadEnvelope envelope,
            Type targetType,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var value = ParsePayloadValue(envelope);
            if (targetType == typeof(CounterInput))
            {
                return ValueTask.FromResult<object>(new CounterInput(value));
            }

            if (targetType == typeof(MutableCounterState))
            {
                return ValueTask.FromResult<object>(new MutableCounterState(value));
            }

            throw new InvalidOperationException(UnexpectedTargetTypeMessage);
        }
    }

    /// <summary>Stores mutable counter state.</summary>
    private sealed class MutableCounterState
    {
        /// <summary>Initializes a new instance of the <see cref="MutableCounterState"/> class.</summary>
        /// <param name="sum">The initial sum.</param>
        public MutableCounterState(int sum) => Sum = sum;

        /// <summary>Gets the current sum.</summary>
        public int Sum { get; private set; }

        /// <summary>Replaces the current sum.</summary>
        /// <param name="value">The replacement value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Replace(int value) => Sum = value;
    }

    /// <summary>Stores a counter input.</summary>
    /// <param name="Delta">The input delta.</param>
    private sealed record CounterInput(int Delta);

    /// <summary>Stores immutable counter state.</summary>
    /// <param name="Sum">The current sum.</param>
    private sealed record CounterState(int Sum);
}
