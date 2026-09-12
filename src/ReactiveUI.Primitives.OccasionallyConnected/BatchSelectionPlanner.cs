// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Selects a bounded FIFO batch prefix from caller-owned encoded metadata.</summary>
internal static class BatchSelectionPlanner
{
    /// <summary>
    /// Plans one batch without retaining or copying the candidate list. Only the effective operation ceiling worth of
    /// candidates is inspected; the caller owns the list's stability and validation of any uninspected tail.
    /// </summary>
    /// <param name="candidates">Caller-owned, FIFO-ordered candidate metadata.</param>
    /// <param name="options">The local, negotiated, envelope, and elapsed-dwell inputs.</param>
    /// <returns>The planned bounded prefix and its disposition.</returns>
    /// <exception cref="ArgumentNullException">A required argument is missing.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An inspected candidate or planning limit is invalid.</exception>
    /// <exception cref="InvalidOperationException">The local batching configuration is invalid.</exception>
    internal static BatchSelectionResult Plan(IReadOnlyList<BatchSelectionItem> candidates, BatchSelectionOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(candidates);
        ArgumentExceptionHelper.ThrowIfNull(options);
        ValidateOptions(options);
        var maximumOperations = Math.Min(options.Batching.MaximumOperations, options.NegotiatedMaximumOperations);
        var maximumBytes = Math.Min(options.Batching.MaximumBytes, options.NegotiatedMaximumBytes);
        var inspectedCount = Math.Min(candidates.Count, maximumOperations);
        var encodedBytes = options.EnvelopeBytes;
        var previousSequence = 0L;
        for (var index = 0; index < inspectedCount; index++)
        {
            var item = candidates[index];
            ValidateCandidate(in item, previousSequence);
            if (item.EncodedBytes > maximumBytes - encodedBytes)
            {
                var kind = index == 0 ? BatchSelectionResultKind.OversizedHead : BatchSelectionResultKind.Ready;
                return new(kind, index, encodedBytes);
            }

            encodedBytes += item.EncodedBytes;
            previousSequence = item.ClientSequence;
            var prefixCount = index + 1;
            if (encodedBytes == maximumBytes || prefixCount == maximumOperations)
            {
                return new(BatchSelectionResultKind.Ready, prefixCount, encodedBytes);
            }
        }

        var disposition = inspectedCount > 0 && options.FirstEligibleElapsed >= options.Batching.MaximumDwellTime
            ? BatchSelectionResultKind.Ready
            : BatchSelectionResultKind.WaitForDwell;
        return new(disposition, inspectedCount, encodedBytes);
    }

    /// <summary>Validates local limits and caller-sampled planning metadata.</summary>
    /// <param name="options">The planning options.</param>
    /// <exception cref="ArgumentNullException">The local batching configuration is missing.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A planning limit is invalid.</exception>
    /// <exception cref="InvalidOperationException">The local batching configuration is invalid.</exception>
    private static void ValidateOptions(BatchSelectionOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options.Batching);
        options.Batching.Validate();
        if (options.NegotiatedMaximumOperations <= 0 || options.NegotiatedMaximumBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Negotiated batch ceilings must be positive.");
        }

        var maximumBytes = Math.Min(options.Batching.MaximumBytes, options.NegotiatedMaximumBytes);
        if (options.EnvelopeBytes >= 0 && options.EnvelopeBytes <= maximumBytes && options.FirstEligibleElapsed >= TimeSpan.Zero)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(options), "The envelope must fit the batch and elapsed dwell must not be negative.");
    }

    /// <summary>Validates a candidate without reading later entries.</summary>
    /// <param name="item">The inspected candidate.</param>
    /// <param name="previousSequence">The preceding sequence, or zero before the first candidate.</param>
    /// <exception cref="ArgumentOutOfRangeException">The inspected sequence or encoded size is invalid.</exception>
    private static void ValidateCandidate(in BatchSelectionItem item, long previousSequence)
    {
        if (item.ClientSequence > previousSequence && item.EncodedBytes > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(item), "Candidates must have increasing positive sequences and positive encoded sizes.");
    }
}
