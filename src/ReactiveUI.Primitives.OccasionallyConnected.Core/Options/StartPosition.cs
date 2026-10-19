// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Defines where a subscription starts consuming a stream.</summary>
[DebuggerDisplay("{Kind,nq}")]
public sealed record StartPosition
{
    /// <summary>The maximum permitted number of UTF-8 bytes in a server-issued cursor.</summary>
    private const int MaximumCursorUtf8Bytes = 4096;

    /// <summary>Initializes a new instance of the <see cref="StartPosition"/> class.</summary>
    /// <param name="kind">The position kind.</param>
    /// <param name="timestamp">The optional timestamp position.</param>
    /// <param name="sequence">The optional server sequence position.</param>
    /// <param name="cursor">The optional server-issued cursor position.</param>
    private StartPosition(
        StartPositionKind kind,
        DateTimeOffset? timestamp = null,
        long? sequence = null,
        string? cursor = null)
    {
        Kind = kind;
        Timestamp = timestamp;
        Sequence = sequence;
        Cursor = cursor;
    }

    /// <summary>Gets a position that starts with subsequently published events.</summary>
    public static StartPosition Latest { get; } = new(StartPositionKind.Latest);

    /// <summary>Gets the kind of position.</summary>
    public StartPositionKind Kind { get; }

    /// <summary>Gets the timestamp used by <see cref="StartPositionKind.FromTimestamp"/> positions.</summary>
    public DateTimeOffset? Timestamp { get; }

    /// <summary>Gets the server sequence used by <see cref="StartPositionKind.FromSequence"/> positions.</summary>
    public long? Sequence { get; }

    /// <summary>Gets the opaque server-issued cursor used by <see cref="StartPositionKind.FromCursor"/> positions.</summary>
    public string? Cursor { get; }

    /// <summary>Creates a position that starts with events published at or after a timestamp.</summary>
    /// <param name="timestamp">The timestamp at which to begin.</param>
    /// <returns>A timestamp-based start position.</returns>
    public static StartPosition FromTimestamp(DateTimeOffset timestamp) =>
        new(StartPositionKind.FromTimestamp, timestamp: timestamp);

    /// <summary>Creates a position that starts with events at or after a server-assigned sequence.</summary>
    /// <param name="sequence">The non-negative server-assigned sequence at which to begin.</param>
    /// <returns>A sequence-based start position.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="sequence"/> is negative.</exception>
    public static StartPosition FromSequence(long sequence)
    {
        if (sequence < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(sequence), sequence, "Sequence must be non-negative.");
        }

        return new(StartPositionKind.FromSequence, sequence: sequence);
    }

    /// <summary>Creates a position that starts after a server-issued resume cursor.</summary>
    /// <param name="cursor">The opaque server-issued cursor.</param>
    /// <returns>A cursor-based start position.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="cursor"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="cursor"/> is empty, malformed, or exceeds 4096 UTF-8 bytes.</exception>
    public static StartPosition FromCursor(string cursor)
    {
        ArgumentExceptionHelper.ThrowIfNull(cursor);

        if (cursor.Length == 0 || !IsWellFormedUnicode(cursor) || Encoding.UTF8.GetByteCount(cursor) > MaximumCursorUtf8Bytes)
        {
            throw new ArgumentException("Cursor must be non-empty, well-formed Unicode, and no more than 4096 UTF-8 bytes.", nameof(cursor));
        }

        return new(StartPositionKind.FromCursor, cursor: cursor);
    }

    /// <summary>Determines whether a string contains only well-formed UTF-16 surrogate pairs.</summary>
    /// <param name="value">The value to validate.</param>
    /// <returns><see langword="true"/> when the value contains no unpaired surrogates; otherwise, <see langword="false"/>.</returns>
    private static bool IsWellFormedUnicode(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            var character = value[index];
            if (char.IsHighSurrogate(character))
            {
                if (index == value.Length - 1 || !char.IsLowSurrogate(value[index + 1]))
                {
                    return false;
                }

                index++;
            }
            else if (char.IsLowSurrogate(character))
            {
                return false;
            }
        }

        return true;
    }
}
