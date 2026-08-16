// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.ObservableEvents.CodeGeneration;

/// <summary>A fluent builder for generated source, backed by thread-local pooled character buffers.</summary>
/// <remarks>
/// <para>
/// Emission builds a great many short fragments - a payload type here, a handler parameter list there - and one
/// large file per target. Accumulating into a pooled <c>char[]</c> lets the same buffers carry every fragment and
/// every file in a pass, so the steady state is a handful of arrays rather than a builder and its grown chunk
/// chain per fragment.
/// </para>
/// <para>
/// The free list is thread-local rather than a shared pool: source-output callbacks run concurrently, fragment
/// builders nest inside file builders, and nothing here outlives the call that rented it. Returning is what buys
/// the reuse; forgetting to costs reuse, never correctness.
/// </para>
/// </remarks>
internal sealed class PooledStringBuilder
{
    /// <summary>The smallest buffer worth renting, sized to hold a typical fragment without growing.</summary>
    private const int DefaultCapacity = 256;

    /// <summary>The factor the buffer grows by when exhausted.</summary>
    private const int GrowthFactor = 2;

    /// <summary>The number of buffers cached per thread, covering how deeply emission nests fragments.</summary>
    private const int MaxPooledPerThread = 16;

    /// <summary>The base of the decimal rendering used by <see cref="Append(int)"/>.</summary>
    private const int DecimalBase = 10;

    /// <summary>The widest decimal rendering of a non-negative <see cref="int"/>.</summary>
    private const int MaxIntegerDigits = 10;

    /// <summary>The line terminator, fixed so generated output does not vary by host platform.</summary>
    private const char NewLine = '\n';

    /// <summary>The per-thread free list of reusable buffers.</summary>
    [ThreadStatic]
    private static char[][]? _pool;

    /// <summary>The number of populated slots in <see cref="_pool"/>.</summary>
    [ThreadStatic]
    private static int _pooledCount;

    /// <summary>The pooled array currently backing this builder.</summary>
    private char[] _buffer;

    /// <summary>The write position within <see cref="_buffer"/>.</summary>
    private int _position;

    /// <summary>Initializes a new instance of the <see cref="PooledStringBuilder"/> class.</summary>
    /// <param name="capacity">The capacity to rent up front.</param>
    internal PooledStringBuilder(int capacity = DefaultCapacity) =>
        _buffer = RentBuffer(capacity < DefaultCapacity ? DefaultCapacity : capacity);

    /// <summary>Gets the number of characters accumulated so far.</summary>
    internal int Length => _position;

    /// <summary>Materializes the accumulated content, leaving the builder usable.</summary>
    /// <returns>The accumulated string.</returns>
    public override string ToString() => _position == 0 ? string.Empty : new string(_buffer, 0, _position);

    /// <summary>Appends a string.</summary>
    /// <param name="value">The string to append, which may be null or empty.</param>
    /// <returns>This builder, for chaining.</returns>
    internal PooledStringBuilder Append(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return this;
        }

        EnsureCapacity(_position + value!.Length);
        value.CopyTo(0, _buffer, _position, value.Length);
        _position += value.Length;
        return this;
    }

    /// <summary>Appends a single character.</summary>
    /// <param name="value">The character to append.</param>
    /// <returns>This builder, for chaining.</returns>
    internal PooledStringBuilder Append(char value)
    {
        EnsureCapacity(_position + 1);
        _buffer[_position] = value;
        _position++;
        return this;
    }

    /// <summary>Appends the invariant decimal rendering of a non-negative integer.</summary>
    /// <param name="value">The value to append; the only callers pass a name length, so it is never negative.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <remarks>
    /// Formats digits straight into the buffer. These appends sit in the per-event loop that builds the mangled
    /// static property names, where going through <c>ToString</c> would allocate a string per name segment.
    /// </remarks>
    internal PooledStringBuilder Append(int value)
    {
        EnsureCapacity(_position + MaxIntegerDigits);

        var remaining = value;
        var digitStart = _position;

        do
        {
            _buffer[_position] = (char)('0' + (remaining % DecimalBase));
            _position++;
            remaining /= DecimalBase;
        }
        while (remaining != 0);

        ReverseDigits(digitStart);
        return this;
    }

    /// <summary>Appends another builder's content, then returns that builder's buffer to the pool.</summary>
    /// <param name="other">The fragment builder to drain; it must not be appended to afterwards.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <remarks>Copies buffer to buffer, so a nested fragment joins its file without materializing a string.</remarks>
    internal PooledStringBuilder Append(PooledStringBuilder other)
    {
        if (other._position != 0)
        {
            EnsureCapacity(_position + other._position);
            Array.Copy(other._buffer, 0, _buffer, _position, other._position);
            _position += other._position;
        }

        other.Return();
        return this;
    }

    /// <summary>Appends a line terminator.</summary>
    /// <returns>This builder, for chaining.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal PooledStringBuilder AppendLine() => Append(NewLine);

    /// <summary>Appends a string followed by a line terminator.</summary>
    /// <param name="value">The string to append, which may be null or empty.</param>
    /// <returns>This builder, for chaining.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal PooledStringBuilder AppendLine(string? value) => Append(value).Append(NewLine);

    /// <summary>Appends the requested number of leading spaces.</summary>
    /// <param name="spaces">The indentation width.</param>
    /// <returns>This builder, for chaining.</returns>
    internal PooledStringBuilder AppendIndent(int spaces)
    {
        EnsureCapacity(_position + spaces);
        for (var index = 0; index < spaces; index++)
        {
            _buffer[_position] = ' ';
            _position++;
        }

        return this;
    }

    /// <summary>Appends a block of newline-separated lines, indenting each non-empty one.</summary>
    /// <param name="value">The block to append, which may be empty.</param>
    /// <param name="spaces">The indentation width applied to every non-empty line.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <remarks>
    /// Blank lines are left bare rather than filled with spaces, so an indented block never carries trailing
    /// whitespace into the generated file.
    /// </remarks>
    internal PooledStringBuilder AppendIndentedLines(string value, int spaces)
    {
        var start = 0;
        while (start < value.Length)
        {
            var end = value.IndexOf(NewLine, start);
            if (end < 0)
            {
                end = value.Length;
            }

            if (end > start)
            {
                _ = AppendIndent(spaces);
                EnsureCapacity(_position + (end - start));
                value.CopyTo(start, _buffer, _position, end - start);
                _position += end - start;
            }

            _ = AppendLine();
            start = end + 1;
        }

        return this;
    }

    /// <summary>Hands the buffer back to the thread's free list.</summary>
    /// <remarks>The builder must not be appended to afterwards.</remarks>
    internal void Return()
    {
        var toReturn = _buffer;
        _buffer = [];
        _position = 0;
        ReturnBuffer(toReturn);
    }

    /// <summary>Materializes the accumulated content and hands the buffer back.</summary>
    /// <returns>The accumulated string.</returns>
    internal string ToStringAndReturn()
    {
        var result = ToString();
        Return();
        return result;
    }

    /// <summary>Takes a buffer of at least the requested length from the thread's free list, or allocates one.</summary>
    /// <param name="minimumLength">The minimum length required.</param>
    /// <returns>A buffer at least <paramref name="minimumLength"/> long.</returns>
    private static char[] RentBuffer(int minimumLength)
    {
        var pool = _pool;
        if (pool is not null)
        {
            for (var index = _pooledCount - 1; index >= 0; index--)
            {
                var candidate = pool[index];
                if (candidate.Length < minimumLength)
                {
                    continue;
                }

                _pooledCount--;
                pool[index] = pool[_pooledCount];
                pool[_pooledCount] = null!;
                return candidate;
            }
        }

        return new char[minimumLength];
    }

    /// <summary>Puts a buffer back on the thread's free list, dropping it when the list is full.</summary>
    /// <param name="buffer">The buffer to return.</param>
    private static void ReturnBuffer(char[] buffer)
    {
        if (buffer.Length == 0)
        {
            return;
        }

        var pool = _pool ??= new char[MaxPooledPerThread][];
        if (_pooledCount >= MaxPooledPerThread)
        {
            return;
        }

        pool[_pooledCount] = buffer;
        _pooledCount++;
    }

    /// <summary>Grows the buffer when the requested length no longer fits.</summary>
    /// <param name="required">The total capacity required.</param>
    private void EnsureCapacity(int required)
    {
        if (required <= _buffer.Length)
        {
            return;
        }

        // Doubling unless the caller asked for more outright, so a run of small appends does not re-rent per append.
        var next = RentBuffer(Math.Max(required, _buffer.Length * GrowthFactor));
        Array.Copy(_buffer, next, _position);
        var toReturn = _buffer;
        _buffer = next;
        ReturnBuffer(toReturn);
    }

    /// <summary>Reverses the digits written from a position, which were emitted least significant first.</summary>
    /// <param name="start">The index the digits start at.</param>
    private void ReverseDigits(int start)
    {
        for (var end = _position - 1; start < end; start++, end--)
        {
            (_buffer[end], _buffer[start]) = (_buffer[start], _buffer[end]);
        }
    }
}
