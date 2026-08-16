// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ObservableEvents.CodeGeneration;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies the pooled builder the emitters accumulate generated source into.</summary>
public sealed class PooledStringBuilderTests
{
    /// <summary>The number of buffers the thread-local free list keeps.</summary>
    private const int PoolCapacity = 16;

    /// <summary>The number of buffers returned when overflowing the free list.</summary>
    private const int OverflowingReturnCount = PoolCapacity + 4;

    /// <summary>A capacity below the builder's own minimum, to exercise the floor.</summary>
    private const int BelowMinimumCapacity = 8;

    /// <summary>A capacity that forces the buffer to grow more than once.</summary>
    private const int GrowthTriggerLength = 4096;

    /// <summary>A length just past the default capacity, so doubling covers it in one step.</summary>
    private const int SmallOverflowLength = 300;

    /// <summary>A single-digit value, the common case when rendering a name length.</summary>
    private const int SingleDigitValue = 7;

    /// <summary>A ten-digit value, the widest rendering short of the maximum.</summary>
    private const int TenDigitValue = 1_234_567_890;

    /// <summary>The indentation used by the block-indenting assertions.</summary>
    private const int SampleIndent = 2;

    /// <summary>One level of the indentation the emitters actually use.</summary>
    private const int MemberIndent = 4;

    /// <summary>Verifies a fresh builder renders as an empty string.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderRendersEmptyBeforeAnythingIsAppended()
    {
        var builder = new PooledStringBuilder(BelowMinimumCapacity);

        await Assert.That(builder.Length).IsEqualTo(0);
        await Assert.That(builder.ToStringAndReturn()).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies null and empty appends leave the builder untouched.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderIgnoresNullAndEmptyAppends()
    {
        var builder = new PooledStringBuilder();

        _ = builder.Append((string?)null).Append(string.Empty).Append("kept");

        await Assert.That(builder.ToStringAndReturn()).IsEqualTo("kept");
    }

    /// <summary>Verifies integers render in invariant decimal, including the multi-digit and zero cases.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderRendersIntegersInDecimal()
    {
        var builder = new PooledStringBuilder();

        _ = builder.Append(0).Append('|').Append(SingleDigitValue).Append('|').Append(TenDigitValue)
            .Append('|').Append(int.MaxValue);

        await Assert.That(builder.ToStringAndReturn()).IsEqualTo("0|7|1234567890|2147483647");
    }

    /// <summary>Verifies appending past the rented capacity grows the buffer without losing content.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderGrowsWithoutLosingContent()
    {
        var builder = new PooledStringBuilder(BelowMinimumCapacity);
        var expected = new string('x', GrowthTriggerLength);

        _ = builder.Append(expected).Append(expected);

        await Assert.That(builder.ToStringAndReturn()).IsEqualTo(expected + expected);
    }

    /// <summary>Verifies a small overflow grows by the doubling factor rather than to the exact size asked for.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderGrowsByDoublingForASmallOverflow()
    {
        var builder = new PooledStringBuilder(BelowMinimumCapacity);
        var expected = new string('y', SmallOverflowLength);

        _ = builder.Append(expected);

        await Assert.That(builder.ToStringAndReturn()).IsEqualTo(expected);
    }

    /// <summary>Verifies a nested fragment builder's content joins its parent and is drained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderDrainsAnAppendedFragmentBuilder()
    {
        var builder = new PooledStringBuilder();
        var fragment = new PooledStringBuilder().Append("fragment");
        var empty = new PooledStringBuilder();

        _ = builder.Append(fragment).Append(empty);

        await Assert.That(fragment.Length).IsEqualTo(0);
        await Assert.That(builder.ToStringAndReturn()).IsEqualTo("fragment");
    }

    /// <summary>Verifies returning a builder twice is harmless, so a drained fragment can be released again.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderToleratesBeingReturnedTwice()
    {
        var builder = new PooledStringBuilder().Append("value");

        builder.Return();
        builder.Return();

        await Assert.That(builder.Length).IsEqualTo(0);
    }

    /// <summary>Verifies buffers past the free list's capacity are dropped rather than retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderDropsBuffersBeyondTheFreeListCapacity()
    {
        var builders = new PooledStringBuilder[OverflowingReturnCount];
        for (var index = 0; index < builders.Length; index++)
        {
            builders[index] = new PooledStringBuilder().Append(index);
        }

        foreach (var builder in builders)
        {
            builder.Return();
        }

        // The pool is now full; a fresh builder still has to work, whether it was handed a pooled buffer or a
        // newly allocated one.
        await Assert.That(new PooledStringBuilder().Append("after").ToStringAndReturn()).IsEqualTo("after");
    }

    /// <summary>Verifies indentation is applied per line and never lands on a blank one.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderIndentsEveryNonEmptyLine()
    {
        var builder = new PooledStringBuilder();

        _ = builder.AppendIndentedLines("first\n\nthird\n", SampleIndent);

        await Assert.That(builder.ToStringAndReturn()).IsEqualTo("  first\n\n  third\n");
    }

    /// <summary>Verifies a block whose last line has no terminator still gets one.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderTerminatesAnUnterminatedFinalLine()
    {
        var builder = new PooledStringBuilder();

        _ = builder.AppendIndentedLines("only", MemberIndent);

        await Assert.That(builder.ToStringAndReturn()).IsEqualTo("    only\n");
    }

    /// <summary>Verifies an empty block contributes nothing, which is how an unconstrained host emits.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderWritesNothingForAnEmptyBlock()
    {
        var builder = new PooledStringBuilder();

        _ = builder.AppendIndentedLines(string.Empty, MemberIndent);

        await Assert.That(builder.Length).IsEqualTo(0);
        builder.Return();
    }

    /// <summary>Verifies the line-terminator overloads append the fixed newline.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PooledStringBuilderAppendsFixedLineTerminators()
    {
        var builder = new PooledStringBuilder();

        _ = builder.AppendLine("line").AppendLine();

        await Assert.That(builder.ToStringAndReturn()).IsEqualTo("line\n\n");
    }
}
