// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace ReactiveUI.Primitives.Tests;

internal static class Assert
{
    public static void True(bool condition)
    {
        if (condition)
        {
            return;
        }

        throw new InvalidOperationException("Expected condition to be true.");
    }

    public static void True(bool? condition) => True(condition == true);

    public static void False(bool condition)
    {
        if (!condition)
        {
            return;
        }

        throw new InvalidOperationException("Expected condition to be false.");
    }

    public static void False(bool? condition) => False(condition == true);

    public static void Equal<T>(IEnumerable<T> expected, IEnumerable<T> actual)
    {
        if (expected.SequenceEqual(actual))
        {
            return;
        }

        throw new InvalidOperationException($"Expected {Format(expected)}, actual {Format(actual)}.");
    }

    public static void Equal(object? expected, object? actual)
    {
        if (Equals(expected, actual))
        {
            return;
        }

        throw new InvalidOperationException($"Expected {Format(expected)}, actual {Format(actual)}.");
    }

    public static void Equal<T>(T expected, T actual)
    {
        if (EqualityComparer<T>.Default.Equals(expected, actual))
        {
            return;
        }

        throw new InvalidOperationException($"Expected {Format(expected)}, actual {Format(actual)}.");
    }

    public static void NotEqual<T>(T notExpected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(notExpected, actual))
        {
            return;
        }

        throw new InvalidOperationException($"Did not expect {Format(actual)}.");
    }

    public static void Same<T>(T expected, T actual)
        where T : class
    {
        if (ReferenceEquals(expected, actual))
        {
            return;
        }

        throw new InvalidOperationException("Expected both references to point to the same instance.");
    }

    public static void NotNull(object? value)
    {
        if (value is not null)
        {
            return;
        }

        throw new InvalidOperationException("Expected value not to be null.");
    }

    public static void Contains<T>(T expected, IEnumerable<T> collection)
    {
        if (collection.Contains(expected))
        {
            return;
        }

        throw new InvalidOperationException($"Expected collection to contain {Format(expected)}.");
    }

    public static void DoesNotContain<T>(T expected, IEnumerable<T> collection)
    {
        if (!collection.Contains(expected))
        {
            return;
        }

        throw new InvalidOperationException($"Expected collection not to contain {Format(expected)}.");
    }

    public static TException Throws<TException>(Action action)
        where TException : Exception
    {
        try
        {
            action();
        }
        catch (TException exception)
        {
            return exception;
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException(
                $"Expected exception {typeof(TException).FullName}, actual {exception.GetType().FullName}.",
                exception);
        }

        throw new InvalidOperationException($"Expected exception {typeof(TException).FullName}, but no exception was thrown.");
    }

    private static string Format<T>(T value)
    {
        if (value is null)
        {
            return "<null>";
        }

        if (value is string text)
        {
            return "\"" + text + "\"";
        }

        if (value is IEnumerable enumerable)
        {
            return "[" + string.Join(", ", enumerable.Cast<object?>()) + "]";
        }

        return value.ToString() ?? "<null>";
    }
}
