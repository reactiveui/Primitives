// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Minimal assertion helpers used by the TUnit tests.
/// </summary>
internal static class Assert
{
    /// <summary>
    /// Verifies that a condition is true.
    /// </summary>
    /// <param name="condition">The condition to verify.</param>
    public static void True(bool condition)
    {
        if (condition)
        {
            return;
        }

        throw new InvalidOperationException($"Expected {nameof(condition)} to be true.");
    }

    /// <summary>
    /// Verifies that a nullable condition is true.
    /// </summary>
    /// <param name="condition">The nullable condition to verify.</param>
    public static void True(bool? condition) => True(condition.GetValueOrDefault());

    /// <summary>
    /// Verifies that a condition is false.
    /// </summary>
    /// <param name="condition">The condition to verify.</param>
    public static void False(bool condition)
    {
        if (!condition)
        {
            return;
        }

        throw new InvalidOperationException($"Expected {nameof(condition)} to be false.");
    }

    /// <summary>
    /// Verifies that a nullable condition is false.
    /// </summary>
    /// <param name="condition">The nullable condition to verify.</param>
    public static void False(bool? condition) => False(condition.GetValueOrDefault());

    /// <summary>
    /// Verifies that two sequences contain equal values in order.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="expected">The expected sequence.</param>
    /// <param name="actual">The actual sequence.</param>
    public static void Equal<T>(IEnumerable<T> expected, IEnumerable<T> actual)
    {
        if (expected is null)
        {
            throw new ArgumentNullException(nameof(expected));
        }

        if (actual is null)
        {
            throw new ArgumentNullException(nameof(actual));
        }

        if (AreSequencesEqual(expected, actual))
        {
            return;
        }

        throw new InvalidOperationException($"Expected {Format(expected)}, {nameof(actual)} {Format(actual)}.");
    }

    /// <summary>
    /// Verifies that two boxed values are equal.
    /// </summary>
    /// <param name="expected">The expected value.</param>
    /// <param name="actual">The actual value.</param>
    public static void Equal(object? expected, object? actual)
    {
        if (Equals(expected, actual))
        {
            return;
        }

        throw new InvalidOperationException($"Expected {Format(expected)}, {nameof(actual)} {Format(actual)}.");
    }

    /// <summary>
    /// Verifies that two values are equal.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="expected">The expected value.</param>
    /// <param name="actual">The actual value.</param>
    public static void Equal<T>(T expected, T actual)
    {
        if (EqualityComparer<T>.Default.Equals(expected, actual))
        {
            return;
        }

        throw new InvalidOperationException($"Expected {Format(expected)}, {nameof(actual)} {Format(actual)}.");
    }

    /// <summary>
    /// Verifies that two values are not equal.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="notExpected">The value that should not be produced.</param>
    /// <param name="actual">The actual value.</param>
    public static void NotEqual<T>(T notExpected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(notExpected, actual))
        {
            return;
        }

        throw new InvalidOperationException($"Did not expect {Format(actual)}.");
    }

    /// <summary>
    /// Verifies that two references point to the same instance.
    /// </summary>
    /// <typeparam name="T">The reference type.</typeparam>
    /// <param name="expected">The expected instance.</param>
    /// <param name="actual">The actual instance.</param>
    public static void Same<T>(T expected, T actual)
        where T : class
    {
        if (ReferenceEquals(expected, actual))
        {
            return;
        }

        throw new InvalidOperationException("Expected both references to point to the same instance.");
    }

    /// <summary>
    /// Verifies that a value is not null.
    /// </summary>
    /// <param name="value">The value to verify.</param>
    public static void NotNull(object? value)
    {
        if (value is not null)
        {
            return;
        }

        throw new InvalidOperationException($"Expected {nameof(value)} not to be null.");
    }

    /// <summary>
    /// Verifies that a collection contains a value.
    /// </summary>
    /// <typeparam name="T">The collection element type.</typeparam>
    /// <param name="expected">The expected value.</param>
    /// <param name="collection">The collection to inspect.</param>
    public static void Contains<T>(T expected, IEnumerable<T> collection)
    {
        if (collection is null)
        {
            throw new ArgumentNullException(nameof(collection));
        }

        if (ContainsValue(expected, collection))
        {
            return;
        }

        throw new InvalidOperationException($"Expected {nameof(collection)} to contain {Format(expected)}.");
    }

    /// <summary>
    /// Verifies that a collection does not contain a value.
    /// </summary>
    /// <typeparam name="T">The collection element type.</typeparam>
    /// <param name="expected">The value that should not be present.</param>
    /// <param name="collection">The collection to inspect.</param>
    public static void DoesNotContain<T>(T expected, IEnumerable<T> collection)
    {
        if (collection is null)
        {
            throw new ArgumentNullException(nameof(collection));
        }

        if (!ContainsValue(expected, collection))
        {
            return;
        }

        throw new InvalidOperationException($"Expected {nameof(collection)} not to contain {Format(expected)}.");
    }

    /// <summary>
    /// Verifies that an action throws the expected exception type.
    /// </summary>
    /// <typeparam name="TException">The expected exception type.</typeparam>
    /// <param name="action">The action to execute.</param>
    /// <param name="expectedException">An optional marker used to infer the expected exception type.</param>
    /// <returns>The thrown exception.</returns>
    public static TException Throws<TException>(Action action, TException? expectedException = null)
        where TException : Exception
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var expectedExceptionType = expectedException?.GetType() ?? typeof(TException);

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
                $"Expected exception {expectedExceptionType.FullName}, caught {exception.GetType().FullName}.",
                exception);
        }

        throw new InvalidOperationException($"Expected exception {expectedExceptionType.FullName}, but no exception was thrown.");
    }

    /// <summary>
    /// Formats a value for assertion failure messages.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The value to format.</param>
    /// <returns>The formatted value.</returns>
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
            return FormatEnumerable(enumerable);
        }

        return value.ToString() ?? "<null>";
    }

    /// <summary>
    /// Determines whether two sequences contain equal values in order.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="expected">The expected sequence.</param>
    /// <param name="actual">The actual sequence.</param>
    /// <returns><see langword="true"/> when both sequences contain the same values; otherwise, <see langword="false"/>.</returns>
    private static bool AreSequencesEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual)
    {
        using var expectedEnumerator = expected.GetEnumerator();
        using var actualEnumerator = actual.GetEnumerator();

        while (expectedEnumerator.MoveNext())
        {
            if (!actualEnumerator.MoveNext())
            {
                return false;
            }

            if (!EqualityComparer<T>.Default.Equals(expectedEnumerator.Current, actualEnumerator.Current))
            {
                return false;
            }
        }

        return !actualEnumerator.MoveNext();
    }

    /// <summary>
    /// Determines whether a sequence contains a value.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="expected">The expected value.</param>
    /// <param name="collection">The collection to inspect.</param>
    /// <returns><see langword="true"/> when the collection contains the value; otherwise, <see langword="false"/>.</returns>
    private static bool ContainsValue<T>(T expected, IEnumerable<T> collection)
    {
        foreach (var item in collection)
        {
            if (EqualityComparer<T>.Default.Equals(expected, item))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Formats a sequence for assertion failure messages.
    /// </summary>
    /// <param name="enumerable">The sequence to format.</param>
    /// <returns>The formatted sequence.</returns>
    private static string FormatEnumerable(IEnumerable enumerable)
    {
        var values = new List<string>();

        foreach (var item in enumerable)
        {
            values.Add(item?.ToString() ?? "<null>");
        }

        return "[" + string.Join(", ", values) + "]";
    }
}
