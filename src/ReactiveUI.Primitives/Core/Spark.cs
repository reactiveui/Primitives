// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>
/// Provides a set of static methods for constructing spark.
/// </summary>
public static class Spark
{
    /// <summary>
    /// Creates an object that represents an OnNext spark to an observer.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the elements received by the observer.
    /// Upon dematerialization of the spark into an observable sequence, this type is used as the element type for the sequence.
    /// </typeparam>
    /// <param name="value">The value contained in the spark.</param>
    /// <returns>The OnNext spark containing the value.</returns>
    public static Spark<T> CreateOnNext<T>(T value) => new Spark<T>.OnNextSpark(value);

    /// <summary>
    /// Creates an object that represents an OnError spark to an observer.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the elements received by the observer.
    /// Upon dematerialization of the spark into an observable sequence, this type is used as the element type for the sequence.
    /// </typeparam>
    /// <param name="error">The exception contained in the spark.</param>
    /// <returns>The OnError spark containing the exception.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is null.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Sonar Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter determines the returned spark value type.")]
    public static Spark<T> CreateOnError<T>(Exception error)
    {
        if (error == null)
        {
            throw new ArgumentNullException(nameof(error));
        }

        return new Spark<T>.OnErrorSpark(error);
    }

    /// <summary>
    /// Creates an object that represents an OnCompleted spark to an observer.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the elements received by the observer.
    /// Upon dematerialization of the spark into an observable sequence, this type is used as the element type for the sequence.
    /// </typeparam>
    /// <returns>The OnCompleted spark.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Sonar Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter determines the cached completed spark value type.")]
    public static Spark<T> CreateOnCompleted<T>() => CompletedSparkCache<T>.Instance;

    /// <summary>
    /// Holds the cached completed spark for a value type.
    /// </summary>
    /// <typeparam name="T">The cached spark value type.</typeparam>
    private static class CompletedSparkCache<T>
    {
        /// <summary>
        /// Gets the cached completed spark.
        /// </summary>
        public static readonly Spark<T> Instance = new Spark<T>.OnCompletedSpark();
    }
}
