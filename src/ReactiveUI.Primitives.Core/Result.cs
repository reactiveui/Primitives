// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives;

/// <summary>Represents the outcome of an operation, indicating success or failure and carrying the failure exception.</summary>
[System.Diagnostics.DebuggerDisplay("Result: IsSuccess = {IsSuccess}, Exception = {Exception}")]
public readonly record struct Result
{
    /// <summary>Initializes a new instance of the <see cref="Result"/> struct representing a failure.</summary>
    /// <param name="exception">The exception that caused the failure.</param>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    public Result(Exception exception)
    {
        ArgumentExceptionHelper.ThrowIfNull(exception);

        Exception = exception;
    }

    /// <summary>Gets a predefined result instance that indicates a successful operation.</summary>
    public static Result Success => default;

    /// <summary>Gets the exception that caused the current operation to fail, if any.</summary>
    public Exception? Exception { get; }

    /// <summary>Gets a value indicating whether the operation completed successfully without an exception.</summary>
    /// <remarks>When <see langword="false"/>, <see cref="Exception"/> is non-null.</remarks>
    [MemberNotNullWhen(false, nameof(Exception))]
    public bool IsSuccess => Exception is null;

    /// <summary>Gets a value indicating whether the operation has failed.</summary>
    /// <remarks>When <see langword="true"/>, <see cref="Exception"/> is non-null.</remarks>
    [MemberNotNullWhen(true, nameof(Exception))]
    public bool IsFailure => Exception is not null;

    /// <summary>Creates a failed result that encapsulates the specified exception.</summary>
    /// <param name="exception">The exception that describes the failure.</param>
    /// <returns>A result representing a failure, containing the supplied exception.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    public static Result Failure(Exception exception) => new(exception);

    /// <summary>Rethrows the failure exception, preserving its original stack trace; a no-op on success.</summary>
    [ExcludeFromCodeCoverage]
    public void TryThrow()
    {
        var failure = Exception;
        if (failure is null)
        {
            return;
        }

        ExceptionDispatchInfo.Capture(failure).Throw();
    }

    /// <summary>Returns a string that represents the result status of the operation.</summary>
    /// <returns>A string indicating "Success" if the operation was successful; otherwise, a string in the format
    /// "Failure{exception message}" containing the associated exception message.</returns>
    public override string ToString() => IsSuccess ? "Success" : $"Failure{{{Exception.Message}}}";
}
