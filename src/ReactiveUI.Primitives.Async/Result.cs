// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Represents the outcome of an operation, indicating success or failure and providing error details when applicable.</summary>
/// <remarks>The <see cref="Result"/> struct is used to encapsulate the result of an operation, including whether
/// it succeeded and, if not, the exception that caused the failure. Use the <see cref="Success"/> property for
/// successful results and <see cref="Failure(Exception)"/> to create failed results. The <see cref="IsSuccess"/> and
/// <see cref="IsFailure"/> properties allow callers to check the operation's status before accessing error information
/// or propagating exceptions. This struct is immutable and thread-safe.</remarks>
[System.Diagnostics.DebuggerDisplay("IsSuccess = {IsSuccess}, Exception = {Exception}")]
public readonly record struct Result
{
    /// <summary>Initializes a new instance of the <see cref="Result"/> struct. Initializes a new instance of the Result class with the specified exception.</summary>
    /// <param name="exception">The exception that represents the error condition for this result. Cannot be null.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="exception"/> is null.</exception>
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
    /// <remarks>If <see langword="false"/>, the <c>Exception</c> property is guaranteed to be non-null,
    /// providing details about the failure.</remarks>
    [MemberNotNullWhen(false, nameof(Exception))]
    public bool IsSuccess => Exception is null;

    /// <summary>Gets a value indicating whether the operation has failed.</summary>
    /// <remarks>When <see langword="true"/>, the <c>Exception</c> property is guaranteed to be non-null. Use
    /// this property to check for failure before accessing error details.</remarks>
    [MemberNotNullWhen(true, nameof(Exception))]
    public bool IsFailure => Exception is not null;

    /// <summary>Creates a failed result that encapsulates the specified exception.</summary>
    /// <param name="exception">The exception that describes the failure. Cannot be null.</param>
    /// <returns>A result representing a failure, containing the provided exception.</returns>
    public static Result Failure(Exception exception) => new(exception);

    /// <summary>Throws the associated exception if the result represents a failure.</summary>
    /// <remarks>Excluded from coverage: the unreachable sequence point after <see cref="ExceptionDispatchInfo"/> rethrow cannot be credited by cobertura.</remarks>
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
    /// <remarks>This method provides a concise textual representation of the operation's outcome, which can
    /// be useful for logging or debugging purposes.</remarks>
    /// <returns>A string indicating "Success" if the operation was successful; otherwise, a string in the format
    /// "Failure{exception message}" containing the associated exception message.</returns>
    public override string ToString() => IsSuccess ? "Success" : $"Failure{{{Exception.Message}}}";
}
