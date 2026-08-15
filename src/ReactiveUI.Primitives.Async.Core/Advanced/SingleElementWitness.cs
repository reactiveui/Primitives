// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Shared observer used by both <c>SingleAsync</c> and <c>SingleOrDefaultAsync</c>. The two operator
/// surfaces previously held near-identical observer classes; the only behavioural difference is
/// whether an empty sequence throws or returns a caller-supplied default. That difference is now a
/// single flag on this type, so the OnNext / OnErrorResume / OnCompleted bodies live in one place.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="predicate">An optional predicate to filter elements; <c>null</c> matches all elements.</param>
/// <param name="requireExactlyOne">
/// When <c>true</c> (the <c>SingleAsync</c> shape), an empty sequence completes the result task with
/// an <see cref="InvalidOperationException"/>. When <c>false</c> (the <c>SingleOrDefaultAsync</c>
/// shape), an empty sequence resolves the result task with <paramref name="defaultValue"/>.
/// </param>
/// <param name="defaultValue">The value to return on empty when <paramref name="requireExactlyOne"/> is <c>false</c>.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("HasValue = {_hasValue}, Value = {_value}")]
public sealed class SingleElementWitness<T>(
    Func<T, bool>? predicate,
    bool requireExactlyOne,
    T? defaultValue,
    CancellationToken cancellationToken) : TaskResultWitnessAsyncBase<T, T?>(cancellationToken)
{
    /// <summary>A value indicating whether a matching element has been found.</summary>
    private bool _hasValue;

    /// <summary>The single matching element, or the default value if no match has been found.</summary>
    private T? _value = defaultValue;

    /// <inheritdoc/>
    protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        if (predicate is not null && !predicate(value))
        {
            return;
        }

        if (_hasValue)
        {
            var message = predicate is null
                ? "Sequence contains more than one element."
                : "Sequence contains more than one matching element.";
            await SetExceptionAndDisposeAsync(new InvalidOperationException(message)).ConfigureAwait(false);
            return;
        }

        _hasValue = true;
        _value = value;
    }

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        SetExceptionAndDisposeAsync(error);

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result)
    {
        if (!result.IsSuccess)
        {
            return SetExceptionAndDisposeAsync(result.Exception);
        }

        if (!_hasValue && requireExactlyOne)
        {
            var message = predicate is null
                ? "Sequence contains no elements."
                : "Sequence contains no matching elements.";
            return SetExceptionAndDisposeAsync(new InvalidOperationException(message));
        }

        return SetResultAndDisposeAsync(_value);
    }
}
