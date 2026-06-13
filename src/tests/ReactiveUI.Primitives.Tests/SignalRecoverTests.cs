// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Signal"/> recover and catch sequence contracts.</summary>
public class SignalRecoverTests
{
    /// <summary>The first expected value.</summary>
    private const int First = 1;

    /// <summary>The second expected value.</summary>
    private const int Second = 2;

    /// <summary>Reused first-error message.</summary>
    private const string FirstMessage = "first";

    /// <summary>Expected values produced by the catch params overload.</summary>
    private static readonly int[] CatchRecoveryExpected = [First, Second];

    /// <summary>Covers catch sequence recovery, final error, empty completion, null source, and enumerator failure branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CatchParamsFactoryCoversRecoveryAndFailureBranches()
    {
        List<int> recovered = [];
        Signal.Recover(
            Signal.Fail<int>(new InvalidOperationException(FirstMessage)),
            Signal.FromEnumerable(CatchRecoveryExpected),
            Signal.Fail<int>(new InvalidOperationException("unused"))).Subscribe(recovered.Add);
        await Assert.That(recovered.SequenceEqual(CatchRecoveryExpected)).IsTrue();
        List<Exception> finalErrors = [];
        InvalidOperationException finalError = new("last");
        Signal.Recover(
            Signal.Fail<int>(new InvalidOperationException(FirstMessage)),
            Signal.Fail<int>(finalError)).Subscribe(_ => { }, finalErrors.Add, () => { });
        await Assert.That(finalErrors[0]).IsSameReferenceAs(finalError);
        var completed = 0;
        var completedSubscription = Signal.Recover<int>().Subscribe(_ => { }, ex => throw ex, () => completed++);
        completedSubscription.Dispose();
        completedSubscription.Dispose();
        await Assert.That(completed).IsEqualTo(1);
        var activeSubscription = Signal.Recover(Signal.Silent<int>()).Subscribe(_ => { }, ex => throw ex, () => { });
        activeSubscription.Dispose();
        List<Exception> nullSourceErrors = [];
        Signal.Recover(new IObservable<int>?[] { null! }!).Subscribe(_ => { }, nullSourceErrors.Add, () => { });
        await Assert.That(nullSourceErrors[0] is InvalidOperationException).IsTrue();
        List<Exception> moveNextErrors = [];
        InvalidOperationException moveNextError = new("move-next");
        new ThrowingMoveNextEnumerable<IObservable<int>>(moveNextError).Recover()
            .Subscribe(_ => { }, moveNextErrors.Add, () => { });
        await Assert.That(moveNextErrors[0]).IsSameReferenceAs(moveNextError);
        InvalidOperationException getEnumeratorError = new("enumerator");
        Assert.Throws<InvalidOperationException>(() => new ThrowingEnumerable<IObservable<int>>(getEnumeratorError)
            .Recover()
            .Subscribe(_ => { }, _ => { }, () => { }));
    }

    /// <summary>Enumerable test double whose enumerator throws from <see cref="IEnumerator.MoveNext"/>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ThrowingMoveNextEnumerable<T> : IEnumerable<T>
    {
        /// <summary>Error thrown by the enumerator.</summary>
        private readonly Exception _error;

        /// <summary>Initializes a new instance of the <see cref="ThrowingMoveNextEnumerable{T}"/> class.</summary>
        /// <param name="error">The error to throw.</param>
        public ThrowingMoveNextEnumerable(Exception error) => _error = error;

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator() => new ThrowingMoveNextEnumerator(_error);

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>Enumerator test double that fails on movement.</summary>
        private sealed class ThrowingMoveNextEnumerator : IEnumerator<T>
        {
            /// <summary>Error thrown by movement.</summary>
            private readonly Exception _error;

            /// <summary>Initializes a new instance of the <see cref="ThrowingMoveNextEnumerator"/> class.</summary>
            /// <param name="error">The error to throw.</param>
            public ThrowingMoveNextEnumerator(Exception error) => _error = error;

            /// <inheritdoc/>
            public T Current => default!;

            /// <inheritdoc/>
            object IEnumerator.Current => Current!;

            /// <inheritdoc/>
            public bool MoveNext() => throw _error;

            /// <inheritdoc/>
            public void Reset() => throw new NotSupportedException();

            /// <inheritdoc/>
            public void Dispose()
            {
            }
        }
    }

    /// <summary>Enumerable test double that throws when enumeration starts.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ThrowingEnumerable<T> : IEnumerable<T>
    {
        /// <summary>Error thrown by enumeration.</summary>
        private readonly Exception _error;

        /// <summary>Initializes a new instance of the <see cref="ThrowingEnumerable{T}"/> class.</summary>
        /// <param name="error">The error to throw.</param>
        public ThrowingEnumerable(Exception error) => _error = error;

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator() => throw _error;

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
