// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Focused coverage for the Rx-style on-error resume sequence coordinator.</summary>
public sealed class OnErrorResumeNextSignalTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>Verifies enumerable creation failures are reported to the observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextReportsEnumeratorCreationFailure()
    {
        InvalidOperationException expected = new("enumerator");
        Exception? error = null;
        using var subscription = Signal.OnErrorResumeNext(new ThrowingEnumerable<int>(expected))
            .Subscribe(static _ => { }, captured => error = captured);

        await Assert.That(error).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies a null enumerator is treated as an empty source list.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextCompletesWhenEnumeratorIsNull()
    {
        var completed = 0;
        using var subscription = Signal.OnErrorResumeNext(new NullEnumeratorEnumerable<int>(returnsNull: true))
            .Subscribe(static _ => { }, error => throw error, () => completed++);

        await Assert.That(completed).IsEqualTo(One);
    }

    /// <summary>Verifies a null source inside the list is reported as an invalid sequence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextReportsNullSourceEntry()
    {
        List<int> values = [];
        Exception? error = null;
        List<IObservable<int>> sources = [Signal.Emit(One), null!];
        using var subscription = Signal.OnErrorResumeNext(sources)
            .Subscribe(values.Add, captured => error = captured);

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(error).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Enumerable that throws when asked for an enumerator.</summary>
    /// <param name="error">The error to throw.</param>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class ThrowingEnumerable<T>(Exception error) : IEnumerable<IObservable<T>>
    {
        /// <inheritdoc/>
        public IEnumerator<IObservable<T>> GetEnumerator() => throw error;

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Enumerable that returns a null enumerator to cover the defensive null path.</summary>
    /// <param name="returnsNull">Whether <see cref="IEnumerable{T}.GetEnumerator"/> returns null.</param>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class NullEnumeratorEnumerable<T>(bool returnsNull) : IEnumerable<IObservable<T>>
    {
        /// <inheritdoc/>
        public IEnumerator<IObservable<T>> GetEnumerator() => returnsNull ? null! : throw new InvalidOperationException();

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
