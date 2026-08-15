// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for combining operators: Merge, Concat, CombineLatest, Zip, Prepend, StartWith.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>Sample integer value 1.</summary>
    private const int SampleValue1 = 1;

    /// <summary>Sample integer value 2.</summary>
    private const int SampleValue2 = 2;

    /// <summary>Sample integer value 3.</summary>
    private const int SampleValue3 = 3;

    /// <summary>Sample integer value 4.</summary>
    private const int SampleValue4 = 4;

    /// <summary>Sample integer value 5.</summary>
    private const int SampleValue5 = 5;

    /// <summary>Sample integer value 10.</summary>
    private const int SampleValue10 = 10;

    /// <summary>Sample integer value 20.</summary>
    private const int SampleValue20 = 20;

    /// <summary>Sample integer value 30.</summary>
    private const int SampleValue30 = 30;

    /// <summary>Sample integer value 100.</summary>
    private const int SampleValue100 = 100;

    /// <summary>Zip pair expected sum 1 + 10.</summary>
    private const int ZipPair11 = 11;

    /// <summary>Zip pair expected sum 3 + 10.</summary>
    private const int ZipPair13 = 13;

    /// <summary>Sentinel value 42.</summary>
    private const int Sentinel42 = 42;

    /// <summary>Sentinel value 99.</summary>
    private const int Sentinel99 = 99;

    /// <summary>Combined sentinel 1 + 10 + 100.</summary>
    private const int Combined111 = 111;

    /// <summary>Range start offset 101.</summary>
    private const int RangeOffset101 = 101;

    /// <summary>Range start offset 103.</summary>
    private const int RangeOffset103 = 103;

    /// <summary>Range start offset 105.</summary>
    private const int RangeOffset105 = 105;

    /// <summary>Common inner-fail error message used by tests.</summary>
    private const string InnerFailMessage = "inner fail";

    /// <summary>Common inner-warning error message used by tests.</summary>
    private const string InnerWarningMessage = "inner warning";

    /// <summary>Common first-fail error message used by tests.</summary>
    private const string FirstFailMessage = "first fail";

    /// <summary>Common subscribe-boom error message used by tests.</summary>
    private const string SubscribeBoomMessage = "subscribe boom";

    /// <summary>Common late-error message used by tests.</summary>
    private const string LateErrorMessage = "late error";

    /// <summary>Common outer-warning error message used by tests.</summary>
    private const string OuterWarningMessage = "outer warning";

    /// <summary>Literal &quot;first&quot;.</summary>
    private const string FirstLiteral = "first";

    /// <summary>Literal &quot;second&quot;.</summary>
    private const string SecondLiteral = "second";

    /// <summary>Zip-test strings &quot;a&quot;, &quot;b&quot;, &quot;c&quot;.</summary>
    private static readonly string[] ZipStringsAbc = ["a", "b", "c"];

    /// <summary>Zip-test strings &quot;x&quot;, &quot;y&quot;.</summary>
    private static readonly string[] ZipStringsXy = ["x", "y"];

#if NET9_0_OR_GREATER
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly Lock _gate = new();
#else
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly object _gate = new();
#endif

    /// <summary>A trackable async disposable resource for verifying disposal in Using tests.</summary>
    private sealed class TrackingAsyncDisposable : IAsyncDisposable
    {
        /// <summary>Gets a value indicating whether this resource has been disposed.</summary>
        public bool IsDisposed { get; private set; }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>An async disposable that throws on disposal, used to test error handling during cleanup.</summary>
    private sealed class ThrowingDisposable : IAsyncDisposable
    {
        /// <summary>Throws an <see cref="InvalidOperationException"/> when disposal is attempted.</summary>
        /// <returns>Never returns normally.</returns>
        /// <exception cref="InvalidOperationException">Always, because the throwing disposal is the behaviour under test.</exception>
        [SuppressMessage("Maintainability", "SST1485:Members that must not throw should not throw", Justification = "The throwing disposal is the behaviour under test.")]
        public ValueTask DisposeAsync() => throw new InvalidOperationException("dispose boom");
    }

    /// <summary>An enumerable that throws during enumeration, used to trigger the error path in MergeEnumerable BeginSubscribing.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class ThrowingEnumerable<T> : IEnumerable<IObservableAsync<T>>
    {
        /// <inheritdoc/>
        public IEnumerator<IObservableAsync<T>> GetEnumerator() =>
            throw new InvalidOperationException("enumerable boom");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// An enumerable whose enumerator throws on both <see cref="System.Collections.IEnumerator.MoveNext"/>
    /// and <see cref="IDisposable.Dispose"/>, used to exercise the catch block in
    /// <c>ChainEnumerableSignal.SubscribeAsyncCore</c>.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class MoveNextAndDisposeThrowingEnumerable<T> : IEnumerable<IObservableAsync<T>>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<IObservableAsync<T>> GetEnumerator() => new ThrowingEnumerator();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>An enumerator that throws on both <see cref="MoveNext"/> and <see cref="Dispose"/>.</summary>
        private sealed class ThrowingEnumerator : IEnumerator<IObservableAsync<T>>
        {
            /// <inheritdoc/>
            public IObservableAsync<T> Current => null!;

            /// <inheritdoc/>
            object System.Collections.IEnumerator.Current => Current!;

            /// <inheritdoc/>
            public bool MoveNext() => throw new InvalidOperationException("enumerator MoveNext boom");

            /// <inheritdoc/>
            public void Reset() => throw new NotSupportedException();

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => ThrowDisposeBoom();

            /// <summary>Throws an <see cref="ObjectDisposedException"/>; extracted so the analyzer doesn't flag Dispose itself.</summary>
            /// <exception cref="ObjectDisposedException">Always, so disposal failure can be exercised.</exception>
            private static void ThrowDisposeBoom() => throw new ObjectDisposedException("enumerator Dispose boom");
        }
    }
}
