// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Coordinator helpers for multi-source combine-latest signal operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Adapts a typed source to the shared multi-source coordinator.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    private interface ICombineLatestSource<TResult>
    {
        /// <summary>Subscribes the coordinator to the source.</summary>
        /// <param name="coordinator">The shared subscription coordinator.</param>
        /// <param name="index">The source index.</param>
        /// <returns>The source subscription.</returns>
        IDisposable Subscribe(CombineLatestCoordinator<TResult> coordinator, int index);
    }

    /// <summary>Observable implementation for generated multi-source combine-latest overloads.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    private sealed class CombineLatestSignal<TResult> : IObservable<TResult>
    {
        /// <summary>The first source slot.</summary>
        private const int FirstSourceIndex = 0;

        /// <summary>The second source slot.</summary>
        private const int SecondSourceIndex = 1;

        /// <summary>The third source slot.</summary>
        private const int ThirdSourceIndex = 2;

        /// <summary>The fourth source slot.</summary>
        private const int FourthSourceIndex = 3;

        /// <summary>The fifth source slot.</summary>
        private const int FifthSourceIndex = 4;

        /// <summary>The sixth source slot.</summary>
        private const int SixthSourceIndex = 5;

        /// <summary>The seventh source slot.</summary>
        private const int SeventhSourceIndex = 6;

        /// <summary>The eighth source slot.</summary>
        private const int EighthSourceIndex = 7;

        /// <summary>The ninth source slot.</summary>
        private const int NinthSourceIndex = 8;

        /// <summary>The tenth source slot.</summary>
        private const int TenthSourceIndex = 9;

        /// <summary>The eleventh source slot.</summary>
        private const int EleventhSourceIndex = 10;

        /// <summary>The twelfth source slot.</summary>
        private const int TwelfthSourceIndex = 11;

        /// <summary>The thirteenth source slot.</summary>
        private const int ThirteenthSourceIndex = 12;

        /// <summary>The fourteenth source slot.</summary>
        private const int FourteenthSourceIndex = 13;

        /// <summary>The fifteenth source slot.</summary>
        private const int FifteenthSourceIndex = 14;

        /// <summary>The sixteenth source slot.</summary>
        private const int SixteenthSourceIndex = 15;

        /// <summary>The typed source adapters.</summary>
        private readonly ICombineLatestSource<TResult>[] _sources;

        /// <summary>The selector applied once every source has produced a value.</summary>
        private readonly Func<object?[], TResult> _selector;

        /// <summary>Initializes a new instance of the <see cref="CombineLatestSignal{TResult}"/> class.</summary>
        /// <param name="selector">The array-based selector wrapper.</param>
        /// <param name="sources">The typed source adapters.</param>
        internal CombineLatestSignal(Func<object?[], TResult> selector, ICombineLatestSource<TResult>[] sources)
        {
            _selector = selector;
            _sources = sources;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new CombineLatestCoordinator<TResult>(observer, _selector, _sources.Length).Run(_sources);
        }

        /// <summary>Creates an arity-3 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            Func<T1, T2, T3, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3));

        /// <summary>Creates an arity-4 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            Func<T1, T2, T3, T4, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4));

        /// <summary>Creates an arity-5 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            Func<T1, T2, T3, T4, T5, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5));

        /// <summary>Creates an arity-6 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            Func<T1, T2, T3, T4, T5, T6, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6));

        /// <summary>Creates an arity-7 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            Func<T1, T2, T3, T4, T5, T6, T7, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7));

        /// <summary>Creates an arity-8 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex),
                    Value<T8>(values, EighthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7),
                CreateSource(source8));

        /// <summary>Creates an arity-9 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex),
                    Value<T8>(values, EighthSourceIndex),
                    Value<T9>(values, NinthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7),
                CreateSource(source8),
                CreateSource(source9));

        /// <summary>Creates an arity-10 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex),
                    Value<T8>(values, EighthSourceIndex),
                    Value<T9>(values, NinthSourceIndex),
                    Value<T10>(values, TenthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7),
                CreateSource(source8),
                CreateSource(source9),
                CreateSource(source10));

        /// <summary>Creates an arity-11 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex),
                    Value<T8>(values, EighthSourceIndex),
                    Value<T9>(values, NinthSourceIndex),
                    Value<T10>(values, TenthSourceIndex),
                    Value<T11>(values, EleventhSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7),
                CreateSource(source8),
                CreateSource(source9),
                CreateSource(source10),
                CreateSource(source11));

        /// <summary>Creates an arity-12 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex),
                    Value<T8>(values, EighthSourceIndex),
                    Value<T9>(values, NinthSourceIndex),
                    Value<T10>(values, TenthSourceIndex),
                    Value<T11>(values, EleventhSourceIndex),
                    Value<T12>(values, TwelfthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7),
                CreateSource(source8),
                CreateSource(source9),
                CreateSource(source10),
                CreateSource(source11),
                CreateSource(source12));

        /// <summary>Creates an arity-13 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <typeparam name="T13">The thirteenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="source13">The thirteenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            IObservable<T13> source13,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex),
                    Value<T8>(values, EighthSourceIndex),
                    Value<T9>(values, NinthSourceIndex),
                    Value<T10>(values, TenthSourceIndex),
                    Value<T11>(values, EleventhSourceIndex),
                    Value<T12>(values, TwelfthSourceIndex),
                    Value<T13>(values, ThirteenthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7),
                CreateSource(source8),
                CreateSource(source9),
                CreateSource(source10),
                CreateSource(source11),
                CreateSource(source12),
                CreateSource(source13));

        /// <summary>Creates an arity-14 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <typeparam name="T13">The thirteenth source element type.</typeparam>
        /// <typeparam name="T14">The fourteenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="source13">The thirteenth source observable.</param>
        /// <param name="source14">The fourteenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            IObservable<T13> source13,
            IObservable<T14> source14,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex),
                    Value<T8>(values, EighthSourceIndex),
                    Value<T9>(values, NinthSourceIndex),
                    Value<T10>(values, TenthSourceIndex),
                    Value<T11>(values, EleventhSourceIndex),
                    Value<T12>(values, TwelfthSourceIndex),
                    Value<T13>(values, ThirteenthSourceIndex),
                    Value<T14>(values, FourteenthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7),
                CreateSource(source8),
                CreateSource(source9),
                CreateSource(source10),
                CreateSource(source11),
                CreateSource(source12),
                CreateSource(source13),
                CreateSource(source14));

        /// <summary>Creates an arity-15 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <typeparam name="T13">The thirteenth source element type.</typeparam>
        /// <typeparam name="T14">The fourteenth source element type.</typeparam>
        /// <typeparam name="T15">The fifteenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="source13">The thirteenth source observable.</param>
        /// <param name="source14">The fourteenth source observable.</param>
        /// <param name="source15">The fifteenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            IObservable<T13> source13,
            IObservable<T14> source14,
            IObservable<T15> source15,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex),
                    Value<T8>(values, EighthSourceIndex),
                    Value<T9>(values, NinthSourceIndex),
                    Value<T10>(values, TenthSourceIndex),
                    Value<T11>(values, EleventhSourceIndex),
                    Value<T12>(values, TwelfthSourceIndex),
                    Value<T13>(values, ThirteenthSourceIndex),
                    Value<T14>(values, FourteenthSourceIndex),
                    Value<T15>(values, FifteenthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7),
                CreateSource(source8),
                CreateSource(source9),
                CreateSource(source10),
                CreateSource(source11),
                CreateSource(source12),
                CreateSource(source13),
                CreateSource(source14),
                CreateSource(source15));

        /// <summary>Creates an arity-16 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <typeparam name="T13">The thirteenth source element type.</typeparam>
        /// <typeparam name="T14">The fourteenth source element type.</typeparam>
        /// <typeparam name="T15">The fifteenth source element type.</typeparam>
        /// <typeparam name="T16">The sixteenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="source13">The thirteenth source observable.</param>
        /// <param name="source14">The fourteenth source observable.</param>
        /// <param name="source15">The fifteenth source observable.</param>
        /// <param name="source16">The sixteenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - expected for arity-N CombineLatest factory surface.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            IObservable<T13> source13,
            IObservable<T14> source14,
            IObservable<T15> source15,
            IObservable<T16> source16,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> selector) =>
            Build(
                values => selector(
                    Value<T1>(values, FirstSourceIndex),
                    Value<T2>(values, SecondSourceIndex),
                    Value<T3>(values, ThirdSourceIndex),
                    Value<T4>(values, FourthSourceIndex),
                    Value<T5>(values, FifthSourceIndex),
                    Value<T6>(values, SixthSourceIndex),
                    Value<T7>(values, SeventhSourceIndex),
                    Value<T8>(values, EighthSourceIndex),
                    Value<T9>(values, NinthSourceIndex),
                    Value<T10>(values, TenthSourceIndex),
                    Value<T11>(values, EleventhSourceIndex),
                    Value<T12>(values, TwelfthSourceIndex),
                    Value<T13>(values, ThirteenthSourceIndex),
                    Value<T14>(values, FourteenthSourceIndex),
                    Value<T15>(values, FifteenthSourceIndex),
                    Value<T16>(values, SixteenthSourceIndex)),
                CreateSource(source),
                CreateSource(source2),
                CreateSource(source3),
                CreateSource(source4),
                CreateSource(source5),
                CreateSource(source6),
                CreateSource(source7),
                CreateSource(source8),
                CreateSource(source9),
                CreateSource(source10),
                CreateSource(source11),
                CreateSource(source12),
                CreateSource(source13),
                CreateSource(source14),
                CreateSource(source15),
                CreateSource(source16));

        /// <summary>Creates the typed multi-source signal.</summary>
        /// <param name="selector">The array-based selector wrapper.</param>
        /// <param name="sources">The typed source adapters.</param>
        /// <returns>The observable that coordinates the sources.</returns>
        private static CombineLatestSignal<TResult> Build(
            Func<object?[], TResult> selector,
            params ICombineLatestSource<TResult>[] sources) =>
            new(selector, sources);

        /// <summary>Creates a typed source adapter.</summary>
        /// <typeparam name="T">The source element type.</typeparam>
        /// <param name="observable">The source observable.</param>
        /// <returns>The source adapter.</returns>
        private static CombineLatestSource<TResult, T> CreateSource<T>(IObservable<T> observable) =>
            new(observable);

        /// <summary>Reads a typed value from the latest-value array.</summary>
        /// <typeparam name="T">The value type stored at the slot.</typeparam>
        /// <param name="values">The latest-value array.</param>
        /// <param name="index">The source index.</param>
        /// <returns>The typed value.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S4018:Generic methods should provide type parameters",
            Justification = "The caller supplies the source slot type when casting from the shared latest-value array.")]
        private static T Value<T>(object?[] values, int index) => (T)values[index]!;
    }

    /// <summary>Adapts a typed observable source to the shared coordinator shape.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <typeparam name="T">The source element type.</typeparam>
    /// <param name="source">The source observable.</param>
    private sealed class CombineLatestSource<TResult, T>(IObservable<T> source) : ICombineLatestSource<TResult>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(CombineLatestCoordinator<TResult> coordinator, int index) =>
            source.Subscribe(value => coordinator.OnNext(index, value), coordinator.OnError, () => coordinator.OnCompleted(index));
    }

    /// <summary>Coordinates latest values, completion, and errors for a multi-source combine-latest subscription.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    private sealed class CombineLatestCoordinator<TResult> : IDisposable
    {
        /// <summary>Serializes notifications across all sources.</summary>
        private readonly Lock _gate = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _observer;

        /// <summary>The selector applied once every source has produced a value.</summary>
        private readonly Func<object?[], TResult> _selector;

        /// <summary>The latest value for each source.</summary>
        private readonly object?[] _values;

        /// <summary>Tracks whether each source has produced at least one value.</summary>
        private readonly bool[] _hasValues;

        /// <summary>Tracks whether each source has completed.</summary>
        private readonly bool[] _isDone;

        /// <summary>The active source subscriptions.</summary>
        private readonly MultipleDisposable _subscriptions = [];

        /// <summary>The number of sources still waiting for their first value.</summary>
        private int _missingValues;

        /// <summary>The number of sources that have not completed.</summary>
        private int _remainingCompletions;

        /// <summary>Whether a terminal notification has already been forwarded.</summary>
        private bool _completed;

        /// <summary>Initializes a new instance of the <see cref="CombineLatestCoordinator{TResult}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="selector">The selector applied once every source has produced a value.</param>
        /// <param name="sourceCount">The number of sources being coordinated.</param>
        internal CombineLatestCoordinator(IObserver<TResult> observer, Func<object?[], TResult> selector, int sourceCount)
        {
            _observer = observer;
            _selector = selector;
            _values = new object?[sourceCount];
            _hasValues = new bool[sourceCount];
            _isDone = new bool[sourceCount];
            _missingValues = sourceCount;
            _remainingCompletions = sourceCount;
        }

        /// <inheritdoc/>
        public void Dispose() => _subscriptions.Dispose();

        /// <summary>Subscribes to every source and returns this coordinator as the subscription.</summary>
        /// <param name="sources">The source adapters to subscribe to.</param>
        /// <returns>This coordinator.</returns>
        internal CombineLatestCoordinator<TResult> Run(ICombineLatestSource<TResult>[] sources)
        {
            try
            {
                for (var i = 0; i < sources.Length; i++)
                {
                    _subscriptions.Add(sources[i].Subscribe(this, i));
                }
            }
            catch
            {
                _subscriptions.Dispose();
                throw;
            }

            return this;
        }

        /// <summary>Records a latest source value and emits a projected value once every source has produced one.</summary>
        /// <param name="index">The source index.</param>
        /// <param name="value">The source value.</param>
        internal void OnNext(int index, object? value)
        {
            lock (_gate)
            {
                if (_completed)
                {
                    return;
                }

                _values[index] = value;
                if (!_hasValues[index])
                {
                    _hasValues[index] = true;
                    _missingValues--;
                }

                if (_missingValues == 0)
                {
                    _observer.OnNext(_selector(_values));
                }
            }
        }

        /// <summary>Forwards an error and disposes all source subscriptions.</summary>
        /// <param name="error">The source error.</param>
        internal void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_completed)
                {
                    return;
                }

                _completed = true;
                _observer.OnError(error);
            }

            _subscriptions.Dispose();
        }

        /// <summary>Tracks source completion and completes downstream after every source completes.</summary>
        /// <param name="index">The source index.</param>
        internal void OnCompleted(int index)
        {
            lock (_gate)
            {
                if (_completed || _isDone[index])
                {
                    return;
                }

                _isDone[index] = true;
                _remainingCompletions--;
                if (_remainingCompletions != 0)
                {
                    return;
                }

                _completed = true;
                _observer.OnCompleted();
            }

            _subscriptions.Dispose();
        }
    }
}
