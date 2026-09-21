// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Windowing operators that hand back a signal per window.</summary>
public static partial class LinqExtensions
{
    /// <summary>Windowing operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the source values.</typeparam>
    /// <param name="source">The source sequence.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Splits the source into windows of a fixed number of values.</summary>
        /// <param name="count">The number of values in each window.</param>
        /// <returns>A sequence of window signals; the last window is shorter when the source ends mid-window.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice(int count) => new SliceCountSignal<T>(source, count, count);

        /// <summary>Splits the source into windows of a fixed number of values, opening a new window every <paramref name="skip"/> values.</summary>
        /// <param name="count">The number of values in each window.</param>
        /// <param name="skip">The number of values between the starts of consecutive windows.</param>
        /// <returns>A sequence of window signals; windows overlap when <paramref name="skip"/> is smaller than <paramref name="count"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> or <paramref name="skip"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice(int count, int skip) => new SliceCountSignal<T>(source, count, skip);

        /// <summary>Splits the source into consecutive windows of a fixed duration.</summary>
        /// <param name="timeSpan">The duration of each window.</param>
        /// <returns>A sequence of window signals.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice(TimeSpan timeSpan) =>
            new SliceTimeSignal<T>(source, timeSpan, timeSpan, Sequencer.Default);

        /// <summary>Splits the source into consecutive windows of a fixed duration on a sequencer.</summary>
        /// <param name="timeSpan">The duration of each window.</param>
        /// <param name="sequencer">The sequencer that schedules the window timer.</param>
        /// <returns>A sequence of window signals.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice(TimeSpan timeSpan, ISequencer sequencer) =>
            new SliceTimeSignal<T>(source, timeSpan, timeSpan, sequencer);

        /// <summary>Splits the source into windows of a fixed duration, opening a new window every <paramref name="timeShift"/>.</summary>
        /// <param name="timeSpan">The duration of each window.</param>
        /// <param name="timeShift">The time between the starts of consecutive windows.</param>
        /// <returns>A sequence of window signals; windows overlap when <paramref name="timeShift"/> is shorter than <paramref name="timeSpan"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="timeShift"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice(TimeSpan timeSpan, TimeSpan timeShift) =>
            new SliceTimeSignal<T>(source, timeSpan, timeShift, Sequencer.Default);

        /// <summary>Splits the source into windows of a fixed duration, opening a new window every <paramref name="timeShift"/>, on a sequencer.</summary>
        /// <param name="timeSpan">The duration of each window.</param>
        /// <param name="timeShift">The time between the starts of consecutive windows.</param>
        /// <param name="sequencer">The sequencer that schedules the window timer.</param>
        /// <returns>A sequence of window signals; windows overlap when <paramref name="timeShift"/> is shorter than <paramref name="timeSpan"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="timeShift"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice(TimeSpan timeSpan, TimeSpan timeShift, ISequencer sequencer) =>
            new SliceTimeSignal<T>(source, timeSpan, timeShift, sequencer);

        /// <summary>Splits the source into windows that end after a duration or a number of values, whichever comes first.</summary>
        /// <param name="timeSpan">The maximum duration of each window.</param>
        /// <param name="count">The maximum number of values in each window.</param>
        /// <returns>A sequence of window signals; each new window restarts the duration.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="count"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice(TimeSpan timeSpan, int count) =>
            new SliceTimeCountSignal<T>(source, timeSpan, count, Sequencer.Default);

        /// <summary>Splits the source into windows that end after a duration or a number of values, whichever comes first, on a sequencer.</summary>
        /// <param name="timeSpan">The maximum duration of each window.</param>
        /// <param name="count">The maximum number of values in each window.</param>
        /// <param name="sequencer">The sequencer that schedules the window timer.</param>
        /// <returns>A sequence of window signals; each new window restarts the duration.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="count"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice(TimeSpan timeSpan, int count, ISequencer sequencer) =>
            new SliceTimeCountSignal<T>(source, timeSpan, count, sequencer);

        /// <summary>Splits the source into windows, starting the next window each time a boundary signal emits.</summary>
        /// <typeparam name="TBoundary">The value type of the boundary signal.</typeparam>
        /// <param name="boundaries">The signal whose emissions start the next window.</param>
        /// <returns>A sequence of window signals; the sequence completes when the boundary signal completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="boundaries"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice<TBoundary>(IObservable<TBoundary> boundaries) =>
            new SliceBoundarySignal<T, TBoundary>(source, boundaries);

        /// <summary>Splits the source into consecutive windows, each ended by the signal the selector supplies for it.</summary>
        /// <typeparam name="TClosing">The value type of the closing signals.</typeparam>
        /// <param name="closingSelector">Supplies the signal that ends the window that has just opened.</param>
        /// <returns>A sequence of window signals; a window ends when its closing signal emits or completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="closingSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice<TClosing>(Func<IObservable<TClosing>> closingSelector) =>
            new SliceClosingSignal<T, TClosing>(source, closingSelector);

        /// <summary>Opens a window each time an opening signal emits and ends it with the signal the selector supplies for it.</summary>
        /// <typeparam name="TOpening">The value type of the opening signal.</typeparam>
        /// <typeparam name="TClosing">The value type of the closing signals.</typeparam>
        /// <param name="openings">The signal whose emissions open a window.</param>
        /// <param name="closingSelector">Supplies the signal that ends the window an opening started.</param>
        /// <returns>A sequence of window signals; windows can overlap.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="openings"/> or <paramref name="closingSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Slice<TOpening, TClosing>(
            IObservable<TOpening> openings,
            Func<TOpening, IObservable<TClosing>> closingSelector) =>
            new SliceOpeningSignal<T, TOpening, TClosing>(source, openings, closingSelector);
    }
}
