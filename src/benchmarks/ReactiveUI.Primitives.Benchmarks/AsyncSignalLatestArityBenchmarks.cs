// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Signals;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsResult = ReactiveUI.Extensions.Async.Result;
using ExtensionsSource = ReactiveUI.Extensions.Async.IObservableAsync<int>;
using ExtensionsSubject = ReactiveUI.Extensions.Async.Subjects.ISubjectAsync<int>;
using ExtensionsSubjectAsync = ReactiveUI.Extensions.Async.Subjects.SubjectAsync;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures combining the latest values of a fixed number of async sources, through the typed arity overloads and the enumerable overload.</summary>
[MemoryDiagnoser]
public class AsyncSignalLatestArityBenchmarks
{
    /// <summary>The number of values pushed into the first source after every source has a value.</summary>
    private const int PushCount = 32;

    /// <summary>The smallest source count with a typed overload; the combiner tables start at this arity.</summary>
    private const int MinimumSourceCount = 2;

    /// <summary>The largest source count with a typed overload.</summary>
    private const int MaximumSourceCount = 16;

    /// <summary>Combines primitives sources through the typed overload for each arity, indexed from <see cref="MinimumSourceCount"/>.</summary>
    private static readonly Func<ISignalAsync<int>[], IObservableAsync<int>>[] TypedCombiners =
    [
        static s => s[0].CombineLatest(s[1], static (a, _) => a),
        static s => s[0].CombineLatest(s[1], s[2], static (a, _, _) => a),
        static s => s[0].CombineLatest(s[1], s[2], s[3], static (a, _, _, _) => a),
        static s => s[0].CombineLatest(s[1], s[2], s[3], s[4], static (a, _, _, _, _) => a),
        static s => s[0].CombineLatest(s[1], s[2], s[3], s[4], s[5], static (a, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(s[1], s[2], s[3], s[4], s[5], s[6], static (a, _, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(s[1], s[2], s[3], s[4], s[5], s[6], s[7], static (a, _, _, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], static (a, _, _, _, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            static (a, _, _, _, _, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            static (a, _, _, _, _, _, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            static (a, _, _, _, _, _, _, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            s[12],
            static (a, _, _, _, _, _, _, _, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            s[12],
            s[13],
            static (a, _, _, _, _, _, _, _, _, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            s[12],
            s[13],
            s[14],
            static (a, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => a),
        static s => s[0].CombineLatest(
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            s[12],
            s[13],
            s[14],
            s[15],
            static (a, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => a),
    ];

    /// <summary>Combines ReactiveUI.Extensions subjects through the typed overload for each arity, indexed from <see cref="MinimumSourceCount"/>.</summary>
    private static readonly Func<ExtensionsSubject[], ExtensionsSource>[] ExtensionsTypedCombiners =
    [
        static s => ExtensionsAsyncObservable.CombineLatest(s[0], s[1], static (a, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(s[0], s[1], s[2], static (a, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(s[0], s[1], s[2], s[3], static (a, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(s[0], s[1], s[2], s[3], s[4], static (a, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(s[0], s[1], s[2], s[3], s[4], s[5], static (a, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(s[0], s[1], s[2], s[3], s[4], s[5], s[6], static (a, _, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], static (a, _, _, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], static (a, _, _, _, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(
            s[0],
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            static (a, _, _, _, _, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(
            s[0],
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            static (a, _, _, _, _, _, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(
            s[0],
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            static (a, _, _, _, _, _, _, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(
            s[0],
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            s[12],
            static (a, _, _, _, _, _, _, _, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(
            s[0],
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            s[12],
            s[13],
            static (a, _, _, _, _, _, _, _, _, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(
            s[0],
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            s[12],
            s[13],
            s[14],
            static (a, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => a),
        static s => ExtensionsAsyncObservable.CombineLatest(
            s[0],
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            s[8],
            s[9],
            s[10],
            s[11],
            s[12],
            s[13],
            s[14],
            s[15],
            static (a, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => a),
    ];

    /// <summary>Gets every source count that has a typed overload.</summary>
    public static IEnumerable<int> SourceCounts => Enumerable.Range(MinimumSourceCount, MaximumSourceCount - MinimumSourceCount + 1);

    /// <summary>Gets or sets the number of combined sources; every arity with a typed overload is measured.</summary>
    [ParamsSource(nameof(SourceCounts))]
    public int SourceCount { get; set; }

    /// <summary>Combines the sources through the typed overload for <see cref="SourceCount"/> and pushes values into the first.</summary>
    /// <returns>The number of combined values observed.</returns>
    [Benchmark(Baseline = true)]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesCombineLatestTypedAsync() => CombineTypedAsync(SourceCount);

    /// <summary>Combines the subjects through the typed ReactiveUI.Extensions overload for <see cref="SourceCount"/> and pushes values into the first.</summary>
    /// <returns>The number of combined values observed.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExtensionsCombineLatestTypedAsync() => CombineExtensionsTypedAsync(SourceCount);

    /// <summary>Combines the sources through the enumerable overload.</summary>
    /// <returns>The number of combined values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesEnumerableCombineLatestAsync()
    {
        var sources = CreateSources(SourceCount);
        IObservableAsync<int>[] observables = [.. sources];
        var combined = observables.CombineLatest(static values => values[0]);
        return await DrivePrimitivesAsync(sources, combined).ConfigureAwait(false);
    }

    /// <summary>Combines the subjects through the ReactiveUI.Extensions enumerable overload.</summary>
    /// <returns>The number of combined values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsEnumerableCombineLatestAsync()
    {
        var subjects = CreateSubjects(SourceCount);
        ExtensionsSource[] observables = [.. subjects];
        return await DriveExtensionsAsync(subjects, ExtensionsAsyncObservable.CombineLatest(observables))
            .ConfigureAwait(false);
    }

    /// <summary>Creates the sources for a typed arity, combines them and drives the combined stream.</summary>
    /// <param name="sourceCount">The number of combined sources.</param>
    /// <returns>The number of combined values observed.</returns>
    private static async Task<int> CombineTypedAsync(int sourceCount)
    {
        var sources = CreateSources(sourceCount);
        var combined = TypedCombiners[sourceCount - MinimumSourceCount](sources);
        return await DrivePrimitivesAsync(sources, combined).ConfigureAwait(false);
    }

    /// <summary>Creates the ReactiveUI.Extensions subjects for a typed arity, combines them and drives the combined stream.</summary>
    /// <param name="subjectCount">The number of combined subjects.</param>
    /// <returns>The number of combined values observed.</returns>
    private static async Task<int> CombineExtensionsTypedAsync(int subjectCount)
    {
        var subjects = CreateSubjects(subjectCount);
        var combined = ExtensionsTypedCombiners[subjectCount - MinimumSourceCount](subjects);
        return await DriveExtensionsAsync(subjects, combined).ConfigureAwait(false);
    }

    /// <summary>Subscribes to the combined stream, pushes values into the first source, completes every source and waits for completion.</summary>
    /// <typeparam name="TResult">The combined element type.</typeparam>
    /// <param name="sources">The combined sources.</param>
    /// <param name="combined">The combined stream.</param>
    /// <returns>The number of combined values observed.</returns>
    private static async Task<int> DrivePrimitivesAsync<TResult>(ISignalAsync<int>[] sources, IObservableAsync<TResult> combined)
    {
        AsyncTallyWitness<TResult> witness = new();
        var subscription = await combined.SubscribeAsync(witness, CancellationToken.None).ConfigureAwait(false);
        for (var i = 0; i < PushCount; i++)
        {
            await sources[0].OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
        }

        foreach (var source in sources)
        {
            await source.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        }

        var count = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        foreach (var source in sources)
        {
            await source.DisposeAsync().ConfigureAwait(false);
        }

        return count;
    }

    /// <summary>Subscribes to the ReactiveUI.Extensions combined stream, pushes values into the first subject, completes every subject and waits for completion.</summary>
    /// <typeparam name="TResult">The combined element type.</typeparam>
    /// <param name="subjects">The combined subjects.</param>
    /// <param name="combined">The combined stream.</param>
    /// <returns>The number of combined values observed.</returns>
    private static async Task<int> DriveExtensionsAsync<TResult>(
        ExtensionsSubject[] subjects,
        ReactiveUI.Extensions.Async.IObservableAsync<TResult> combined)
    {
        ExtensionsAsyncTallyWitness<TResult> witness = new();
        var subscription = await combined.SubscribeAsync(witness, CancellationToken.None).ConfigureAwait(false);
        var first = subjects[0];
        for (var value = 0; value < PushCount; value++)
        {
            await first.OnNextAsync(value, CancellationToken.None).ConfigureAwait(false);
        }

        for (var index = 0; index < subjects.Length; index++)
        {
            await subjects[index].OnCompletedAsync(ExtensionsResult.Success).ConfigureAwait(false);
        }

        var observed = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        for (var index = 0; index < subjects.Length; index++)
        {
            await subjects[index].DisposeAsync().ConfigureAwait(false);
        }

        return observed;
    }

    /// <summary>Creates one behavior signal per combined source, each seeded with its index.</summary>
    /// <param name="sourceCount">The number of sources to create.</param>
    /// <returns>The sources.</returns>
    private static ISignalAsync<int>[] CreateSources(int sourceCount)
    {
        var sources = new ISignalAsync<int>[sourceCount];
        for (var i = 0; i < sources.Length; i++)
        {
            sources[i] = PrimitivesAsyncSignalFactory.CreateBehavior(i);
        }

        return sources;
    }

    /// <summary>Creates one ReactiveUI.Extensions behavior subject per combined source, each seeded with its index.</summary>
    /// <param name="subjectCount">The number of subjects to create.</param>
    /// <returns>The subjects.</returns>
    private static ExtensionsSubject[] CreateSubjects(int subjectCount)
    {
        var subjects = new ExtensionsSubject[subjectCount];
        for (var index = 0; index < subjects.Length; index++)
        {
            subjects[index] = ExtensionsSubjectAsync.CreateBehavior(index);
        }

        return subjects;
    }
}
