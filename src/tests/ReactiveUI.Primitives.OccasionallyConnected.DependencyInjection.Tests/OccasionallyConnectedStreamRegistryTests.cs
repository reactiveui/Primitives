// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;
using CounterStream = ReactiveUI.Primitives.OccasionallyConnected.IOccasionallyConnectedStream<
    ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests.DependencyInjectionTestDoubles.CounterState,
    ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests.DependencyInjectionTestDoubles.CounterInput>;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests;

/// <summary>Tests for <see cref="IOccasionallyConnectedStreamRegistry"/>.</summary>
public sealed class OccasionallyConnectedStreamRegistryTests
{
    /// <summary>The main counter stream name used by registry tests.</summary>
    private const string CounterName = "counter";

    /// <summary>The number of forced finalizer passes used by the UTE regression.</summary>
    private const int FinalizerPasses = 2;

    /// <summary>The input delta used after provider disposal.</summary>
    private const int DisposedPublishDelta = 1;

    /// <summary>Verifies named stream resolution returns a singleton stream instance.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetRequiredStreamReturnsSameSingletonStreamForSameName()
    {
        await using var provider = CreateProvider(static builder => builder.AddStream(
            CounterName,
            DependencyInjectionTestDoubles.CreateDefinition));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        var first = streams.GetRequiredStream(CounterKey(CounterName));
        var second = streams.GetRequiredStream(CounterKey(CounterName));

        await Assert.That(second).IsSameReferenceAs(first);
    }

    /// <summary>Verifies typed stream keys compare both name and generic stream type.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamKeysUseNameAndGenericTypeForEquality()
    {
        var first = CounterKey(CounterName);
        var second = CounterKey(CounterName);
        var differentName = CounterKey("counter-other");
        OccasionallyConnectedStreamKey<DependencyInjectionTestDoubles.OtherState, DependencyInjectionTestDoubles.CounterInput> differentType = new(CounterName);

        await Assert.That(first.Equals(second)).IsTrue();
        await Assert.That(first == second).IsTrue();
        await Assert.That(first.GetHashCode()).IsEqualTo(second.GetHashCode());
        await Assert.That(first.Equals(differentName)).IsFalse();
        await Assert.That(first.Equals(differentType)).IsFalse();
        await Assert.That(first.Equals((object)differentType)).IsFalse();
    }

    /// <summary>Verifies resolving with a null name rejects before lookup.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetRequiredStreamRejectsNullNameWithParameterName()
    {
        await using var provider = CreateProvider(static builder => builder.AddStream(
            CounterName,
            DependencyInjectionTestDoubles.CreateDefinition));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        var exception = Assert.ThrowsExactly<ArgumentNullException>(() =>
            streams.GetRequiredStream(CounterKey(NullReference<string>())));

        await Assert.That(exception.ParamName).IsEqualTo("name");
    }

    /// <summary>Verifies resolving with blank names rejects before lookup.</summary>
    /// <param name="name">The invalid stream name.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("")]
    [Arguments(" \t")]
    public async Task GetRequiredStreamRejectsBlankNameWithParameterName(string name)
    {
        await using var provider = CreateProvider(static builder => builder.AddStream(
            CounterName,
            DependencyInjectionTestDoubles.CreateDefinition));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        var exception = Assert.ThrowsExactly<ArgumentException>(() =>
            streams.GetRequiredStream(CounterKey(name)));
        await Assert.That(exception.ParamName).IsEqualTo("name");
    }

    /// <summary>Verifies resolving a registered name with another stream type is rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetRequiredStreamRejectsRegisteredNameWithDifferentType()
    {
        await using var provider = CreateProvider(static builder => builder.AddStream(
            CounterName,
            DependencyInjectionTestDoubles.CreateDefinition));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        await Assert.That(() => streams.GetRequiredStream(new OccasionallyConnectedStreamKey<
                DependencyInjectionTestDoubles.OtherState,
                DependencyInjectionTestDoubles.CounterInput>(CounterName)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a cached stream handle cannot publish after the owning provider is disposed.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CachedStreamAfterProviderDisposeRejectsPublish()
    {
        var provider = CreateProvider(static builder => builder.AddStream(
            CounterName,
            DependencyInjectionTestDoubles.CreateDefinition));
        var providerDisposed = false;
        try
        {
            var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();
            var stream = streams.GetRequiredStream(CounterKey(CounterName));

            await provider.DisposeAsync();
            providerDisposed = true;

            await Assert.That(async () => await stream.PublishAsync(
                    new(DisposedPublishDelta),
                    null,
                    CancellationToken.None).AsTask().WaitAsync(DependencyInjectionTestDoubles.GuardTimeout))
                .ThrowsExactly<ObjectDisposedException>();
        }
        finally
        {
            if (!providerDisposed)
            {
                await provider.DisposeAsync();
            }
        }
    }

    /// <summary>Verifies a named factory can resolve another stream without a registry lock deadlock.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetRequiredStreamDoesNotInvokeUserFactoriesUnderRegistryLock()
    {
        using StreamFactoryGate gate = new();
        Task<CounterStream>? dependencyTask = null;
        Exception? dependencyFailure = null;

        await using var provider = CreateProvider(builder => builder
            .AddStream("dependency", DependencyInjectionTestDoubles.CreateDefinition)
            .AddStream("outer", gate.CreateOuterDefinition));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        var outerTask = StartStreamResolution(streams, "outer");

        try
        {
            await Assert.That(gate.WaitForOuterFactory()).IsTrue();
            dependencyTask = StartStreamResolution(streams, "dependency");

            var dependency = await dependencyTask.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout);
            await Assert.That(dependency).IsNotNull();
        }
        catch (Exception exception)
        {
            dependencyFailure = exception;
        }
        finally
        {
            gate.ReleaseOuterFactory();
        }

        await AssertResolutionCleanupAsync(outerTask, dependencyTask, dependencyFailure is not null);

        if (dependencyFailure is not null)
        {
            ExceptionDispatchInfo.Capture(dependencyFailure).Throw();
        }
    }

    /// <summary>Verifies recursive same-thread resolution of one name is rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetRequiredStreamRejectsRecursiveSameNameResolution()
    {
        RecursiveResolutionFactory factory = new();
        await using var provider = CreateProvider(builder => builder.AddStream(CounterName, factory.CreateDefinition));
        factory.Registry = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        await Assert.That(() => factory.Registry.GetRequiredStream(CounterKey(CounterName)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies concurrent same-name misses share one stream initialization.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConcurrentSameNameResolutionInvokesFactoryOnce()
    {
        using SameNameFactoryGate gate = new();
        await using var provider = CreateProvider(builder => builder.AddStream(CounterName, gate.CreateDefinition));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();
        StreamResolutionRunner? firstResolution = null;
        StreamResolutionRunner? secondResolution = null;

        try
        {
            firstResolution = StreamResolutionRunner.Start(streams, CounterName);
            await Assert.That(gate.WaitForFactory()).IsTrue();
            secondResolution = StreamResolutionRunner.Start(streams, CounterName);
            await Assert.That(secondResolution.WaitForBlockedJoin()).IsTrue();
            gate.ReleaseFactory();

            var first = await firstResolution.Task.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout);
            var second = await secondResolution.Task.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout);

            await Assert.That(second).IsSameReferenceAs(first);
            await Assert.That(gate.FactoryCalls).IsEqualTo(1);
        }
        finally
        {
            gate.ReleaseFactory();
            await JoinResolutionsAsync(firstResolution, secondResolution);
        }
    }

    /// <summary>Verifies concurrent same-name waiters observe an owner factory failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConcurrentSameNameResolutionPropagatesFactoryFailureToWaiter()
    {
        using FailingSameNameFactoryGate gate = new();
        await using var provider = CreateProvider(builder => builder.AddStream(CounterName, _ => gate.CreateDefinition()));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();
        StreamResolutionRunner? ownerResolution = null;
        StreamResolutionRunner? waiterResolution = null;

        try
        {
            ownerResolution = StreamResolutionRunner.Start(streams, CounterName);
            await Assert.That(gate.WaitForFactory()).IsTrue();
            waiterResolution = StreamResolutionRunner.Start(streams, CounterName);
            await Assert.That(waiterResolution.WaitForBlockedJoin()).IsTrue();
            gate.ReleaseFactory();

            var ownerFailure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
                await ownerResolution.Task.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout));
            var waiterFailure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
                await waiterResolution.Task.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout));

            await Assert.That(ownerFailure).IsSameReferenceAs(gate.Failure);
            await Assert.That(waiterFailure).IsSameReferenceAs(gate.Failure);
            await Assert.That(gate.FactoryCalls).IsEqualTo(1);
            ownerResolution.ObserveExpectedFailure(gate.Failure);
            waiterResolution.ObserveExpectedFailure(gate.Failure);
        }
        finally
        {
            gate.ReleaseFactory();
            await JoinResolutionsAsync(ownerResolution, waiterResolution);
        }
    }

    /// <summary>Verifies a failed owner-only factory does not publish an unobserved task fault.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task FailedFactoryWithoutJoinerDoesNotPublishUnobservedTaskException()
    {
        var failure = new InvalidOperationException("factory failed");
        var unobserved = new TaskCompletionSource<AggregateException>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        void Handler(object? _, UnobservedTaskExceptionEventArgs args)
        {
            if (ContainsOriginalFailure(args.Exception, failure))
            {
                _ = unobserved.TrySetResult(args.Exception);
            }
        }

        TaskScheduler.UnobservedTaskException += Handler;
        try
        {
            var exception = await ResolveOwnerOnlyFailureAsync(failure);

            await Assert.That(exception).IsSameReferenceAs(failure);
            ForceFinalizers();
            await AssertNoUnobservedExceptionAsync(unobserved.Task);
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= Handler;
        }
    }

    /// <summary>Verifies missing names are rejected deterministically.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetRequiredStreamRejectsUnknownName()
    {
        await using var provider = CreateProvider(static _ => { });
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        await Assert.That(() => streams.GetRequiredStream(CounterKey("missing")))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a service provider with required services.</summary>
    /// <param name="configure">Additional builder configuration.</param>
    /// <returns>The service provider.</returns>
    private static ServiceProvider CreateProvider(Action<OccasionallyConnectedDependencyInjectionBuilder> configure)
    {
        ServiceCollection services = new();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingTransportAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.CounterProjection>();
        _ = services.AddOccasionallyConnected(builder =>
        {
            _ = builder
                .UseClient(new(DependencyInjectionTestDoubles.ClientId))
                .UseStoreIdentity(DependencyInjectionTestDoubles.StoreIdentity)
                .UseStoreInitialization(DependencyInjectionTestDoubles.CreateStoreInitialization())
                .UseStore(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))
                .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))
                .UseJsonSerializer()
                .AddJsonContract(
                    DependencyInjectionTestDoubles.InputContract,
                    1,
                    DependencyInjectionJsonContext.Default.CounterInput)
                .AddJsonContract(
                    DependencyInjectionTestDoubles.StateContract,
                    1,
                    DependencyInjectionJsonContext.Default.CounterState);
            configure(builder);
        });
        return services.BuildServiceProvider(validateScopes: true);
    }

    /// <summary>Runs a failed owner-only resolution behind a non-inlined helper boundary.</summary>
    /// <param name="failure">The factory failure to throw.</param>
    /// <returns>The exception returned by the registry.</returns>
    [MethodImpl(MethodImplOptions.NoInlining | MethodImplOptions.NoOptimization)]
    private static async Task<InvalidOperationException> ResolveOwnerOnlyFailureAsync(Exception failure)
    {
        await using var provider = CreateProvider(builder => builder.AddStream<
            DependencyInjectionTestDoubles.CounterState,
            DependencyInjectionTestDoubles.CounterInput>(
                CounterName,
                _ => throw failure));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();
        var exception = Assert.ThrowsExactly<InvalidOperationException>(() =>
            streams.GetRequiredStream(CounterKey(CounterName)));
        return exception;
    }

    /// <summary>Forces finalizers so unobserved task faults are published deterministically.</summary>
    [MethodImpl(MethodImplOptions.NoInlining | MethodImplOptions.NoOptimization)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Allocations", "PSH1021:Do not force garbage collection", Justification = "Test helper to force finalizers.")]
    private static void ForceFinalizers()
    {
        for (var pass = 0; pass < FinalizerPasses; pass++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }

    /// <summary>Checks whether an aggregate contains the expected factory failure instance.</summary>
    /// <param name="exception">The aggregate exception.</param>
    /// <param name="failure">The original factory failure.</param>
    /// <returns>A value indicating whether the original failure was observed.</returns>
    private static bool ContainsOriginalFailure(AggregateException exception, Exception failure)
    {
        var exceptions = exception.Flatten().InnerExceptions;
        for (var i = 0; i < exceptions.Count; i++)
        {
            if (ReferenceEquals(exceptions[i], failure))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Creates a null reference for a non-nullable malformed-input fixture.</summary>
    /// <typeparam name="T">The non-nullable reference type.</typeparam>
    /// <returns>A null reference typed as <typeparamref name="T" />.</returns>
    private static T NullReference<T>()
        where T : class
    {
        object? value = null;
        return Unsafe.As<object?, T>(ref value);
    }

    /// <summary>Creates a typed counter stream key.</summary>
    /// <param name="name">The stream name.</param>
    /// <returns>The typed stream key.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedStreamKey<
        DependencyInjectionTestDoubles.CounterState,
        DependencyInjectionTestDoubles.CounterInput> CounterKey(string name) => new(name);

    /// <summary>Asserts no unobserved task exception is reported after finalization.</summary>
    /// <param name="unobservedTask">The unobserved exception signal.</param>
    /// <returns>A task representing the assertion.</returns>
    private static async Task AssertNoUnobservedExceptionAsync(Task<AggregateException> unobservedTask)
    {
        await Task.Yield();
        await Assert.That(unobservedTask.IsCompleted).IsFalse();
    }

    /// <summary>Starts stream resolution on a separate thread.</summary>
    /// <param name="registry">The registry under test.</param>
    /// <param name="name">The stream name.</param>
    /// <returns>The stream resolution task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<CounterStream> StartStreamResolution(
        IOccasionallyConnectedStreamRegistry registry,
        string name) =>
        Task.Factory.StartNew(
            static state =>
            {
                if (state is not StreamResolutionRequest request)
                {
                    throw new InvalidOperationException("The stream resolution request was not supplied.");
                }

                return request.Registry.GetRequiredStream(CounterKey(request.Name));
            },
            new StreamResolutionRequest(registry, name),
            CancellationToken.None,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);

    /// <summary>Joins two raw-thread stream resolutions while preserving cleanup failures.</summary>
    /// <param name="first">The first optional resolution to join.</param>
    /// <param name="second">The second optional resolution to join.</param>
    /// <returns>A task representing the bounded joins.</returns>
    private static async Task JoinResolutionsAsync(StreamResolutionRunner? first, StreamResolutionRunner? second)
    {
        Exception? cleanupFailure = null;
        try
        {
            await JoinResolutionAsync(first);
        }
        catch (Exception exception)
        {
            cleanupFailure = exception;
        }
        finally
        {
            try
            {
                await JoinResolutionAsync(second);
            }
            catch (Exception exception)
            {
                cleanupFailure ??= exception;
            }
        }

        if (cleanupFailure is not null)
        {
            ExceptionDispatchInfo.Capture(cleanupFailure).Throw();
        }
    }

    /// <summary>Joins a raw-thread stream resolution.</summary>
    /// <param name="resolution">The optional resolution to join.</param>
    /// <returns>A task representing the bounded join.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task JoinResolutionAsync(StreamResolutionRunner? resolution) =>
        resolution?.JoinAsync() ?? Task.CompletedTask;

    /// <summary>Waits for background stream resolution tasks before surfacing any cleanup failure.</summary>
    /// <param name="outerTask">The outer stream resolution task.</param>
    /// <param name="dependencyTask">The dependency stream resolution task.</param>
    /// <param name="dependencyFailureCaptured">Whether dependency failure was already captured.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertResolutionCleanupAsync(
        Task<CounterStream> outerTask,
        Task<CounterStream>? dependencyTask,
        bool dependencyFailureCaptured)
    {
        Exception? cleanupFailure = null;

        try
        {
            var outer = await outerTask.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout);
            await Assert.That(outer).IsNotNull();
        }
        catch (Exception exception)
        {
            cleanupFailure = exception;
        }

        try
        {
            if (dependencyTask is not null)
            {
                _ = await dependencyTask.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout);
            }
        }
        catch (Exception) when (dependencyFailureCaptured)
        {
        }
        catch (Exception exception)
        {
            cleanupFailure ??= exception;
        }

        if (cleanupFailure is not null)
        {
            ExceptionDispatchInfo.Capture(cleanupFailure).Throw();
        }
    }

    /// <summary>Creates a recursive same-name factory without nullable captured locals.</summary>
    private sealed class RecursiveResolutionFactory
    {
        /// <summary>Gets or sets the registry used by the recursive factory.</summary>
        public IOccasionallyConnectedStreamRegistry? Registry { get; set; }

        /// <summary>Creates a stream definition after recursively resolving the same name.</summary>
        /// <param name="services">The service provider.</param>
        /// <returns>The stream definition.</returns>
        public StreamDefinition<
            DependencyInjectionTestDoubles.CounterState,
            DependencyInjectionTestDoubles.CounterInput> CreateDefinition(IServiceProvider services)
        {
            _ = GetRequiredCounterStream();
            return DependencyInjectionTestDoubles.CreateDefinition(services);
        }

        /// <summary>Resolves the counter stream through the captured registry.</summary>
        /// <returns>The resolved stream.</returns>
        /// <exception cref="InvalidOperationException">The registry has not been assigned.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CounterStream GetRequiredCounterStream()
        {
            var registry = Registry ?? throw new InvalidOperationException("The registry was not assigned.");
            return registry.GetRequiredStream(CounterKey(CounterName));
        }
    }

    /// <summary>Runs a stream resolution on a dedicated observable thread.</summary>
    private sealed class StreamResolutionRunner
    {
        /// <summary>Signals that the thread has entered the registry call.</summary>
        private readonly ManualResetEventSlim _callStarted = new();

        /// <summary>Stores the dedicated resolution thread.</summary>
        private readonly Thread _thread;

        /// <summary>Stores the registry under test.</summary>
        private readonly IOccasionallyConnectedStreamRegistry _registry;

        /// <summary>Stores the stream name to resolve.</summary>
        private readonly string _name;

        /// <summary>Stores the resolution completion.</summary>
        private readonly TaskCompletionSource<CounterStream> _completion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Stores an expected resolution failure that the test already observed.</summary>
        private Exception? _observedExpectedFailure;

        /// <summary>Stores a value indicating whether the thread was joined.</summary>
        private int _joined;

        /// <summary>Initializes a new instance of the <see cref="StreamResolutionRunner" /> class.</summary>
        /// <param name="registry">The registry under test.</param>
        /// <param name="name">The stream name.</param>
        private StreamResolutionRunner(IOccasionallyConnectedStreamRegistry registry, string name)
        {
            _registry = registry;
            _name = name;
            _thread = new(Run) { IsBackground = true, Name = "DI stream resolution" };
            _thread.Start();
        }

        /// <summary>Gets the resolution task.</summary>
        public Task<CounterStream> Task => _completion.Task;

        /// <summary>Starts a raw-thread stream resolution.</summary>
        /// <param name="registry">The registry under test.</param>
        /// <param name="name">The stream name.</param>
        /// <returns>The started resolution.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StreamResolutionRunner Start(IOccasionallyConnectedStreamRegistry registry, string name) =>
            new(registry, name);

        /// <summary>Records an expected resolution failure that has already been asserted by the test.</summary>
        /// <param name="failure">The expected failure.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ObserveExpectedFailure(Exception failure) => _observedExpectedFailure = failure;

        /// <summary>Waits until the resolution is blocked joining the in-flight owner slot.</summary>
        /// <returns>A value indicating whether the resolution reached a blocked join state.</returns>
        public bool WaitForBlockedJoin()
        {
            if (!_callStarted.Wait(DependencyInjectionTestDoubles.GuardTimeout, CancellationToken.None))
            {
                return false;
            }

            var started = Stopwatch.GetTimestamp();
            while (Stopwatch.GetElapsedTime(started) < DependencyInjectionTestDoubles.GuardTimeout)
            {
                if (Task.IsCompleted)
                {
                    return false;
                }

                if ((_thread.ThreadState & System.Threading.ThreadState.WaitSleepJoin) != 0)
                {
                    return true;
                }

                _ = Thread.Yield();
            }

            return false;
        }

        /// <summary>Joins the resolution task and backing thread.</summary>
        /// <returns>A task representing the bounded join.</returns>
        public async Task JoinAsync()
        {
            Exception? resolutionFailure = null;
            try
            {
                _ = await Task.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout);
            }
            catch (Exception exception) when (ReferenceEquals(exception, _observedExpectedFailure))
            {
            }
            catch (Exception exception)
            {
                resolutionFailure = exception;
            }
            finally
            {
                JoinThread();
            }

            if (resolutionFailure is not null)
            {
                ExceptionDispatchInfo.Capture(resolutionFailure).Throw();
            }
        }

        /// <summary>Joins the backing thread and disposes thread-owned signals after it exits.</summary>
        /// <exception cref="TimeoutException">The backing thread did not exit in time.</exception>
        private void JoinThread()
        {
            if (Interlocked.Exchange(ref _joined, 1) != 0)
            {
                return;
            }

            if (!_thread.Join(DependencyInjectionTestDoubles.GuardTimeout))
            {
                throw new TimeoutException("The stream resolution thread did not exit before the guard timeout.");
            }

            _callStarted.Dispose();
        }

        /// <summary>Runs the stream resolution on the dedicated thread.</summary>
        private void Run()
        {
            _callStarted.Set();
            try
            {
                _completion.SetResult(_registry.GetRequiredStream(CounterKey(_name)));
            }
            catch (Exception exception)
            {
                _completion.SetException(exception);
            }
        }
    }

    /// <summary>Coordinates failing concurrent same-name factory resolution.</summary>
    private sealed class FailingSameNameFactoryGate : IDisposable
    {
        /// <summary>Signals that the same-name factory was entered.</summary>
        private readonly ManualResetEventSlim _factoryEntered = new();

        /// <summary>Releases the same-name factory.</summary>
        private readonly ManualResetEventSlim _releaseFactory = new();

        /// <summary>Gets the failure thrown by the factory.</summary>
        public InvalidOperationException Failure { get; } = new("factory failed");

        /// <summary>Gets the number of factory calls.</summary>
        public int FactoryCalls { get; private set; }

        /// <summary>Creates a stream definition by throwing after the test releases the factory.</summary>
        /// <returns>The stream definition.</returns>
        /// <exception cref="TimeoutException">The factory release signal is not observed in time.</exception>
        public StreamDefinition<
            DependencyInjectionTestDoubles.CounterState,
            DependencyInjectionTestDoubles.CounterInput> CreateDefinition()
        {
            FactoryCalls++;
            _factoryEntered.Set();
            if (!_releaseFactory.Wait(DependencyInjectionTestDoubles.GuardTimeout, CancellationToken.None))
            {
                throw new TimeoutException("The test did not release the failing same-name stream factory.");
            }

            throw Failure;
        }

        /// <summary>Waits for the factory to begin.</summary>
        /// <returns>A value indicating whether the factory began.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool WaitForFactory() =>
            _factoryEntered.Wait(DependencyInjectionTestDoubles.GuardTimeout, CancellationToken.None);

        /// <summary>Releases the factory.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseFactory() => _releaseFactory.Set();

        /// <inheritdoc />
        public void Dispose()
        {
            _factoryEntered.Dispose();
            _releaseFactory.Dispose();
        }
    }

    /// <summary>Coordinates concurrent same-name factory resolution.</summary>
    private sealed class SameNameFactoryGate : IDisposable
    {
        /// <summary>Signals that the same-name factory was entered.</summary>
        private readonly ManualResetEventSlim _factoryEntered = new();

        /// <summary>Releases the same-name factory.</summary>
        private readonly ManualResetEventSlim _releaseFactory = new();

        /// <summary>Gets the number of factory calls.</summary>
        public int FactoryCalls { get; private set; }

        /// <summary>Creates a stream definition after the test releases the factory.</summary>
        /// <param name="services">The service provider.</param>
        /// <returns>The stream definition.</returns>
        /// <exception cref="TimeoutException">The factory release signal is not observed in time.</exception>
        public StreamDefinition<
            DependencyInjectionTestDoubles.CounterState,
            DependencyInjectionTestDoubles.CounterInput> CreateDefinition(IServiceProvider services)
        {
            FactoryCalls++;
            _factoryEntered.Set();
            if (!_releaseFactory.Wait(DependencyInjectionTestDoubles.GuardTimeout, CancellationToken.None))
            {
                throw new TimeoutException("The test did not release the same-name stream factory.");
            }

            return DependencyInjectionTestDoubles.CreateDefinition(services, $"counter/{FactoryCalls}");
        }

        /// <summary>Waits for the factory to begin.</summary>
        /// <returns>A value indicating whether the factory began.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool WaitForFactory() =>
            _factoryEntered.Wait(DependencyInjectionTestDoubles.GuardTimeout, CancellationToken.None);

        /// <summary>Releases the factory.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseFactory() => _releaseFactory.Set();

        /// <inheritdoc />
        public void Dispose()
        {
            _factoryEntered.Dispose();
            _releaseFactory.Dispose();
        }
    }

    /// <summary>Coordinates the factory lock proof.</summary>
    private sealed class StreamFactoryGate : IDisposable
    {
        /// <summary>Signals that the outer factory was entered.</summary>
        private readonly ManualResetEventSlim _outerFactoryEntered = new();

        /// <summary>Releases the outer factory.</summary>
        private readonly ManualResetEventSlim _releaseOuterFactory = new();

        /// <summary>Creates the outer stream definition after the test releases the factory.</summary>
        /// <param name="services">The service provider.</param>
        /// <returns>The stream definition.</returns>
        /// <exception cref="TimeoutException">The outer factory release signal is not observed in time.</exception>
        public StreamDefinition<
            DependencyInjectionTestDoubles.CounterState,
            DependencyInjectionTestDoubles.CounterInput> CreateOuterDefinition(IServiceProvider services)
        {
            _outerFactoryEntered.Set();
            if (!_releaseOuterFactory.Wait(DependencyInjectionTestDoubles.GuardTimeout, CancellationToken.None))
            {
                throw new TimeoutException("The test did not release the outer stream factory.");
            }

            return DependencyInjectionTestDoubles.CreateDefinition(services, "counter/outer");
        }

        /// <summary>Waits for the outer factory to begin.</summary>
        /// <returns>A value indicating whether the factory began.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool WaitForOuterFactory() =>
            _outerFactoryEntered.Wait(DependencyInjectionTestDoubles.GuardTimeout, CancellationToken.None);

        /// <summary>Releases the outer factory.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseOuterFactory() => _releaseOuterFactory.Set();

        /// <inheritdoc />
        public void Dispose()
        {
            _outerFactoryEntered.Dispose();
            _releaseOuterFactory.Dispose();
        }
    }

    /// <summary>Stores a stream resolution request for a long-running task.</summary>
    /// <param name="Registry">The registry under test.</param>
    /// <param name="Name">The stream name.</param>
    private sealed record StreamResolutionRequest(IOccasionallyConnectedStreamRegistry Registry, string Name);
}
