// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Web;

namespace ReactiveUI.Primitives.OccasionallyConnected.Web.Tests;

/// <summary>Tests browser lifecycle composition and bounded cancellation.</summary>
public sealed partial class BrowserLifecycleAdapterTests
{
    /// <summary>The number of hints emitted while startup is paused.</summary>
    private const int HintBurst = 1000;

    /// <summary>The number of starts after one resume.</summary>
    private const int ResumedStartCount = 2;

    /// <summary>Checks unknown hints, store reuse, and caller ownership.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task InitialStateAndStorageComposition()
    {
        var runtime = new BrowserJsRuntime();
        var context = new BrowserContext();
        await using var adapter = new BrowserLifecycleAdapter(runtime, context);
        await using var store = BrowserLifecycleAdapter.CreateLocalStore(runtime);
        await Assert.That(adapter.ConnectivityHint).IsEqualTo(BrowserConnectivityHint.Unknown);
        await Assert.That(adapter.IsSuspended).IsTrue();
        await Assert.That((store.Capabilities & LocalStoreCapabilities.MultiProcessCoordination) != 0).IsFalse();
        await Assert.That((store.Capabilities & LocalStoreCapabilities.AuthenticatedEncryptionAtRest) != 0).IsFalse();
        await Assert.That(store.GetType().Name).IsEqualTo("IndexedDbLocalStoreAdapter");
        await adapter.DisposeAsync();
        await Assert.That(context.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Checks module import, listener installation, and idempotent teardown.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task ListenerLifetimeIsOwned()
    {
        var runtime = new BrowserJsRuntime();
        var context = new BrowserContext();
        await using var adapter = new BrowserLifecycleAdapter(runtime, context);
        await adapter.StartAsync(CancellationToken.None);
        await adapter.StartAsync(CancellationToken.None);
        await Assert.That(runtime.ImportPath).IsEqualTo(
            "./_content/ReactiveUI.Primitives.OccasionallyConnected.Web/browserLifecycle.js");
        await Assert.That(runtime.ImportCount).IsEqualTo(1);
        await Assert.That(runtime.Module.ObserveCount).IsEqualTo(1);
        await adapter.DisposeAsync();
        await adapter.DisposeAsync();
        await Assert.That(runtime.Module.Listeners.RemoveCount).IsEqualTo(1);
        await Assert.That(runtime.Module.Listeners.DisposeCount).IsEqualTo(1);
        await Assert.That(runtime.Module.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Checks network hints never report authenticated Online state.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task NetworkHintsOnlyTriggerSynchronization()
    {
        var context = new BrowserContext();
        await using var adapter = new BrowserLifecycleAdapter(new BrowserJsRuntime(), context);
        await adapter.OnBrowserStateChangedAsync(false, false);
        await adapter.DrainAsync();
        await Assert.That(adapter.ConnectivityHint).IsEqualTo(BrowserConnectivityHint.Unavailable);
        await Assert.That(context.StartCount).IsEqualTo(1);
        await Assert.That(context.TriggerCount).IsEqualTo(0);
        await adapter.OnBrowserStateChangedAsync(true, false);
        await adapter.DrainAsync();
        await Assert.That(adapter.ConnectivityHint).IsEqualTo(BrowserConnectivityHint.PossiblyAvailable);
        await Assert.That(context.StartCount).IsEqualTo(1);
        await Assert.That(context.TriggerCount).IsEqualTo(1);
        await Assert.That(context.StopCount).IsEqualTo(0);
    }

    /// <summary>Checks visibility suspension and resume use context lifecycle contracts.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task SuspensionStopsAndResumeRestartsContext()
    {
        var context = new BrowserContext();
        await using var adapter = new BrowserLifecycleAdapter(new BrowserJsRuntime(), context);
        await adapter.OnBrowserStateChangedAsync(true, false);
        await adapter.DrainAsync();
        await adapter.OnBrowserStateChangedAsync(true, true);
        await adapter.DrainAsync();
        await Assert.That(adapter.IsSuspended).IsTrue();
        await Assert.That(context.StopCount).IsEqualTo(1);
        await adapter.OnBrowserStateChangedAsync(false, false);
        await adapter.DrainAsync();
        await Assert.That(adapter.IsSuspended).IsFalse();
        await Assert.That(context.StartCount).IsEqualTo(ResumedStartCount);
        await Assert.That(context.TriggerCount).IsEqualTo(1);
    }

    /// <summary>Checks pending hints stay bounded while suspension cancels blocked startup.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task SuspensionCancelsStartupAndCoalescesPendingHints()
    {
        var context = new BrowserContext { BlockStart = true };
        await using var adapter = new BrowserLifecycleAdapter(new BrowserJsRuntime(), context);
        await adapter.OnBrowserStateChangedAsync(true, false);
        await context.StartEntered.Task;
        for (var i = 0; i < HintBurst; i++)
        {
            await adapter.OnBrowserStateChangedAsync((i & 1) == 0, false);
        }

        await adapter.OnBrowserStateChangedAsync(false, true);
        await adapter.DrainAsync();
        await Assert.That(context.StartCount).IsEqualTo(1);
        await Assert.That(context.TriggerCount).IsEqualTo(0);
        await Assert.That(context.StopCount).IsEqualTo(1);
        await Assert.That(adapter.LastError).IsNull();
    }

    /// <summary>Checks disposal cancels network work before stopping the context.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task DisposalCancelsActiveSynchronization()
    {
        var context = new BrowserContext { BlockTrigger = true };
        var runtime = new BrowserJsRuntime();
        await using var adapter = new BrowserLifecycleAdapter(runtime, context);
        await adapter.StartAsync(CancellationToken.None);
        await adapter.OnBrowserStateChangedAsync(true, false);
        await context.TriggerEntered.Task;
        await adapter.DisposeAsync();
        await adapter.OnBrowserStateChangedAsync(true, false);
        await Assert.That(context.TriggerCount).IsEqualTo(1);
        await Assert.That(context.StopCount).IsEqualTo(1);
        await Assert.That(context.MaximumConcurrency).IsEqualTo(1);
    }

    /// <summary>Checks lifecycle failures are reported and a later event can recover.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task LifecycleFailureIsReportedWithoutHiddenRetry()
    {
        var context = new BrowserContext { FailStart = true };
        await using var adapter = new BrowserLifecycleAdapter(new BrowserJsRuntime(), context);
        await adapter.OnBrowserStateChangedAsync(true, false);
        await adapter.DrainAsync();
        await Assert.That(adapter.LastError).IsTypeOf<InvalidOperationException>();
        await Assert.That(context.StartCount).IsEqualTo(1);
        context.FailStart = false;
        await adapter.OnBrowserStateChangedAsync(true, false);
        await adapter.DrainAsync();
        await Assert.That(adapter.LastError).IsNull();
        await Assert.That(context.TriggerCount).IsEqualTo(1);
    }

    /// <summary>Checks synchronous context reentrancy does not deadlock or overlap lifecycle work.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task ReentrantSuspensionIsSerialized()
    {
        var context = new BrowserContext();
        await using var adapter = new BrowserLifecycleAdapter(new BrowserJsRuntime(), context);
        context.OnStart = () => adapter.OnBrowserStateChangedAsync(false, true);
        await adapter.OnBrowserStateChangedAsync(true, false);
        await adapter.DrainAsync();
        await Assert.That(adapter.IsSuspended).IsTrue();
        await Assert.That(context.StopCount).IsEqualTo(1);
        await Assert.That(context.MaximumConcurrency).IsEqualTo(1);
    }

    /// <summary>Checks a cancelled listener setup can be retried.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task InitializationCancellationCanBeRetried()
    {
        var runtime = new BrowserJsRuntime();
        await using var adapter = new BrowserLifecycleAdapter(runtime, new BrowserContext());
        using var source = new CancellationTokenSource();
        await source.CancelAsync();
        await Assert.That(async () => await adapter.StartAsync(source.Token)).Throws<OperationCanceledException>();
        await adapter.StartAsync(CancellationToken.None);
        await Assert.That(runtime.Module.ObserveCount).IsEqualTo(1);
    }

    /// <summary>Checks an import failure is visible and can be retried.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task InitializationFailureCanBeRetried()
    {
        var runtime = new BrowserJsRuntime { FailImport = true };
        await using var adapter = new BrowserLifecycleAdapter(runtime, new BrowserContext());
        await Assert.That(async () => await adapter.StartAsync(CancellationToken.None)).Throws<Microsoft.JSInterop.JSException>();
        await Assert.That(adapter.LastError).IsTypeOf<Microsoft.JSInterop.JSException>();
        runtime.FailImport = false;
        await adapter.StartAsync(CancellationToken.None);
        await Assert.That(runtime.Module.ObserveCount).IsEqualTo(1);
    }

    /// <summary>Checks disconnected listener teardown still disposes references and stops context.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task DisconnectedTeardownStopsContext()
    {
        var runtime = new BrowserJsRuntime();
        var context = new BrowserContext();
        await using var adapter = new BrowserLifecycleAdapter(runtime, context);
        await adapter.StartAsync(CancellationToken.None);
        runtime.Module.Listeners.Disconnected = true;
        await adapter.DisposeAsync();
        await Assert.That(context.StopCount).IsEqualTo(1);
        await Assert.That(runtime.Module.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Checks invalid dependencies fail before any listener work.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task NullDependenciesAreRejected()
    {
        await Assert.That(static () => new BrowserLifecycleAdapter(null!, new BrowserContext())).Throws<ArgumentNullException>();
        await Assert.That(static () => new BrowserLifecycleAdapter(new BrowserJsRuntime(), null!)).Throws<ArgumentNullException>();
        await Assert.That(static () => BrowserLifecycleAdapter.CreateLocalStore(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>Checks a cancelled registration removes listeners by identity even without a returned JS reference.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task CancelledRegistrationRemovesListeners()
    {
        var runtime = new BrowserJsRuntime();
        runtime.Module.CancelObserve = true;
        await using var adapter = new BrowserLifecycleAdapter(runtime, new BrowserContext());
        await Assert.That(async () => await adapter.StartAsync(CancellationToken.None)).Throws<OperationCanceledException>();
        await Assert.That(runtime.Module.Listeners.RemoveCount).IsEqualTo(1);
        runtime.Module.CancelObserve = false;
        await adapter.StartAsync(CancellationToken.None);
        await Assert.That(runtime.ImportCount).IsEqualTo(1);
        await Assert.That(runtime.Module.ObserveCount).IsEqualTo(ResumedStartCount);
    }

    /// <summary>Checks disconnected JS references do not prevent context shutdown.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task DisconnectedReferenceDisposalStopsContext()
    {
        var runtime = new BrowserJsRuntime();
        var context = new BrowserContext();
        await using var adapter = new BrowserLifecycleAdapter(runtime, context);
        await adapter.StartAsync(CancellationToken.None);
        runtime.Module.Listeners.DisconnectOnDispose = true;
        runtime.Module.DisconnectOnDispose = true;
        await adapter.DisposeAsync();
        await Assert.That(context.StopCount).IsEqualTo(1);
        await Assert.That(runtime.Module.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Checks repeated disposal shares the original failure after all references are released.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task DisposalFailureIsSharedWithRepeatedCallers()
    {
        var runtime = new BrowserJsRuntime();
        var context = new BrowserContext { FailStop = true };
        var adapter = new BrowserLifecycleAdapter(runtime, context);
        await adapter.StartAsync(CancellationToken.None);
        await Assert.That(async () => await adapter.DisposeAsync()).Throws<InvalidOperationException>();
        await Assert.That(async () => await adapter.DisposeAsync()).Throws<InvalidOperationException>();
        await Assert.That(context.StopCount).IsEqualTo(1);
        await Assert.That(runtime.Module.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Checks an adapter cannot install listeners after disposal.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task StartAfterDisposalIsRejected()
    {
        var runtime = new BrowserJsRuntime();
        await using var adapter = new BrowserLifecycleAdapter(runtime, new BrowserContext());
        await adapter.DisposeAsync();
        await Assert.That(async () => await adapter.StartAsync(CancellationToken.None)).Throws<ObjectDisposedException>();
        await Assert.That(runtime.ImportCount).IsEqualTo(0);
    }

    /// <summary>Checks failed suspension can resume through the context startup contract.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task FailedSuspensionCanResume()
    {
        var context = new BrowserContext();
        await using var adapter = new BrowserLifecycleAdapter(new BrowserJsRuntime(), context);
        await adapter.OnBrowserStateChangedAsync(false, false);
        await adapter.DrainAsync();
        context.FailStop = true;
        await adapter.OnBrowserStateChangedAsync(false, true);
        await adapter.DrainAsync();
        await Assert.That(adapter.LastError).IsTypeOf<InvalidOperationException>();
        context.FailStop = false;
        await adapter.OnBrowserStateChangedAsync(true, false);
        await adapter.DrainAsync();
        await Assert.That(context.StartCount).IsEqualTo(ResumedStartCount);
        await Assert.That(adapter.LastError).IsNull();
    }

    /// <summary>Checks faulty context cancellation callbacks cannot leak browser listeners during disposal.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task CancellationCallbackFailureStillRemovesListeners()
    {
        var runtime = new BrowserJsRuntime();
        var context = new BrowserContext { BlockStart = true, ThrowOnCancellation = true };
        await using var adapter = new BrowserLifecycleAdapter(runtime, context);
        await adapter.StartAsync(CancellationToken.None);
        await adapter.OnBrowserStateChangedAsync(true, false);
        await context.StartEntered.Task;
        await adapter.DisposeAsync();
        await Assert.That(context.StopCount).IsEqualTo(1);
        await Assert.That(runtime.Module.Listeners.RemoveCount).IsEqualTo(1);
        await Assert.That(runtime.Module.DisposeCount).IsEqualTo(1);
    }
}
