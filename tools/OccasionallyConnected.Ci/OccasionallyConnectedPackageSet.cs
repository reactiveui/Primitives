namespace OccasionallyConnected.Ci;

internal static class OccasionallyConnectedPackageSet
{
    internal const string WebSockets = "ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets";

    internal static readonly string[] NeutralFrameworkProperties =
    [
        "-p:AndroidPrimitivesTargetFrameworks=",
        "-p:ApplePrimitivesTargetFrameworks=",
        "-p:MobilePlatformTargetFrameworks=",
    ];

    internal static readonly string[] Dependencies =
    [
        "ReactiveUI.Disposables",
        "ReactiveUI.Primitives.Core",
        "ReactiveUI.Primitives",
        "ReactiveUI.Primitives.Reactive",
    ];

    internal static readonly string[] Names =
    [
        "ReactiveUI.Primitives.OccasionallyConnected.Core",
        "ReactiveUI.Primitives.OccasionallyConnected",
        "ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection",
        "ReactiveUI.Primitives.OccasionallyConnected.Hosting",
        "ReactiveUI.Primitives.OccasionallyConnected.Mobile",
        "ReactiveUI.Primitives.OccasionallyConnected.Reactive",
        "ReactiveUI.Primitives.OccasionallyConnected.Server",
        "ReactiveUI.Primitives.OccasionallyConnected.SignalR",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite",
        "ReactiveUI.Primitives.OccasionallyConnected.Transport.Http",
        WebSockets,
        "ReactiveUI.Primitives.OccasionallyConnected.Web",
    ];

    internal static readonly string[] NewConsumerPackages =
    [
        "ReactiveUI.Primitives.OccasionallyConnected.Reactive",
        "ReactiveUI.Primitives.OccasionallyConnected.Mobile",
        "ReactiveUI.Primitives.OccasionallyConnected.SignalR",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb",
        WebSockets,
        "ReactiveUI.Primitives.OccasionallyConnected.Web",
    ];
}
