# ReactiveUI.Primitives.OccasionallyConnected remaining tasks

- Run the coverage, crash, soak, and performance matrix on Linux, Windows, and macOS across supported target frameworks. Local Windows gates pass. The 17,138-test solution suite and the WebSocket, DI, and FileSystem suites pass on `net8.0` through `net11.0`.
- Run the NativeAOT publish-and-execute gate on a Windows x64 host with the Visual Studio C++ linker installed. The local package gate passed its other checks but skipped NativeAOT because this toolchain is unavailable.
- Complete release acceptance by checking API compatibility against the previous stable package, freezing the public API, protocol, store, and metric policies for RC1, and verifying preview/RC installation and upgrade/rollback behavior.
