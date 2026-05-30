// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Specifies the available options for publishing operations, indicating whether actions are performed serially or
/// concurrently.
/// </summary>
/// <remarks>Use this enumeration to control the execution mode of publishing processes. Selecting 'Serial'
/// ensures that operations are performed one after another, while 'Concurrent' allows multiple operations to be
/// executed in parallel. The appropriate option depends on the requirements for performance and resource management in
/// your application.</remarks>
public enum PublishingOption
{
    /// <summary>
    /// Represents a serial communication interface for transmitting and receiving data over serial ports.
    /// </summary>
    /// <remarks>Use this type to interact with devices that communicate via serial protocols, such as RS-232
    /// or UART. Serial communication is commonly used for connecting to hardware peripherals, embedded systems, or
    /// legacy equipment. Thread safety and supported features may vary depending on the implementation.</remarks>
    Serial,

    /// <summary>
    /// Provides thread-safe operations for managing shared resources in concurrent programming scenarios.
    /// </summary>
    /// <remarks>Use this type to coordinate access to data or resources that may be accessed by multiple
    /// threads simultaneously. Thread safety is ensured for all public members, allowing safe usage in multi-threaded
    /// environments without additional synchronization.</remarks>
    Concurrent
}
