// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

/// <summary>Replaces complete journal files and persists their directory entries.</summary>
internal static
#if NET7_0_OR_GREATER
    partial
#endif
    class FileSystemJournalReplacement
{
    /// <summary>The maximum time spent waiting for another Windows file handle.</summary>
    internal static readonly TimeSpan SharingRetryTimeout = TimeSpan.FromSeconds(2);

    /// <summary>The delay between attempts blocked by a Windows file handle.</summary>
    private static readonly TimeSpan SharingRetryDelay = TimeSpan.FromMilliseconds(25);

    /// <summary>Replaces the destination without copying or deleting it first.</summary>
    /// <param name="source">The flushed replacement in the destination directory.</param>
    /// <param name="destination">The existing journal path.</param>
    /// <param name="cancellationToken">Cancellation before the atomic replacement.</param>
    /// <returns>The replacement task.</returns>
    internal static async ValueTask ReplaceAsync(string source, string destination, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                Replace(source, destination);
                return;
            }
            catch (IOException error) when (IsSharingViolation(error)
                && (Stopwatch.GetTimestamp() - started) / (double)Stopwatch.Frequency < SharingRetryTimeout.TotalSeconds)
            {
                await Task.Delay(SharingRetryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Identifies only the Windows errors for sharing and byte-range locks.</summary>
    /// <param name="error">The failed native operation.</param>
    /// <returns>Whether waiting for another handle may allow replacement.</returns>
    internal static bool IsSharingViolation(IOException error) =>
        Environment.OSVersion.Platform == PlatformID.Win32NT
        && error.HResult is unchecked((int)0x80070020) or unchecked((int)0x80070021);

    /// <summary>Persists the journal name on Unix after creation or replacement.</summary>
    /// <param name="directory">The parent directory containing the journal.</param>
    internal static void FlushDirectory(string directory)
    {
        if (Environment.OSVersion.Platform == PlatformID.Win32NT)
        {
            return;
        }

        var descriptor = NativeMethods.OpenDirectory(directory, 0);
        if (descriptor < 0)
        {
            throw CreateIOException(Marshal.GetLastWin32Error());
        }

        try
        {
            if (NativeMethods.SyncDirectory(descriptor) != 0)
            {
                throw CreateIOException(Marshal.GetLastWin32Error());
            }
        }
        finally
        {
            // Retrying close after EINTR can close a descriptor reused by another thread.
            _ = NativeMethods.CloseDirectory(descriptor);
        }
    }

    /// <summary>Performs one atomic same-directory replacement.</summary>
    /// <param name="source">The flushed replacement file.</param>
    /// <param name="destination">The journal to replace.</param>
    private static void Replace(string source, string destination)
    {
        if (Environment.OSVersion.Platform != PlatformID.Win32NT)
        {
            File.Replace(source, destination, null);
            return;
        }

        // No COPY_ALLOWED: both names must remain on the same volume.
        CheckDeleteSharing(destination);
        CopyDiscretionaryAccessControl(source, destination);
        const int replaceExistingAndWriteThrough = 0x1 | 0x8;
        if (NativeMethods.MoveFile(source, destination, replaceExistingAndWriteThrough) == 0)
        {
            throw CreateIOException(Marshal.GetLastWin32Error());
        }
    }

    /// <summary>Distinguishes a blocked delete share from a permanent access denial.</summary>
    /// <param name="destination">The existing journal.</param>
    private static void CheckDeleteSharing(string destination)
    {
        const int deleteAccess = 0x10000;
        const int shareAll = 7;
        const int openExisting = 3;
        using var handle = NativeMethods.OpenForDelete(destination, deleteAccess, shareAll, IntPtr.Zero, openExisting, 0, IntPtr.Zero);
        if (handle.IsInvalid)
        {
            throw CreateIOException(Marshal.GetLastWin32Error());
        }
    }

    /// <summary>Preserves the committed journal's access rules before publishing new contents.</summary>
    /// <param name="source">The unpublished replacement.</param>
    /// <param name="destination">The committed journal.</param>
    private static void CopyDiscretionaryAccessControl(string source, string destination)
    {
        const uint daclInformation = 4;
        const int insufficientBuffer = 122;
        const int minimumDescriptorLength = 20;
        const int maximumDescriptorLength = 131_072;
        const int invalidSecurityDescriptor = 1338;
        _ = NativeMethods.GetFileSecurity(destination, daclInformation, IntPtr.Zero, 0, out var length);
        var error = Marshal.GetLastWin32Error();
        if (error != insufficientBuffer || length is < minimumDescriptorLength or > maximumDescriptorLength)
        {
            throw CreateIOException(error == insufficientBuffer ? invalidSecurityDescriptor : error);
        }

        var descriptor = Marshal.AllocHGlobal(checked((int)length));
        try
        {
            if (NativeMethods.GetFileSecurity(destination, daclInformation, descriptor, length, out _) == 0)
            {
                throw CreateIOException(Marshal.GetLastWin32Error());
            }

            // SECURITY_DESCRIPTOR_RELATIVE starts with revision, reserved byte, then control.
            const int daclProtected = 0x1000;
            const int controlOffset = 2;
            const uint protectedDaclInformation = 0x80000000;
            const uint unprotectedDaclInformation = 0x20000000;
            var inheritance = (Marshal.ReadInt16(descriptor, controlOffset) & daclProtected) != 0
                ? protectedDaclInformation
                : unprotectedDaclInformation;
            if (NativeMethods.SetFileSecurity(source, daclInformation | inheritance, descriptor) == 0)
            {
                throw CreateIOException(Marshal.GetLastWin32Error());
            }
        }
        finally
        {
            Marshal.FreeHGlobal(descriptor);
        }
    }

    /// <summary>Preserves the native error code for precise retry classification.</summary>
    /// <param name="error">The operating system error.</param>
    /// <returns>The reported I/O failure.</returns>
    private static IOException CreateIOException(int error) =>
        new(new Win32Exception(error).Message, unchecked((int)0x80070000) | error);

    /// <summary>Declares the fixed operating system entry points.</summary>
    private static
#if NET7_0_OR_GREATER
        partial
#endif
        class NativeMethods
    {
#if NET7_0_OR_GREATER
        [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
        [LibraryImport("advapi32.dll", EntryPoint = "GetFileSecurityW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
        internal static partial int GetFileSecurity(string path, uint information, IntPtr descriptor, uint length, out uint required);

        [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
        [LibraryImport("advapi32.dll", EntryPoint = "SetFileSecurityW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
        internal static partial int SetFileSecurity(string path, uint information, IntPtr descriptor);

        [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
        [LibraryImport("kernel32.dll", EntryPoint = "CreateFileW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
        internal static partial SafeFileHandle OpenForDelete(
            string path, int access, int share, IntPtr security, int mode, int flags, IntPtr template);

        [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
        [LibraryImport("kernel32.dll", EntryPoint = "MoveFileExW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
        internal static partial int MoveFile(string source, string destination, int flags);

        [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
        [LibraryImport("libc", EntryPoint = "open", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int OpenDirectory(string path, int flags);

        [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
        [LibraryImport("libc", EntryPoint = "fsync", SetLastError = true)]
        internal static partial int SyncDirectory(int descriptor);

        [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
        [LibraryImport("libc", EntryPoint = "close", SetLastError = true)]
        internal static partial int CloseDirectory(int descriptor);
#else
    /// <summary>Reads the requested file security descriptor.</summary>
    /// <param name="path">The file path.</param>
    /// <param name="information">The requested security sections.</param>
    /// <param name="descriptor">The output buffer.</param>
    /// <param name="length">The output buffer length.</param>
    /// <param name="required">The required buffer length.</param>
    /// <returns>The native success indicator.</returns>
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [DllImport("advapi32.dll", EntryPoint = "GetFileSecurityW", ExactSpelling = true, SetLastError = true, CharSet = CharSet.Unicode)]
    internal static extern int GetFileSecurity(string path, uint information, IntPtr descriptor, uint length, out uint required);

    /// <summary>Applies the requested file security descriptor.</summary>
    /// <param name="path">The file path.</param>
    /// <param name="information">The requested security sections.</param>
    /// <param name="descriptor">The security descriptor.</param>
    /// <returns>The native success indicator.</returns>
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [DllImport("advapi32.dll", EntryPoint = "SetFileSecurityW", ExactSpelling = true, SetLastError = true, CharSet = CharSet.Unicode)]
    internal static extern int SetFileSecurity(string path, uint information, IntPtr descriptor);

    /// <summary>Opens the journal for a delete sharing check.</summary>
    /// <param name="path">The journal path.</param>
    /// <param name="access">The requested native access.</param>
    /// <param name="share">The allowed sharing.</param>
    /// <param name="security">The security attributes.</param>
    /// <param name="mode">The native creation disposition.</param>
    /// <param name="flags">The native flags.</param>
    /// <param name="template">The template handle.</param>
    /// <returns>The owned file handle.</returns>
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [DllImport("kernel32.dll", EntryPoint = "CreateFileW", ExactSpelling = true, SetLastError = true, CharSet = CharSet.Unicode)]
    internal static extern SafeFileHandle OpenForDelete(string path, int access, int share, IntPtr security, int mode, int flags, IntPtr template);

    /// <summary>Atomically renames a file on Windows.</summary>
    /// <param name="source">The source path.</param>
    /// <param name="destination">The destination path.</param>
    /// <param name="flags">The move flags.</param>
    /// <returns>The native success indicator.</returns>
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [DllImport("kernel32.dll", EntryPoint = "MoveFileExW", ExactSpelling = true, SetLastError = true, CharSet = CharSet.Unicode)]
    internal static extern int MoveFile(string source, string destination, int flags);

    /// <summary>Opens a Unix directory.</summary>
    /// <param name="path">The directory path.</param>
    /// <param name="flags">The open flags.</param>
    /// <returns>The owned descriptor.</returns>
    internal static int OpenDirectory(string path, int flags) =>
        OpenDirectoryUtf8(System.Text.Encoding.UTF8.GetBytes(path + '\0'), flags);

    /// <summary>Flushes a Unix directory.</summary>
    /// <param name="descriptor">The owned descriptor.</param>
    /// <returns>The native result.</returns>
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "fsync", ExactSpelling = true, SetLastError = true)]
    internal static extern int SyncDirectory(int descriptor);

    /// <summary>Closes a Unix directory.</summary>
    /// <param name="descriptor">The owned descriptor.</param>
    /// <returns>The native result.</returns>
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "close", ExactSpelling = true, SetLastError = true)]
    internal static extern int CloseDirectory(int descriptor);

    /// <summary>Opens a Unix directory using its null terminated UTF-8 path.</summary>
    /// <param name="path">The encoded path.</param>
    /// <param name="flags">The open flags.</param>
    /// <returns>The owned descriptor.</returns>
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "open", ExactSpelling = true, SetLastError = true)]
    private static extern int OpenDirectoryUtf8(byte[] path, int flags);
#endif
    }
}
