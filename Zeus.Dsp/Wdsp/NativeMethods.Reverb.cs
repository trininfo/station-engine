// SPDX-License-Identifier: GPL-2.0-or-later
//
// FORK-LOCAL FILE. The one WDSP import the plate reverb needs, kept out of
// NativeMethods.cs so an upstream merge of that file has nothing of ours in
// it to conflict with.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Zeus.Dsp.Wdsp;

internal static partial class NativeMethods
{
    /// <summary>
    /// Install (fn != 0) or remove (fn == 0) WDSP's post-CFC insert on a TX
    /// channel. fn is <c>void (*)(void *ctx, double *iq, int frames)</c> and
    /// runs on WDSP's DSP thread, inside csDSP; this call takes the same lock,
    /// so once it returns no block is still using the old ctx. Fork-local:
    /// native/wdsp/TXA.c.
    /// </summary>
    [LibraryImport(LibraryName)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void SetTXAPostCfcInsert(int channel, nint fn, nint ctx);
}
