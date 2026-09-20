// SPDX-License-Identifier: GPL-2.0-or-later
//
// Zeus — OpenHPSDR Protocol-1 / Protocol-2 client.
// Copyright (C) 2025-2026 Douglas J. Cerrato (KB2UKA),
//                         Christian Suarez (N9WAR), and contributors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 2 of the License, or (at your
// option) any later version. See the LICENSE file at the root of this
// repository for the full text, or https://www.gnu.org/licenses/.
//
// See ATTRIBUTIONS.md at the repository root for the full provenance
// statement and per-component attribution.
//
// The equalizer and TX noise-gate surfaces follow Warren Pratt's (NR0V)
// WDSP eqp/amsq stages and the ten-band convention Thetis drives them with;
// see native/wdsp/eq.c and native/wdsp/amsq.c.
//
// FORK-LOCAL FILE. Upstream Zeus has no REST surface for these stages —
// only SetTXAEQRun was ever bound, with no way to set a profile. Kept in
// its own file rather than appended to Dtos.cs so an upstream merge has
// nothing to collide with.

namespace Zeus.Contracts;

/// <summary>
/// A ten-band graphic equalizer profile, in the shape WDSP's
/// <c>SetTXAGrphEQ10</c> / <c>SetRXAGrphEQ10</c> take it.
/// </summary>
/// <remarks>
/// <para>The band frequencies are NOT configurable: the GrphEQ10 setters
/// write them into the stage themselves (eq.c:692), which is precisely why
/// they are the right entry point for a ten-band UI and
/// <c>SetTXAEQProfile</c> is not. <see cref="BandFrequenciesHz"/> is
/// documentation of what the sliders sit at, not an input.</para>
///
/// <para>Gains are whole dB because the native contract is
/// <c>int[11]</c> — WDSP casts them to double on the way in, so a
/// fractional slider would quantise silently somewhere the operator could
/// not see it.</para>
/// </remarks>
/// <param name="Enabled">Drives SetTXAEQRun / SetRXAEQRun.</param>
/// <param name="PreampDb">Element [0] of the native array: overall gain
/// applied across the band set, not a band of its own.</param>
/// <param name="BandsDb">Ten band gains, low to high.</param>
public sealed record GraphicEqConfig(
    bool Enabled,
    int PreampDb,
    int[] BandsDb)
{
    public const int BandCount = 10;

    /// <summary>Points in WDSP's plotted response curve. Fixed at 1024 by
    /// create_nurbs inside create_eqimp (eq.c:64); GetTXAEQDraw memcpys
    /// that many doubles into the caller's buffers and never says how
    /// many, so this is load-bearing, not advisory.</summary>
    public const int DrawPoints = 1024;

    /// <summary>What the ten sliders are, in Hz. Fixed by eq.c:694-703.</summary>
    public static readonly double[] BandFrequenciesHz =
        [32, 63, 125, 250, 500, 1000, 2000, 4000, 8000, 16000];

    /// <summary>Thetis clamps its EQ sliders to ±12 dB; WDSP itself does
    /// not clamp at all, and a ±40 dB band through a 16384-tap FIR is a
    /// good way to make an unrecoverable mess of a transmission.</summary>
    public const int MinGainDb = -20;
    public const int MaxGainDb = 20;

    /// <summary>Flat and off — what a stage that has never been configured
    /// should look like, rather than WDSP's shaped `default_G` (TXA.c:116),
    /// which is a voice curve nobody asked for.</summary>
    public static GraphicEqConfig Default => new(false, 0, new int[BandCount]);

    public bool IsWellFormed =>
        BandsDb is { Length: BandCount }
        && PreampDb >= MinGainDb && PreampDb <= MaxGainDb
        && Array.TrueForAll(BandsDb, g => g >= MinGainDb && g <= MaxGainDb);

    /// <summary>The native int[11]: [0] preamp, [1..10] bands.</summary>
    public int[] ToNativeArray()
    {
        var a = new int[BandCount + 1];
        a[0] = PreampDb;
        for (int i = 0; i < BandCount; i++) a[i + 1] = BandsDb[i];
        return a;
    }

    /// <summary>Record equality is reference equality on the array, so a
    /// config that differs only in band gains compares equal. Every
    /// apply-on-change path needs this instead.</summary>
    public bool ValueEquals(GraphicEqConfig? other) =>
        other is not null
        && other.Enabled == Enabled
        && other.PreampDb == PreampDb
        && other.BandsDb.Length == BandsDb.Length
        && ((ReadOnlySpan<int>)other.BandsDb).SequenceEqual(BandsDb);
}

/// <summary>
/// TX noise gate — WDSP's AMSQ stage run on the mic, which is what Thetis's
/// TX noise gate is.
/// </summary>
/// <remarks>
/// It sits BEFORE the equalizer in the TX chain (TXA.c:98 then :118), so it
/// gates the raw mic and the EQ shapes whatever survives. That order is not
/// ours to change: every TXA stage reads and writes the same midbuff in the
/// sequence create_txa builds them.
/// </remarks>
/// <param name="Enabled">SetTXAAMSQRun.</param>
/// <param name="ThresholdDb">Level that unmutes, dB. The tail threshold is
/// 0.9x that internally (amsq.c:265), which is where the hysteresis comes
/// from — it is not exposed separately.</param>
/// <param name="MutedGainDb">What the gate attenuates TO, not a hard mute.
/// Negative. A gate at -14 dB still passes room tone 14 dB down, which is
/// far less obvious on air than chopping to digital silence.</param>
public sealed record TxGateConfig(
    bool Enabled,
    double ThresholdDb,
    double MutedGainDb)
{
    public const double MinDb = -60.0;
    public const double MaxDb = 0.0;

    /// <summary>WDSP's own construction values (TXA.c:98-112): unmute at
    /// 0.200 linear and muted gain 0.200 linear, both -13.98 dB. Shipping
    /// the numbers the stage is actually built with beats inventing round
    /// ones.</summary>
    public static TxGateConfig Default => new(false, -13.98, -13.98);

    public bool IsWellFormed =>
        double.IsFinite(ThresholdDb) && double.IsFinite(MutedGainDb)
        && ThresholdDb >= MinDb && ThresholdDb <= MaxDb
        && MutedGainDb >= MinDb && MutedGainDb <= MaxDb;
}

// ---- REST bodies ----------------------------------------------------

public sealed record GraphicEqSetRequest(GraphicEqConfig Config, int Receiver = 0);

public sealed record TxGateSetRequest(TxGateConfig Config);
