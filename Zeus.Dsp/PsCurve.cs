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
// PureSignal display surfaces follow Warren Pratt's (NR0V) WDSP calcc
// display buffers; see native/wdsp/calcc.c GetPSDisp and the disp.* members
// it copies from.

namespace Zeus.Dsp;

/// <summary>
/// One PureSignal correction curve, as calcc last fitted it — the data
/// behind the AM/AM and AM/PM plots.
/// </summary>
/// <remarks>
/// <para>Sampled from <c>GetPSDisp</c> (native/wdsp/calcc.c:2282), which
/// refreshes its display buffers at the end of every accepted calc() pass.
/// A curve therefore changes only when a fit lands, which is why the hub
/// broadcasts it on a <c>CalibrationAttempts</c> edge rather than on a
/// timer.</para>
///
/// <para>The x axis of both curves is implicit: point k of
/// <paramref name="MagCorrection"/> and <paramref name="PhaseDeg"/> sits at
/// <c>k / (Points - 1)</c> of normalised drive, exactly as calcc builds
/// them (calcc.c:1684-1730). It is not serialised.</para>
///
/// <para>The scatter is calcc's own collected sample set, decimated. Its
/// x is the normalised FEEDBACK magnitude (<c>rx_scale * rx_c</c>), not the
/// TX envelope — calcc indexes its fit by what came back from the PA
/// (calcc.c:1173).</para>
/// </remarks>
/// <param name="MagCorrection">calcc's <c>ym_cor</c>: the TX/RX magnitude
/// ratio the predistorter applies, against normalised drive. Flat means a
/// linear PA; rising at the top means gain folding that PureSignal is
/// compensating for.</param>
/// <param name="PhaseDeg">calcc's <c>ya_cor</c>: unwrapped phase correction
/// in degrees, offset so full drive reads 0. This is the AM/PM curve.</param>
/// <param name="ScatterX">Decimated <c>disp.x</c> — normalised feedback
/// magnitude per collected sample.</param>
/// <param name="ScatterMag">Decimated <c>disp.ym</c> — magnitude ratio for
/// the same samples, the cloud the magnitude curve is fitted through.</param>
/// <param name="ScatterPhaseDeg">Per-sample phase, degrees, from
/// <c>atan2(disp.ys, disp.yc)</c>.</param>
/// <param name="PhsRefDeg">calcc's <c>phs_ref_deg</c> — the PA's total phase
/// shift at full drive before the curve is re-centred on zero. The one
/// number here that says something about the amplifier rather than about
/// the correction.</param>
/// <param name="CalibrationAttempts">info[5] at the time of capture, so a
/// client can tell a fresh curve from a repeat of the one it already has.</param>
public sealed record PsCurve(
    float[] MagCorrection,
    float[] PhaseDeg,
    float[] ScatterX,
    float[] ScatterMag,
    float[] ScatterPhaseDeg,
    float PhsRefDeg,
    int CalibrationAttempts)
{
    /// <summary>calcc's DISP_PTS (calcc.c:308). Fixed, not negotiable.</summary>
    public const int CurvePoints = 512;

    /// <summary>calcc's collection size: SAMPLE_NBUCKS (16) buckets of 256
    /// samples each (calcc.c:339-369). The native buffers GetPSDisp memcpys
    /// into MUST be at least this long — it writes <c>disp.nsamps</c> doubles
    /// before it tells the caller what nsamps was.</summary>
    public const int NativeSampleCount = 4096;

    /// <summary>Scatter points kept for the wire. Every 8th native sample,
    /// which keeps 32 from each of calcc's 16 buckets and so preserves the
    /// density the operator is looking at. 4096 points at f32 would be 48 KB
    /// a fit for a cloud nobody can resolve on screen.</summary>
    public const int ScatterPoints = 512;

    public int Points => MagCorrection.Length;
}
