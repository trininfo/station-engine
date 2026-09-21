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
// FORK-LOCAL FILE. Upstream Zeus has no reverb.

namespace Zeus.Contracts;

/// <summary>
/// The TX plate reverb, in the units a person sets it in.
/// </summary>
/// <remarks>
/// <para>It runs inside WDSP, at the fork's post-CFC insert: after the gate,
/// both EQs, the leveler and the CFC, and ahead of the compressor and ALC,
/// which therefore still guard the peaks the reverb adds. The DSP itself is
/// <c>plate_reverb.dll</c>, loaded only if present — this repository carries
/// the hook and the controls, not the reverb.</para>
///
/// <para>Levels are dB and converted at the engine seam, so a saved profile
/// reads the way the controls do. A level at or below <see cref="FloorDb"/>
/// is off, not merely quiet.</para>
/// </remarks>
/// <param name="Enabled">Off removes the insert from WDSP entirely, so off is
/// bit-exact rather than "mix 0 through a float conversion".</param>
/// <param name="Mix">0..1. The voice holds full level up to 0.5 and only
/// then ducks; a plain crossfade would cost talk power.</param>
/// <param name="DryDb">Trim on the voice after Mix, <see cref="FloorDb"/>..0.</param>
/// <param name="WetDb">Trim on the reverb after Mix, <see cref="FloorDb"/>..0.
/// The control for "subtle".</param>
/// <param name="OutputDb">Overall, -12..+6.</param>
/// <param name="DecaySeconds">Undamped RT60, 0.1..7.</param>
/// <param name="PreDelayMs">0..100.</param>
/// <param name="Damping">0..0.9, higher is darker.</param>
/// <param name="LowCutHz">High-pass on the reverb only, 20..1000.</param>
/// <param name="HighCutHz">Low-pass on the reverb only, 1000..20000.</param>
/// <param name="Diffusion">0..1, lower is a spikier attack.</param>
/// <param name="ModRateHz">0.05..5.</param>
/// <param name="ModDepth">0..2 times Dattorro's excursion; 0 is a still tank.</param>
public sealed record TxReverbConfig(
    bool Enabled,
    double Mix,
    double DryDb,
    double WetDb,
    double OutputDb,
    double DecaySeconds,
    double PreDelayMs,
    double Damping,
    double LowCutHz,
    double HighCutHz,
    double Diffusion,
    double ModRateHz,
    double ModDepth)
{
    public const double FloorDb = -60.0;

    /// <summary>Off, and set up to be subtle the moment it is switched on.</summary>
    public static TxReverbConfig Default { get; } = new(
        Enabled: false, Mix: 0.2, DryDb: 0, WetDb: 0, OutputDb: 0,
        DecaySeconds: 1.5, PreDelayMs: 20, Damping: 0.2,
        LowCutHz: 150, HighCutHz: 6000, Diffusion: 1, ModRateHz: 0.8, ModDepth: 1);

    public bool IsWellFormed =>
        In(Mix, 0, 1) && In(DryDb, FloorDb, 0) && In(WetDb, FloorDb, 0) && In(OutputDb, -12, 6)
        && In(DecaySeconds, 0.1, 7) && In(PreDelayMs, 0, 100) && In(Damping, 0, 0.9)
        && In(LowCutHz, 20, 1000) && In(HighCutHz, 1000, 20000) && LowCutHz < HighCutHz
        && In(Diffusion, 0, 1) && In(ModRateHz, 0.05, 5) && In(ModDepth, 0, 2);

    private static bool In(double v, double lo, double hi) => double.IsFinite(v) && v >= lo && v <= hi;

    /// <summary>dB to linear, with the floor meaning silence.</summary>
    public static double Linear(double db) => db <= FloorDb ? 0.0 : Math.Pow(10, db / 20);
}

public sealed record TxReverbSetRequest(TxReverbConfig Config);
