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
// FORK-LOCAL FILE. Upstream Zeus never drove WDSP's dexp stage.

namespace Zeus.Contracts;

/// <summary>
/// TX downward expander — WDSP's <c>dexp</c> stage, and what Thetis's
/// operators mean by the TX noise gate.
/// </summary>
/// <remarks>
/// <para>This is NOT <see cref="TxGateConfig"/>. That drives AMSQ, a simple
/// squelch WDSP builds into TXA. DEXP is a proper expander with attack,
/// hold, release, a ratio, hysteresis, an optional side-channel filter to
/// pick what triggers it, and an optional look-ahead delay. A Thetis
/// profile's gate settings are DEXP settings; AMSQ cannot reproduce
/// them.</para>
///
/// <para>Fields are in the units Thetis's panel shows — milliseconds and
/// dB — and converted at the WDSP seam, because that is where Thetis
/// converts them too (setup.cs: ms/1000, and <c>pow(10, dB/20)</c> for the
/// ratios and the threshold). Storing WDSP's linear values instead would
/// make an imported profile unreadable next to the Thetis panel it came
/// from.</para>
///
/// <para>WDSP keeps DEXP instances in a plain global array and the engine
/// creates exactly one, for TX. It runs on the mic ahead of TXA — the seam
/// ChannelMaster uses in Thetis — and therefore ahead of the TX audio
/// plugin chain as well, so an effect added there keeps its tail instead of
/// being expanded away.</para>
/// </remarks>
/// <param name="Enabled">Run the expander in the audio path.</param>
/// <param name="ThresholdDb">Level that opens it. Thetis calls this the VOX
/// threshold and sends <c>pow(10, dB/20)</c> to SetDEXPAttackThreshold.</param>
/// <param name="AttackMs">Time to open.</param>
/// <param name="HoldMs">How long it stays open after the trigger drops.</param>
/// <param name="ReleaseMs">Time to close once the hold expires.</param>
/// <param name="DetectorTauMs">Smoothing on the level detector.</param>
/// <param name="ExpansionRatioDb">How far it ducks: sent as
/// <c>pow(10, dB/20)</c>.</param>
/// <param name="HysteresisRatioDb">Gap between the open and close
/// thresholds. Sent as <c>pow(10, -dB/20)</c> — note the SIGN; it is the
/// one conversion in this record that is not the same as the others.</param>
/// <param name="SideChannelFilterEnabled">Trigger on a filtered copy of the
/// mic rather than the mic itself, so (for example) only speech band energy
/// opens the gate.</param>
/// <param name="SideChannelLowCutHz">Side-channel filter low cut.</param>
/// <param name="SideChannelHighCutHz">Side-channel filter high cut.</param>
/// <param name="LookAheadEnabled">Delay the audio so the expander can open
/// before the sound that triggered it arrives — no clipped word openings.</param>
/// <param name="LookAheadMs">That delay.</param>
public sealed record TxDexpConfig(
    bool Enabled,
    double ThresholdDb,
    double AttackMs,
    double HoldMs,
    double ReleaseMs,
    double DetectorTauMs,
    double ExpansionRatioDb,
    double HysteresisRatioDb,
    bool SideChannelFilterEnabled,
    double SideChannelLowCutHz,
    double SideChannelHighCutHz,
    bool LookAheadEnabled,
    double LookAheadMs)
{
    /// <summary>WDSP's own construction values (TXA-side create_dexp in
    /// ChannelMaster): 10 ms detector, 25 ms attack, 100 ms release, 1 s
    /// hold, ratio 4.0 and hysteresis 0.75 linear, threshold 0.05, and a
    /// 1000-2000 Hz side channel, all with the stage switched off.</summary>
    public static TxDexpConfig Default => new(
        Enabled: false,
        ThresholdDb: -26.02,          // 0.050 linear
        AttackMs: 25.0,
        HoldMs: 1000.0,
        ReleaseMs: 100.0,
        DetectorTauMs: 10.0,
        ExpansionRatioDb: 12.04,      // 4.000 linear
        HysteresisRatioDb: 2.50,      // 0.750 linear, negated on the way in
        SideChannelFilterEnabled: false,
        SideChannelLowCutHz: 1000.0,
        SideChannelHighCutHz: 2000.0,
        LookAheadEnabled: true,
        LookAheadMs: 60.0);

    public const double MinThresholdDb = -100.0;
    public const double MaxThresholdDb = 0.0;
    /// <summary>WDSP allocates the look-ahead delay line from this, so it is
    /// a memory bound as well as a taste one.</summary>
    public const double MaxLookAheadMs = 500.0;

    public bool IsWellFormed =>
        Fin(ThresholdDb) && ThresholdDb >= MinThresholdDb && ThresholdDb <= MaxThresholdDb
        && Ms(AttackMs) && Ms(HoldMs) && Ms(ReleaseMs) && Ms(DetectorTauMs)
        && Fin(ExpansionRatioDb) && ExpansionRatioDb >= 0.0 && ExpansionRatioDb <= 60.0
        && Fin(HysteresisRatioDb) && HysteresisRatioDb >= 0.0 && HysteresisRatioDb <= 20.0
        && Fin(SideChannelLowCutHz) && Fin(SideChannelHighCutHz)
        && SideChannelLowCutHz >= 0.0
        && SideChannelHighCutHz > SideChannelLowCutHz
        && SideChannelHighCutHz <= 20000.0
        && Fin(LookAheadMs) && LookAheadMs >= 0.0 && LookAheadMs <= MaxLookAheadMs;

    private static bool Fin(double v) => double.IsFinite(v);
    private static bool Ms(double v) => double.IsFinite(v) && v >= 0.0 && v <= 5000.0;

    // ---- the conversions, in one place ------------------------------
    // Each mirrors setup.cs, so a value shown in Poseidon and the same
    // value shown in Thetis reach WDSP identically.

    public double ThresholdLinear => Math.Pow(10.0, ThresholdDb / 20.0);
    public double ExpansionRatioLinear => Math.Pow(10.0, ExpansionRatioDb / 20.0);
    /// <summary>Negated exponent — Thetis sends pow(10, -dB/20) here.</summary>
    public double HysteresisRatioLinear => Math.Pow(10.0, -HysteresisRatioDb / 20.0);
    public double AttackSec => AttackMs / 1000.0;
    public double HoldSec => HoldMs / 1000.0;
    public double ReleaseSec => ReleaseMs / 1000.0;
    public double DetectorTauSec => DetectorTauMs / 1000.0;
    public double LookAheadSec => LookAheadMs / 1000.0;
}

public sealed record TxDexpSetRequest(TxDexpConfig Config);
