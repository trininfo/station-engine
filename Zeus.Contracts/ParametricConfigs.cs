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
// FORK-LOCAL FILE. The parametric equalizer and CFC come from the Thetis
// WDSP port (ramdor/Thetis, GPL-2.0-or-later); these carry their settings.

namespace Zeus.Contracts;

/// <summary>
/// One point of a parametric curve: where it sits, how far it moves, and
/// how sharp it is.
/// </summary>
/// <remarks>
/// Matches Thetis's own stored shape one for one — its TX profiles keep
/// <c>{frequency_hz, gain_db, q}</c> per point — so a profile exported from
/// Thetis maps across without reinterpretation.
/// </remarks>
public sealed record ParametricPoint(double FrequencyHz, double GainDb, double Q)
{
    public bool IsWellFormed =>
        double.IsFinite(FrequencyHz) && FrequencyHz >= 0.0
        && double.IsFinite(GainDb) && GainDb >= -40.0 && GainDb <= 40.0
        && double.IsFinite(Q) && Q > 0.0 && Q <= 100.0;
}

/// <summary>
/// A parametric equalizer curve, in the shape WDSP's
/// <c>SetTXAEQProfile</c> / <c>SetRXAEQProfile</c> take it after the Thetis
/// port.
/// </summary>
/// <remarks>
/// <para><paramref name="GlobalGainDb"/> is NOT a band. It becomes element
/// [0] of the native arrays (with frequency 0 and Q 0), which is how Thetis
/// passes its preamp — see the comment on the P/Invoke.</para>
///
/// <para>Thetis writes 5, 10 or 18 points. Nothing here fixes the count:
/// the stage takes whatever it is given, and a profile imported from a
/// Thetis database keeps its own.</para>
/// </remarks>
public sealed record ParametricEqConfig(
    bool Enabled,
    double GlobalGainDb,
    ParametricPoint[] Points)
{
    /// <summary>Thetis's largest band count, and a sane ceiling: every
    /// point is a filter the FIR designer has to resolve.</summary>
    public const int MaxPoints = 64;

    public static ParametricEqConfig Default => new(false, 0.0, []);

    public bool IsWellFormed =>
        Points is not null
        && Points.Length <= MaxPoints
        && double.IsFinite(GlobalGainDb) && GlobalGainDb >= -40.0 && GlobalGainDb <= 40.0
        && Array.TrueForAll(Points, p => p is not null && p.IsWellFormed);

    /// <summary>
    /// The native arrays: nfreqs+1 long, element [0] carrying the preamp.
    /// </summary>
    public void ToNativeArrays(out double[] f, out double[] g, out double[] q)
    {
        int n = Points.Length;
        f = new double[n + 1];
        g = new double[n + 1];
        q = new double[n + 1];
        f[0] = 0.0;
        g[0] = GlobalGainDb;
        q[0] = 0.0;
        for (int i = 0; i < n; i++)
        {
            f[i + 1] = Points[i].FrequencyHz;
            g[i + 1] = Points[i].GainDb;
            q[i + 1] = Points[i].Q;
        }
    }

    /// <summary>Arrays make record equality reference equality, so every
    /// apply-on-change path needs this instead.</summary>
    public bool ValueEquals(ParametricEqConfig? o) =>
        o is not null
        && o.Enabled == Enabled
        && o.GlobalGainDb.Equals(GlobalGainDb)
        && o.Points.Length == Points.Length
        && !Points.Where((p, i) => !p.Equals(o.Points[i])).Any();
}

/// <summary>
/// The CFC as Thetis drives it: two parametric curves, compression and
/// post-EQ, and the two flat gains that go with them.
/// </summary>
/// <remarks>
/// <para>The native call takes ONE frequency array for both curves.
/// Thetis's own CFC form passes the compression curve's frequencies and
/// discards the post-EQ curve's — so the post-EQ gains land on the
/// compression frequencies. That is reproduced here rather than corrected,
/// because matching Thetis is the point; <paramref name="PostEq"/>'s own
/// frequencies are carried for round-tripping and ignored on apply.</para>
///
/// <para><c>Compression.GlobalGainDb</c> is Thetis's Pre-comp and
/// <c>PostEq.GlobalGainDb</c> its Pre-PEQ, both flat gains rather than
/// per-band.</para>
/// </remarks>
public sealed record ParametricCfcConfig(
    bool Enabled,
    bool PostEqEnabled,
    ParametricEqConfig Compression,
    ParametricEqConfig PostEq)
{
    public static ParametricCfcConfig Default =>
        new(false, false, ParametricEqConfig.Default, ParametricEqConfig.Default);

    public bool IsWellFormed =>
        Compression is { } c && PostEq is { } p
        && c.IsWellFormed && p.IsWellFormed
        && c.Points.Length == p.Points.Length;   // one shared frequency set

    public bool ValueEquals(ParametricCfcConfig? o) =>
        o is not null
        && o.Enabled == Enabled
        && o.PostEqEnabled == PostEqEnabled
        && Compression.ValueEquals(o.Compression)
        && PostEq.ValueEquals(o.PostEq);
}

// ---- REST bodies ----------------------------------------------------

public sealed record ParametricEqSetRequest(ParametricEqConfig Config);

public sealed record ParametricCfcSetRequest(ParametricCfcConfig Config);
