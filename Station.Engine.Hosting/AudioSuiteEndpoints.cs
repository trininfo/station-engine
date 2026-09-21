// SPDX-License-Identifier: GPL-2.0-or-later
//
// FORK-LOCAL FILE. Upstream Zeus exposes no REST for the equalizer or the
// TX noise gate — only SetTXAEQRun was ever bound, with no way to give the
// stage a curve. Kept in its own endpoints file so an upstream merge has
// nothing to collide with.

using Zeus.Contracts;

namespace Zeus.Server;

/// <summary>
/// TX Audio Suite control: the ten-band TX and RX equalizers and the TX
/// noise gate, driven through WDSP's own entry points.
/// </summary>
/// <remarks>
/// Every stage here is OPTIONAL and ships off. WDSP builds them into the
/// TX chain in a fixed order — gate, then EQ, then preemph, leveler, CFC,
/// compressor, ALC (TXA.c create_txa) — and nothing in this API changes
/// that order, because every stage reads and writes the same midbuff in
/// the sequence it was constructed. What the operator chooses is which of
/// them run at all.
/// </remarks>
public static class AudioSuiteEndpoints
{
    public static IEndpointRouteBuilder MapAudioSuiteEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        var log = endpoints.ServiceProvider.GetRequiredService<ILogger<object>>();

        endpoints.MapPost("/api/tx/eq", (GraphicEqSetRequest req, RadioService r) =>
        {
            if (req?.Config is not { } cfg)
                return Results.BadRequest(new { error = "Config required" });
            if (!cfg.IsWellFormed)
                return Results.BadRequest(new { error = EqError(cfg) });
            log.LogInformation(
                "api.tx.eq enabled={On} preamp={Preamp}dB bands=[{Bands}]",
                cfg.Enabled, cfg.PreampDb, string.Join(",", cfg.BandsDb));
            return Results.Ok(r.SetTxEq(cfg));
        });

        endpoints.MapPost("/api/rx/eq", (GraphicEqSetRequest req, RadioService r) =>
        {
            if (req?.Config is not { } cfg)
                return Results.BadRequest(new { error = "Config required" });
            if (!cfg.IsWellFormed)
                return Results.BadRequest(new { error = EqError(cfg) });
            log.LogInformation(
                "api.rx.eq enabled={On} preamp={Preamp}dB", cfg.Enabled, cfg.PreampDb);
            return Results.Ok(r.SetRxEq(cfg));
        });

        endpoints.MapPost("/api/tx/gate", (TxGateSetRequest req, RadioService r) =>
        {
            if (req?.Config is not { } cfg)
                return Results.BadRequest(new { error = "Config required" });
            if (!cfg.IsWellFormed)
                return Results.BadRequest(new
                {
                    error = $"threshold and muted gain must be "
                          + $"{TxGateConfig.MinDb:F0}..{TxGateConfig.MaxDb:F0} dB"
                });
            log.LogInformation(
                "api.tx.gate enabled={On} thresh={Thresh:F1}dB muted={Muted:F1}dB",
                cfg.Enabled, cfg.ThresholdDb, cfg.MutedGainDb);
            return Results.Ok(r.SetTxGate(cfg));
        });

        // The TX downward expander — Thetis's TX noise gate. Values are in
        // the units its panel shows (ms, dB); the conversion to WDSP's
        // seconds and linear ratios happens at the engine seam, as in
        // Thetis's own setup.cs.
        // FORK: the TX plate reverb. 409 rather than a silent accept when
        // plate_reverb.dll is not loaded: a control that does nothing is
        // worse than one that says why.
        endpoints.MapPost("/api/tx/reverb", (TxReverbSetRequest req, RadioService r) =>
        {
            if (req?.Config is not { } cfg)
                return Results.BadRequest(new { error = "Config required" });
            if (!cfg.IsWellFormed)
                return Results.BadRequest(new
                {
                    error = "mix 0..1, dry/wet -60..0 dB, output -12..6 dB, decay 0.1..7 s, "
                          + "pre-delay 0..100 ms, damping 0..0.9, low cut 20..1000 Hz below "
                          + "high cut 1000..20000 Hz, diffusion 0..1, mod 0.05..5 Hz x0..2"
                });
            if (cfg.Enabled && !Zeus.Dsp.Wdsp.WdspDspEngine.TxReverbLibraryAvailable)
                return Results.Conflict(new
                {
                    error = "plate_reverb.dll is not loaded: put it next to the engine or set PLATE_REVERB_LIB"
                });
            log.LogInformation(
                "api.tx.reverb enabled={On} mix={Mix:F2} wet={Wet:F1}dB decay={Decay:F2}s",
                cfg.Enabled, cfg.Mix, cfg.WetDb, cfg.DecaySeconds);
            return Results.Ok(r.SetTxReverb(cfg));
        });

        endpoints.MapPost("/api/tx/dexp", (TxDexpSetRequest req, RadioService r) =>
        {
            if (req?.Config is not { } cfg)
                return Results.BadRequest(new { error = "Config required" });
            if (!cfg.IsWellFormed)
                return Results.BadRequest(new
                {
                    error = "threshold -100..0 dB, times 0..5000 ms, ratio 0..60 dB, "
                          + "hysteresis 0..20 dB, side-channel high cut above low cut, "
                          + $"look-ahead 0..{TxDexpConfig.MaxLookAheadMs:F0} ms"
                });
            log.LogInformation(
                "api.tx.dexp enabled={On} thresh={Thresh:F1}dB ratio={Ratio:F1}dB scf={Scf}",
                cfg.Enabled, cfg.ThresholdDb, cfg.ExpansionRatioDb, cfg.SideChannelFilterEnabled);
            return Results.Ok(r.SetTxDexp(cfg));
        });

        /* ---- parametric (Q) profiles, from the Thetis WDSP port ------
         *
         * These and the ten-band routes above drive the SAME stages; the
         * last write wins. A client should offer one editor or the other.
         */

        endpoints.MapPost("/api/tx/eq/parametric", (ParametricEqSetRequest req, RadioService r) =>
        {
            if (req?.Config is not { } cfg)
                return Results.BadRequest(new { error = "Config required" });
            if (!cfg.IsWellFormed)
                return Results.BadRequest(new { error = ParametricError(cfg) });
            log.LogInformation(
                "api.tx.eq.parametric enabled={On} points={N} preamp={Preamp:F1}dB",
                cfg.Enabled, cfg.Points.Length, cfg.GlobalGainDb);
            return Results.Ok(r.SetTxEqParametric(cfg));
        });

        endpoints.MapPost("/api/rx/eq/parametric", (ParametricEqSetRequest req, RadioService r) =>
        {
            if (req?.Config is not { } cfg)
                return Results.BadRequest(new { error = "Config required" });
            if (!cfg.IsWellFormed)
                return Results.BadRequest(new { error = ParametricError(cfg) });
            log.LogInformation(
                "api.rx.eq.parametric enabled={On} points={N}", cfg.Enabled, cfg.Points.Length);
            return Results.Ok(r.SetRxEqParametric(cfg));
        });

        endpoints.MapPost("/api/tx/cfc/parametric", (ParametricCfcSetRequest req, RadioService r) =>
        {
            if (req?.Config is not { } cfg)
                return Results.BadRequest(new { error = "Config required" });
            if (cfg.Compression is null || cfg.PostEq is null)
                return Results.BadRequest(new { error = "both Compression and PostEq curves are required" });
            if (cfg.Compression.Points.Length != cfg.PostEq.Points.Length)
                return Results.BadRequest(new
                {
                    error = "the compression and post-EQ curves must have the same number of points — "
                          + "the native profile takes one shared frequency set"
                });
            if (!cfg.IsWellFormed)
                return Results.BadRequest(new { error = ParametricError(cfg.Compression) });
            log.LogInformation(
                "api.tx.cfc.parametric enabled={On} peq={Peq} points={N}",
                cfg.Enabled, cfg.PostEqEnabled, cfg.Compression.Points.Length);
            return Results.Ok(r.SetCfcParametric(cfg));
        });

        // The response curve WDSP built for the stage, so the panel plots
        // what is actually running rather than redrawing the sliders as a
        // curve. Returns 503 rather than an empty array when there is no
        // channel — an empty curve and a flat curve are different claims.
        endpoints.MapGet("/api/tx/eq/curve", (DspPipelineService pipe) =>
            EqCurve(pipe, transmit: true));

        endpoints.MapGet("/api/rx/eq/curve", (DspPipelineService pipe) =>
            EqCurve(pipe, transmit: false));

        // The band frequencies are WDSP's, not ours, and a client that
        // hardcodes them drifts the day upstream changes eq.c. Hand them
        // over with the limits the sliders should honour.
        endpoints.MapGet("/api/audio-suite/eq/bands", () => Results.Ok(new
        {
            frequenciesHz = GraphicEqConfig.BandFrequenciesHz,
            bandCount = GraphicEqConfig.BandCount,
            minGainDb = GraphicEqConfig.MinGainDb,
            maxGainDb = GraphicEqConfig.MaxGainDb,
            drawPoints = GraphicEqConfig.DrawPoints,
        }));

        return endpoints;

        static string ParametricError(ParametricEqConfig cfg) =>
            cfg.Points is null || cfg.Points.Length > ParametricEqConfig.MaxPoints
                ? $"at most {ParametricEqConfig.MaxPoints} points"
                : "each point needs a finite frequency >= 0, gain within +/-40 dB and Q in (0, 100]";

        static string EqError(GraphicEqConfig cfg) =>
            cfg.BandsDb is not { Length: GraphicEqConfig.BandCount }
                ? $"BandsDb must have exactly {GraphicEqConfig.BandCount} entries; "
                  + $"got {cfg.BandsDb?.Length ?? 0}"
                : $"gains must be {GraphicEqConfig.MinGainDb}..{GraphicEqConfig.MaxGainDb} dB";

        static IResult EqCurve(DspPipelineService pipe, bool transmit)
        {
            if (pipe.CurrentEngine is not { } engine)
                return Results.Json(new { error = "no DSP engine" }, statusCode: 503);

            var x = new double[GraphicEqConfig.DrawPoints];
            var y = new double[GraphicEqConfig.DrawPoints];
            if (!engine.TryGetEqDraw(transmit, 0, x, y))
                return Results.Json(new { error = "no equalizer channel" }, statusCode: 503);

            return Results.Ok(new { points = x.Length, xHz = x, yDb = y });
        }
    }
}
