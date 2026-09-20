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
