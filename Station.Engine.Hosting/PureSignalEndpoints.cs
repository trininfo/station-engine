// SPDX-License-Identifier: GPL-2.0-or-later

using Zeus.Contracts;

namespace Zeus.Server;

/// <summary>Maps PureSignal control and correction-file routes.</summary>
public static class PureSignalEndpoints
{
    public static IEndpointRouteBuilder MapPureSignalEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        var log = endpoints.ServiceProvider.GetRequiredService<ILogger<object>>();

        // PureSignal master arm + cal-mode. RadioService.SetPs sets the
        // StateDto bit; DspPipelineService then sequences the active P1 or P2
        // feedback wire path before arming the WDSP engine.
        endpoints.MapPost("/api/tx/ps", (PsControlSetRequest req, RadioService r) =>
        {
            log.LogInformation(
                "api.tx.ps enabled={On} auto={Auto} single={Single}",
                req.Enabled, req.Auto, req.Single);
            return Results.Ok(r.SetPs(req));
        });

        endpoints.MapPost("/api/tx/ps/advanced", (PsAdvancedSetRequest req, RadioService r) =>
        {
            if (req.HwPeak is double p && (p <= 0.0 || p > 2.0 || double.IsNaN(p)))
                return Results.BadRequest(new { error = "hwPeak must be in (0, 2]" });
            if (req.MoxDelaySec is double mox && (mox < PsTimingLimits.MinMoxDelaySec || mox > PsTimingLimits.MaxMoxDelaySec || double.IsNaN(mox)))
                return Results.BadRequest(new { error = $"moxDelaySec must be {PsTimingLimits.MinMoxDelaySec:F1}..{PsTimingLimits.MaxMoxDelaySec:F1}" });
            if (req.LoopDelaySec is double loop && (loop < PsTimingLimits.MinLoopDelaySec || loop > PsTimingLimits.MaxLoopDelaySec || double.IsNaN(loop)))
                return Results.BadRequest(new { error = $"loopDelaySec must be {PsTimingLimits.MinLoopDelaySec:F0}..{PsTimingLimits.MaxLoopDelaySec:F0}" });
            if (req.AmpDelayNs is double amp && (amp < 0.0 || double.IsNaN(amp) || double.IsInfinity(amp)))
                return Results.BadRequest(new { error = $"ampDelayNs must be 0..{PsTimingLimits.MaxAmpDelayNs:F0}" });
            if ((req.Ints is null) != (req.Spi is null))
                return Results.BadRequest(new { error = "ints and spi must be set together" });
            if (req.Ints is int ints && req.Spi is int spi && !PsCalccLayout.IsAllowed(ints, spi))
                return Results.BadRequest(new { error = "ints/spi must be 16/256, 8/512 or 4/1024" });
            log.LogInformation("api.tx.ps.advanced");
            return Results.Ok(r.SetPsAdvanced(req));
        });

        // PS feedback antenna selector. Internal coupler vs External (Bypass).
        // On G2/MkII this flips ALEX_RX_ANTENNA_BYPASS in alex0 during xmit + PS
        // armed. WDSP cal/iqc are unaffected — same DDC0/DDC1 paired feed either
        // way; only the radio routes a different physical signal into DDC0.
        endpoints.MapPost("/api/tx/ps/feedback-source",
            (PsFeedbackSourceSetRequest req, RadioService r) =>
        {
            log.LogInformation("api.tx.ps.feedbackSource source={Source}", req.Source);
            return Results.Ok(r.SetPsFeedbackSource(req));
        });

        // Manual PS TX feedback attenuation — routes through DspPipelineService
        // (it owns both the P1 and P2 clients) to push the wire byte, then
        // persists + surfaces it via RadioService. Operator alternative to
        // AutoAttenuate for a fixed external-tap chain.
        endpoints.MapPost("/api/tx/ps/feedback-attenuation",
            (PsFeedbackAttenuationSetRequest req, DspPipelineService pipe, RadioService r) =>
        {
            log.LogInformation("api.tx.ps.feedbackAttenuation db={Db}", req.Db);
            pipe.SetPsFeedbackAttenuationDb(req.Db);
            return Results.Ok(r.Snapshot());
        });

        // PS-Monitor — operator-facing toggle that swaps the TX panadapter source
        // from the predistorted-IQ analyzer to the PS-feedback (post-PA) analyzer.
        // Pure UI/source-routing flag; no WDSP setter, no wire-format change.
        // Default off and session-only; unlike the distinct PS ArmIntent, this
        // display-source preference is never rehydrated. See issue #121.
        endpoints.MapPost("/api/tx/ps/monitor",
            (PsMonitorSetRequest req, RadioService r) =>
        {
            log.LogInformation("api.tx.ps.monitor enabled={Enabled}", req.Enabled);
            return Results.Ok(r.SetPsMonitor(req));
        });

        endpoints.MapPost("/api/tx/ps/reset", (DspPipelineService pipe) =>
        {
            log.LogInformation("api.tx.ps.reset");
            pipe.CurrentEngine?.ResetPs();
            return Results.Ok(new { reset = true });
        });

        endpoints.MapPost("/api/tx/ps/save", (PsSaveRequest req, DspPipelineService pipe) =>
        {
            if (string.IsNullOrWhiteSpace(req.Filename))
                return Results.BadRequest(new { error = "filename required" });
            log.LogInformation("api.tx.ps.save filename={Filename}", req.Filename);
            pipe.CurrentEngine?.SavePsCorrection(req.Filename);
            return Results.Ok(new { saved = req.Filename });
        });

        endpoints.MapPost("/api/tx/ps/restore", (PsRestoreRequest req, DspPipelineService pipe) =>
        {
            if (string.IsNullOrWhiteSpace(req.Filename))
                return Results.BadRequest(new { error = "filename required" });
            log.LogInformation("api.tx.ps.restore filename={Filename}", req.Filename);
            pipe.CurrentEngine?.RestorePsCorrection(req.Filename);
            return Results.Ok(new { restored = req.Filename });
        });

        return endpoints;
    }
}
