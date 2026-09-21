// SPDX-License-Identifier: GPL-2.0-or-later
//
// FORK-LOCAL FILE. Upstream applies one fixed 100 ms average to BOTH RX
// display outputs, so the waterfall line it sends is exactly as smoothed as
// the panadapter line, and no client can change either. Kept in its own
// endpoints file so an upstream merge has nothing to collide with.

namespace Zeus.Server;

public sealed record RxDisplayAveragingRequest(double PanTauMs, double WfTauMs);

public static class RxDisplayEndpoints
{
    public const double MaxTauMs = 2000.0;

    public static IEndpointRouteBuilder MapRxDisplayEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var log = endpoints.ServiceProvider.GetRequiredService<ILogger<object>>();

        endpoints.MapGet("/api/rx/display-averaging", (DspPipelineService pipe) =>
        {
            var (pan, wf) = pipe.RxDisplayAveraging;
            return Results.Ok(new { panTauMs = pan, wfTauMs = wf });
        });

        endpoints.MapPut("/api/rx/display-averaging", (RxDisplayAveragingRequest req, DspPipelineService pipe) =>
        {
            static bool Ok(double v) => double.IsFinite(v) && v >= 0 && v <= MaxTauMs;
            if (req is null || !Ok(req.PanTauMs) || !Ok(req.WfTauMs))
                return Results.BadRequest(new { error = $"panTauMs and wfTauMs must be 0..{MaxTauMs:F0}" });
            log.LogInformation("api.rx.displayAveraging pan={Pan:F0}ms wf={Wf:F0}ms", req.PanTauMs, req.WfTauMs);
            pipe.ApplyRxDisplayAveraging(req.PanTauMs, req.WfTauMs);
            var (pan, wf) = pipe.RxDisplayAveraging;
            return Results.Ok(new { panTauMs = pan, wfTauMs = wf });
        });

        return endpoints;
    }
}
