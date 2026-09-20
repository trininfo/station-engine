// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Douglas J. Cerrato (KB2UKA) and contributors.

using System.Globalization;
using System.Net;
using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.Extensions.Primitives;
using Zeus.Contracts;
using Zeus.Server.Diagnostics;
using Zeus.Server.SpeTaurus;
using Zeus.Server.Tdoa;

namespace Zeus.Server;

/// <summary>Maps the standalone station engine's HTTP and WebSocket surface.</summary>
public static class StationEngineEndpoints
{
    internal static readonly HashSet<string> AllowedBrowserOrigins = new(StringComparer.Ordinal)
    {
        "https://app.zeussdr.com",
        // Staging Pages alias for the develop-branch SPA. Production stays
        // app.zeussdr.com, which deploys only from main; this alias lets a
        // staging-channel engine install serve the develop web app without a
        // local dev server.
        "https://staging.zeus-web-app-7ag.pages.dev",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    };

    public static void ConfigureCors(
        CorsPolicyBuilder policy,
        bool allowLanSameHost = false,
        bool allowLanHttpsSameHost = false,
        Func<string?>? requestHost = null)
    {
        ArgumentNullException.ThrowIfNull(policy);
        policy
            .SetIsOriginAllowed(origin => IsBrowserOriginAllowed(
                origin,
                allowLanSameHost,
                allowLanHttpsSameHost,
                requestHost?.Invoke()))
            .AllowAnyHeader()
            .AllowAnyMethod()
            // navigator.sendBeacon (workspace-layout unload flush) always sends
            // a credentialed request, and its application/json Blob forces a
            // CORS preflight. Without Access-Control-Allow-Credentials the
            // browser rejects that preflight and the beacon never reaches the
            // engine. Safe here: the origin check is the pinned/loopback
            // predicate plus, only in LAN mode, a local/private form of the
            // request's own host — never a wildcard (and ASP.NET would throw
            // on AllowAnyOrigin + AllowCredentials).
            .AllowCredentials();
    }

    /// <summary>
    /// Allows the pinned remote origins plus any plain-HTTP loopback origin.
    /// Loopback mode retains the Zeus Link desktop-webview rule: the bundle
    /// serves the app from 127.0.0.1 on a dynamic port. LAN mode additionally
    /// accepts a local/private origin whose host equals the engine request's
    /// Host header. Plain HTTP follows the existing LAN-bind behavior; HTTPS
    /// is accepted only when the host explicitly enabled its second LAN HTTPS
    /// listener. Public DNS names remain excluded to avoid trusting a
    /// DNS-rebound origin.
    /// </summary>
    internal static bool IsBrowserOriginAllowed(
        string origin,
        bool allowLanSameHost = false,
        bool allowLanHttpsSameHost = false,
        string? requestHost = null)
    {
        if (NativeWrapperCorsPolicy.IsAllowedOrigin(origin)) return true;
        if (AllowedBrowserOrigins.Contains(origin)) return true;
        if (!Uri.TryCreate(origin, UriKind.Absolute, out var uri)) return false;
        if (uri.Scheme == Uri.UriSchemeHttp && IsLoopbackHost(uri.Host)) return true;
        var allowedScheme = uri.Scheme == Uri.UriSchemeHttp
            ? allowLanSameHost
            : uri.Scheme == Uri.UriSchemeHttps && allowLanHttpsSameHost;
        return allowedScheme
            && HostsEqual(uri.Host, requestHost)
            && IsLanApplianceHost(uri.Host);
    }

    /// <summary>
    /// Narrow origin allowlist for the raw native-microphone websocket feed.
    /// Accepts the pinned remote browser surfaces, or — once a product has
    /// genuinely completed the attach + lease handshake
    /// (<see cref="ProductAudioRingPort.TryGetActiveProductEndpoint"/>) —
    /// any plain-HTTP loopback origin, matching the desktop-webview CORS
    /// policy above (<see cref="IsBrowserOriginAllowed"/>).
    ///
    /// This used to pin further to the product's own exact advertised port
    /// (or a single pre-configured "dev origin" env var meant to cover a
    /// local Vite server). That was brittle in practice: a local dev
    /// server's port varies across worktrees and portOffsets, and any
    /// mismatch denied the request with zero signal — indistinguishable,
    /// from the operator's chair, from a genuinely dead microphone (the
    /// 2026-08-05 friend/net PTT field failure). The leased product
    /// attachment is the real trust boundary here: it can only exist
    /// because a real local process completed the loopback-only attach/lease
    /// handshake in <see cref="StationProtocolEndpoints"/>, so pinning the
    /// browser tab's own port on top of that adds negligible security for
    /// real brittleness cost.
    /// </summary>
    internal static bool IsNativeMicOriginAllowed(string origin, int? productPort)
    {
        if (origin is "https://app.zeussdr.com"
            or "https://staging.zeus-web-app-7ag.pages.dev") return true;
        if (productPort is not > 0) return false;
        return Uri.TryCreate(origin, UriKind.Absolute, out var uri)
            && uri.Scheme == Uri.UriSchemeHttp
            && IsLoopbackHost(uri.Host);
    }

    private static Func<bool> CreateNativeMicStreamAuthorization(HttpContext context) =>
        CreateNativeMicStreamAuthorization(
            context.Connection.RemoteIpAddress,
            context.Request.Headers.Origin,
            context.RequestServices.GetService<ProductAudioRingPort>());

    internal static Func<bool> CreateNativeMicStreamAuthorization(
        IPAddress? remoteAddress,
        StringValues origins,
        ProductAudioRingPort? productAudio)
    {
        // Copy request-owned values now; the returned evaluator must never
        // retain HttpContext or its mutable header collection after handshake.
        bool isLoopback = false;
        if (remoteAddress is not null)
        {
            if (remoteAddress.IsIPv4MappedToIPv6) remoteAddress = remoteAddress.MapToIPv4();
            isLoopback = IPAddress.IsLoopback(remoteAddress);
        }
        var origin = origins.Count == 1 ? origins[0] : null;

        return () =>
        {
            int? productPort = productAudio is not null
                && productAudio.TryGetActiveProductEndpoint(out var advertisedPort)
                    ? advertisedPort
                    : null;
            return isLoopback
                && origin is not null
                && IsNativeMicOriginAllowed(origin, productPort);
        };
    }

    internal static bool IsTrustedNativeMicRequest(
        IPAddress? remoteAddress,
        StringValues origins,
        int? productPort)
    {
        if (remoteAddress is null) return false;
        if (remoteAddress.IsIPv4MappedToIPv6) remoteAddress = remoteAddress.MapToIPv4();
        return IPAddress.IsLoopback(remoteAddress)
            && origins.Count == 1
            && origins[0] is { } origin
            && IsNativeMicOriginAllowed(origin, productPort);
    }

    public static IEndpointRouteBuilder MapStationEngineEndpoints(
        this IEndpointRouteBuilder endpoints,
        bool allowLanSameHost = false,
        bool allowLanHttpsSameHost = false)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Keep the same registration order used by the product host while
        // limiting this surface to the modules extracted into the engine.
        endpoints.MapStationProtocolEndpoints();
        // Field gap: ImdMeasureService is registered by AddStationEngine, but
        // the route used to be mapped only by the product host, so the IMD
        // tool 404'd against a standalone engine (attach mode).
        endpoints.MapImdMeasure();
        endpoints.MapTdoaEndpoints();
        endpoints.MapTdoaContributionEndpoints();
        // Same field gap as ImdMeasure: the SPA's Windows Firewall control
        // (/api/system/windows-firewall[+ /allow]) was product-host-only, so
        // it 404'd against a standalone engine in attach mode.
        endpoints.MapWindowsFirewallEndpoints();
        endpoints.MapStationEngineCapabilitiesEndpoint();
        endpoints.MapGodsEyeEndpoints();
        endpoints.MapNativeAudioEndpoints();
        endpoints.MapRadioStateEndpoint();
        endpoints.MapRadioConnectionEndpoints();
        endpoints.MapVfoEndpoints();
        endpoints.MapReceiverConfigurationEndpoints();
        endpoints.MapKiwiEndpoints();
        endpoints.MapRadioTuningEndpoints();
        endpoints.MapReceiverLoEndpoint();
        endpoints.MapCtunEndpoint();
        endpoints.MapFullDuplexMultiRxEndpoint();
        endpoints.MapModeEndpoint();
        endpoints.MapFilterEndpoints();
        endpoints.MapRadioDspControlEndpoints();
        endpoints.MapTxPhaseRotatorUtilityEndpoints();
        endpoints.MapTxFidelityPolicyEndpoints();
        endpoints.MapReceiverGainProtectionEndpoints();
        endpoints.MapTxControlEndpoints();
        endpoints.MapTxTimingAndTestEndpoints();
        endpoints.MapTxDiagnosticsEndpoint();
        endpoints.MapPureSignalEndpoints();
        endpoints.MapAudioSuiteEndpoints();
        endpoints.MapTxMonitorEndpoint();
        endpoints.MapReceiverDspEndpoints();
        endpoints.MapCfcEndpoint();
        endpoints.MapCfcPresetEndpoints();
        endpoints.MapSpectralZoomEndpoint();
        endpoints.MapWorkspaceZoomEndpoint();
        endpoints.MapOperatorUiSettingsEndpoints();
        endpoints.MapOperatorSettingsEndpoints();
        // The attached SPA sends the same waterfall render-state beacon as the
        // full host. Without the shared route Zeus Link logs a 404 after every
        // waterfall mount even though the WebGL fallback itself is healthy.
        endpoints.MapWaterfallDiagnosticEndpoint();
        // Same field gap as the operator UI prefs: the SPA's Zeus Digital
        // settings tab reads/writes /api/ft8/settings (+ autocq-ack) against
        // the engine in attach mode, and the standalone host mapped neither.
        endpoints.MapDigitalSettingsEndpoints();
        // First-run wizard progress — served by BOTH hosts (the workspace-
        // layout pattern) so the SPA's onboarding state persists identically
        // whether it is talking to the engine (attach) or the full host.
        endpoints.MapOnboardingEndpoints();
        endpoints.MapWorkspaceLayoutEndpoints();
        endpoints.MapContestLogEndpoints();
        endpoints.MapBandPlanEndpoints();
        endpoints.MapStationFavoriteEndpoints();
        endpoints.MapPaSettingsEndpoints();
        endpoints.MapRadioSelectionEndpoints();
        endpoints.MapRadioCapabilitiesEndpoint();
        endpoints.MapExternalPttEndpoint();
        endpoints.MapPttStatusEndpoints();
        endpoints.MapSerialPttEndpoints();
        endpoints.MapRadioAudioEndpoints();
        endpoints.MapPaThermalEndpoint();
        endpoints.MapRadioHardwareEndpoints();
        endpoints.MapRadioCalibrationEndpoints();
        endpoints.MapVnaEndpoints();
        endpoints.MapTciEndpoints();
        endpoints.MapHfAutoEndpoints();
        endpoints.MapCatEndpoints();
        endpoints.MapSpeTaurusEndpoints();
        // Client-error beacon fallback transport (the /ws diagnostic frame is
        // preferred); without this route uncaught SPA errors vanish in local
        // attach whenever the websocket is closed or reconnecting.
        endpoints.MapClientDiagnosticLogEndpoint();
        // Field gap: the prefs-database (profile) routes were product-host-only,
        // so the connect splash's Database row 404'd and hid itself in attach
        // mode — Zeus Link operators could not import, export, create, or switch
        // settings databases. Mapped from the same shared mapper the product
        // host uses so both surfaces behave identically.
        endpoints.MapPrefsDatabaseEndpoints();
        // Same field gap: /api/app/restart was product-host-only, and switching
        // the active database only applies on relaunch. The engine variant
        // exits gracefully and lets the Zeus Link launcher supervisor respawn
        // it — it must NOT self-relaunch like the desktop AppRestartService.
        endpoints.MapEngineAppControlEndpoints();
        // Zeus Link bundle settings mirror: the product reads its feature
        // toggles and amplifier configs back from the engine (the exportable
        // zeus-prefs.db) so they survive updates and ride splash-row exports.
        endpoints.MapProductBundleSettingsEndpoints();
        // Support lifeline for "the engine wrote no zeus-app.log" field
        // reports: which build actually runs, where its log should live,
        // whether the on-disk sink is healthy (and why not), and the in-memory
        // ring tail when the file itself cannot be written.
        endpoints.MapEngineLogDiagnosticsEndpoint();
        endpoints.MapEngineRadioDiagnosticsEndpoint();

        endpoints.Map(
            "/ws",
            context => AttachWebSocketAsync(
                context,
                allowLanSameHost,
                allowLanHttpsSameHost));
        return endpoints;
    }

    private static async Task AttachWebSocketAsync(
        HttpContext context,
        bool allowLanSameHost,
        bool allowLanHttpsSameHost)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        var origins = context.Request.Headers.Origin;
        if (origins.Count > 1
            || (origins.Count == 1
                && (origins[0] is not { } origin
                    // Must match the CORS policy exactly, including the
                    // bind-lan same-host allowance, or the product-served
                    // panadapter WebSocket is rejected after HTTP requests
                    // have already succeeded.
                    || !IsBrowserOriginAllowed(
                        origin,
                        allowLanSameHost,
                        allowLanHttpsSameHost,
                        context.Request.Host.Host))))
        {
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }

        if (!TryParseSessionOptions(
                context.Request.Query,
                out var displayRxId,
                out var suppressAudio))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync(
                $"displayRxId must be an integer from 1 through {WireContract.MaxReceiverSlots - 1}",
                context.RequestAborted).ConfigureAwait(false);
            return;
        }

        var hub = context.RequestServices.GetRequiredService<StreamingHub>();
        using var socket = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
        await hub.AttachClientAsync(
            socket,
            context.RequestAborted,
            displayRxId: displayRxId,
            suppressAudio: suppressAudio,
            nativeMicStreamAuthorization: CreateNativeMicStreamAuthorization(context)).ConfigureAwait(false);
    }

    private static bool IsLoopbackHost(string host) =>
        NormalizeHost(host) is "127.0.0.1" or "localhost" or "::1";

    private static bool HostsEqual(string originHost, string? requestHost) =>
        !string.IsNullOrWhiteSpace(requestHost)
        && string.Equals(
            NormalizeHost(originHost),
            NormalizeHost(requestHost),
            StringComparison.OrdinalIgnoreCase);

    internal static bool IsLanApplianceHost(string host)
    {
        var normalized = NormalizeHost(host);
        if (IPAddress.TryParse(normalized, out var address))
        {
            if (IPAddress.IsLoopback(address)) return true;
            if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
            {
                var bytes = address.GetAddressBytes();
                return bytes[0] == 10
                    || (bytes[0] == 172 && bytes[1] is >= 16 and <= 31)
                    || (bytes[0] == 192 && bytes[1] == 168)
                    || (bytes[0] == 169 && bytes[1] == 254);
            }

            return address.IsIPv6LinkLocal;
        }

        return (!normalized.Contains('.') && !normalized.Contains(':'))
            || (normalized.Length > ".local".Length
                && normalized.EndsWith(
                    ".local",
                    StringComparison.OrdinalIgnoreCase));
    }

    private static string NormalizeHost(string host) =>
        host.Trim().TrimStart('[').TrimEnd(']');

    internal static bool TryParseSessionOptions(
        IQueryCollection query,
        out int? displayRxId,
        out bool suppressAudio)
    {
        displayRxId = null;
        suppressAudio = query.TryGetValue("audio", out var audioValues)
            && audioValues.Count == 1
            && string.Equals(audioValues[0], "0", StringComparison.Ordinal);

        if (!query.TryGetValue("displayRxId", out var displayRxValues))
            return true;

        if (displayRxValues.Count != 1
            || !int.TryParse(
                displayRxValues[0],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var parsed)
            || parsed < 1
            || parsed >= WireContract.MaxReceiverSlots)
            return false;

        displayRxId = parsed;
        return true;
    }
}
