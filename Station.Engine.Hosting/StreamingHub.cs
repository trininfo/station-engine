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
// Zeus is an independent reimplementation in .NET — not a fork. Its
// Protocol-1 / Protocol-2 framing, WDSP integration, meter pipelines, and
// TX behaviour were informed by studying the Thetis project
// (https://github.com/ramdor/Thetis), the authoritative reference
// implementation in the OpenHPSDR ecosystem. Zeus gratefully acknowledges
// the Thetis contributors whose work made this possible:
//
//   Richard Samphire (MW0LGE), Warren Pratt (NR0V),
//   Laurence Barker (G8NJJ),   Rick Koch (N1GP),
//   Bryan Rambo (W4WMT),       Chris Codella (W2PA),
//   Doug Wigley (W5WC),        FlexRadio Systems,
//   Richard Allen (W5SD),      Joe Torrey (WD5Y),
//   Andrew Mansfield (M0YGG),  Reid Campbell (MI0BOT),
//   Sigi Jetzlsperger (DH1KLM).
//
// Thetis itself continues the GPL-governed lineage of FlexRadio PowerSDR
// and the OpenHPSDR (TAPR/OpenHPSDR) ecosystem; that lineage is preserved
// here. See ATTRIBUTIONS.md at the repository root for the full provenance
// statement and per-component attribution.
//
// Protocol-2 / PureSignal / Saturn-class behaviour was additionally informed
// by pihpsdr (https://github.com/dl1ycf/pihpsdr), maintained by Christoph
// Wüllen (DL1YCF); and by DeskHPSDR
// (https://github.com/dl1bz/deskhpsdr), maintained by Heiko (DL1BZ).
// Both are GPL-2.0-or-later.
//
// WDSP — loaded by Zeus via P/Invoke — is Copyright (C) Warren Pratt
// (NR0V), distributed under GPL v2 or later.
//
// Zeus is distributed WITHOUT ANY WARRANTY; see the GNU General Public
// License for details.

using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using Zeus.Contracts;

namespace Zeus.Server;

public sealed class StreamingHub
{
    // Attach primes wisdom, spots, diagnostics, and five chat snapshots before
    // the send loop starts. Keep enough bounded room for that control-plane
    // burst plus live edges that can race it.
    // RX audio shares this queue with display snapshots and control-plane
    // traffic. A rich 3D draw or a burst of VFO updates can keep a WebView from
    // draining its socket for several hundred milliseconds; 16 entries held
    // only ~350 ms of 46 Hz audio and caused audible dropouts. Keep enough
    // bounded room for a 1 s UI stall while the queue's priority policy below
    // discards replaceable display/meter telemetry before PCM.
    private const int MaxBacklogPerClient = 48;

    // Matches MsgType.MicPcm; the client→server uplink type-byte.
    private const byte MsgTypeMicPcm = 0x20;
    private const int MicPcmFrameSamples = 960;
    private const int MicPcmFrameBytes = MicPcmFrameSamples * sizeof(float);

    // Matches MsgType.AudioStreamRequest; client asks to (start|stop) the RX
    // audio stream. Used by GatedWebSocketAudioSink in desktop mode.
    private const byte MsgTypeAudioStreamRequest = 0x21;

    // Client asks to (start|stop) the high-rate display stream for this
    // session. Control-plane telemetry still flows to every client; display
    // frames are only generated and fanned out while at least one visible
    // spectrum surface is mounted somewhere.
    private const byte MsgTypeDisplayStreamRequest = 0x22;

    // Matches MsgType.ClientDiagnosticLog; the frontend clientErrorBeacon's
    // first-choice transport. Websockets ride outside the desktop webview's
    // six-slot per-host HTTP pool, so a wedge that starves every HTTP slot
    // (held-open SSE streams) can still be reported here when the sendBeacon /
    // keepalive-fetch fallbacks would queue forever. Payload:
    // [type:1][UTF-8 JSON {where,message,stack?,realm?}].
    private const byte MsgTypeClientDiagnosticLog = 0x23;

    // Trusted loopback clients may request the native host microphone for
    // friend PTT. Audio is returned only to the requesting session as 0x3D.
    private const byte MsgTypeNativeMicStreamRequest = 0x24;

    // Matches MsgType.CwDecoderRequest; clients ask to start/stop the
    // first-party receive decoder for their session.
    private const byte MsgTypeCwDecoderRequest = 0x25;

    // Largest client→server payload we'll reassemble. A mic PCM frame is
    // 1 + 960*4 = 3841 bytes; 16 KB leaves comfortable headroom if the
    // contract ever adds a control frame. Receives larger than this are
    // dropped to bound memory.
    private const int MaxInboundMessageBytes = 16 * 1024;

    private readonly ILogger<StreamingHub> _log;
    // Keyed by Guid; values are IClientSink so a remote-access WebRTC sink can
    // ride the same fan-out as WebSocket ClientSessions (see AttachSink).
    private readonly ConcurrentDictionary<Guid, IClientSink> _clients = new();
    private readonly ConcurrentDictionary<Guid, ClientSession> _webSocketClients = new();
    // Latest WDSP wisdom phase + status string. Set by Program.cs wiring on
    // phase- and status-changed events; read on every AttachClientAsync so
    // late joiners see the current state without waiting for the next
    // transition. Volatile because the writer is on a worker thread and
    // readers can be on any hub caller.
    private volatile byte _wisdomPhase;
    private volatile string _wisdomStatus = string.Empty;

    private readonly IProductStreamSource _productStreamSource;
    private readonly IClientDiagnosticSink _clientDiagnosticSink;

    // ---- Step 1 drop-counter probe for issue #299 -----------------------
    // Each per-client send queue is bounded to MaxBacklogPerClient. When
    // the producer outruns SendLoopAsync — e.g. under a TCP/WebView stall —
    // high-rate telemetry is discarded before control-plane frames. Display
    // snapshots for the same RX stream coalesce in place. These counters
    // distinguish actual evictions from display coalescing while retaining
    // the #299 log names.
    //
    // Buckets:
    //   audio   — RX audio frames (the user-audible path)
    //   display — panadapter + waterfall display frames
    //   meter   — TX/RX/PS/PA-temp meter frames (5 Hz typical)
    //   other   — wisdom / alert / VST / band-plan / mic-priming
    //
    // A System.Threading.Timer fires every 1 s and logs deltas when any
    // bucket grew. Zero overhead when no drops are occurring. Single timer
    // lives the lifetime of the hub (singleton); no Dispose needed.
    private long _dropsAudio;
    private long _dropsDisplay;
    private long _dropsMeter;
    private long _dropsOther;
    private long _displayCoalesced;
    private long _micInboundFrames;
    private long _micInboundBytes;
    private long _lastMicInboundUnixMs;
    private int _lastMicPayloadBytes;
    private int _lastMicPeakBits;
    private long _lastLoggedAudio;
    private long _lastLoggedDisplay;
    private long _lastLoggedMeter;
    private long _lastLoggedOther;
    private long _lastLoggedDisplayCoalesced;
    private long _micUplinkFrames;
    private long _micUplinkSamples;
    private long _micUplinkBytes;
    private long _micUplinkInvalidFrames;
    private long _micUplinkOversizeMessages;
    private long _micUplinkUnknownFrames;
    private long _micUplinkLastUtcTicks;
    private long _micUplinkLastBytes;
    private long _micUplinkLastSamples;
    private readonly System.Threading.Timer _dropLogTimer;
    private readonly RxAudioMuteState? _rxAudioMuteState;
    private long _rxAudioFramesObserved;
    private long _lastLoggedRxAudioFrames;

    // Aggregate count of connected clients that have asked for the RX audio
    // stream (MsgType.AudioStreamRequest). GatedWebSocketAudioSink reads this
    // once per DSP tick (Volatile) to decide whether to broadcast 0x02 in
    // desktop mode. Per-client state lives on ClientSession.WantsAudio so a
    // disconnect/reload can't leak a pinned stream.
    private int _audioStreamRequests;

    // Aggregate count of clients with a mounted CW Decoder panel. The DSP tap
    // checks this with one volatile read; per-session state prevents duplicate
    // enables and disconnects from leaking demand.
    private int _cwDecodeRequests;

    // Aggregate count of connected clients that need the high-rate display
    // stream (panadapter / waterfall / mini-pan). DspPipelineService reads
    // this once per tick to skip WDSP analyzer reads and frame serialisation
    // for control-only clients.
    private int _displayStreamRequests;
    private int _preferredDisplayStreamRequests;
    private int _nativeMicStreamRequests;

    /// <summary>
    /// True when at least one connected client has requested the RX audio
    /// stream. Cheap (one volatile read) — safe to call on the DSP tick.
    /// </summary>
    internal bool AudioStreamRequested => Volatile.Read(ref _audioStreamRequests) > 0;

    internal bool CwDecodeRequested => Volatile.Read(ref _cwDecodeRequests) > 0;

    /// <summary>Raised only when aggregate CW demand crosses zero.</summary>
    internal event Action? CwDecodeRequestChanged;

    /// <summary>Raised after the last websocket client goes away, i.e. no UI is
    /// connected any more. Subscribers must be cheap and must not throw.</summary>
    internal event Action? LastClientDisconnected;

    public bool DisplayStreamRequested => Volatile.Read(ref _displayStreamRequests) > 0;

    internal int DisplaySubscriberCount => Volatile.Read(ref _displayStreamRequests);

    internal int PreferredDisplaySubscriberCount => Volatile.Read(ref _preferredDisplayStreamRequests);

    /// <summary>True while at least one trusted local client needs selected live-mic PCM.</summary>
    internal bool NativeMicStreamRequested => Volatile.Read(ref _nativeMicStreamRequests) > 0;

    internal int NativeMicSubscriberCount => Volatile.Read(ref _nativeMicStreamRequests);

    public StreamingHub(
        ILogger<StreamingHub> log,
        IProductStreamSource? productStreamSource = null,
        IClientDiagnosticSink? clientDiagnosticSink = null,
        RxAudioMuteState? rxAudioMuteState = null)
    {
        _log = log;
        _productStreamSource = productStreamSource ?? NullProductStreamSource.Instance;
        _clientDiagnosticSink = clientDiagnosticSink ?? NullClientDiagnosticSink.Instance;
        _rxAudioMuteState = rxAudioMuteState;
        _productStreamSource.FrameAvailable += BroadcastProductFrame;
        _dropLogTimer = new System.Threading.Timer(
            _ =>
            {
                LogDropsIfAny();
                LogRxAudioBroadcastDiagnostics();
            },
            null,
            1000,
            1000);
    }

    private void LogRxAudioBroadcastDiagnostics()
    {
        long frames = Interlocked.Read(ref _rxAudioFramesObserved);
        long frameDelta = frames - Interlocked.Read(ref _lastLoggedRxAudioFrames);
        int connected = ClientCount;
        if (connected == 0 && frameDelta == 0) return;

        int recipients = AudioRecipientCount;
        bool muted = _rxAudioMuteState?.IsMuted == true;
        bool sending = frameDelta > 0 && recipients > 0 && !muted;
        _log.LogInformation(
            "rx.audio.broadcast frames/s={Frames} recipients={Recipients} muted={Muted} sending={Sending}",
            frameDelta,
            recipients,
            muted ? "true" : "false",
            sending ? "true" : "false");
        Interlocked.Exchange(ref _lastLoggedRxAudioFrames, frames);
    }

    /// <summary>Test-only: run one diagnostics tick deterministically. Pair
    /// with <see cref="StopDiagnosticsTimerForTest"/> so the constructor's
    /// 1 Hz timer cannot interleave an extra log entry on a slow host.</summary>
    internal void LogRxAudioBroadcastDiagnosticsForTest() =>
        LogRxAudioBroadcastDiagnostics();

    /// <summary>Test-only: park the 1 Hz drop/diagnostics timer so unit tests
    /// asserting on logged entries are deterministic.</summary>
    internal void StopDiagnosticsTimerForTest() =>
        _dropLogTimer.Change(Timeout.Infinite, Timeout.Infinite);

    private void LogDropsIfAny()
    {
        long a = System.Threading.Interlocked.Read(ref _dropsAudio);
        long d = System.Threading.Interlocked.Read(ref _dropsDisplay);
        long m = System.Threading.Interlocked.Read(ref _dropsMeter);
        long o = System.Threading.Interlocked.Read(ref _dropsOther);
        long c = System.Threading.Interlocked.Read(ref _displayCoalesced);
        long da = a - _lastLoggedAudio;
        long dd = d - _lastLoggedDisplay;
        long dm = m - _lastLoggedMeter;
        long doo = o - _lastLoggedOther;
        long dc = c - _lastLoggedDisplayCoalesced;
        if (da > 0 || dd > 0 || dm > 0 || doo > 0 || dc > 0)
        {
            _log.LogWarning(
                "hub.drops audio={A} (+{Da}) display={D} (+{Dd}) meter={M} (+{Dm}) other={O} (+{Do}) displayCoalesced={C} (+{Dc})",
                a, da, d, dd, m, dm, o, doo, c, dc);
            _lastLoggedAudio = a;
            _lastLoggedDisplay = d;
            _lastLoggedMeter = m;
            _lastLoggedOther = o;
            _lastLoggedDisplayCoalesced = c;
        }
    }

    public int ClientCount => _clients.Count;

    internal int AudioRecipientCount
    {
        get
        {
            int count = 0;
            foreach (var client in _clients.Values)
            {
                if (client is ClientSession { SuppressAudio: true }) continue;
                count++;
            }
            return count;
        }
    }

    internal void RecordRxAudioFrame() =>
        Interlocked.Increment(ref _rxAudioFramesObserved);

    /// <summary>
    /// Read-only websocket-hub health for the diagnostics v2 surface: connected
    /// clients, audio/display subscriber counts, and the per-bucket frame-drop
    /// counters (issue #299). All reads are Interlocked/Volatile so this is safe
    /// to call from any thread off the broadcast path.
    /// </summary>
    public object DiagnosticsSnapshot() => new
    {
        schemaVersion = 1,
        generatedUtc = DateTimeOffset.UtcNow,
        connectedClients = ClientCount,
        audioSubscribers = Volatile.Read(ref _audioStreamRequests),
        displaySubscribers = Volatile.Read(ref _displayStreamRequests),
        preferredDisplaySubscribers = Volatile.Read(ref _preferredDisplayStreamRequests),
        drops = new
        {
            audio = System.Threading.Interlocked.Read(ref _dropsAudio),
            display = System.Threading.Interlocked.Read(ref _dropsDisplay),
            meter = System.Threading.Interlocked.Read(ref _dropsMeter),
            other = System.Threading.Interlocked.Read(ref _dropsOther),
        },
        coalesces = new
        {
            display = System.Threading.Interlocked.Read(ref _displayCoalesced),
        },
        micUplink = MicInboundDiagnosticsSnapshot(DateTimeOffset.UtcNow),
    };

    public MicInboundDiagnostics MicPeakDiagnosticsSnapshot(DateTimeOffset now)
    {
        long lastUnixMs = Volatile.Read(ref _lastMicInboundUnixMs);
        long? ageMs = lastUnixMs > 0 ? Math.Max(0, now.ToUnixTimeMilliseconds() - lastUnixMs) : null;
        return new MicInboundDiagnostics(
            Frames: Interlocked.Read(ref _micInboundFrames),
            Bytes: Interlocked.Read(ref _micInboundBytes),
            LastFrameUnixMs: lastUnixMs > 0 ? lastUnixMs : null,
            LastFrameAgeMs: ageMs,
            LastPayloadBytes: Volatile.Read(ref _lastMicPayloadBytes),
            LastPeak: BitConverter.Int32BitsToSingle(Volatile.Read(ref _lastMicPeakBits)),
            ConnectedClients: ClientCount);
    }

    private void RecordDroppedFrame(MsgType? type)
    {
        switch (type)
        {
            case MsgType.AudioPcm:
            case MsgType.NativeMicPcm:
                Interlocked.Increment(ref _dropsAudio);
                break;
            case MsgType.DisplayFrame:
                Interlocked.Increment(ref _dropsDisplay);
                break;
            case MsgType.TxMeters:
            case MsgType.TxMetersV2:
            case MsgType.PsMeters:
            case MsgType.RxMeter:
            case MsgType.RxMetersV2:
            case MsgType.RxMetersV2Secondary:
            case MsgType.RxSignalQuality:
            case MsgType.PaTemp:
            case MsgType.MicPeak:
                Interlocked.Increment(ref _dropsMeter);
                break;
            default:
                Interlocked.Increment(ref _dropsOther);
                break;
        }
    }

    internal void RecordQueueResult(SendQueueResult result)
    {
        if (result.Change == SendQueueChange.Coalesced)
            Interlocked.Increment(ref _displayCoalesced);
        else if (result.Change is SendQueueChange.DroppedOldest or SendQueueChange.DroppedIncoming)
            RecordDroppedFrame(result.DroppedType);
    }

    internal TxMicUplinkDiagnosticsDto MicInboundDiagnosticsSnapshot(DateTimeOffset generatedUtc)
    {
        long lastTicks = System.Threading.Interlocked.Read(ref _micUplinkLastUtcTicks);
        DateTimeOffset? lastUtc = lastTicks > 0 ? new DateTimeOffset(lastTicks, TimeSpan.Zero) : null;
        double? ageMs = lastUtc is { } t ? Math.Max(0.0, (generatedUtc - t).TotalMilliseconds) : null;
        long frames = System.Threading.Interlocked.Read(ref _micUplinkFrames);
        long invalid = System.Threading.Interlocked.Read(ref _micUplinkInvalidFrames);
        long oversize = System.Threading.Interlocked.Read(ref _micUplinkOversizeMessages);
        string status = MicUplinkStatus(frames, invalid, oversize, ageMs, MicPcmReceived is not null);
        return new TxMicUplinkDiagnosticsDto(
            SchemaVersion: 1,
            Status: status,
            SubscriberAttached: MicPcmReceived is not null,
            ClientCount: ClientCount,
            ExpectedFrameSamples: MicPcmFrameSamples,
            ExpectedFrameBytes: MicPcmFrameBytes,
            TotalFrames: frames,
            TotalSamples: System.Threading.Interlocked.Read(ref _micUplinkSamples),
            TotalBytes: System.Threading.Interlocked.Read(ref _micUplinkBytes),
            LastFrameBytes: System.Threading.Interlocked.Read(ref _micUplinkLastBytes),
            LastFrameSamples: System.Threading.Interlocked.Read(ref _micUplinkLastSamples),
            LastFrameAgeMs: ageMs is { } age ? Math.Round(age, 0) : null,
            LastFrameUtc: lastUtc,
            InvalidFrames: invalid,
            OversizeMessages: oversize,
            UnknownFrames: System.Threading.Interlocked.Read(ref _micUplinkUnknownFrames),
            DiagnosticRecommendation: MicUplinkRecommendation(status));
    }

    private static string MicUplinkStatus(long frames, long invalidFrames, long oversizeMessages, double? ageMs, bool subscriberAttached)
    {
        if (!subscriberAttached) return "no-subscriber";
        if (frames <= 0)
            return invalidFrames > 0 || oversizeMessages > 0 ? "invalid-only" : "waiting-for-mic";
        if (ageMs is null) return "unknown";
        if (ageMs <= 1000.0) return "live";
        if (ageMs <= 5000.0) return "stale";
        return "expired";
    }

    private static string MicUplinkRecommendation(string status) => status switch
    {
        "no-subscriber" => "No TX ingest subscriber is attached to the mic uplink; restart the audio ingest service before judging microphone fidelity.",
        "invalid-only" => "Mic uplink frames are arriving but have invalid size or were oversized; verify the browser/native worklet is sending 960 f32 samples per frame.",
        "waiting-for-mic" => "No client mic PCM frames have reached the websocket uplink yet; grant mic permission, start native capture, or feed TCI/WAV audio before judging TX fidelity.",
        "live" => "Mic PCM uplink is live; continue through ingest, TXA stage meters, DUC packets, RF power, and PureSignal feedback for end-to-end TX fidelity tuning.",
        "stale" => "Mic PCM uplink was recently active but is not fresh; watch for browser sleep, muted capture, websocket stalls, or native audio device changes.",
        "expired" => "Mic PCM uplink is stale; TX may be keyed without fresh speech samples, so verify client capture before increasing drive or processing density.",
        _ => "Mic uplink status is unknown; use ingest counters and TXA stage meters to verify whether speech samples are moving.",
    };

    /// <summary>Updates the hub's cached wisdom phase so clients attaching
    /// after the one-shot broadcast still receive the current state.</summary>
    public void SetWisdomPhase(Zeus.Contracts.WisdomPhase phase)
    {
        _wisdomPhase = (byte)phase;
    }

    /// <summary>Updates the hub's cached wisdom status string so clients
    /// attaching mid-build receive the current sub-step text immediately.</summary>
    public void SetWisdomStatus(string status)
    {
        _wisdomStatus = status ?? string.Empty;
    }

    /// <summary>
    /// Raised when a client sends a <c>MicPcm</c> binary frame. The handler
    /// receives the payload with the 1-byte type prefix stripped — plain
    /// f32le samples. Subscribers must be fast: the handler runs on the
    /// WS receive loop and blocking it will stall further uplink.
    /// </summary>
    public event Action<ReadOnlyMemory<byte>>? MicPcmReceived;

    public async Task AttachClientAsync(
        WebSocket ws,
        CancellationToken ct,
        string? authKind = null,
        string? identity = null,
        int? displayRxId = null,
        bool suppressAudio = false,
        bool allowNativeMicStream = false,
        Func<bool>? nativeMicStreamAuthorization = null)
    {
        if (displayRxId is < 1 or >= WireContract.MaxReceiverSlots)
            throw new ArgumentOutOfRangeException(nameof(displayRxId));

        var id = Guid.NewGuid();
        var session = new ClientSession(
            id,
            ws,
            _log,
            this,
            authKind,
            identity,
            displayRxId,
            suppressAudio,
            nativeMicStreamAuthorization
                ?? (allowNativeMicStream ? static () => true : null));
        _clients[id] = session;
        _webSocketClients[id] = session;
        _log.LogInformation("ws.client.connected id={Id} total={Count}", id, _clients.Count);

        // Prime the new client with the current wisdom phase + status text.
        // Without this a client that joins after the ready event would sit
        // at default (Idle) and render the splash indefinitely; a client
        // joining mid-build needs both the phase byte and any status string
        // already accumulated so the body shows the current step.
        session.TryEnqueue(BuildWisdomPayload((Zeus.Contracts.WisdomPhase)_wisdomPhase, _wisdomStatus));

        // Product extensions are already encoded and own their snapshot/cache
        // policy outside the engine. Their source preserves the existing attach
        // order (spots, diagnostics health, then chat status/roster/history).
        foreach (var frame in _productStreamSource.GetAttachSnapshot())
            session.TryEnqueue(frame);

        try
        {
            await session.RunAsync(ct);
        }
        finally
        {
            // Release any RX-audio-stream request this client held so a
            // disconnect/reload can't pin the desktop on-demand stream on.
            session.SetWantsAudio(false);
            session.SetWantsCwDecode(false);
            session.SetWantsDisplay(false);
            session.SetWantsNativeMic(false);
            _clients.TryRemove(id, out _);
            _webSocketClients.TryRemove(id, out _);
            _log.LogInformation("ws.client.disconnected id={Id} total={Count}", id, _clients.Count);
            // No UI left anywhere. Anything the UI was holding open now has
            // nobody who can release it — see TxService for the MOX case.
            if (_clients.IsEmpty)
            {
                try { LastClientDisconnected?.Invoke(); }
                catch (Exception ex) { _log.LogWarning(ex, "LastClientDisconnected handler threw"); }
            }
        }
    }

    public async Task<int> DisconnectIdentityAsync(
        string authKind,
        string identity,
        CancellationToken ct = default)
    {
        var matches = _webSocketClients.Values
            .Where(session => session.Matches(authKind, identity))
            .ToArray();
        foreach (var session in matches)
            await session.DisconnectAsync(ct).ConfigureAwait(false);
        return matches.Length;
    }

    /// <summary>
    /// Register a non-WebSocket frame sink (a remote-access WebRTC session) so it
    /// receives the same broadcast fan-out as <c>/ws</c> clients. The caller
    /// (RemoteWebRtcSession) only attaches AFTER the SPAKE2+ password unlocks
    /// (ADR-0008), and the sink's send is gated again, so nothing leaks early.
    /// </summary>
    internal void AttachSink(Guid id, IClientSink sink) => _clients[id] = sink;

    /// <summary>
    /// Bump the global display-stream gate by <paramref name="delta"/> (+1/−1).
    /// A non-WebSocket sink (the remote WebRTC session) holds no ClientSession,
    /// so it adjusts the aggregate directly here — mirroring how
    /// <c>ClientSession.SetWantsDisplay</c> moves the same counter — to open the
    /// panadapter frame fan-out (Broadcast(in DisplayFrame) gates on this).
    /// </summary>
    internal void AdjustDisplayRequests(int delta) => Interlocked.Add(ref _displayStreamRequests, delta);

    /// <summary>
    /// Bump the global audio-stream gate by <paramref name="delta"/> (+1/−1) for
    /// a non-WebSocket sink (remote WebRTC session), mirroring
    /// <c>ClientSession.SetWantsAudio</c>. Drives the desktop-mode on-demand
    /// 0x02 RX-audio stream the same way a /ws client's 0x21 does.
    /// </summary>
    internal void AdjustAudioRequests(int delta) => Interlocked.Add(ref _audioStreamRequests, delta);

    /// <summary>Adjust CW-decoder demand for a non-WebSocket remote sink.</summary>
    internal void AdjustCwDecodeRequests(int delta)
    {
        int next = Interlocked.Add(ref _cwDecodeRequests, delta);
        if ((delta > 0 && next == 1) || (delta < 0 && next == 0))
            CwDecodeRequestChanged?.Invoke();
    }

    /// <summary>Remove a previously-attached remote sink.</summary>
    internal void DetachSink(Guid id)
    {
        if (!_clients.TryRemove(id, out _) || !_clients.IsEmpty) return;
        // Match the websocket-finally path: the last remote UI disappearing is
        // an independent in-process fail-safe for UI-owned MOX if its loopback
        // dead-man request could not reach Station Engine.
        try { LastClientDisconnected?.Invoke(); }
        catch (Exception ex) { _log.LogWarning(ex, "LastClientDisconnected handler threw"); }
    }

    /// <summary>
    /// Inject a mic-PCM block (f32le samples, NO type prefix) from a non-WebSocket
    /// source — the remote-access WebRTC audio track, after it has been Opus-
    /// decoded and re-blocked to the front-end's 960-sample/20 ms cadence upstream
    /// (the product-side remote mic audio pipeline). Raises <see cref="MicPcmReceived"/>
    /// exactly as a <c>/ws</c> 0x20 frame would, so a remote operator's voice
    /// reaches the TX audio ingest path identically to local mic
    /// audio takes. MOX-gating + source arbitration happen downstream in the
    /// ingest; this only delivers the samples.
    /// </summary>
    internal void InjectMicPcm(ReadOnlyMemory<byte> f32lePayload)
    {
        RecordMicInbound(f32lePayload);
        if (RecordMicUplinkFrame(f32lePayload.Length) && MicPcmReceived is { } handler)
        {
            try { handler(f32lePayload); }
            catch (Exception ex) { _log.LogWarning(ex, "MicPcmReceived (remote inject) handler threw"); }
        }
    }

    internal void DispatchInbound(ReadOnlyMemory<byte> frame)
    {
        if (frame.Length == 0)
        {
            System.Threading.Interlocked.Increment(ref _micUplinkInvalidFrames);
            return;
        }
        byte type = frame.Span[0];
        switch (type)
        {
            case MsgTypeMicPcm:
                var payload = frame.Slice(1);
                RecordMicInbound(payload);
                if (RecordMicUplinkFrame(frame.Length - 1) && MicPcmReceived is { } handler)
                {
                    try { handler(payload); }
                    catch (Exception ex) { _log.LogWarning(ex, "MicPcmReceived handler threw"); }
                }
                break;
            case MsgTypeClientDiagnosticLog:
                // Parsing, sanitization, rate limiting, and product logging
                // policy stay outside the engine stream core.
                try { _clientDiagnosticSink.Handle(frame.Slice(1)); }
                catch (Exception ex) { _log.LogDebug(ex, "ws.client-log sink threw"); }
                break;
            default:
                System.Threading.Interlocked.Increment(ref _micUplinkUnknownFrames);
                // Unknown uplink type — log once so a misaligned client is obvious,
                // but don't tear down the connection.
                _log.LogDebug("ws.inbound unknown type=0x{Type:X2} len={Len}", type, frame.Length);
                break;
        }
    }

    private void RecordMicInbound(ReadOnlyMemory<byte> payload)
    {
        Interlocked.Increment(ref _micInboundFrames);
        Interlocked.Add(ref _micInboundBytes, payload.Length);
        Volatile.Write(ref _lastMicPayloadBytes, payload.Length);
        Volatile.Write(ref _lastMicInboundUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        float peak = 0f;
        var span = payload.Span;
        int sampleBytes = span.Length - (span.Length % 4);
        for (int offset = 0; offset < sampleBytes; offset += 4)
        {
            float sample = BinaryPrimitives.ReadSingleLittleEndian(span.Slice(offset, 4));
            if (!float.IsFinite(sample)) continue;
            float abs = sample < 0 ? -sample : sample;
            if (abs > peak) peak = abs;
        }
        Volatile.Write(ref _lastMicPeakBits, BitConverter.SingleToInt32Bits(peak));
    }

    private bool RecordMicUplinkFrame(int payloadBytes)
    {
        if (payloadBytes != MicPcmFrameBytes)
        {
            System.Threading.Interlocked.Increment(ref _micUplinkInvalidFrames);
            return false;
        }

        long samples = payloadBytes / sizeof(float);
        System.Threading.Interlocked.Increment(ref _micUplinkFrames);
        System.Threading.Interlocked.Add(ref _micUplinkSamples, samples);
        System.Threading.Interlocked.Add(ref _micUplinkBytes, payloadBytes);
        System.Threading.Interlocked.Exchange(ref _micUplinkLastBytes, payloadBytes);
        System.Threading.Interlocked.Exchange(ref _micUplinkLastSamples, samples);
        System.Threading.Interlocked.Exchange(ref _micUplinkLastUtcTicks, DateTimeOffset.UtcNow.UtcTicks);
        return true;
    }

    private void RecordInboundOversize()
    {
        System.Threading.Interlocked.Increment(ref _micUplinkOversizeMessages);
    }

    private void BroadcastProductFrame(byte[] payload)
    {
        if (payload is null || payload.Length == 0) return;

        // Kiwi/P3 product adapters publish the same Station Protocol
        // DisplayFrame wire type as engine receivers. Keep the bytes opaque,
        // but apply the existing per-session display demand, preference, and
        // receiver routing before enqueueing them.
        if (payload[0] == (byte)MsgType.DisplayFrame)
        {
            if (!DisplayStreamRequested || payload.Length <= WireFormat.HeaderSize) return;
            byte frameRxId = payload[WireFormat.HeaderSize];
            bool preferredDisplayRequested = Volatile.Read(ref _preferredDisplayStreamRequests) > 0;
            foreach (var client in _clients.Values)
            {
                if (client is ClientSession session)
                {
                    if (!session.WantsDisplay) continue;
                    if (preferredDisplayRequested && !session.PrefersDisplay) continue;
                    if (session.DisplayRxId is { } rxId && frameRxId != rxId) continue;
                }
                if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsDisplay);
            }
            return;
        }

        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsOther);
        }
    }

    // Each Broadcast(...) below allocates the wire payload directly into a
    // fresh `byte[total]` and serialises into it via FixedBufferWriter. The
    // earlier shape rented from ArrayPool, serialised into the rented span,
    // then called `new ReadOnlyMemory<byte>(rented, 0, total).ToArray()` to
    // produce the broadcast payload — that .ToArray() was the #1 server-side
    // allocator under idle RX (36% of bytes allocated), and the rent/return
    // pair didn't save anything because the ToArray copy is the same size as
    // the rented buffer. Since each client gets the identical payload (queue
    // shares the byte[]), one `new byte[total]` per broadcast is both cheaper
    // and simpler. perf(server) follow-up to docs/performance_tuning.md.

    public void Broadcast(in DisplayFrame frame)
    {
        if (!DisplayStreamRequested) return;

        int total = frame.TotalByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        var preferredDisplayRequested = Volatile.Read(ref _preferredDisplayStreamRequests) > 0;
        foreach (var client in _clients.Values)
        {
            if (client is ClientSession session)
            {
                if (!session.WantsDisplay) continue;
                if (preferredDisplayRequested && !session.PrefersDisplay) continue;
                if (session.DisplayRxId is { } rxId && frame.RxId != rxId) continue;
            }
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsDisplay);
        }
    }

    public void Broadcast(in AudioFrame frame)
    {
        if (_clients.IsEmpty) return;

        int total = frame.TotalByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (client is ClientSession { SuppressAudio: true }) continue;
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsAudio);
        }
    }

    public void Broadcast(in TxMetersFrame frame)
    {
        if (_clients.IsEmpty) return;

        int total = TxMetersFrame.ByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsMeter);
        }
    }

    public void Broadcast(in TxMetersV2Frame frame)
    {
        if (_clients.IsEmpty) return;

        int total = TxMetersV2Frame.ByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsMeter);
        }
    }

    public void Broadcast(in PsMetersFrame frame)
    {
        if (_clients.IsEmpty) return;

        int total = PsMetersFrame.ByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsMeter);
        }
    }

    public void Broadcast(in PsCurveFrame frame)
    {
        if (_clients.IsEmpty) return;

        int total = frame.TotalByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsMeter);
        }
    }

    public void Broadcast(in RxMeterFrame frame)
    {
        if (_clients.IsEmpty) return;

        int total = RxMeterFrame.ByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsMeter);
        }
    }

    public void Broadcast(in RxMetersV2Frame frame)
    {
        if (_clients.IsEmpty) return;

        int total = RxMetersV2Frame.ByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsMeter);
        }
    }

    public void Broadcast(in RxSignalQualityFrame frame)
    {
        if (_clients.IsEmpty) return;

        int total = RxSignalQualityFrame.ByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsMeter);
        }
    }

    public void Broadcast(in PaTempFrame frame)
    {
        if (_clients.IsEmpty) return;

        int total = PaTempFrame.ByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsMeter);
        }
    }

    public void Broadcast(in MicPeakFrame frame)
    {
        if (_clients.IsEmpty) return;

        int total = MicPeakFrame.ByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsMeter);
        }
    }

    /// <summary>
    /// Fan an already-sanitized 960-sample block from the live TX source selected
    /// by <see cref="TxAudioIngest"/> only to trusted sessions that explicitly
    /// requested it. The source buffer is reused by the host/radio capture path,
    /// so each requesting session queue receives an owned copy.
    /// </summary>
    internal void BroadcastNativeMicPcm(ReadOnlySpan<byte> f32lePayload)
    {
        if (!NativeMicStreamRequested || f32lePayload.Length != MicPcmFrameBytes) return;

        foreach (var session in _webSocketClients.Values)
        {
            if (!session.WantsNativeMic) continue;
            if (!session.IsNativeMicStreamAuthorized())
            {
                // Product lease/origin trust can disappear after enable. Fail
                // closed on the capture thread, clear the aggregate demand,
                // and purge every queued native frame before sending more.
                session.LogNativeMicAuthorizationRevoked();
                session.SetWantsNativeMic(false);
                continue;
            }
            var payload = new byte[1 + sizeof(uint) + MicPcmFrameBytes];
            payload[0] = (byte)MsgType.NativeMicPcm;
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(1, sizeof(uint)), session.NativeMicGeneration);
            f32lePayload.CopyTo(payload.AsSpan(1 + sizeof(uint)));
            if (!session.TryEnqueue(payload)) Interlocked.Increment(ref _dropsAudio);
            else session.RecordNativeMicFrameQueued();
        }
    }

    public void Broadcast(in CwEngineStatusFrame frame)
    {
        if (_clients.IsEmpty) return;
        // Variable-length: 9-byte header + UTF-8 text bytes (capped at
        // MaxTextBytes). Compute exact size up front so the broadcast
        // payload allocates once — same shape as AlertFrame above.
        int textBytes = System.Text.Encoding.UTF8.GetByteCount(frame.Text ?? string.Empty);
        if (textBytes > CwEngineStatusFrame.MaxTextBytes) textBytes = CwEngineStatusFrame.MaxTextBytes;
        int total = CwEngineStatusFrame.HeaderByteLength + textBytes;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsOther);
        }
    }

    public void Broadcast(in CwDecodedTextFrame frame)
    {
        if (_clients.IsEmpty) return;
        // Variable-length: 13-byte header + UTF-8 text bytes (capped at
        // MaxTextBytes). Same shape as CwEngineStatus above.
        int textBytes = System.Text.Encoding.UTF8.GetByteCount(frame.Text ?? string.Empty);
        if (textBytes > CwDecodedTextFrame.MaxTextBytes) textBytes = CwDecodedTextFrame.MaxTextBytes;
        int total = CwDecodedTextFrame.HeaderByteLength + textBytes;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsOther);
        }
    }

    public void Broadcast(in WisdomStatusFrame frame)
    {
        SetWisdomPhase(frame.Phase);
        SetWisdomStatus(frame.Status);
        if (_clients.IsEmpty) return;
        var payload = BuildWisdomPayload(frame.Phase, frame.Status);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsOther);
        }
    }

    private static byte[] BuildWisdomPayload(Zeus.Contracts.WisdomPhase phase, string status)
    {
        var frame = new WisdomStatusFrame(phase, status ?? string.Empty);
        int total = frame.ByteLength;
        var buf = new byte[total];
        var writer = new FixedBufferWriter(buf, total);
        frame.Serialize(writer);
        return buf;
    }

    public void Broadcast(in AlertFrame frame)
    {
        if (_clients.IsEmpty) return;

        // AlertFrame is variable-length: 2-byte header + UTF-8 message bytes.
        // Compute the exact size up front so we can allocate the broadcast
        // payload directly (no rent/copy/return). Same shape as the other
        // Broadcast(...) overloads above.
        int total = 2 + System.Text.Encoding.UTF8.GetByteCount(frame.Message);
        if (total > AlertFrame.MaxByteLength) total = AlertFrame.MaxByteLength;
        var payload = new byte[total];
        var writer = new FixedBufferWriter(payload, total);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsOther);
        }
    }

    public void Broadcast(in MoxStateFrame frame)
    {
        if (_clients.IsEmpty) return;

        var payload = new byte[MoxStateFrame.ByteLength];
        var writer = new FixedBufferWriter(payload, MoxStateFrame.ByteLength);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsOther);
        }
    }

    public void Broadcast(in VfoStateFrame frame)
    {
        if (_clients.IsEmpty) return;

        var payload = new byte[VfoStateFrame.ByteLength];
        var writer = new FixedBufferWriter(payload, VfoStateFrame.ByteLength);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsOther);
        }
    }

    public void Broadcast(in PttStatusFrame frame)
    {
        if (_clients.IsEmpty) return;

        var payload = new byte[PttStatusFrame.ByteLength];
        var writer = new FixedBufferWriter(payload, PttStatusFrame.ByteLength);
        frame.Serialize(writer);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsOther);
        }
    }

    /// <summary>
    /// Broadcasts a BandPlanChanged (0x1B) notification. Payload: type byte +
    /// UTF-8 region ID. Clients refetch /api/bands/current on receipt.
    /// </summary>
    public void BroadcastBandPlanChanged(string regionId)
    {
        if (_clients.IsEmpty) return;
        var regionBytes = System.Text.Encoding.UTF8.GetBytes(regionId);
        var payload = new byte[1 + regionBytes.Length];
        payload[0] = (byte)MsgType.BandPlanChanged;
        regionBytes.CopyTo(payload, 1);
        foreach (var client in _clients.Values)
        {
            if (!client.TryEnqueue(payload)) System.Threading.Interlocked.Increment(ref _dropsOther);
        }
    }

    private sealed class ClientSession : IClientSink
    {
        public Guid Id { get; }
        private readonly WebSocket _ws;
        private readonly ILogger _log;
        private readonly StreamingHub _hub;
        private readonly WebSocketSendQueue _queue = new(MaxBacklogPerClient);
        private readonly string? _authKind;
        private readonly string? _identity;
        public int? DisplayRxId { get; }
        public bool SuppressAudio { get; }
        private readonly Func<bool>? _nativeMicStreamAuthorization;

        public ClientSession(
            Guid id,
            WebSocket ws,
            ILogger log,
            StreamingHub hub,
            string? authKind,
            string? identity,
            int? displayRxId,
            bool suppressAudio,
            Func<bool>? nativeMicStreamAuthorization)
        {
            Id = id; _ws = ws; _log = log; _hub = hub;
            _authKind = authKind;
            _identity = identity;
            DisplayRxId = displayRxId;
            SuppressAudio = suppressAudio;
            _nativeMicStreamAuthorization = nativeMicStreamAuthorization;
        }

        public bool Matches(string authKind, string identity) =>
            string.Equals(_authKind, authKind, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(_identity, identity, StringComparison.OrdinalIgnoreCase);

        public async Task DisconnectAsync(CancellationToken ct)
        {
            try
            {
                if (_ws.State == WebSocketState.Open)
                    await _ws.CloseOutputAsync(
                        WebSocketCloseStatus.PolicyViolation,
                        "QRZ access revoked",
                        ct).ConfigureAwait(false);
            }
            catch { }
            finally
            {
                _queue.Complete();
                _ws.Abort();
            }
        }

        public bool TryEnqueue(byte[] payload)
        {
            var result = _queue.Enqueue(payload);
            _hub.RecordQueueResult(result);

            // ClientSession records the actual evicted frame above. False is
            // reserved for a completed queue so Broadcast does not count the
            // incoming frame as a second drop.
            return result.Change != SendQueueChange.Completed;
        }

        // Whether this client has asked for the RX audio stream (0x21).
        // Mutated only on the receive loop and on disconnect cleanup, which
        // never run concurrently for one session (the finally runs after both
        // loops complete), so no per-session lock is needed; the hub aggregate
        // uses Interlocked for cross-session safety.
        private bool _wantsAudio;
        public bool WantsAudio => _wantsAudio;

        public void SetWantsAudio(bool want)
        {
            if (SuppressAudio) want = false;
            if (want == _wantsAudio) return;
            _wantsAudio = want;
            Interlocked.Add(ref _hub._audioStreamRequests, want ? 1 : -1);
        }

        private bool _wantsCwDecode;
        public bool WantsCwDecode => _wantsCwDecode;

        public void SetWantsCwDecode(bool want)
        {
            if (want == _wantsCwDecode) return;
            _wantsCwDecode = want;
            _hub.AdjustCwDecodeRequests(want ? 1 : -1);
        }

        private int _wantsNativeMic;
        private uint _nativeMicGeneration;
        private long _nativeMicFramesQueued;
        public bool WantsNativeMic => Volatile.Read(ref _wantsNativeMic) != 0;
        public uint NativeMicGeneration => Volatile.Read(ref _nativeMicGeneration);

        public bool IsNativeMicStreamAuthorized()
        {
            try
            {
                return _nativeMicStreamAuthorization?.Invoke() == true;
            }
            catch (Exception ex)
            {
                // Authorization callbacks sit on both the websocket receive
                // and native-capture threads. Any unexpected service failure
                // must revoke audio rather than fault either hot loop.
                _log.LogWarning(ex, "ws.native-mic authorization evaluation failed id={Id}", Id);
                return false;
            }
        }

        public void SetWantsNativeMic(bool want, uint generation = 0)
        {
            // Authorization is intentionally evaluated on every enable edge.
            // Zeus Link may attach its authenticated ProductAudioRingPort after
            // this websocket connected, so a handshake-time bool would deny the
            // session forever. Disable/cleanup never needs authorization.
            if (want && !IsNativeMicStreamAuthorized())
            {
                _log.LogWarning(
                    "ws.native-mic request denied id={Id} generation={Generation}",
                    Id,
                    generation);
                want = false;
                SendNativeMicStreamDenied(generation);
            }
            if (want)
            {
                Volatile.Write(ref _nativeMicGeneration, generation);
                Interlocked.Exchange(ref _nativeMicFramesQueued, 0);
            }
            int next = want ? 1 : 0;
            int prev = Interlocked.Exchange(ref _wantsNativeMic, next);
            if (prev != next)
            {
                Interlocked.Add(ref _hub._nativeMicStreamRequests, next - prev);
                _log.LogInformation(
                    "ws.native-mic request enabled={Enabled} id={Id} generation={Generation} queuedBlocks={QueuedBlocks}",
                    want,
                    Id,
                    NativeMicGeneration,
                    Interlocked.Read(ref _nativeMicFramesQueued));
            }
            if (!want) _queue.RemoveType(MsgType.NativeMicPcm);
        }

        // Lets the PTT UI report the real reason (authorization denied) the
        // instant the press fails instead of guessing "microphone not
        // producing audio" after the capture-side wait times out.
        private void SendNativeMicStreamDenied(uint generation)
        {
            var payload = new byte[1 + sizeof(uint)];
            payload[0] = (byte)MsgType.NativeMicStreamDenied;
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(1, sizeof(uint)), generation);
            TryEnqueue(payload);
        }

        public void LogNativeMicAuthorizationRevoked() =>
            _log.LogWarning(
                "ws.native-mic authorization revoked id={Id} generation={Generation} queuedBlocks={QueuedBlocks}",
                Id,
                NativeMicGeneration,
                Interlocked.Read(ref _nativeMicFramesQueued));

        public void RecordNativeMicFrameQueued()
        {
            var count = Interlocked.Increment(ref _nativeMicFramesQueued);
            if (count == 1)
            {
                _log.LogInformation(
                    "ws.native-mic first-frame id={Id} generation={Generation} samples={Samples}",
                    Id,
                    NativeMicGeneration,
                    MicPcmFrameSamples);
            }
        }

        // Whether this client currently has any mounted display-frame consumer
        // (panadapter / waterfall / mini-pan). Broadcast reads this from the
        // DSP thread, so store it as an int and use Volatile/Interlocked.
        private int _wantsDisplay;
        private int _prefersDisplay;
        public bool WantsDisplay => Volatile.Read(ref _wantsDisplay) != 0;
        public bool PrefersDisplay => Volatile.Read(ref _prefersDisplay) != 0;

        public void SetWantsDisplay(bool want, bool preferred = false)
        {
            int next = want ? 1 : 0;
            int nextPreferred = want && preferred ? 1 : 0;
            int prev = Interlocked.Exchange(ref _wantsDisplay, next);
            int prevPreferred = Interlocked.Exchange(ref _prefersDisplay, nextPreferred);
            if (prev != next)
                Interlocked.Add(ref _hub._displayStreamRequests, next - prev);
            if (prevPreferred != nextPreferred)
                Interlocked.Add(ref _hub._preferredDisplayStreamRequests, nextPreferred - prevPreferred);
        }

        // Intercept the client→server control frame here (we have the session
        // context); everything else goes to the hub's shared dispatcher.
        private void Dispatch(ReadOnlyMemory<byte> frame)
        {
            if (frame.Length >= 1 && frame.Span[0] == MsgTypeAudioStreamRequest)
            {
                SetWantsAudio(frame.Length > 1 && frame.Span[1] != 0);
                return;
            }
            if (frame.Length >= 1 && frame.Span[0] == MsgTypeCwDecoderRequest)
            {
                SetWantsCwDecode(frame.Length > 1 && frame.Span[1] != 0);
                return;
            }
            if (frame.Length >= 1 && frame.Span[0] == MsgTypeDisplayStreamRequest)
            {
                var level = frame.Length > 1 ? frame.Span[1] : (byte)0;
                SetWantsDisplay(level != 0, preferred: level >= 2);
                return;
            }
            if (frame.Length >= 1 && frame.Span[0] == MsgTypeNativeMicStreamRequest)
            {
                if (frame.Length == 2 + sizeof(uint))
                {
                    SetWantsNativeMic(
                        frame.Span[1] != 0,
                        BinaryPrimitives.ReadUInt32LittleEndian(frame.Span.Slice(2, sizeof(uint))));
                }
                return;
            }
            _hub.DispatchInbound(frame);
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var sendTask = SendLoopAsync(ct);
            var recvTask = ReceiveLoopAsync(ct);
            await Task.WhenAny(sendTask, recvTask);
            _queue.Complete();
            try { await Task.WhenAll(sendTask, recvTask); } catch { /* drained */ }
        }

        private async Task SendLoopAsync(CancellationToken ct)
        {
            try
            {
                while (true)
                {
                    var frame = await _queue.DequeueAsync(ct).ConfigureAwait(false);
                    if (frame is null || _ws.State != WebSocketState.Open) return;
                    await _ws.SendAsync(frame, WebSocketMessageType.Binary, true, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "ws send loop ended for {Id}", Id);
            }
        }

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            // 8 KB receive window. A mic PCM frame (3841 B) arrives in one or
            // two ReceiveAsync calls depending on chunking; the accumulator
            // below stitches fragments up to MaxInboundMessageBytes before
            // dispatch.
            var buf = new byte[8 * 1024];
            // Reuse across messages; reset on each EndOfMessage.
            byte[]? accum = null;
            int accumLen = 0;
            try
            {
                while (!ct.IsCancellationRequested && _ws.State == WebSocketState.Open)
                {
                    var result = await _ws.ReceiveAsync(buf, ct);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await _ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, ct);
                        return;
                    }
                    if (result.MessageType != WebSocketMessageType.Binary)
                    {
                        // Ignore text frames — no textual control channel in the MVP.
                        continue;
                    }

                    int chunkLen = result.Count;
                    // Fast path: single-fragment message with no pending accumulator —
                    // dispatch the buffer view directly, no allocation.
                    if (result.EndOfMessage && accum is null)
                    {
                        Dispatch(new ReadOnlyMemory<byte>(buf, 0, chunkLen));
                        continue;
                    }

                    if (accum is null)
                    {
                        accum = ArrayPool<byte>.Shared.Rent(Math.Max(chunkLen, 4096));
                        accumLen = 0;
                    }
                    if (accumLen + chunkLen > MaxInboundMessageBytes)
                    {
                        _log.LogWarning("ws.inbound oversize id={Id} len={Len}", Id, accumLen + chunkLen);
                        _hub.RecordInboundOversize();
                        ArrayPool<byte>.Shared.Return(accum);
                        accum = null;
                        accumLen = 0;
                        continue;
                    }
                    if (accumLen + chunkLen > accum.Length)
                    {
                        int newSize = Math.Min(MaxInboundMessageBytes, accum.Length * 2);
                        while (newSize < accumLen + chunkLen) newSize = Math.Min(MaxInboundMessageBytes, newSize * 2);
                        var grown = ArrayPool<byte>.Shared.Rent(newSize);
                        Buffer.BlockCopy(accum, 0, grown, 0, accumLen);
                        ArrayPool<byte>.Shared.Return(accum);
                        accum = grown;
                    }
                    Buffer.BlockCopy(buf, 0, accum, accumLen, chunkLen);
                    accumLen += chunkLen;

                    if (result.EndOfMessage)
                    {
                        Dispatch(new ReadOnlyMemory<byte>(accum, 0, accumLen));
                        ArrayPool<byte>.Shared.Return(accum);
                        accum = null;
                        accumLen = 0;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (WebSocketException ex)
            {
                _log.LogDebug(ex, "ws recv loop ended for {Id}", Id);
            }
            finally
            {
                if (accum is not null) ArrayPool<byte>.Shared.Return(accum);
            }
        }
    }

    private sealed class FixedBufferWriter : IBufferWriter<byte>
    {
        private readonly byte[] _buf;
        private readonly int _capacity;
        private int _written;

        public FixedBufferWriter(byte[] buf, int capacity) { _buf = buf; _capacity = capacity; }

        public void Advance(int count)
        {
            if (_written + count > _capacity) throw new InvalidOperationException("buffer overflow");
            _written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0) => _buf.AsMemory(_written, _capacity - _written);

        public Span<byte> GetSpan(int sizeHint = 0) => _buf.AsSpan(_written, _capacity - _written);
    }
}

public sealed record MicInboundDiagnostics(
    long Frames,
    long Bytes,
    long? LastFrameUnixMs,
    long? LastFrameAgeMs,
    int LastPayloadBytes,
    float LastPeak,
    int ConnectedClients);
