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

namespace Zeus.Contracts;

public enum MsgType : byte
{
    // Server → client (RX display + audio)
    DisplayFrame = 0x01,
    AudioPcm = 0x02,
    Status = 0x03,

    // Client → server (TX uplink). f32le mono samples at 48 kHz, framed into
    // 960-sample (20 ms) blocks. 0x20 chosen to live in a "2x = uplink" nibble
    // so future client→server types (PTT heartbeats, etc.) cluster together
    // and stay visually distinct from the 0x1x server→client telemetry.
    MicPcm = 0x20,

    // Client → server (control). Asks the server to start/stop streaming RX
    // audio (0x02) over the websocket to this client. Payload: [enable:u8].
    // In server/browser mode 0x02 already flows for playback so this is a
    // hint; in desktop/native-audio mode the server withholds 0x02 until a
    // client requests it (refcounted) so a browser-side consumer (e.g. a CW
    // decoder panel) can be fed without duplicating the audio stream.
    AudioStreamRequest = 0x21,

    // 0x22 — display-stream request (client → server control). Handled as a
    // hub-side constant (StreamingHub.MsgTypeDisplayStreamRequest) rather than
    // an enum member; the byte is taken.

    // Client → server (webview diagnostic log). First-choice transport for the
    // frontend's clientErrorBeacon: websockets do not consume the desktop
    // webview's six-connection per-host HTTP/1.1 pool, so a stalled-fetch /
    // SSE-exhaustion wedge can still be reported when every HTTP slot is
    // starved (see docs/lessons/webview-connection-budget.md). Payload:
    // [type:1][UTF-8 JSON {where,message,stack?,realm?}] — the client caps
    // the JSON at 8 KB (the hub's single-fragment receive fast path); the hub
    // funnels the frame through the same ClientLogIngress sanitize + rate
    // limit as POST /api/diagnostics/client-log and logs it at the identical
    // "webview.error" warning tag.
    ClientDiagnosticLog = 0x23,

    // Client → server (control). Requests an on-demand copy of the native
    // host microphone for this loopback websocket session. The host accepts
    // this only for explicitly trusted local sessions and automatically
    // releases the request when the socket disconnects.
    // Payload: [type:1][enable:u8][generation:u32 LE]. The server echoes the
    // generation on PCM frames so a rapid release/re-press can discard bytes
    // already in flight from the previous press.
    NativeMicStreamRequest = 0x24,

    // Client → server (control). Enables the first-party receive-side CW
    // decoder while at least one Telegraph Console panel is mounted. The
    // request is refcounted per connected session and unwound on disconnect.
    // Payload: [type:1][enable:u8] (2 bytes).
    CwDecoderRequest = 0x25,

    // Server → client (TX telemetry + protection)
    TxMeters = 0x11,
    TxStatus = 0x12,
    Alert = 0x13,

    // Server → client (RX signal strength, dBm)
    RxMeter = 0x14,

    // Server → client (DSP bootstrap state). Broadcast when the WDSPwisdom
    // FFTW plan cache transitions between idle/building/ready; also pushed
    // once per client at WS attach so late joiners get the current state.
    WisdomStatus = 0x15,

    // Server → client (TX telemetry v2). Compatible additive extension of
    // TxMeters (0x11): carries average readings alongside peak for every
    // stage, plus CFC/COMP stages that v1 omitted. Operators need the
    // average to judge level and the peak to catch clipping; v1's peak-only
    // payload hid transient overshoots inside the smoothing window. v1 is
    // left in the enum for decoder interop / historical clients but the
    // server only broadcasts v2 after the feat/tx-audio-meters branch.
    TxMetersV2 = 0x16,

    // Server → client (HL2 PA temperature in °C, MCP9700 sensor). Separate
    // from the TX meter frame because temperature is a protection signal
    // the operator wants to see during RX-only operation too — the HL2
    // gateware auto-disables TX at 55 °C (Q6 sensor) — and it moves on a
    // seconds timescale, so bolting it onto the 10 Hz TX meter cadence
    // would be overkill. Broadcast at 2 Hz always.
    PaTemp = 0x17,

    // PureSignal stage telemetry. Broadcast at 10 Hz only while PsEnabled is
    // armed — keeps the wire quiet during normal operation. Carries WDSP
    // GetPSInfo readouts (info[4] = feedback level, info[14] = correcting
    // bit, info[15] = cal-state enum) plus a derived correction-depth dB
    // and the GetPSMaxTX envelope peak. Bare-payload like TxMetersV2 (no
    // 16-byte header) — same 10 Hz rate logic.
    PsMeters = 0x18,

    // Server -> client. PureSignal correction curves: calcc's AM/AM
    // magnitude correction and AM/PM phase curve (DISP_PTS = 512 points
    // each) plus a decimated copy of the sample cloud they were fitted
    // through, from GetPSDisp (native/wdsp/calcc.c:2282). HEADERED like
    // DisplayFrame — it is a ~10 KB frame and the header timestamp is what
    // lets a client age a curve.
    //
    // Emitted only on a CalibrationAttempts (info[5]) edge while PS is
    // armed, because that is the only moment calcc refreshes those
    // buffers. Sending it on a timer would repeat identical curves.
    //
    // FORK-LOCAL ALLOCATION. 0x28 is unused upstream at the time of
    // writing; if Zeus-SDR takes it for something else, this is the byte
    // that has to move, and both this file and the Poseidon client's
    // src/protocol/msgtype.js move together.
    PsCurve = 0x28,

    // Server → client (RX telemetry v2). Compatible additive extension of
    // RxMeter (0x14): carries the full set of RXA stage meters — signal
    // peak/avg (calibrated dBm), ADC peak/avg (dBFS), AGC gain (signed dB,
    // positive = boosting), AGC envelope peak/avg (calibrated dBm), and the
    // strongest analyzer bin inside the active RX passband (calibrated dBm).
    // Bare-payload like TxMetersV2 (no 16-byte header), broadcast at the
    // same 5 Hz cadence as RxMeter. The legacy 5-byte 0x14 frame stays in
    // flight for older clients (e.g. SMeterLive) — 0x19 is purely additive.
    RxMetersV2 = 0x19,

    // Server → client (RX telemetry v2 for a SECONDARY receiver, RxId ≥ 1).
    // Identical wire layout to RxMetersV2 (0x19) — same RxMetersV2Frame,
    // RxId carried in the trailing byte — but a DISTINCT type byte so the
    // multi-DDC per-receiver broadcasts (#1664) are invisible to clients that
    // predate RxId routing. Such clients fold EVERY 0x19 frame into one global
    // S-meter store; if the secondary receivers also arrived on 0x19 the RX1
    // meter would flip to whichever secondary was broadcast last. Because they
    // ignore unknown types, routing RxId ≥ 1 to 0x27 leaves their RX1 meter
    // exactly as before. RX1 (RxId 0) always stays on 0x19. Serialize/
    // Deserialize pick the type byte from RxId; the frontend dispatches 0x19
    // and 0x27 through the same decoder. VfoState (0x26) is likewise a
    // server→client frame that lives in this nibble, so 0x27 is consistent.
    RxMetersV2Secondary = 0x27,

    // Server -> client measured RX signal quality. Additive to the legacy
    // S-meter frames: estimated passband SNR plus separately integrated signal and
    // noise powers. NaN fields / zero confidence mean unavailable.
    RxSignalQuality = 0x3F,

    // Server → client (VFO state edge). Broadcast whenever a receiver's dial
    // (VfoHz) or display centre (RadioLoHz) moves, from any source — front
    // panel, CAT/TCI, band button, typed entry, web UI. Lets the frontend
    // track a HARDWARE tune immediately instead of waiting for the 1 Hz state
    // poll (dial) and the display-frame echo (pan). See VfoStateFrame.cs.
    // Payload: [type:1][receiver:u8][vfoHz:i64 LE][radioLoHz:i64 LE] = 18 bytes.
    VfoState = 0x26,

    // 0x1A — reserved (previously VstHostEvent on the drifted plugin-host
    // branch). Left as a gap rather than reassigned to avoid colliding with
    // any zeus-web build that hasn't been refreshed yet.

    // Server → client (band plan changed). Broadcast when the active region
    // changes or the operator edits the plan. Payload: [type:1][regionIdUtf8…].
    // Frontend refetches GET /api/bands/current on receipt.
    // Originally 0x18 on the issue-65 branch; renumbered to 0x1B on merge
    // with develop to resolve the collision with PsMeters above.
    BandPlanChanged = 0x1B,

    // Server → client (MOX/TUN state edge). Broadcast on every MOX or TUN
    // transition regardless of source (UI click, TCI trx command, SWR trip,
    // TX timeout). Payload: [type:1][moxOn:u8][tunOn:u8] — 3 bytes total.
    // Allows the frontend to track transmit state even when the source of
    // the edge is not the web UI (e.g. TCI client sends trx:0,true;).
    MoxState = 0x1C,

    // Server → client (mic peak level). Broadcast at ~10 Hz by
    // NativeMicCapture only in desktop host mode — the SPA's getUserMedia
    // analyser is intentionally disabled there (Phase 2c) so the MicMeter
    // would otherwise be flat. Server mode never emits this frame; remote
    // browser operators continue to drive their MicMeter via getUserMedia.
    // Payload: [type:1][peakDbfs:f32 LE][tsUnixMs:i64 LE] = 13 bytes total.
    // See MicPeakFrame.cs. Originally 0x1C on the audio-native branch;
    // renumbered to 0x1D on merge with develop to resolve the collision
    // with MoxState above.
    MicPeak = 0x1D,

    // Server → client (audio plugin chain order). Broadcast whenever a
    // user reorders the chain via the Audio Suite window's tile strip,
    // OR when a plugin is installed / uninstalled (so other connected
    // clients refresh their tile order without polling). Payload:
    // [type:1][csvUtf8…] — comma-separated plugin IDs in chain order
    // (head = first in chain, drives mic first). UTF-8 for forward
    // compatibility with non-ASCII plugin IDs even though current IDs
    // are reverse-DNS ASCII. See AudioChainOrderFrame.cs.
    AudioChainOrder = 0x1E,

    // Server → client (audio suite master bypass). Broadcast on every
    // master-bypass toggle (operator click in the Audio Suite chain
    // rail) so all connected clients update their toggle state in sync.
    // Master bypass disengages the WHOLE plugin chain (NoiseGate / EQ /
    // Comp / Exciter / Bass / Reverb) — per-plugin bypass states are
    // untouched and resume when master bypass is released. CFC is
    // downstream in WDSP and unaffected.
    // Payload: [type:1][bypassed:u8] = 2 bytes total. See
    // AudioMasterBypassFrame.cs.
    AudioMasterBypass = 0x1F,

    // Server → client (CW engine status). Broadcast on every state edge of
    // the host-side CW keyer (Idle ↔ Sending, Stopping, Aborting). Carries
    // enough context for the UI macro pad to show what's in flight and how
    // much queue is left without polling. Payload: [type:1][state:u8]
    // [wpm:u16 LE][queueDepth:u16 LE][textLen:u16 LE][text:UTF-8…].
    // See CwEngineStatusFrame.cs. New nibble 0x3x for control-plane
    // feedback frames (0x1x is full); UI ignores unknown types so older
    // builds tolerate this cleanly.
    CwEngineStatus = 0x30,

    // Server → client (first-party CW receive decoder). Broadcast at no
    // more than 10 Hz while a client requests decoding and RX0 is in CWU/CWL.
    // Payload: [type:1][wpm:u16 LE][snrDb:f32 LE][confidence:f32 LE]
    // [textLen:u16 LE][text:UTF-8…]. See CwDecodedTextFrame.cs.
    CwDecodedText = 0x31,

    // Server → client (TCI spot list snapshot). Broadcast by SpotBroadcastService
    // whenever SpotManager changes (add / remove / clear), and pushed once per
    // client on WS connect. Carries the full list so the frontend can replace
    // its store in one atomic update. Variable-length binary frame; see
    // SpotListFrame.cs for the per-spot layout.
    // Payload: [type:1][count:u16 LE][spot…] — each spot: freqHz:i64 LE,
    // argb:u32 LE, callsignLen:u8, callsign:UTF-8, modeLen:u8, mode:UTF-8,
    // commentLen:u16 LE, comment:UTF-8.
    SpotList = 0x32,

    // Server → client (RX audio plugin chain order). Same wire shape as
    // AudioChainOrder, but dedicated to the receive-side Audio Suite rack
    // so TX and RX order/membership state remain independent across every
    // connected browser.
    // Payload: [type:1][csvUtf8…]. See RxAudioChainOrderFrame.cs.
    RxAudioChainOrder = 0x33,

    // Server → client (RX audio suite master bypass). Same wire shape as
    // AudioMasterBypass, but dedicated to the receive-side insert chain
    // so RX bypass state never shares the TX Audio Suite's control frame.
    // Payload: [type:1][bypassed:u8] = 2 bytes total. See
    // RxAudioMasterBypassFrame.cs.
    RxAudioMasterBypass = 0x34,

    // Server → client (ZeusChat event). Broadcast by ChatService for every
    // operator-to-operator chat update: connection status changes, roster
    // updates, incoming messages, and the message history snapshot pushed
    // once per client on WS attach (mirrors the SpotList push-on-attach).
    // Payload: [type:1][UTF-8 JSON envelope]. The envelope is discriminated
    // by a camelCase "kind" field:
    //   {"kind":"status","status":ChatStatusDto}
    //   {"kind":"roster","roster":ChatOperator[]}
    //   {"kind":"message","message":ChatMessage}
    //   {"kind":"history","messages":ChatMessage[]}
    // JSON (not fixed binary) because the payload is small, low-rate, and the
    // shapes are richer than the other control frames. UI ignores unknown
    // kinds so older builds tolerate additions cleanly. See ChatEventFrame.cs.
    ChatEvent = 0x35,

    // Server → client (Live Diagnostics v2 aggregate health). Broadcast by
    // DiagnosticsFramePublisher at low rate (1-2 Hz) whenever clients are
    // connected, and pushed once per client on WS attach (mirrors SpotList /
    // ChatEvent push-on-attach). Carries the worst-of provider health so a
    // dashboard can render live status without polling the REST endpoints.
    // Payload: [type:1][UTF-8 JSON of DiagnosticsHealthDto]. JSON for the same
    // reasons as ChatEvent; UI ignores unknown types so older builds tolerate
    // it cleanly. Encoded by DiagnosticsFramePublisher (source-gen serialised
    // for the hot push path).
    DiagnosticsHealth = 0x36,

    // Server → client (hardware PTT-IN status edge). Broadcast on every
    // footswitch / mic-PTT / rear-KEY edge so the Radio Settings "PTT-IN:
    // idle / keyed" lamp tracks the physical input. P1 boards are driven by
    // the Protocol1Client HardwarePttChanged event, P2 boards by the UDP-1025
    // hi-priority status PttIn bit. Read-only indicator — does NOT drive MOX
    // (ExternalPttService promotes the same edges into MOX separately through
    // TxService.TrySetMox arbitration). Payload: [type:1][keyed:u8] = 2
    // bytes total. See PttStatusFrame.cs.
    //
    // NOTE: 0x33 was used for this type in the unmerged external-ports squash
    // (78c3c28e); 0x33–0x35 are RxAudioChainOrder / RxAudioMasterBypass /
    // ChatEvent and 0x36 is DiagnosticsHealth on this base, so PttStatus uses
    // the next free byte 0x37.
    PttStatus = 0x37,

    // 0x38 / 0x39 / 0x3A are RESERVED — Zeus Digital plugin era; never reuse.
    // The built-in FT8/FT4/WSPR suite broadcast Ft8Decode (0x38), WsprSpot
    // (0x39) and Ft8TxStatus (0x3A) JSON-envelope frames on these bytes until
    // the suite moved into the installable com.kb2uka.digital plugin, which
    // pushes the same payloads over its own SSE stream instead. Clients ignore
    // unknown types, but a NEW frame reusing one of these bytes would decode as
    // FT8 data on an older build — so the values stay burned.

    // Server → client (MIDI / Stream Deck learn frame). Broadcast by MidiService
    // ONLY while the operator has the MIDI settings panel in "Learn" mode: every
    // incoming control event (a knob turn, a button press, a Stream Deck key) is
    // forwarded as a frame so the panel can highlight the live control and let
    // the operator bind it to a command. Outside learn mode nothing is emitted.
    // Same low-rate JSON-envelope shape as ChatEvent. Payload:
    // [type:1][UTF-8 JSON MidiLearnFrame]. UI ignores unknown types so older
    // builds tolerate it cleanly.
    MidiLearn = 0x3B,

    // Server → client (WSJT-X inbound Reply — GridTracker / JTAlert "call this
    // station" trigger). Broadcast by WsjtxUdpBroadcaster when a Reply (type 4)
    // datagram returns on the opt-in bidirectional WSJT-X socket. Payload:
    // [type:1][UTF-8 JSON WsjtxInboundReplyDto] — the decoded WSJT-X message
    // text, target callsign, audio offset (Hz), slot-parity hint, mode, and a
    // monotonic sequence so the frontend can dedupe replays. The FT8 workspace
    // routes this into the existing click-to-call path (`tx.callStation`); a
    // consumed-sequence watermark prevents commands received while the
    // workspace is closed from replaying on mount. See WsjtxInboundReplyFrame.cs.
    WsjtxReply = 0x3C,

    // Server → client (desktop/local-attach friend PTT microphone). Carries the
    // source selected for live TX (host mic or radio-digitised mic/line/XLR)
    // from the pre-MOX gate. Emitted only to a trusted loopback websocket
    // session while that session holds a NativeMicStreamRequest. Payload:
    // [type:1][generation:u32 LE]
    // [960 × f32 LE mono @ 48 kHz].
    // This is a private host-to-webview transport; friend-to-friend audio
    // continues to travel peer-to-peer as WebRTC Opus.
    NativeMicPcm = 0x3D,

    // Server → client (native mic stream request denied). Sent once, right
    // when a NativeMicStreamRequest(enable=true) fails IsNativeMicStreamAuthorized
    // (e.g. the Zeus Link product-audio lease is not currently attached),
    // instead of the server silently doing nothing. Without this, the client
    // has no way to distinguish "authorization denied" from "device is slow
    // to produce audio" and can only report a misleading timeout error after
    // waiting out the capture-side deadline. Payload: [type:1]
    // [generation:u32 LE] — the client discards stale generations the same
    // way it already does for NativeMicPcm (0x3D).
    NativeMicStreamDenied = 0x3E,
}
