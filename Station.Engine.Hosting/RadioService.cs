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

using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Zeus.Contracts;
using Zeus.Dsp;
using Zeus.Protocol1;
using Zeus.Protocol1.Discovery;
using Zeus.Protocol2;

namespace Zeus.Server;

public sealed class RadioService : IDisposable
{
    private const int DefaultHpsdrPort = 1024;
    internal const int MinDisplayZoomLevel = SyntheticDspEngine.MinZoomLevel;
    // Existing consumers (P3 sidecar, MIDI, P1) use this as the raw-wideband
    // ceiling. Keep it stable; P2's rate-aware hybrid ceiling is exposed only
    // through CurrentMaxDisplayZoomLevel.
    internal const int MaxDisplayZoomLevel = WidebandSpectrumAnalyzer.MaxZoomLevel;
    internal const int LegacyMaxDisplayZoomLevel = MaxDisplayZoomLevel;
    internal int CurrentMaxDisplayZoomLevel
    {
        get
        {
            lock (_sync)
            {
                return _p2Active
                    ? P2WidebandZoomPolicy.MaxGlobalZoomForRate(_state.SampleRate)
                    : LegacyMaxDisplayZoomLevel;
            }
        }
    }
    internal const double DefaultAgcTopDb = 90.0;   // Thetis radio.cs:1021 rx_agc_max_gain default
    // Operator AGC-T baseline range. Below ~30 dB the RX audio is effectively
    // muted; the top is Thetis's +120 slider rail. The 30..90 window used
    // before was proved too narrow on the bench: auto AGC-T legitimately
    // seats above 90 on quiet bands (the floor-derived top is ~96-110 on
    // 40m-class floors), and an operator mirroring Thetis's "crank it up,
    // let auto seat it" workflow needs the lever to reach those values.
    // NOTE: this bounds only the manual baseline (AgcTopDb). Auto-AGC's offset
    // (AgcOffsetDb) and the effective value it pushes to WDSP are NOT bounded
    // by this — auto roams the full Thetis AGC-top clamp of [-20, +120] dB
    // (AgcTopMinDb / AgcTopMaxDb below, console.cs:45997).
    internal const double MinAgcTopDb = 30.0;
    internal const double MaxAgcTopDb = 120.0;
    internal const double MinAgcFixedGainDb = -20.0;

    // Floor (Hz) for the signed RX/TX bandpass width pushed through SetFilter.
    // A zero-width bandpass means WDSP's RXASetPassband passes nothing through
    // and the receiver goes silent (issue #1028 — operator's bandwidth slid to
    // zero, audio dropped, no diagnostic complaint anywhere). 10 Hz is well
    // below the narrowest shipped preset (CW F10 = 25 Hz) so legitimate widths
    // are byte-identical; this is a panic floor, not a UI policy.
    internal const int MinFilterWidthHz = 10;
    internal const int MaxFilterEdgeHz = 10_000;
    // Preset chips are intentionally compact; twelve characters keeps custom
    // names useful without turning every filter surface into a label layout.
    internal const int MaxFilterPresetLabelLength = 12;

    private readonly object _sync = new();
    private bool _paCalibrationInvariantLeaseActive;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<RadioService> _log;
    private readonly DspSettingsStore _dspSettingsStore;
    // NR3 (RNNoise) operator-installed model store. Optional (null in older test
    // constructions); when null, NR3 reports no installed model.
    private readonly Nr3ModelStore? _nr3ModelStore;
    private readonly PaSettingsStore _paStore;
    // Per-band external-antenna selection (external-ports plan — antenna slice,
    // #804). Optional so existing constructions (tests) stay valid; null → the
    // antenna path resolves to ANT1/ANT1/None (byte-identical to today).
    private readonly AntennaSettingsStore? _antennaStore;
    // Thetis-style RF filter windows and bypass policy for the Protocol-2 Alex
    // BPF/LPF words. Optional so older tests keep their constructor shape.
    private readonly RfFilterSettingsStore? _rfFilterStore;
    private readonly PreferredRadioStore? _preferredRadioStore;
    private readonly PsSettingsStore? _psStore;
    private readonly FilterPresetStore? _filterPresetStore;
    private readonly RadioStateStore? _radioStateStore;
    // Per-band last-used (hz, mode) register. Read on a band crossing in SetVfo
    // to recall that band's demod mode server-authoritatively, so mode follows
    // the band no matter how the operator got there — band buttons, favorites,
    // the physical front panel, the VFO knob, or a typed frequency (KB2UKA
    // 2026-06-25). Writes stay frontend/front-panel driven; this is read-only.
    private readonly BandMemoryStore? _bandMemoryStore;
    // Global (per-radio, NOT per-band) TX-audio source selection. Pushed via
    // PushAudioFrontEnd on store edit + connect (external-audio-jacks re-port).
    private readonly AudioSettingsStore? _audioStore;
    private readonly TransverterSettingsStore? _transverterSettingsStore;
    private readonly LayoutStore? _layoutStore;
    // Global (per-radio, NOT per-band) HL2 user GPIO mask (external-ports plan,
    // Phase 5; re-ported in the external-port parity audit). Pushed via
    // PushHl2Gpio on store edit + connect. HL2-only on the wire.
    private readonly Hl2GpioSettingsStore? _hl2GpioStore;
    private Func<bool>? _modemAvailable;
    // Cached PS board key for the currently-connected radio. Set by
    // ApplyPsHwPeakForConnection (P1 or P2 connect path) and read by
    // PersistPsState to route HW Peak writes to the correct per-board slot.
    // Empty when nothing is connected — PersistPsState skips HW Peak persistence
    // in that case (no board → no slot to write).
    private string _currentPsBoardKey = string.Empty;
    // Persisted operator decision, intentionally separate from live
    // StateDto.PsEnabled. Only SetPs changes this field; watchdog, disconnect,
    // and connect-time sanitize disarms are runtime safety transitions.
    private bool _psArmIntent;
    // Mirror of the persisted PS TX feedback attenuation (dB) for the
    // currently-connected board. Loaded from TxAttnByBoard on connect
    // (GetPersistedPsTxAttnDb), updated by SetPsTxAttenuationDb when the
    // auto-attenuate dance (or a manual control) settles on a value, and
    // written back by PersistPsState. -1 = "no value for this board yet" →
    // PersistPsState leaves the slot untouched so we never clobber a good
    // saved value with a default.
    private int _currentPsTxAttnDb = -1;
    // Debounced state flush. Set to true in every Mutate(); a 1 Hz timer
    // calls FlushState() which writes to LiteDB and clears the flag.
    // Avoids hammering LiteDB during rapid VFO scroll or filter drags.
    private volatile bool _stateDirty;
    private volatile bool _disposed;
    private readonly System.Threading.Timer? _stateFlushTimer;
    // Filter writes flush straight to LiteDB rather than waiting for the 1 Hz
    // timer, because an edit must survive an abrupt exit (see
    // RadioServiceBandwidthPersistenceTests.SetFilter_FlushesLiveBandwidthAndFamilyMemory).
    // But a passband drag is ~20 writes/sec, and FlushState() writes the FULL
    // state row each time — that is 20 full-row upserts/sec on the request
    // thread. Coalesce: the first write of a gesture still flushes immediately,
    // and the rest leave _stateDirty set for the 1 Hz timer (and Dispose) to
    // pick up, so the settled value is always persisted within a second.
    private const int FilterFlushCoalesceMs = 250;
    private long _lastFilterFlushMs;
    // Last last-selected-preset value actually written per mode. During a
    // VAR-slot drag the resolved name is identical on every write; without this
    // the preset store takes another LiteDB upsert per drag frame.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<RxMode, string> _lastUpsertedPreset = new();
    // Last-known preset name per mode, preserved across mode switches.
    // RX2 keeps its own cache so VFO B top-bar edits do not affect what VFO A
    // restores on its next mode change.
    // Accessed only from inside Mutate (under _sync) or at init.
    private readonly Dictionary<RxMode, string?> _lastPresetPerMode = new();
    private readonly Dictionary<RxMode, string?> _lastPresetPerModeB = new();
    // Last-commanded slider value in UI percent (0..100). Needed here because
    // the drive byte depends on three inputs — percent, per-band PA gain, and
    // global max-watts — any of which can change independently. When a band
    // edge is crossed or a PA setting is edited, we recompute without needing
    // to wait for the next SetDrive call.
    private int _drivePct;
    // Engine-owned, non-persisted cap for a keyed product-plugin injection
    // lease. -1 means inactive. The operator's DrivePct remains untouched, so
    // lease loss, engine restart, or process death cannot persist the temporary
    // reduction. ProductPluginAudioPort owns the apply/clear lifecycle.
    private int _productPluginDriveCapPct = -1;
    private long _productPluginDriveCapGeneration;
    private readonly object _productPluginDriveCapGate = new();
    internal Action<long>? ProductPluginDriveCapClearAttemptedForTest { get; set; }
    // On-board CW keyer config, forwarded to the connected client and
    // re-pushed on reconnect. Seeded from CwSettingsStore in the ctor; updated
    // at runtime via SetCwKeyerConfig from the CW settings endpoint. Default
    // mode 0 (straight) is safe — see zeus-bks.
    //   P1: speed + mode + weight + paddle direction go to C&C register 0x0B.
    //   P2: all persisted keyer values arm the radio's internal keyer via the
    //       TxSpecific packet — issue #1032. Sidetone freq/gain are cached
    //       here too so the P2 keyer's radio-generated sidetone tracks the
    //       operator's CW pitch/level.
    private int _cwKeyerWpm;
    private int _cwKeyerMode;
    private int _cwSidetoneHz = CwSettingsStore.DefaultSidetoneHz;
    private double _cwSidetoneGainDb = CwSettingsStore.DefaultSidetoneGainDb;
    private int _cwBreakIn = CwSettingsStore.DefaultBreakIn ? 1 : 0;
    private int _cwHangMs = CwSettingsStore.DefaultHangMs;
    private int _cwWeight = CwSettingsStore.DefaultWeight;
    private int _cwPaddleReverse = CwSettingsStore.DefaultPaddleReverse ? 1 : 0;
    // Independent TUN drive %. When TUN is keyed, the recompute uses this in
    // place of _drivePct so the operator can pre-set a lower tune level (and
    // the same per-band PA gain gives equal watts at equal percentages). piHPSDR
    // default is 10 — a 0 default would be "press TUN, nothing happens".
    private int _tunePct = 10;
    // TX pre-key (MOX) delay ms (0..500). Authoritative copy read by TxService
    // on the MOX rising edge to arm the IQ-mute window; the StateDto mirror is
    // for the frontend. Always kept strictly below the PS MOX hold-off so PS
    // never calibrates on muted RF — see SetTxMoxPreKeyDelayMs / ClampPreKeyToPs.
    // Issue #630.
    private int _txMoxPreKeyDelayMs;
    // TX tail (MOX hang) delay ms (0..5000). Held after a UI PTT release so
    // audio in flight through the browser→WDSP→IQ pipeline finishes clocking
    // out before the wire MOX bit drops. Issue #1294.
    private int _txMoxTailDelayMs;
    // Post-TX RX resume mute delay ms. Suppresses RX audio/display briefly
    // after MOX falls so relay/DSP transition splash is not heard.
    private int _txPostTxRxMuteDelayMs = DefaultPostTxRxMuteDelayMs;
    // TX timeout seconds. Authoritative copy read by TxMetersService on every
    // tick to evaluate the FR-6 protection trip; the StateDto mirror is for
    // the frontend + persistence. Issue #1270.
    private int _txTimeoutSec = DefaultTxTimeoutSec;
    // Which drive % the next frame uses. Latched via NotifyTunActive from
    // TxService whenever the MOX/TUN keying state changes so a drag on either
    // slider during a live TX picks the right source without polling.
    private bool _tunActive;
    // Guards the pre-wire TUN window: LO alignment must happen before the P2
    // TUN event emits a keyed packet, while split edits must remain frozen for
    // that whole transition. Guarded by _sync.
    private bool _txFrequencyTransition;

    // Deterministic test seam for changes landing after recall observes state
    // but before its guarded apply. Null in production.
    internal Action? RecallRaceTestHook { get; set; }

    private StateDto _state;
    private volatile int _currentMode;

    // Session-only per-receiver config for the extra DDC receivers (RX3+, index
    // 2..MaxReceivers-1). RX1/RX2 keep their tuning/control on the flat
    // StateDto fields, with per-receiver ADC source carried in Receivers[].
    // These extras feed ProjectReceivers and, via that array, the
    // DspPipelineService multi-DDC path. Not persisted — the operator re-enables
    // extra receivers each session (no silent auto-spin-up of DDCs on restart).
    // Guarded by _sync. Indices 0/1 are unused (RX1/RX2 are the flat fields).
    private sealed class ExtraReceiver
    {
        public bool Enabled;
        public long VfoHz = 14_200_000;
        public RxMode Mode = RxMode.USB;
        public int FilterLowHz = 100;
        public int FilterHighHz = 2850;
        public string? FilterPresetName = "VAR1";
        public double AfGainDb;
        public byte AdcSource;   // 0 = ADC0 (same antenna as RX1) by default
        public bool Muted;       // per-RX audio mute (RXOutputGain=0 equivalent)
        public bool SplitEnabled;
        public long TxVfoHz;
        public int ZoomLevel = 1; // independent DDC display zoom for this slice
    }
    private readonly ExtraReceiver[] _extraReceivers = CreateExtraReceivers();
    // Current Zeus ordinary Protocol-1 ingest decodes one DDC stream and fans it
    // out only to RX1/RX2. The P1 wire and some gateware can carry more, but
    // advertising RX3+ before Protocol1Client has a variable-DDC parser produces
    // enabled receivers with no samples. Keep this host capability separate from
    // the wire ceiling.
    private const int Protocol1OrdinaryMaxReceivers = 2;
    private static ExtraReceiver[] CreateExtraReceivers()
    {
        var a = new ExtraReceiver[Zeus.Contracts.WireContract.MaxReceivers];
        for (int i = 2; i < a.Length; i++) a[i] = new ExtraReceiver();
        return a;
    }

    // Latched MOX bit — populated via SetMox so the auto-ATT loop can pause
    // itself during TX without a service-locator pattern back to TxService.
    private bool _mox;
    // TxService revokes this whenever no transition-committed intent owns the
    // transmitter. Default true preserves isolated RadioService test seams;
    // the production TxService constructor immediately takes ownership and
    // starts it false before any live connection can key.
    private int _txSafetyAuthority = 1;
    private Func<int, TransmitSafetyDecision>? _txDriveSafetyEvaluator;

    private Protocol1Client? _activeClient;
    // Interface-only override used by socketless pipeline tests that need to
    // capture P1 writes. Production and real-client tests always leave it null.
    private IProtocol1Client? _activeClientForTest;
    private P1ConnectionAttempt? _activeP1ConnectionAttempt;
    private Action? _activeClientDisconnectedHandler;
    // Socketless connection seam for tests that must exercise the complete
    // RadioService connect orchestration. Production always leaves this false.
    private bool _bypassP1SocketIoForTests = false;
    private readonly P1StartFailureRecovery? _p1StartFailureRecovery;
    private CancellationTokenSource _operatorConnectionActionCts = new();
    private long _operatorConnectionGeneration;
    // One ownership gate for all in-process radio transports. P1 and P2 must
    // never race into two UDP masters driving the same relay-bearing hardware.
    internal SemaphoreSlim RadioLifecycleGate { get; } = new(1, 1);
    // True while DspPipelineService has a live Protocol2 client and no P1 is
    // active. P2 discovery must supply the board identity; an absent identity
    // remains Unknown so RX can continue while TX is inhibited.
    private bool _p2Active;
    private Protocol2Client? _p2Client;
    // Discovered board kind for the active P2 connection. Set by
    // MarkProtocol2Connected from the connect-API request byte (issue #171 —
    // Brick2 is Hermes-on-P2, not OrionMkII). Unknown stays unknown; Decision 4
    // forbids inventing an OrionMkII identity for TX safety policy.
    private HpsdrBoardKind _p2BoardKind = HpsdrBoardKind.Unknown;
    // True while Zeus is connected through the private N9DSP Protocol 3
    // sidecar. The public host deliberately keeps only connection metadata;
    // the sidecar owns all P3/N9DSP runtime code and binaries.
    private bool _p3Active;
    private int _p3MaxReceivers = Zeus.Contracts.WireContract.MaxReceivers;
    // Firmware / gateware version string for the live connection, captured at
    // connect time from the discovery reply (P1: code-version byte raw[9];
    // P2: raw[13] + beta). Diagnostics-only — surfaced by ConnectionProbe so a
    // "Report a problem" snapshot records the exact firmware the operator is
    // running (it's what disambiguates an ANAN-10E and pins board-specific
    // reports, e.g. issue #1053). Null when no discovery info was available
    // (e.g. a forced/reclaim connect that skips the probe). Guarded by _sync.
    private string? _connectedFirmware;
    private bool _preampOn;
    // Manual notch filters (MNF). Authoritative on the server so notches
    // survive reconnects and backend restarts (a fresh engine starts with an empty WDSP notch DB);
    // not on the StateDto wire format — DspPipelineService reads Notches and
    // listens to NotchesChanged to push them to the engine. Guarded by _sync.
    private List<NotchDto> _notches = new();
    // Auto-ATT defaults on; the user baseline starts at 0 dB and the control
    // loop ramps _attOffsetDb up to 31 dB on observed ADC overloads (Thetis
    // console.cs:22167-22181). The old hard-coded 15 dB masked clipping but
    // cost 15 dB of sensitivity on quiet bands.
    private HpsdrAtten _atten = new(0);

    // Auto-ATT control-loop state. Mutated only under _sync or on the RX-thread
    // overload-event path (which also takes _sync before touching state).
    private int _attOffsetDb;
    private int _adcOverloadLevel;          // 0..5, Thetis-style "red lamp" counter
    private bool _overloadSeenInWindow;     // any overload since last tick
    private bool _hardOverloadSeenInWindow;
    private bool _softMagnitudeSeenInWindow;
    private bool _validMagnitudeSeenInWindow;
    private bool _predictiveMagnitudeControlActive;
    private ushort _maxMagnitudeSeenInWindow;
    private byte _lastAdcOverloadBits;
    private ushort? _lastAdc0MaxMagnitude;
    private ushort? _lastAdc1MaxMagnitude;
    private ushort _adc0MaxMagnitudeAtOverload;
    private ushort _adc1MaxMagnitudeAtOverload;
    private DateTimeOffset? _lastAdcTelemetryUtc;
    private AdcProtectionConfig _adcProtection = new();
    private long _lastTickMs = long.MinValue;
    private long _lastAttAttackMs = long.MinValue;  // monotonic timestamp of the last applied attack step
    private long _lastOverloadMs = long.MinValue;   // wall-clock of the last overload window (release hold-off)
    private long _adcProtectionResumeAfterMs = long.MinValue;
    private int _lastAppliedEffectiveDb = -1;   // so the first send always fires

    // P2 reports the peak absolute value of its raw signed-16 ADC samples.
    // Keep enough headroom for an unexpected crest instead of operating at
    // the 32768 full-scale rail: attack at -2 dBFS, settle at -4 dBFS, and do
    // not release until the input is below -7 dBFS.
    private const int AdcSigned16FullScale = 32_768;
    private const int AdaptiveAttackMagnitude = 26_029;
    private const int AdaptiveTargetMagnitude = 20_676;
    private const int AdaptiveReleaseMagnitude = 14_638;
    private const int PostMoxTelemetryGuardMs = 250;

    // Auto-AGC control-loop state. The band noise floor itself is estimated in
    // DspPipelineService by AutoAgcNoiseFloorTracker (a faithful port of
    // Thetis's display.cs processNoiseFloor: gated quiet-bin mean, 2-tap power
    // smoothing, 2 s attack lerp, fast-attack). This loop consumes the settled
    // floor and JUMPS the effective AGC-T to the servo target each tick —
    // Thetis does the same; the smoothing lives in the floor estimate, not the
    // follower.
    // Thetis's display-grid noise-floor follower requires a 2 dB move before
    // updating. Use the same real hysteresis width here so ordinary 1–2 dB
    // floor ripple cannot keep re-seating the quantized WDSP gain ceiling.
    private const double AgcDeadbandDb = 2.0;

    // ── Auto-AGC-T threshold servo (Thetis parity) ──────────────────────────
    // Auto-AGC-T sets the AGC *threshold* (knee) to the noise floor, exactly as
    // Thetis does (console.cs setAGCThresholdPoint:45969-46016 + WDSP
    // SetRXAAGCThresh/GetRXAAGCTop, wcpAGC.c:477-495). We compute the resulting
    // WDSP max-gain ("AGC-T top") in-process and drive it through the existing
    // SetAgcTop apply path: SetRXAAGCTop(top) and SetRXAAGCThresh(thresh) set the
    // identical max_gain, so the in-process form is bit-faithful to Thetis while
    // avoiding a second engine round-trip on the hot meter path.
    //
    // Thetis user offset on the floor (udRX1AutoAGCOffset). The shipped setup
    // control starts at +20 dB and its startup handler copies that value into
    // AutoAGCOffsetRX1. Zeus has no separate offset knob yet, so use the actual
    // reference default rather than the old 0 dB assumption, which seated the
    // automatic threshold roughly 20 dB too hot.
    private const double AutoAgcOffsetDb = 20.0;
    // In non-Fixed modes Thetis subtracts a 2 dB calibration residual after
    // applying the configured floor shift. Fixed is the sole 0 dB exception
    // (console.cs agcCalOffset), because the result drives RXFixedAGC directly.
    private const double AgcThreshCalOffsetDb = 2.0;
    // FFT size used in WDSP's threshold→max-gain conversion (wcpAGC.c:482:
    // noise_offset = 10·log10(bandwidth·size/rate)). Thetis passes the DISPLAY
    // ANALYZER FFT size here (console.cs:45987 — specRX FFTSize, 4096 by
    // default) because the floor was measured from that analyzer's bins: the
    // term converts per-bin noise to in-passband noise, so it MUST match the
    // FFT the floor came from. Zeus's RX analyzer runs 16384 (8192 on the
    // low-power profile) — plumbed in from DisplayPerformanceOptions by
    // DspPipelineService at construction. (The old hardcoded 1024 — the WDSP
    // channel block size, not any FFT — under-corrected by 12 dB and seated
    // auto AGC-T ~12 dB too hot.)
    private double _autoAgcAnalyzerFftSize = 16_384.0;
    // 20·log10(out_target), out_target = (1−e^−n_tau)·0.9999 with n_tau=4
    // (wcpAGC.c:122, create_wcpagc RXA.c:340/345). Constant across all modes.
    private const double AgcOutTargetDb = -0.1615;
    // Thetis clamps: AGC threshold to [-160,+2] dBm (console.cs:45978) and the
    // resulting AGC top to [-20,+120] dB (console.cs:45997).
    private const double AgcThreshMinDbm = -160.0;
    private const double AgcThreshMaxDbm = 2.0;
    private const double AgcTopMinDb = -20.0;
    private const double AgcTopMaxDb = 120.0;
    private double _agcOffsetDb;
    private double _agcServoSeatedTopDb = double.NaN;
    private long _lastAgcTickMs = long.MinValue;

    // 100 ms between 1-dB steps. Events arrive at ~1.2 kHz (192 kSps), so
    // without throttling the offset would saturate at 31 dB in ~30 ms. At 10 Hz
    // the full-range ramp takes ~3 s — matches Thetis' feel.
    private const int TickIntervalMs = 100;

    public event Action<StateDto>? StateChanged;
    // Installed by TxService. Invoked before a StateDto mutation commits so an
    // already-keyed transmission can reject an unsafe VFO/mode/filter/XIT
    // change before any StateChanged subscriber or protocol write sees it.
    internal Action<StateDto, StateDto>? TransmitSafetyStateChanging { get; set; }
    public event Action<IProtocol1Client>? Connected;
    public event Action? Disconnected;
    // Protocol-2 lifecycle. Parallel to the P1-typed Connected/Disconnected
    // pair so subscribers (TxMetersService for hi-priority status, future
    // P2 consumers) can hook a freshly-opened Protocol2Client without
    // probing DspPipelineService. Issue #174 — needed so the meter service
    // can wire its OnTelemetry handler to client.TelemetryReceived.
    public event Action<Zeus.Protocol2.Protocol2Client>? P2Connected;
    public event Action? P2Disconnected;
    // Fires whenever the effective PA snapshot changes (store edit, VFO band
    // crossing, drive slider). DspPipelineService consumes this to forward the
    // same snapshot into any live Protocol2Client (byte 345 / byte 1401 /
    // CmdGeneral[58]). RadioService pushes to the P1 client directly because
    // it owns _activeClient.
    public event Action<PaRuntimeSnapshot>? PaSnapshotChanged;

    internal bool TryBeginPaCalibrationInvariantLease(out string? error)
    {
        lock (_sync)
        {
            if (_paCalibrationInvariantLeaseActive)
            {
                error = "PA calibration already owns the radio state.";
                return false;
            }
            _paCalibrationInvariantLeaseActive = true;
        }
        error = null;
        return true;
    }

    internal void EndPaCalibrationInvariantLease()
    {
        lock (_sync) _paCalibrationInvariantLeaseActive = false;
    }

    private void ThrowIfPaCalibrationInvariantMutation(string setting)
    {
        if (_paCalibrationInvariantLeaseActive)
            throw new InvalidOperationException(
                $"{setting} cannot change while PA calibration is running.");
    }
    // Fires on every MOX / TUN edge. P1 side is pushed directly via
    // ActiveClient?.SetMox; these events give DspPipelineService the hook it
    // needs to forward the same bit into a live Protocol2Client, which owns
    // its own CmdHighPriority byte 4.
    public event Action<bool>? MoxChanged;
    public event Action<bool>? TunActiveChanged;
    // Fires when the operator toggles the Mercury preamp. P1 path is pushed
    // directly via ActiveClient?.SetPreamp inside SetPreamp; this event lets
    // DspPipelineService mirror the same change into a live Protocol2Client
    // (CmdHighPriority byte 1403, bit 0 = RX0 preamp). Issue #126 — the P2
    // forwarding is the missing link that left the PRE button non-functional
    // on Angelia / ANAN-100D.
    public event Action<bool>? PreampChanged;

    /// <summary>Fires when the DDC sample rate (display bandwidth) changes. P1
    /// is pushed directly via ActiveClient?.SetSampleRate inside SetSampleRate;
    /// this event is the only path that reaches a live Protocol2Client (whose
    /// RX-spec carries the rate) AND lets DspPipelineService re-open the WDSP RX
    /// channel at the new input rate so demod + panadapter axis follow. Without
    /// it, a P2 bandwidth change updated state but never reached the radio
    /// (ActiveClient is P1-only / null on P2). Carries the new rate in Hz.</summary>
    public event Action<int>? SampleRateChanged;

    /// <summary>Raised when the manual-notch list changes. DspPipelineService
    /// forwards the new set to the live DSP engine (WDSP notch database).</summary>
    public event Action<IReadOnlyList<NotchDto>>? NotchesChanged;

    /// <summary>Current manual-notch set. DspPipelineService reads this on a
    /// fresh-engine connect to re-apply notches the new WDSP channel lost.</summary>
    public IReadOnlyList<NotchDto> Notches { get { lock (_sync) return _notches.ToArray(); } }

    /// <summary>Fires whenever the global audio front-end state changes (store
    /// edit or connect). The wire bytes are RESOLVED (board-clamped + source-
    /// encoded) by PushAudioFrontEnd; this event lets DspPipelineService forward
    /// the same state into a live Protocol2Client (TxSpecific bytes 50/51) and
    /// route the radio-mic STREAM gate. Audio is decoupled from PureSignal /
    /// antenna / K36 — it never touches the alex word or PS bit.</summary>
    public event Action<AudioFrontEndPush>? AudioFrontEndChanged;

    // Shared TX IQ source threaded through Protocol1Client. TxAudioIngest
    // writes into the same instance; this is the seam between "mic arrived
    // over WS" and "EP2 packet got real IQ". When null the client falls back
    // to its internal test-tone generator (dev / tests without a hub).
    private readonly Zeus.Protocol1.ITxIqSource? _txIqSource;

    // Optional RX-audio source for the Protocol-1 EP2 L/R slots (radio-codec
    // speaker output). Drained by Protocol1Client during RX; fed host-side by
    // RadioSpeakerAudioSink. Null in tests / hosts without the feature wired,
    // in which case P1 frames carry no RX audio (legacy behaviour).
    private readonly Zeus.Protocol1.IRxAudioSource? _rxAudioSource;

    // Optional non-hardware external slice receiver. When present its entry is
    // appended to the projected receiver list (reserved index
    // WireContract.KiwiReceiverIndex) and a change re-broadcasts state. Null in
    // tests / hosts without the Kiwi feature wired.
    private readonly IExternalReceiverSource _externalReceiverSource;
    private readonly int? _defaultConnectSampleRateHz;

    public RadioService(ILoggerFactory loggerFactory, DspSettingsStore dspSettingsStore, PaSettingsStore paStore, FilterPresetStore? filterPresetStore = null, Zeus.Protocol1.ITxIqSource? txIqSource = null, PreferredRadioStore? preferredRadioStore = null, PsSettingsStore? psStore = null, RadioStateStore? radioStateStore = null, CwSettingsStore? cwSettingsStore = null, IInitialTxAudioConfigSource? initialTxAudioConfigSource = null, AntennaSettingsStore? antennaStore = null, AudioSettingsStore? audioStore = null, Nr3ModelStore? nr3ModelStore = null, Hl2GpioSettingsStore? hl2GpioStore = null, BandMemoryStore? bandMemoryStore = null, IExternalReceiverSource? externalReceiverSource = null, Zeus.Protocol1.IRxAudioSource? rxAudioSource = null, RfFilterSettingsStore? rfFilterStore = null, IConfiguration? configuration = null, IRadioDiscovery? p1Discovery = null, TransverterSettingsStore? transverterSettingsStore = null, LayoutStore? layoutStore = null)
    {
        _loggerFactory = loggerFactory;
        _log = loggerFactory.CreateLogger<RadioService>();
        _dspSettingsStore = dspSettingsStore;
        _nr3ModelStore = nr3ModelStore;
        _paStore = paStore;
        _antennaStore = antennaStore;
        _rfFilterStore = rfFilterStore;
        _preferredRadioStore = preferredRadioStore;
        _psStore = psStore;
        _filterPresetStore = filterPresetStore;
        _radioStateStore = radioStateStore;
        _bandMemoryStore = bandMemoryStore;
        _audioStore = audioStore;
        _transverterSettingsStore = transverterSettingsStore;
        _layoutStore = layoutStore;
        if (p1Discovery is not null)
        {
            _p1StartFailureRecovery = new P1StartFailureRecovery(
                p1Discovery,
                loggerFactory.CreateLogger<P1StartFailureRecovery>());
        }
        _defaultConnectSampleRateHz = DisplayPerformanceOptions.Resolve(configuration).DefaultConnectSampleRateHz;
        _paStore.Changed += RecomputePaAndPush;
        // An antenna edit re-pushes the active band's selection server-
        // authoritatively through the same RecomputePaAndPush fan-out (P1
        // SetAntennaRx directly / P2 via PaSnapshotChanged → SetAntennas).
        if (_antennaStore is not null)
            _antennaStore.Changed += RecomputePaAndPush;
        if (_rfFilterStore is not null)
            _rfFilterStore.Changed += RecomputePaAndPush;
        // Audio front-end is global per-radio (not per-band), so it has its own
        // store + push rather than riding the PA snapshot. A store edit re-pushes
        // the resolved wire bytes + StateDto (PR #359/#360 anti-clobber pattern).
        if (_audioStore is not null)
            _audioStore.Changed += PushAudioFrontEnd;
        // HL2 user GPIO (external-ports plan, Phase 5; re-ported in the external-
        // port parity audit). Global per-radio (NOT per-band), like the audio
        // front-end: a store edit re-pushes the persisted mask to the live client.
        // HL2-only; PushHl2Gpio gates on the HasHl2UserGpio capability.
        _hl2GpioStore = hl2GpioStore;
        if (_hl2GpioStore is not null)
            _hl2GpioStore.Changed += PushHl2Gpio;
        if (_preferredRadioStore is not null)
            _preferredRadioStore.Changed += RecomputePaAndPush;
        if (_transverterSettingsStore is not null)
            _transverterSettingsStore.Changed += OnTransverterSettingsChanged;
        if (_layoutStore is not null)
            _layoutStore.ActiveTransverterEnabledChanged += OnActiveTransverterSelectionChanged;
        // External slice mutations re-project and re-broadcast state exactly
        // like a hardware DDC mutation.
        _externalReceiverSource = externalReceiverSource ?? new NullExternalReceiverSource();
        _externalReceiverSource.ReceiverChanged += () => StateChanged?.Invoke(Snapshot());
        _txIqSource = txIqSource;
        _rxAudioSource = rxAudioSource;
        // Seed the on-board CW keyer config from persisted settings so a
        // reconnect after restart re-applies the operator's mode/speed
        // before they touch the panel — otherwise a paddle op who saved
        // iambic would key as straight (default) on first connect. See
        // zeus-bks.
        if (cwSettingsStore is not null)
        {
            var cw = cwSettingsStore.Get();
            Volatile.Write(ref _cwKeyerWpm, cw.Wpm);
            Volatile.Write(ref _cwKeyerMode, (int)cw.KeyerMode);
            _cwSidetoneHz = cw.SidetoneHz;
            _cwSidetoneGainDb = cw.SidetoneGainDb;
            Volatile.Write(ref _cwBreakIn, cw.BreakIn ? 1 : 0);
            Volatile.Write(ref _cwHangMs, cw.HangMs);
            Volatile.Write(ref _cwWeight, cw.Weight);
            Volatile.Write(ref _cwPaddleReverse, cw.PaddleReverse ? 1 : 0);
        }

        // Load persisted DSP settings from the store, or use defaults if not found
        var persistedNr = NormalizeNrConfig(_dspSettingsStore.Get() ?? new NrConfig());
        // CFC — issue #123. Persisted globally; null on a fresh install or
        // legacy DB row falls back to the default-OFF baseline so the operator
        // sees no behaviour change unless they enable.
        var persistedCfc = _dspSettingsStore.GetCfc() ?? CfcConfig.Default;
        // TX/RX equalizer + TX noise gate (fork-local). Same rule as CFC:
        // null on a fresh install or a row that predates these fields falls
        // back to a flat, OFF baseline, so a first connect behaves exactly
        // as it did before the suite existed.
        var persistedTxEq = _dspSettingsStore.GetTxEq() ?? GraphicEqConfig.Default;
        var persistedRxEq = _dspSettingsStore.GetRxEq() ?? GraphicEqConfig.Default;
        var persistedTxGate = _dspSettingsStore.GetTxGate() ?? TxGateConfig.Default;
        // Parametric curves stay NULL when never set, so a station that has
        // only ever used the ten-band editor does not get an empty
        // parametric profile pushed over it at connect.
        var persistedTxEqP = _dspSettingsStore.GetTxEqParametric();
        var persistedRxEqP = _dspSettingsStore.GetRxEqParametric();
        var persistedCfcP = _dspSettingsStore.GetCfcParametric();
        var persistedDexp = _dspSettingsStore.GetTxDexp() ?? TxDexpConfig.Default;
        // AGC mode + custom params. Null on a fresh install / legacy DB row
        // falls back to the Med default so first-connect behaviour is unchanged.
        var persistedAgc = NormalizeAgcConfig(
            _dspSettingsStore.GetAgc() ?? new AgcConfig(AgcMode.Med));
        // RX squelch. Null on a fresh install / legacy DB row falls back to the
        // off default so first-connect behaviour is unchanged (Thetis §5).
        var persistedSquelch = _dspSettingsStore.GetSquelch() ?? new SquelchConfig();
        // TX leveling. Null on a fresh install / legacy DB row falls back to the
        // TxLevelingConfig defaults so first-connect behaviour is unchanged
        // (Thetis §6.1-6.3). The Leveler max-gain stays on LevelerMaxGainDb.
        var persistedTxLeveling = _dspSettingsStore.GetTxLeveling() ?? new TxLevelingConfig();
        // TX phase rotator. Null on a fresh install / legacy DB row falls back
        // to disabled defaults; Auto Tune or the operator can enable and lock it
        // in later. Reverse stays an explicit operator polarity choice.
        var persistedTxPhaseRotator = NormalizeTxPhaseRotator(
            _dspSettingsStore.GetTxPhaseRotator() ?? new TxPhaseRotatorConfig());
        var persistedAmTxProfile = NormalizeAmTxProfile(
            _dspSettingsStore.GetAmTxProfile() ?? new AmTxProfile());
        // SSB bandpass "rectangularity" (issue #871). Null on a fresh install
        // falls back to BandpassWindow.Normal, which resolves to the WDSP
        // open-time tap count (nc = max(2048, dsp_size)), so first-connect audio
        // is byte-identical to pre-#871 builds. A pre-#871 persisted row stored
        // the old two-value "Sharp" as byte 1, which now deserialises to Normal
        // (same byte) — also today's behaviour. RX and TX are independent.
        var persistedRxFilterWindow = _dspSettingsStore.GetRxFilterWindow() ?? BandpassWindow.Normal;
        var persistedTxFilterWindow = _dspSettingsStore.GetTxFilterWindow() ?? BandpassWindow.Normal;
        var persistedRxFilterPhase = _dspSettingsStore.GetRxFilterPhase() ?? FilterPhaseMode.Linear;
        var persistedTxFilterPhase = _dspSettingsStore.GetTxFilterPhase() ?? FilterPhaseMode.Linear;

        // TX Audio Profile startup overlay. If the operator has a "last loaded"
        // unified TX Audio Profile, its scalar/config values overlay the
        // per-setting stores BEFORE _state is built, so the radio comes up on
        // that profile rather than the last ad-hoc live values. The heavier
        // chain/plugin-state replay runs later via TxAudioProfileService.
        // StartAsync. When there is NO last-loaded id (fresh install / never
        // used profiles) nothing is overlaid — byte-identical to current
        // defaults. PureSignal and every excluded field are untouched.
        int? overlayMicGain = null;
        double? overlayLevelerMaxGain = null;
        int? overlayTxFilterLow = null, overlayTxFilterHigh = null;
        var initialTxAudioConfig = initialTxAudioConfigSource?.GetInitialConfig();
        if (initialTxAudioConfig is not null)
        {
            persistedCfc = initialTxAudioConfig.Cfc ?? persistedCfc;
            persistedTxLeveling = initialTxAudioConfig.TxLeveling ?? persistedTxLeveling;
            persistedTxPhaseRotator = NormalizeTxPhaseRotator(
                initialTxAudioConfig.TxPhaseRotator ?? persistedTxPhaseRotator);
            overlayMicGain = Math.Clamp(initialTxAudioConfig.MicGainDb, -40, 10);
            overlayLevelerMaxGain = Math.Clamp(initialTxAudioConfig.LevelerMaxGainDb, 0.0, 20.0);
            // Re-sign the operator-typed positive magnitudes for the startup
            // mode so the TX bandpass comes up correctly.
            int loAbs = Math.Min(
                Math.Abs(initialTxAudioConfig.TxFilterLowHz),
                Math.Abs(initialTxAudioConfig.TxFilterHighHz));
            int hiAbs = Math.Max(
                Math.Abs(initialTxAudioConfig.TxFilterLowHz),
                Math.Abs(initialTxAudioConfig.TxFilterHighHz));
            var startupMode = radioStateStore?.Get()?.Mode ?? RxMode.USB;
            var (sLo, sHi) = SignedFilterForMode(startupMode, loAbs, hiAbs);
            overlayTxFilterLow = sLo;
            overlayTxFilterHigh = sHi;
        }

        // Seed the last-preset cache from persisted store for all modes so
        // the first mode-switch in a session recalls the correct slot.
        if (filterPresetStore != null)
        {
            foreach (RxMode m in Enum.GetValues<RxMode>())
            {
                _lastPresetPerMode[m] = filterPresetStore.GetLastSelectedPreset(m);
                _lastPresetPerModeB[m] = _lastPresetPerMode[m];
            }
        }

        // Load persisted PS operator intent, calibration, and tuning. Runtime
        // PsEnabled still starts false in every process; ArmIntent is applied
        // only by the connect-time sanitize cycle after the fresh engine has
        // received its calibration state. MOX/TUN/TwoToneEnabled remain
        // session-only. PsHwPeak is resolved per-radio in
        // ApplyPsHwPeakForConnection (called from ConnectAsync /
        // ConnectP2Async), which prefers the persisted per-board value when
        // present and falls back to the factory default otherwise.
        var ps = _psStore?.Get();
        _psArmIntent = ps?.ArmIntent ?? false;

        // RadioStateStore snapshot — hydrates active mode/VFO/filter/volume/zoom
        // and the per-mode-family filter memory. Null on first run; falls through
        // to the hardcoded defaults below. Snapshot wins for the fields it knows
        // about; existing domain stores (DspSettings, PsSettings, etc.) still
        // hydrate the wider config they own.
        var rsSnap = _radioStateStore?.Get();
        // Keep the private hardware baseline and the public snapshot in lockstep.
        // Previously StateDto.AttenDb was hydrated while every wire/startup and
        // auto-ATT calculation still read this field's 0 dB initializer.
        _atten = new HpsdrAtten(rsSnap?.AttenDb ?? 0);
        _adcProtection = NormalizeAdcProtection(new AdcProtectionConfig(
            Enabled: rsSnap?.AutoAttEnabled ?? true,
            AttackMs: rsSnap?.AdcProtectionAttackMs ?? 100,
            ReleaseMs: rsSnap?.AdcProtectionReleaseMs ?? 100,
            AttackStepDb: rsSnap?.AdcProtectionAttackStepDb ?? 1,
            ReleaseStepDb: rsSnap?.AdcProtectionReleaseStepDb ?? 1,
            MaxOffsetDb: rsSnap?.AdcProtectionMaxOffsetDb ?? 31,
            WarningThreshold: rsSnap?.AdcProtectionWarningThreshold ?? 3,
            MagnitudeSoftLimit: rsSnap?.AdcProtectionMagnitudeSoftLimit ?? 0,
            ReleaseHoldMs: rsSnap?.AdcProtectionReleaseHoldMs ?? 2000));

        // Restore per-mode-family filter memory from snapshot if available so
        // an AM→USB mode-switch at startup recalls the last SSB width, not the
        // compile-time default.
        bool familyFilterMigrated = false;
        if (rsSnap is not null)
        {
            familyFilterMigrated = rsSnap.SsbFilterLoAbs == rsSnap.SsbFilterHiAbs
                || rsSnap.DigFilterLoAbs == rsSnap.DigFilterHiAbs;
            _ssbFilter = rsSnap.SsbFilterLoAbs == rsSnap.SsbFilterHiAbs
                ? new(150, 2850) : new(rsSnap.SsbFilterLoAbs, rsSnap.SsbFilterHiAbs);
            _digFilter = rsSnap.DigFilterLoAbs == rsSnap.DigFilterHiAbs
                ? new(0, 800) : new(rsSnap.DigFilterLoAbs, rsSnap.DigFilterHiAbs);
            _digFilterB = _digFilter;
            _amFilter = new(rsSnap.AmFilterLoAbs, rsSnap.AmFilterHiAbs);
            _fmFilter = new(rsSnap.FmFilterLoAbs, rsSnap.FmFilterHiAbs);
            _cwFilter = new(rsSnap.CwFilterLoAbs, rsSnap.CwFilterHiAbs);
            _ssbTxFilter = new(rsSnap.SsbTxFilterLoAbs, rsSnap.SsbTxFilterHiAbs);
            _amTxFilter = new(rsSnap.AmTxFilterLoAbs, rsSnap.AmTxFilterHiAbs);
            _fmTxFilter = new(rsSnap.FmTxFilterLoAbs, rsSnap.FmTxFilterHiAbs);
            _cwTxFilter = new(rsSnap.CwTxFilterLoAbs, rsSnap.CwTxFilterHiAbs);
            _preampOn = rsSnap.PreampOn;
            _notches = rsSnap.Notches
                .Select(n => new NotchDto(n.CenterHz, n.WidthHz, n.Active, NormalizeNotchSource(n.Source)))
                .ToList();
            _drivePct = Math.Clamp(rsSnap.DrivePct, 0, 100);
            _tunePct = Math.Clamp(rsSnap.TunePct, 0, 100);
            // Hydrate the TX pre-key delay, then clamp it below the persisted PS
            // MOX hold-off so a hand-edited DB row can't break the invariant.
            double hydPsMoxDelaySec = PsTimingLimits.ClampMoxDelaySec(ps?.MoxDelaySec ?? PsTimingLimits.DefaultMoxDelaySec);
            _txMoxPreKeyDelayMs = ClampPreKeyToPs(
                Math.Clamp(rsSnap.TxMoxPreKeyDelayMs, 0, MaxPreKeyDelayMs),
                hydPsMoxDelaySec);
            _txMoxTailDelayMs = Math.Clamp(rsSnap.TxMoxTailDelayMs, 0, MaxTailDelayMs);
            _txPostTxRxMuteDelayMs = Math.Clamp(
                rsSnap.TxPostTxRxMuteDelayMs,
                0,
                MaxPostTxRxMuteDelayMs);
            _txTimeoutSec = ClampTxTimeoutSec(rsSnap.TxTimeoutSec);
        }

        // Missing on legacy RadioState rows -> 100, preserving the historical
        // unrestricted slider. Clamp restored DRV/TUN before StateDto exists so
        // startup cannot briefly exceed the saved amplifier-safe maximum.
        int startupDriveMaxPct = Math.Clamp(rsSnap?.DriveMaxPct ?? 100, 1, 100);
        _drivePct = Math.Min(_drivePct, startupDriveMaxPct);
        _tunePct = Math.Min(_tunePct, startupDriveMaxPct);

        // RX2 (VFO-B) tuning is hydrated into the canonical Receivers[1] entry —
        // the flat VFO-B StateDto fields were retired in the A/B wire collapse.
        // Legacy rows that never stored RX2 fall back to the RX1 values, exactly
        // as the old flat-field hydration did.
        long hydVfoB = (rsSnap?.VfoBHz ?? 0L) != 0L ? rsSnap!.VfoBHz : (rsSnap?.VfoHz ?? 14_200_000);
        RxMode hydModeB = rsSnap?.ModeB ?? rsSnap?.Mode ?? RxMode.USB;
        int hydFilterLowB = rsSnap?.FilterLowHzB ?? rsSnap?.FilterLowHz ?? 100;
        int hydFilterHighB = rsSnap?.FilterHighHzB ?? rsSnap?.FilterHighHz ?? 2850;
        string? hydPresetB = rsSnap?.FilterPresetNameB ?? rsSnap?.FilterPresetName ?? "VAR1";
        double hydAfGainB = SanitizeAfGainDb(rsSnap?.Rx2AfGainDb);

        _state = new(
            Status: ConnectionStatus.Disconnected,
            Endpoint: null,
            VfoHz: rsSnap?.VfoHz ?? 14_200_000,
            Mode: rsSnap?.Mode ?? RxMode.USB,
            FilterLowHz: rsSnap?.FilterLowHz ?? 100,
            FilterHighHz: rsSnap?.FilterHighHz ?? 2850,
            SampleRate: 192_000,    // set at connect time; not in global snapshot
            // Thetis / WDSP AGC_MEDIUM baseline. This gives the RX AGC enough
            // headroom to normalize weak post-demod audio immediately after a
            // fresh start. Operator overrides persist via
            // DspSettingsStore.SetAgcTopDb so deliberate lower AGC-T settings
            // still stick across restarts. Clamp the rehydrated value into the
            // operator range so a legacy out-of-range persisted baseline (from
            // the old -20..120 slider) can't park the thumb off the rail.
            AgcTopDb: Math.Clamp(_dspSettingsStore.GetAgcTopDb() ?? DefaultAgcTopDb, MinAgcTopDb, MaxAgcTopDb),
            Agc: persistedAgc,
            Squelch: persistedSquelch,
            TxLeveling: persistedTxLeveling,
            AttenDb: _atten.ClampedDb,
            Nr: persistedNr,
            // NR3 (RNNoise): native availability is a static probe of the loaded
            // libwdsp's RNNR exports; the installed-model name comes from the
            // operator's model store. The frontend reveals NR3 only when both
            // are present (symbols available AND a model installed).
            WdspNr3RnnrAvailable: Zeus.Dsp.Wdsp.WdspDspEngine.Nr3RnnrAvailable,
            Nr3ModelName: _nr3ModelStore?.GetActiveModelName(),
            Nr3UsingBundledDefault: _nr3ModelStore?.UsingBundledDefault() ?? false,
            ZoomLevel: rsSnap?.ZoomLevel ?? 1,
            WorkspaceZoomPct: ClampWorkspaceZoomPct(rsSnap?.WorkspaceZoomPct ?? DefaultWorkspaceZoomPct),
            AutoAttEnabled: _adcProtection.Enabled,
            AttOffsetDb: 0,         // always reset — control-loop accumulator
            AdcOverloadWarning: false,
            FilterPresetName: rsSnap?.FilterPresetName ?? "VAR1",
            FilterAdvancedPaneOpen: filterPresetStore?.GetAdvancedPaneOpen() ?? false,
            TxFilterLowHz: overlayTxFilterLow ?? rsSnap?.TxFilterLowHz ?? 150,
            TxFilterHighHz: overlayTxFilterHigh ?? rsSnap?.TxFilterHighHz ?? 2850,
            RxFilterWindow: persistedRxFilterWindow,
            TxFilterWindow: persistedTxFilterWindow,
            RxFilterPhase: persistedRxFilterPhase,
            TxFilterPhase: persistedTxFilterPhase,
            RxAfGainDb: SanitizeAfGainDb(rsSnap?.RxAfGainDb),
            Rx1AfGainDb: SanitizeAfGainDb(rsSnap?.Rx1AfGainDb),
            // 0 dB unity matches the engine's TXA fresh-open default; legacy
            // rows missing the field hydrate to that same default. A last-loaded
            // TX Audio Profile overlays its mic gain ahead of the snapshot.
            MicGainDb: overlayMicGain ?? Math.Clamp(rsSnap?.MicGainDb ?? 0, -40, 10),
            // 8.0 dB matches WdspDspEngine.DefaultLevelerMaxGainDb. Clamp range
            // widened to 0..20 for Thetis parity (radio.cs leveler top 0..20).
            LevelerMaxGainDb: overlayLevelerMaxGain ?? Math.Clamp(rsSnap?.LevelerMaxGainDb ?? 8.0, 0.0, 20.0),
            AutoAgcEnabled: rsSnap?.AutoAgcEnabled ?? false,
            RxLevelerEnabled: rsSnap?.RxLevelerEnabled ?? false,
            RxLeveler: NormalizeRxLevelerConfig(rsSnap is null
                ? null
                : new RxLevelerConfig(
                    rsSnap.RxLevelerMode,
                    rsSnap.RxLevelerTargetRmsDb,
                    rsSnap.RxLevelerMaxBoostDb,
                    rsSnap.RxLevelerAttackMs,
                    rsSnap.RxLevelerReleaseMs,
                    rsSnap.RxLevelerHangMs)),
            AgcOffsetDb: 0.0,       // always reset — control-loop accumulator
            // AGC knee removed: AGC-T is the single manual AGC control, so the
            // threshold is never operator-driven (it and AGC-T are the same WDSP
            // register — driving both clobbered each other). Always null.
            AgcThresholdDbm: null,
            // PS persisted fields (or DTO defaults when not persisted yet).
            PsEnabled: false, // runtime arm starts off; persisted intent is sanitized and applied after connect
            PsAuto: ps?.Auto ?? true,
            PsAutoAttenuate: ps?.AutoAttenuate ?? true,
            PsMoxDelaySec: PsTimingLimits.ClampMoxDelaySec(ps?.MoxDelaySec ?? PsTimingLimits.DefaultMoxDelaySec),
            PsLoopDelaySec: PsTimingLimits.ClampLoopDelaySec(ps?.LoopDelaySec ?? PsTimingLimits.DefaultLoopDelaySec),
            PsAmpDelayNs: PsTimingLimits.ClampAmpDelayNs(ps?.AmpDelayNs ?? PsTimingLimits.DefaultAmpDelayNs),
            PsFeedbackSource: ps?.Source ?? PsFeedbackSource.Internal,
            // Two-tone test generator dial-in. Defaults match pihpsdr / Thetis
            // (700/1900 Hz, 0.49 each — peak ~0.98 just under WDSP IQ clip).
            TwoToneFreq1: ps?.TwoToneFreq1 ?? 700.0,
            TwoToneFreq2: ps?.TwoToneFreq2 ?? 1900.0,
            TwoToneMag: ps?.TwoToneMag ?? 0.49,
            Cfc: persistedCfc,
            TxEq: persistedTxEq,
            RxEq: persistedRxEq,
            TxGate: persistedTxGate,
            TxEqParametric: persistedTxEqP,
            RxEqParametric: persistedRxEqP,
            CfcParametric: persistedCfcP,
            TxDexp: persistedDexp,
            // Hydrate drive sliders from RadioStateStore so a fresh frontend
            // connect lands on the operator's last-set values. The private
            // fields above (_drivePct / _tunePct) were already hydrated in the
            // rsSnap block; mirror them into the StateDto so SetDrive doesn't
            // become the only path that puts these into the broadcast.
            DrivePct: Volatile.Read(ref _drivePct),
            DriveMaxPct: startupDriveMaxPct,
            TunePct: Volatile.Read(ref _tunePct),
            TxMoxPreKeyDelayMs: Volatile.Read(ref _txMoxPreKeyDelayMs),
            TxMoxTailDelayMs: Volatile.Read(ref _txMoxTailDelayMs),
            TxPostTxRxMuteDelayMs: Volatile.Read(ref _txPostTxRxMuteDelayMs),
            TxTimeoutSec: Volatile.Read(ref _txTimeoutSec),
            // Preserve a persisted frozen NCO only when CTUN is enabled.
            // CTUN-off always starts with the radio following the dial, even
            // when an earlier ruler pan persisted an offset RadioLoHz.
            RadioLoHz: (rsSnap?.CtunEnabled ?? false) && rsSnap.RadioLoHz != 0L
                ? rsSnap.RadioLoHz
                : CwOffset.EffectiveLoHz(
                    rsSnap?.Mode ?? RxMode.USB,
                    rsSnap?.VfoHz ?? 14_200_000),
            Rx2Enabled: rsSnap?.Rx2Enabled ?? false,
            Rx2AudioMode: rsSnap?.Rx2AudioMode ?? Zeus.Contracts.Rx2AudioMode.Both,
            TxVfo: rsSnap?.TxVfo ?? TxVfo.A,
            CwPitchHz: CwOffset.CwPitchHz,
            CtunEnabled: rsSnap?.CtunEnabled ?? false,
            FullDuplexMultiRxEnabled: rsSnap?.FullDuplexMultiRxEnabled ?? false,
            PreampOn: rsSnap?.PreampOn ?? false,
            RogerBeepEnabled: rsSnap?.RogerBeepEnabled ?? false,
            SplitEnabled: false,
            SplitTxHz: 0);
        _currentMode = (int)_state.Mode;

        _state = _state with
        {
            TxPhaseRotator = persistedTxPhaseRotator,
            AmTxProfile = persistedAmTxProfile,
        };

        // One-time repair for the master/RX1 AF split. Before the split the
        // station-wide master WAS the operator's AF slider; the split shipped
        // with no UI control bound to the master, so a saved level was stranded
        // in a field nothing could reach while it still biased every receiver
        // through EffectiveAfGainDb(master, trim). Fold it into RX1's own trim
        // and zero the master exactly once:
        //   * RX1's effective gain (master + trim) is preserved to the dB, and
        //     the trim the operator can actually reach regains its full travel.
        //   * RX2..N stop inheriting the stranded bias, which is the pre-split
        //     behaviour their own sliders have always claimed.
        // A deliberate master set after the fold is never re-folded: the marker
        // rides along on every later snapshot write.
        bool afMasterFolded = false;
        if (rsSnap is not null && !rsSnap.AfMasterSplitMigrated && _state.RxAfGainDb != 0.0)
        {
            double strandedMasterDb = _state.RxAfGainDb;
            _state = _state with
            {
                Rx1AfGainDb = Math.Clamp(
                    strandedMasterDb + _state.Rx1AfGainDb, MinRxAfGainDb, MaxRxAfGainDb),
                RxAfGainDb = 0.0,
            };
            // RX2 takes the master only if the operator's last session actually
            // heard it there. A row with no Rx1AfGainDb predates the split, so
            // RX2 ran on its own slider alone and its stored value is already
            // the level to keep; a split-era row has been composing
            // master + trim into RX2's audio, so fold the master in to hold
            // that level. Either way RX2 neither jumps nor drops here, and its
            // slider starts telling the truth about what it produces.
            if (rsSnap.Rx1AfGainDb is not null)
            {
                hydAfGainB = Math.Clamp(
                    strandedMasterDb + hydAfGainB, MinRxAfGainDb, MaxRxAfGainDb);
            }
            afMasterFolded = true;
        }

        // Seed the canonical Receivers[] so RX2's hydrated tuning is the live
        // source of truth from the very first snapshot. RX1 (index 0) is rebuilt
        // from the flat RX1 fields by ProjectReceivers on every later Mutate;
        // index 1's tuning is carried forward (the flat VFO-B fields are gone).
        _state = _state with
        {
            Receivers = new ReceiverDto[]
            {
                new(Index: 0, Enabled: true, AdcSource: 0,
                    VfoHz: _state.VfoHz, Mode: _state.Mode,
                    FilterLowHz: _state.FilterLowHz, FilterHighHz: _state.FilterHighHz,
                    FilterPresetName: _state.FilterPresetName, AfGainDb: _state.Rx1AfGainDb,
                    SampleRateHz: _state.SampleRate, Muted: _state.Rx1Muted),
                new(Index: 1, Enabled: _state.Rx2Enabled, AdcSource: 0,
                    VfoHz: hydVfoB, Mode: hydModeB,
                    FilterLowHz: hydFilterLowB, FilterHighHz: hydFilterHighB,
                    FilterPresetName: hydPresetB, AfGainDb: hydAfGainB,
                    SampleRateHz: _state.SampleRate, Muted: _state.Rx2Muted),
            },
        };

        // Persist the fold immediately. A crash before the next natural flush
        // would otherwise re-run it on the following start; the fold is applied
        // to the same stored values either way, so a lost flush is harmless,
        // but writing now keeps the stored row and the live state in agreement.
        if (afMasterFolded)
        {
            // The fold assigns _state directly rather than going through
            // Mutate(), so mark the row dirty for FlushState()'s own guard.
            _stateDirty = true;
            FlushState();
        }

        // Upgrade state written by the old symmetric DIGU/DIGL preset table.
        // Mutating once here makes the persisted command, StateDto/UI/TCI view,
        // and every DSP consumer agree immediately after an upgrade.
        var primaryFilter = NormalizeLegacyDigitalFilter(
            _state.Mode, _state.FilterLowHz, _state.FilterHighHz);
        var secondary = _state.Rx2();
        var secondaryFilter = NormalizeLegacyDigitalFilter(
            secondary.Mode, secondary.FilterLowHz, secondary.FilterHighHz);
        if (familyFilterMigrated
            || primaryFilter != (_state.FilterLowHz, _state.FilterHighHz)
            || secondaryFilter != (secondary.FilterLowHz, secondary.FilterHighHz))
        {
            Mutate(s => WithRx2(
                s with
                {
                    FilterLowHz = primaryFilter.low,
                    FilterHighHz = primaryFilter.high,
                },
                r => r with
                {
                    FilterLowHz = secondaryFilter.low,
                    FilterHighHz = secondaryFilter.high,
                }));
            FlushState();
        }

        // The last-loaded TX Audio Profile is authoritative at startup. Keep
        // the active mode-family memory in lockstep with its overlaid bandpass
        // so a later SetMode/band recall — or the legacy-upgrade flush above —
        // cannot resurrect a stale saved slot. Runs AFTER the upgrade block so
        // its final persisted TX-family value always reflects the profile.
        if (overlayTxFilterLow.HasValue && overlayTxFilterHigh.HasValue)
        {
            int loAbs = Math.Min(Math.Abs(overlayTxFilterLow.Value), Math.Abs(overlayTxFilterHigh.Value));
            int hiAbs = Math.Max(Math.Abs(overlayTxFilterLow.Value), Math.Abs(overlayTxFilterHigh.Value));
            var family = TxFamilyFilterFor(_state.Mode);
            if (family.LoAbs != loAbs || family.HiAbs != hiAbs)
            {
                StoreTxFamilyFilter(_state.Mode, loAbs, hiAbs);
                _stateDirty = true;
                FlushState();
            }
        }

        // Kick off the debounce flush timer. Fires every 1 s; only writes to
        // LiteDB when _stateDirty is set (i.e., at least one Mutate() has fired
        // since the last flush). Keeps RadioService latency unaffected by disk IO
        // during rapid VFO scroll or filter drags.
        if (_radioStateStore is not null)
            _stateFlushTimer = new System.Threading.Timer(_ =>
            {
                if (_disposed) return;
                try { FlushState(); }
                catch { /* never escape on a timer thread */ }
            }, null, 1_000, 1_000);
    }

    /// <summary>
    /// Single-source-of-truth Upsert helper for the PS settings store. Reads
    /// the current StateDto snapshot and writes the full PsSettingsEntry so
    /// callers don't drop fields by writing only what they touched. Called
    /// from SetPs, SetPsAdvanced, SetPsFeedbackSource, and SetTwoTone.
    ///
    /// Runtime PsEnabled is never written to the store. ArmIntent is the
    /// separate persisted operator decision; only SetPs changes its in-memory
    /// mirror, so every other caller preserves it through a full-row rewrite.
    /// TwoToneEnabled remains session-only because it can key the transmitter.
    /// PsHwPeak IS persisted per-connected-board via the HwPeakByBoard dictionary;
    /// when no board is currently connected the HwPeak portion of the write
    /// is skipped (existing per-board entries are preserved untouched).
    /// </summary>
    private void PersistPsState()
    {
        if (_psStore is null) return;
        var snap = Snapshot();
        bool armIntent;
        lock (_sync) armIntent = _psArmIntent;
        // Preserve any existing per-board HW Peak map and only mutate the
        // slot owned by the currently-connected radio. Reset / disconnect
        // paths set _currentPsBoardKey back to empty, which skips the HW
        // Peak write entirely — operators don't lose other-board entries.
        var existing = _psStore.Get();
        var hwPeakByBoard = existing?.HwPeakByBoard is { } map
            ? new Dictionary<string, double>(map)
            : new Dictionary<string, double>();
        if (!string.IsNullOrEmpty(_currentPsBoardKey))
        {
            hwPeakByBoard[_currentPsBoardKey] = snap.PsHwPeak;
        }
        // Same per-board preserve-then-mutate as HW Peak. Only write the TX
        // attenuation slot when we actually have a value for the connected
        // board (>= 0); otherwise carry the existing map through untouched so
        // a HW-Peak-triggered persist never wipes a saved attenuation.
        var txAttnByBoard = existing?.TxAttnByBoard is { } amap
            ? new Dictionary<string, int>(amap)
            : new Dictionary<string, int>();
        if (!string.IsNullOrEmpty(_currentPsBoardKey) && _currentPsTxAttnDb >= 0)
        {
            txAttnByBoard[_currentPsBoardKey] = _currentPsTxAttnDb;
        }
        _psStore.Upsert(new PsSettingsEntry
        {
            ArmIntent = armIntent,
            Auto = snap.PsAuto,
            AutoAttenuate = snap.PsAutoAttenuate,
            MoxDelaySec = snap.PsMoxDelaySec,
            LoopDelaySec = snap.PsLoopDelaySec,
            AmpDelayNs = snap.PsAmpDelayNs,
            Source = snap.PsFeedbackSource,
            TwoToneFreq1 = snap.TwoToneFreq1,
            TwoToneFreq2 = snap.TwoToneFreq2,
            TwoToneMag = snap.TwoToneMag,
            HwPeakByBoard = hwPeakByBoard,
            TxAttnByBoard = txAttnByBoard,
            // Carry the migration marker forward — this rebuild replaces the
            // whole entry, and dropping the marker back to 0 would re-run the
            // HermesC10 poison wipe on the next startup, deleting a freshly
            // calibrated value. A brand-new entry (no prior record) is stamped
            // at the current version for the same reason: it was created after
            // the poison window and must never be wiped.
            // See PsSettingsStore.MigrateTxAttnPoison.
            TxAttnMigration = existing?.TxAttnMigration ?? PsSettingsStore.TxAttnMigrationCurrent,
        });
    }

    /// <summary>
    /// Record + persist the PS TX feedback attenuation the auto-attenuate
    /// dance (or a manual operator control) settled on, for the currently-
    /// connected board. Restored to the radio on the next connect by
    /// DspPipelineService so a hot external-tap feedback chain doesn't boot
    /// at 0 dB and re-saturate the feedback ADC. No-op persistence when no
    /// board is connected (no slot to write).
    /// </summary>
    public void SetPsTxAttenuationDb(int db)
    {
        _currentPsTxAttnDb = db;
        PersistPsState();
        // Surface the live value so the PURESIGNAL panel's manual control and
        // the "differs" hint track what's actually applied.
        Mutate(s => s.PsTxFeedbackAttenuationDb == db ? s : s with { PsTxFeedbackAttenuationDb = db });
    }

    /// <summary>
    /// Surface a live servo attenuation value in state WITHOUT persisting.
    /// The G2E (HermesC10) two-tone servo walks the wire value live but only
    /// persists a value that produced an in-window fit (a completed
    /// calibration) — mid-walk values must stay visible to the operator's
    /// PURESIGNAL panel yet never land in the per-board store (the #1249
    /// poison-ratchet class). The eventual in-window persist goes through
    /// <see cref="SetPsTxAttenuationDb"/> as usual.
    /// </summary>
    public void SetPsTxAttenuationDbStateOnly(int db)
        => Mutate(s => s.PsTxFeedbackAttenuationDb == db ? s : s with { PsTxFeedbackAttenuationDb = db });

    /// <summary>
    /// Persisted PS TX feedback attenuation (dB) for the currently-connected
    /// board, or null if none has been saved yet. Called on connect to
    /// restore the radio's feedback attenuation before the operator arms PS.
    /// Side effect: seeds <see cref="_currentPsTxAttnDb"/> so a later
    /// PersistPsState preserves the slot rather than treating it as unset.
    /// </summary>
    public int? GetPersistedPsTxAttnDb()
    {
        if (string.IsNullOrEmpty(_currentPsBoardKey)) return null;
        var persisted = _psStore?.Get();
        if (persisted?.TxAttnByBoard is { } map
            && map.TryGetValue(_currentPsBoardKey, out int db))
        {
            _currentPsTxAttnDb = db;
            return db;
        }
        return null;
    }

    /// <summary>
    /// Build the per-board PS settings key used by PsSettingsEntry.HwPeakByBoard.
    /// Format: `{p1|p2}:{board}[:variant]` where the variant suffix is only
    /// present when board is `OrionMkII` and we're on P2 (the 0x0A wire-byte
    /// alias family — G2, G2_1K, Anan7000DLE, Anan8000DLE, OrionMkII original,
    /// AnvelinaPro3, RedPitaya — each has a distinct feedback chain).
    /// </summary>
    internal static string GetPsBoardKey(bool isProtocol2, HpsdrBoardKind board, OrionMkIIVariant variant)
    {
        string proto = isProtocol2 ? "p2" : "p1";
        if (isProtocol2 && board == HpsdrBoardKind.OrionMkII)
            return $"{proto}:{board}:{variant}";
        return $"{proto}:{board}";
    }

    // Ribbon-visibility setter — frontend toggles via REST, server broadcasts
    // a StateDto so other browser tabs stay in sync.
    public StateDto SetFilterAdvancedPaneOpen(bool open)
    {
        _filterPresetStore?.SetAdvancedPaneOpen(open);
        Mutate(s => s with { FilterAdvancedPaneOpen = open });
        return Snapshot();
    }

    public IProtocol1Client? ActiveClient
    {
        get { lock (_sync) return _activeClientForTest ?? _activeClient; }
    }

    /// <summary>
    /// Firmware / gateware version string for the live connection (e.g.
    /// <c>"10.3"</c>), captured at connect from the discovery reply, or
    /// <c>null</c> when no discovery info was available. Read-only; consumed by
    /// the diagnostics ConnectionProbe so a "Report a problem" snapshot records
    /// the exact firmware the operator is running.
    /// </summary>
    public string? ConnectedFirmware
    {
        get { lock (_sync) return _connectedFirmware; }
    }

    /// <summary>
    /// True when any backend (P1 or P2) has a live connection. Needed by
    /// TxService's MOX / TUN interlock — a G2 on P2 has no ActiveClient
    /// (Protocol1Client is null) but still wants to accept TX requests.
    /// </summary>
    public bool IsConnected
    {
        get { lock (_sync) return _activeClientForTest is not null || _activeClient is not null || _p2Active || _p3Active; }
    }

    /// <summary>True while a Protocol-1 client owns the connection — i.e. the
    /// EP2 TX loop (and its RX-audio L/R slots) is live. RadioSpeakerAudioSink
    /// gates on this so it only feeds the P1 RxAudioRing when something will
    /// actually drain it.</summary>
    internal bool IsProtocol1Active
    {
        get { lock (_sync) return _activeClientForTest is not null || _activeClient is not null; }
    }

    /// <summary>True while a Protocol-2 connection owns the radio — the symmetric
    /// counterpart to <see cref="IsProtocol1Active"/>. SaturnSpeakerAudioSink
    /// gates on this so the P2 UDP speaker path (port 1028) only opens under a
    /// real P2 connection, never cross-firing at a P1 radio (which doesn't bind
    /// 1028) when a dual-protocol codec board is on P1.</summary>
    internal bool IsProtocol2Active
    {
        get { lock (_sync) return _p2Active; }
    }

    internal bool IsProtocol3Active
    {
        get { lock (_sync) return _p3Active; }
    }

    /// <summary>Cheap MOX accessor (no Receivers projection, unlike Snapshot).
    /// Used on the per-AudioFrame path to skip feeding RX audio to the radio
    /// codec while transmitting.</summary>
    internal bool IsMox
    {
        get { lock (_sync) return _mox; }
    }

    /// <summary>Atomic CAT IF projection. Frequency, mode, split, and the transient
    /// wire MOX latch must come from the same radio-state acquisition so an
    /// unsolicited status frame cannot mix opposite sides of a MOX edge.</summary>
    internal (long VfoHz, RxMode Mode, bool Mox, bool Split) SnapshotCatIfState()
    {
        lock (_sync)
        {
            var projected = ProjectedStateUnderLock();
            return (_state.VfoHz, _state.Mode, _mox,
                RadioFrequencyResolver.IsSplitEnabledForTx(projected));
        }
    }

    // Caller holds _sync. TX consumers need the same canonical receiver view
    // Snapshot publishes, including session-only RX3+ split state.
    private StateDto ProjectedStateUnderLock() => _state with
    {
        Receivers = ProjectReceivers(_state),
        MaxReceivers = EffectiveMaxReceivers,
        ConnectedProtocol = ConnectedProtocolLocked(),
    };

    public StateDto Snapshot()
    {
        lock (_sync) return ProjectedStateUnderLock();
    }

    /// <summary>Current RX1 mode without taking the radio-state lock or
    /// projecting a StateDto. Intended for protocol RX edge handlers.</summary>
    internal RxMode CurrentMode => (RxMode)_currentMode;

    /// <summary>Current operator preamp toggle. PreampOn isn't on the
    /// StateDto wire format, so DspPipelineService reads it directly when
    /// it needs to push the value into a freshly-opened Protocol2Client
    /// (issue #126). Lock-safe so a connect-time read can't tear against
    /// a concurrent SetPreamp.</summary>
    public bool PreampOn { get { lock (_sync) return _preampOn; } }

    /// <summary>Effective RX step attenuator in dB — operator baseline
    /// (<see cref="StateDto.AttenDb"/>) plus any auto-ATT overload offset
    /// (<see cref="StateDto.AttOffsetDb"/>), clamped to 0..31. This is the
    /// value that lands on the wire (CmdHighPriority byte 1443 on P2;
    /// CC0=0x14 on P1). Exposed for DspPipelineService.ConnectP2Async so a
    /// fresh P2 client is initialised with the operator's current effective
    /// atten before its first CmdHighPriority emission.</summary>
    public int EffectiveAttenDb
    {
        get
        {
            lock (_sync)
                return Math.Clamp(_atten.ClampedDb + _attOffsetDb, HpsdrAtten.MinDb, HpsdrAtten.MaxDb);
        }
    }

    internal int ResolveConnectSampleRateHz(
        HpsdrBoardKind discoveredKind,
        int requestedHz,
        bool protocol2,
        bool requestedExplicitly = true)
    {
        int requested = MapSampleRate(requestedHz).SampleRateHz();
        var board = ResolveBoardKindForPreferences(
            discoveredKind,
            protocol2 ? HpsdrBoardKind.OrionMkII : HpsdrBoardKind.Unknown);
        int maxHz = MaxAllowedSampleRateHz(board, protocol2);
        if (requested > maxHz)
        {
            _log.LogWarning(
                "radio.connect requested sample-rate {Requested} exceeds board={Board} max={Max}; clamping",
                requested, board, maxHz);
            requested = maxHz;
        }

        if (_radioStateStore is not null && board != HpsdrBoardKind.Unknown)
        {
            var storedHz = _radioStateStore.GetBoardSampleRate(board, EffectiveOrionMkIIVariant);
            if (storedHz.HasValue)
            {
                var stored = MapSampleRate(storedHz.Value).SampleRateHz();
                if (stored > maxHz)
                {
                    _log.LogWarning(
                        "radio.connect sample-rate store has rate={Rate} above board={Board} max={Max}; using requested rate={Requested}",
                        stored, board, maxHz, requested);
                    return requested;
                }

                return stored;
            }
        }

        if (!requestedExplicitly &&
            _defaultConnectSampleRateHz is int configuredHz &&
            TryResolveConfiguredConnectSampleRateHz(configuredHz, board, maxHz, protocol2, requested, out var configured))
        {
            return configured;
        }

        return requested;
    }

    private bool TryResolveConfiguredConnectSampleRateHz(
        int configuredHz,
        HpsdrBoardKind board,
        int maxHz,
        bool protocol2,
        int requested,
        out int resolved)
    {
        resolved = requested;
        int configured;
        try
        {
            configured = MapSampleRate(configuredHz).SampleRateHz();
        }
        catch (ArgumentException)
        {
            _log.LogWarning(
                "radio.connect configured default sample-rate {Rate} is not supported; using requested rate={Requested}",
                configuredHz,
                requested);
            return false;
        }

        if (!protocol2 && configured > HpsdrSampleRate.Rate384k.SampleRateHz())
        {
            _log.LogWarning(
                "radio.connect configured default sample-rate {Rate} is Protocol-2 only; using requested rate={Requested}",
                configured,
                requested);
            return false;
        }

        if (configured > maxHz)
        {
            _log.LogWarning(
                "radio.connect configured default sample-rate {Rate} exceeds board={Board} max={Max}; clamping",
                configured,
                board,
                maxHz);
            configured = maxHz;
        }

        resolved = configured;
        return true;
    }

    private int MaxAllowedSampleRateHz(HpsdrBoardKind board, bool protocol2)
    {
        var boardMax = BoardCapabilitiesTable
            .For(board, EffectiveOrionMkIIVariant)
            .MaxRxSampleRateHz;
        return protocol2
            ? boardMax
            : Math.Min(boardMax, 384_000);
    }

    private HpsdrBoardKind ResolveBoardKindForPreferences(HpsdrBoardKind discoveredKind, HpsdrBoardKind fallbackKind)
    {
        if (_preferredRadioStore?.GetOverrideDetection() == true)
        {
            var preferred = _preferredRadioStore.Get();
            if (preferred.HasValue && preferred.Value != HpsdrBoardKind.Unknown)
                return preferred.Value;
        }

        return discoveredKind != HpsdrBoardKind.Unknown
            ? discoveredKind
            : fallbackKind;
    }

    /// <summary>
    /// Applies the current operator board selection to a live Protocol-1
    /// client only while its EP6 framing and transmit path are idle. A deferred
    /// selection is deliberately left for the next connection rather than
    /// changing receiver-count framing on an armed or keyed stream.
    /// </summary>
    internal void ApplyBoardKindToActiveClientIfSafe()
    {
        Protocol1Client client;
        HpsdrBoardKind previous;
        string? deferredReason = null;

        HpsdrBoardKind discovered;
        lock (_sync)
        {
            client = _activeClient!;
            if (client is null) return;
            discovered = _activeP1ConnectionAttempt?.DiscoveredKind
                ?? HpsdrBoardKind.Unknown;
        }

        // PreferredRadioStore can raise Changed while holding its own lock.
        // Resolve outside _sync so this path does not add the inverse lock edge.
        var resolved = ResolveBoardKindForPreferences(
            discovered,
            HpsdrBoardKind.Unknown);
        if (resolved == HpsdrBoardKind.Unknown) return;

        lock (_sync)
        {
            if (!ReferenceEquals(_activeClient, client)) return;
            previous = client.BoardKind;
            if (resolved == previous) return;

            if (_state.PsEnabled || client.PsEnabled)
                deferredReason = "PureSignal is armed";
            else if (_tunActive)
                deferredReason = "TUNE is active";
            else if (_mox)
                deferredReason = "MOX is active";

            if (deferredReason is null)
                client.SetBoardKind(resolved);
        }

        if (deferredReason is not null)
        {
            _log.LogWarning(
                "radio.board override change deferred because {Reason}; board={Board} will be retried when safe and otherwise takes effect on the next connect",
                deferredReason,
                resolved);
            return;
        }

        _log.LogInformation(
            "radio.board override applied to live Protocol-1 client previous={Previous} board={Board}",
            previous,
            resolved);
        // PreferredRadioStore.Changed fired before the client board latch was
        // safe to update, so replay PA/filter-derived state once the new board
        // is authoritative for the live P1 client.
        RecomputePaAndPush();
    }

    public async Task<StateDto> ConnectAsync(string endpoint, int sampleRate, CancellationToken ct = default,
        HpsdrBoardKind discoveredKind = HpsdrBoardKind.Unknown, string? firmware = null,
        PhysicalAddress? mac = null)
    {
        var (generation, operatorActionToken) = BeginOperatorConnectionAction();
        var attempt = new P1ConnectionAttempt(
            endpoint,
            sampleRate,
            discoveredKind,
            firmware,
            mac,
            generation,
            operatorActionToken,
            IsAutomaticRetry: false);
        await RadioLifecycleGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await DisconnectSupersededP1AutomaticRetryAsync().ConfigureAwait(false);
            return await ConnectCoreAsync(attempt, ct)
                .ConfigureAwait(false);
        }
        finally
        {
            RadioLifecycleGate.Release();
        }
    }

    private async Task<StateDto> ConnectCoreAsync(
        P1ConnectionAttempt attempt,
        CancellationToken ct)
    {
        string endpoint = attempt.Endpoint;
        int sampleRate = attempt.SampleRate;
        var discoveredKind = attempt.DiscoveredKind;
        string? firmware = attempt.Firmware;
        if (!TryParseEndpoint(endpoint, out var ipEndpoint))
            throw new ArgumentException($"Invalid endpoint '{endpoint}'.", nameof(endpoint));

        // This is the Protocol 1 connect path (it constructs a Protocol1Client).
        // P1's 2-bit rate field caps at 384 kHz; 768/1536 kHz are Protocol 2 only,
        // so reject rather than silently wrap on the wire.
        if (sampleRate > 384_000)
            throw new ArgumentException(
                $"Protocol 1 supports up to 384 kHz; {sampleRate} Hz requires Protocol 2.", nameof(sampleRate));

        var hpsdrRate = MapSampleRate(sampleRate);

        Protocol1Client client;
        Action? clientDisconnectedHandler = null;
        lock (_sync)
        {
            if (_operatorConnectionGeneration != attempt.Generation
                || attempt.OperatorActionToken.IsCancellationRequested)
                throw new OperationCanceledException(attempt.OperatorActionToken);
            if (_activeClient is not null || _p2Active || _p3Active)
                throw new InvalidOperationException("Already connected. Disconnect first.");

            // Drop any RX-audio buffered from a previous session so a fresh
            // P1 connect starts from silence rather than replaying a stale tail.
            (_rxAudioSource as Zeus.Protocol1.RxAudioRing)?.Clear();
            client = new Protocol1Client(
                _loggerFactory.CreateLogger<Protocol1Client>(),
                _txIqSource,
                _rxAudioSource);
            client.AdcOverloadObserved += OnAdcOverload;
            clientDisconnectedHandler = () => OnClientDisconnected(client, attempt);
            client.Disconnected += clientDisconnectedHandler;
            _activeClientDisconnectedHandler = clientDisconnectedHandler;
            // #1302 F4: PS feedback watchdog — fires when an armed HermesC10
            // stream stops yielding parseable 4-DDC packets for 2 s while
            // datagrams still arrive. Auto-disarm PS through the normal
            // StateDto flow so the UI reflects it and DspPipelineService
            // restarts the radio out of the misframed state.
            client.PsFeedbackStalled += OnPsFeedbackStalled;
            _activeClient = client;
            _activeP1ConnectionAttempt = attempt;
            // Record the discovered firmware for the diagnostics snapshot.
            _connectedFirmware = firmware;
            _state = _state with
            {
                Status = ConnectionStatus.Connecting,
                Endpoint = endpoint,
                SampleRate = hpsdrRate.SampleRateHz(),
                // A connection always begins from runtime disarm. Persisted
                // intent is applied only at the completed-connect sanitize
                // hook, after calibration and attenuation restoration.
                PsEnabled = false,
            };
            // Fresh connection — reset per-session auto-ATT state so a sticky
            // offset from a previous session doesn't leak onto new hardware.
            _attOffsetDb = 0;
            _predictiveMagnitudeControlActive = false;
            _adcOverloadLevel = 0;
            ResetAdcProtectionWindowNoLock();
            _lastTickMs = long.MinValue;
            _lastAttAttackMs = long.MinValue;
            _adcProtectionResumeAfterMs = long.MinValue;
            _lastOverloadMs = long.MinValue;
            _lastAppliedEffectiveDb = -1;
        }
        StateChanged?.Invoke(Snapshot());

        try
        {
            if (!_bypassP1SocketIoForTests)
                await client.ConnectAsync(ipEndpoint, ct).ConfigureAwait(false);
            // Latch the effective board before any board-gated configuration
            // reads the client. With Override Detection off (or Auto selected),
            // discovery still wins exactly as before. Unknown keeps the fresh
            // client's backwards-compatible default.
            var resolvedKind = ResolveBoardKindForPreferences(
                discoveredKind,
                HpsdrBoardKind.Unknown);
            if (resolvedKind != HpsdrBoardKind.Unknown)
                client.SetBoardKind(resolvedKind);
            // #1302 F2: every fresh client starts explicitly disarmed before
            // StartAsync, so no PS-enable bit can precede completed-connect
            // calibration restore. The connect-tail sanitize later performs
            // the required live disarm/rearm transition when ArmIntent is set.
            // Board-gated effects only: on non-PS P1 boards this stores a
            // state-tracking flag with zero wire impact.
            client.SetPsEnabled(false);
            // Angelia and the other legacy P1 gateware reset internal_CW off
            // and only latch it from C&C address 0x0F. Seed both CW registers
            // before StartAsync so a cold connection in CW mode does not
            // depend on another client having primed the radio first.
            var connectCwState = Snapshot();
            client.SetCwKeyerConfig(
                Volatile.Read(ref _cwKeyerWpm),
                (CwKeyerMode)Volatile.Read(ref _cwKeyerMode),
                Volatile.Read(ref _cwWeight),
                Volatile.Read(ref _cwPaddleReverse) != 0);
            int connectSidetoneHz;
            double connectSidetoneGainDb;
            lock (_sync) { connectSidetoneHz = _cwSidetoneHz; connectSidetoneGainDb = _cwSidetoneGainDb; }
            client.SetCwSidetone(HardwareSidetoneLevel(connectSidetoneGainDb), connectSidetoneHz);
            client.SetCwKeyerEnabled(IsCwMode(RadioFrequencyResolver.TxMode(connectCwState)));
            int restoredHz = ResolveConnectSampleRateHz(client.BoardKind, hpsdrRate.SampleRateHz(), protocol2: false);
            if (restoredHz != hpsdrRate.SampleRateHz())
            {
                hpsdrRate = MapSampleRate(restoredHz);
                lock (_sync)
                    _state = _state with { SampleRate = hpsdrRate.SampleRateHz() };
            }
            if (!_bypassP1SocketIoForTests)
                await client.StartAsync(new StreamConfig(hpsdrRate, _preampOn, _atten), ct).ConfigureAwait(false);
            ThrowIfSuperseded(attempt);
            // StreamConfig carries the legacy ADC0 attenuator value. Re-apply
            // the primary receiver route now that the client is running so a
            // receiver assigned to ADC1 gets C&C 0x0B instead of ADC0's 0x0A.
            ApplyPrimaryAttenuatorToActiveClient(EffectiveAttenDb);
            // Retune the radio to the persisted hardware NCO (RadioLoHz). The
            // dial (VfoHz) may sit elsewhere; WDSP's shift stage covers the
            // gap. Hydration above already guarantees RadioLoHz != 0 by
            // snapping to VfoHz on legacy rows, so a plain SetVfoAHz here is
            // always valid. See docs/prd/panfall_behavior.md.
            var connectSnap = Snapshot();
            client.SetVfoAHz(ToHardwareFrequencyHz(connectSnap.RadioLoHz));

            // Default-on the N2ADR 7-relay filter board for HL2 — mirrors
            // Thetis's HERCULES preset (setup.cs:14642). Most HL2 deployments
            // ship with N2ADR; without this the OC pins stay 0 and the LPF
            // relays never click. Operators on bare HL2 (no filter board) can
            // override via PA Settings once that knob is exposed.
            if (client.BoardKind == HpsdrBoardKind.HermesLite2)
                client.SetHasN2adr(true);

            // HL2 Band Volts PWM enable (issue #279) — rehydrate the
            // persisted operator preference into the fresh client so the
            // very first outgoing Config frame carries the correct bit.
            // Honoured on HL2 only; on every other board the flag is set
            // but the wire effect (legacy LT2208 DITHER) is not requested
            // here, since Zeus only flips it from the HL2 settings panel.
            if (client.BoardKind == HpsdrBoardKind.HermesLite2
                && _preferredRadioStore is not null)
            {
                client.EnableHl2BandVolts = _preferredRadioStore.GetEnableHl2BandVolts();
            }

            // LT2208 ADC dither / digital-output randomizer — rehydrate the
            // persisted operator preference into the fresh client so the first
            // Config frame carries the correct C3 bits 3/4. Skipped on HL2 (no
            // LT2208; its bit 3 is Band Volts, seeded above). Default off, so a
            // board the operator has never configured stays byte-identical.
            if (client.BoardKind != HpsdrBoardKind.HermesLite2)
            {
                ApplyAdcOptionsToP1Client(client, client.BoardKind);
            }

            // Frequency-correction factor (issue #325) — rehydrate so the
            // first tune-write on the fresh client carries the operator's
            // calibrated correction. Cheap default when no calibration has
            // run (factor = 1.0).
            if (_preferredRadioStore is not null)
            {
                client.SetFrequencyCorrectionFactor(_preferredRadioStore.GetFrequencyCorrectionFactor());
            }

            ThrowIfSuperseded(attempt);
            Mutate(s => s with { Status = ConnectionStatus.Connected, PsEnabled = false });
            _log.LogInformation("radio.connected endpoint={Ep} rate={Rate}", ipEndpoint, hpsdrRate);
            Connected?.Invoke(client);
            // N2ADR 7-relay low-pass filter board is standard equipment on HL2.
            // Enable it unconditionally on connect so band changes immediately
            // drive the relay coils. Future work: make this a user toggle IFF a
            // compelling reason to ship bare HL2 without N2ADR emerges.
            if (ConnectedBoardKind == HpsdrBoardKind.HermesLite2)
                client.SetHasN2adr(true);
            // Replay PA settings into the fresh client — drive byte, OC masks,
            // and (for P2 downstream) PA-enable. Without this the client sits
            // at the protocol defaults (drive=0, OC=0) until something else
            // moves.
            RecomputePaAndPush();
            // Replay the global audio front-end (external-audio-jacks re-port)
            // so the operator's source/boost/bias/gain selection is on the first
            // outgoing frame, not deferred until they touch the panel. Per-board
            // clamp + the OFF defaults make this a no-op (byte-identical) on
            // boards without an audio front-end.
            PushAudioFrontEnd();
            // Replay the persisted HL2 user-GPIO mask (external-port parity audit)
            // onto the fresh client. Gated on HasHl2UserGpio, so on non-HL2 boards
            // it pushes mask 0 → byte-identical.
            PushHl2Gpio();
            return Snapshot();
        }
        catch
        {
            if (clientDisconnectedHandler is not null)
                client.Disconnected -= clientDisconnectedHandler;
            lock (_sync)
            {
                if (ReferenceEquals(_activeClient, client))
                {
                    _activeClient = null;
                    _activeP1ConnectionAttempt = null;
                    if (ReferenceEquals(_activeClientDisconnectedHandler, clientDisconnectedHandler))
                        _activeClientDisconnectedHandler = null;
                }
            }
            await TearDownClientAsync(client).ConfigureAwait(false);
            Mutate(s => s with { Status = ConnectionStatus.Error, Endpoint = null });
            throw;
        }
    }

    public Task<StateDto> DisconnectAsync(CancellationToken ct = default)
    {
        BeginOperatorConnectionAction();
        return DisconnectClientAsync(expectedClient: null, ct);
    }

    private async Task<StateDto> DisconnectClientAsync(
        Protocol1Client? expectedClient,
        CancellationToken ct)
    {
        await RadioLifecycleGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await DisconnectClientCoreAsync(expectedClient, ct).ConfigureAwait(false);
        }
        finally
        {
            RadioLifecycleGate.Release();
        }
    }

    private async Task<StateDto> DisconnectClientCoreAsync(
        Protocol1Client? expectedClient,
        CancellationToken ct)
    {
        Protocol1Client? client;
        Action? disconnectedHandler;
        lock (_sync)
        {
            // The RX callback is asynchronous. An operator may have already
            // disconnected and installed a replacement client while it waited;
            // never let the old callback claim the new session.
            if (expectedClient is not null && !ReferenceEquals(_activeClient, expectedClient))
                return _state;
            if (_activeClient is null && (_p2Active || _p3Active))
                return _state;
            client = _activeClient;
            _activeClient = null;
            _activeP1ConnectionAttempt = null;
            disconnectedHandler = _activeClientDisconnectedHandler;
            _activeClientDisconnectedHandler = null;
            _connectedFirmware = null;
        }

        if (client is not null)
        {
            if (disconnectedHandler is not null)
                client.Disconnected -= disconnectedHandler;
            client.AdcOverloadObserved -= OnAdcOverload;
            client.PsFeedbackStalled -= OnPsFeedbackStalled;
            Disconnected?.Invoke();
            await TearDownClientAsync(client, ct).ConfigureAwait(false);
            _log.LogInformation("radio.disconnected");
        }

        Mutate(s => s with
        {
            Status = ConnectionStatus.Disconnected,
            Endpoint = null,
            PsEnabled = false,
            AttOffsetDb = 0,
            AdcOverloadWarning = false,
        });
        // Drop the PS board key — any SetPsAdvanced call between now and the
        // next connect (e.g. operator dialling in the panel while
        // disconnected) should NOT write into the previous radio's slot.
        // ApplyPsHwPeakForConnection sets it again on next connect.
        _currentPsBoardKey = string.Empty;
        // Same for the TX-attn mirror — next connect re-seeds it from the
        // persisted slot via GetPersistedPsTxAttnDb.
        _currentPsTxAttnDb = -1;
        return Snapshot();
    }

    private TransverterSettingsDto ActiveTransverterSettings()
    {
        if (_transverterSettingsStore is null || _layoutStore is null)
            return new TransverterSettingsDto();
        return _transverterSettingsStore.GetForConnectedRadio(_layoutStore, this);
    }

    internal bool IsTransverterActive => ActiveTransverterSettings().Enabled;

    private long _productPluginNativeFrequencyHz;
    private readonly object _productPluginFrequencyConfigSync = new();

    // Constrained native-channel TX must not translate its NCO using a
    // transverter setting concurrently saved before its change callback runs.
    // Held from admission through wire unkey; never persisted.
    internal void SetProductPluginNativeFrequencyHold(long? frequencyHz)
    {
        if (frequencyHz is null)
        {
            // Clearing follows wire unkey and never competes with calibration
            // admission. Do not acquire the config lock here: an ordinary TX
            // trip can already hold radio state while calibration waits for it.
            lock (_sync) Interlocked.Exchange(ref _productPluginNativeFrequencyHz, 0);
            return;
        }
        lock (_productPluginFrequencyConfigSync)
        lock (_sync) Interlocked.Exchange(ref _productPluginNativeFrequencyHz, frequencyHz.Value);
    }

    internal StateDto ReconcileProductPluginTxState(StateDto state) =>
        Interlocked.Read(ref _productPluginNativeFrequencyHz) > 0 ? Snapshot() : state;

    // Zeus P1 shares its primary host frequency slot with TX. Serialize the final write with the
    // channel hold, so a delayed RX setter cannot overwrite the admitted TX
    // carrier after key-on. P2 has a separate DUC and reconciles its snapshot.
    internal void PushPrimaryRadioFrequency(long frequencyHz)
    {
        lock (_sync)
        {
            long held = Interlocked.Read(ref _productPluginNativeFrequencyHz);
            ActiveClient?.SetVfoAHz(held > 0 && _mox ? held : ToHardwareFrequencyHz(frequencyHz));
        }
    }

    internal bool TryResolveTransverterBand(long rfHz, out TransverterBandDto band)
    {
        var settings = ActiveTransverterSettings();
        band = null!;
        return settings.Enabled
            && TransverterFrequencyConverter.TryResolveBandByRfHz(rfHz, settings, out band);
    }

    /// <summary>
    /// Translate an operator-facing RF value at the final hardware seam. The
    /// active profile remains authoritative just outside its dial range too,
    /// because CW pitch and CTUN move the physical NCO while the displayed RF
    /// stays at the profile boundary.
    /// </summary>
    internal long ToHardwareFrequencyHz(long rfHz)
    {
        if (Interlocked.Read(ref _productPluginNativeFrequencyHz) > 0)
            return Math.Clamp(rfHz, 0L, TransverterFrequencyConverter.MaximumRadioFrequencyHz);
        var settings = ActiveTransverterSettings();
        if (!settings.Enabled)
            return Math.Clamp(rfHz, 0L, TransverterFrequencyConverter.MaximumRadioFrequencyHz);
        TransverterBandDto? band = null;
        if (TransverterFrequencyConverter.TryResolveBandByRfHz(rfHz, settings, out var byRange))
            band = byRange;
        else if (rfHz <= TransverterFrequencyConverter.MaximumRadioFrequencyHz)
            return Math.Max(0, rfHz);
        else if (TransverterFrequencyConverter.TryResolveActiveBand(settings, out var active))
            band = active;
        if (band is null) return 0;
        try
        {
            long ifHz = checked(rfHz - band.LoOffsetHz + band.LoErrorHz);
            return ifHz is >= 0 and <= TransverterFrequencyConverter.MaximumTransverterIfFrequencyHz
                ? ifHz
                : 0;
        }
        catch (OverflowException)
        {
            return 0;
        }
    }

    internal double TransverterMeterCorrectionDb(long rfHz) =>
        TryResolveTransverterBand(rfHz, out var band) ? -band.RxGainDb : 0.0;

    internal string? RuntimeBandKey(long rfHz) =>
        TryResolveTransverterBand(rfHz, out var band)
            ? $"xvtr:{band.Id}"
            : BandUtils.FreqToBand(rfHz);

    internal bool IsExternalFrequencyAvailable(long hz, bool allowZero = false)
    {
        if (hz < (allowZero ? 0 : 1)) return false;
        var settings = ActiveTransverterSettings();
        if (settings.Enabled
            && TransverterFrequencyConverter.TryResolveBandByRfHz(hz, settings, out _))
            return true;
        return hz <= TransverterFrequencyConverter.MaximumRadioFrequencyHz;
    }

    internal long ClampTuningFrequency(long hz)
    {
        var settings = ActiveTransverterSettings();
        if (!settings.Enabled || hz <= TransverterFrequencyConverter.MaximumRadioFrequencyHz)
            return Math.Clamp(hz, 0L, TransverterFrequencyConverter.MaximumRadioFrequencyHz);
        if (TransverterFrequencyConverter.TryResolveBandByRfHz(hz, settings, out _))
            return hz;
        if (TransverterFrequencyConverter.TryResolveActiveBand(settings, out var active))
            return Math.Clamp(hz, active.BeginFrequencyHz, active.EndFrequencyHz);
        return TransverterFrequencyConverter.MaximumRadioFrequencyHz;
    }

    private void OnTransverterSettingsChanged()
    {
        var current = Snapshot();
        RevalidateTransverterChange(current);
        current = Snapshot();
        if (!IsExternalFrequencyAvailable(current.VfoHz, allowZero: true))
        {
            SetVfo(current.VfoHz, fromExternal: true);
            return;
        }
        PushPrimaryRadioFrequency(current.RadioLoHz);
        RecomputePaAndPush();
        StateChanged?.Invoke(current);
    }

    private void OnActiveTransverterSelectionChanged(string radioKey, bool enabled)
    {
        if (!string.Equals(radioKey, ConnectedBoardKind.ToString(), StringComparison.Ordinal))
            return;
        var before = Snapshot();
        RevalidateTransverterChange(before);
        if (enabled && TryPromoteActiveTransverterIfToRf())
            return;
        var current = Snapshot();
        if (!IsExternalFrequencyAvailable(current.VfoHz, allowZero: true))
        {
            SetVfo(current.VfoHz, fromExternal: true);
            return;
        }
        PushPrimaryRadioFrequency(current.RadioLoHz);
        RecomputePaAndPush();
        StateChanged?.Invoke(current);
    }

    private void RevalidateTransverterChange(StateDto state)
    {
        bool constrained = Interlocked.Read(ref _productPluginNativeFrequencyHz) > 0;
        try { TransmitSafetyStateChanging?.Invoke(state, state); }
        catch (TransmitSafetyRejectedException ex)
        {
            ex.CompleteAfterStateUnlock();
            // The settings were already saved before their change notification.
            // Once the fixed-channel key is fully down, finish applying those
            // settings to RX. A contested/pending transition still throws.
            if (!constrained || !CanApplyTransverterAfterChannelTrip()) throw;
        }
    }

    private bool CanApplyTransverterAfterChannelTrip()
    {
        lock (_sync)
            return _productPluginNativeFrequencyHz == 0 && !_mox && !_tunActive;
    }

    /// <summary>
    /// A workspace can enable its selected transverter while one or more
    /// receivers still carry physical IF values tuned in native mode. Promote
    /// every enabled hardware receiver's matching VFO and split-TX frequency,
    /// plus RX1's hardware-centre state, to external RF before the next state
    /// broadcast. The selected profile's RF bounds are the guard: unrelated
    /// native HF frequencies remain native even though transverter mode is on.
    /// </summary>
    private bool TryPromoteActiveTransverterIfToRf()
    {
        var settings = ActiveTransverterSettings();
        if (!settings.Enabled
            || !TransverterFrequencyConverter.TryResolveActiveBand(settings, out var active))
            return false;

        long radioLoHz = 0;
        Mutate(
            s =>
            {
                bool changed = false;
                var next = s;

                if (TryPromoteActiveBandIf(s.VfoHz, active, out long vfoRf))
                {
                    next = next with { VfoHz = vfoRf };
                    changed = true;
                    // CTUN and CW can place the hardware centre just outside
                    // the profile's dial range. Once RX1's dial is identified
                    // as this profile's IF, translate that native centre by the
                    // same LO equation without imposing the dial-range guard.
                    if (TryTranslateNativeIf(s.RadioLoHz, active, out long radioLoRf))
                        next = next with { RadioLoHz = radioLoRf };
                }

                if (TryPromoteActiveBandIf(s.SplitTxHz, active, out long splitTxRf))
                {
                    next = next with { SplitTxHz = splitTxRf };
                    changed = true;
                }

                if (s.Rx2Enabled)
                {
                    var rx2 = s.Rx2();
                    long rx2VfoHz = rx2.VfoHz;
                    long rx2TxVfoHz = rx2.TxVfoHz;
                    bool rx2Changed = false;
                    if (TryPromoteActiveBandIf(rx2VfoHz, active, out long rx2Rf))
                    {
                        rx2VfoHz = rx2Rf;
                        rx2Changed = true;
                    }
                    if (TryPromoteActiveBandIf(rx2TxVfoHz, active, out long rx2TxRf))
                    {
                        rx2TxVfoHz = rx2TxRf;
                        rx2Changed = true;
                    }
                    if (rx2Changed)
                    {
                        next = WithRx2(next, r => r with
                        {
                            VfoHz = rx2VfoHz,
                            TxVfoHz = rx2TxVfoHz,
                        });
                        changed = true;
                    }
                }

                for (int i = 2; i < _extraReceivers.Length; i++)
                {
                    var extra = _extraReceivers[i];
                    if (extra is not { Enabled: true }) continue;
                    if (TryPromoteActiveBandIf(extra.VfoHz, active, out long extraRf))
                    {
                        extra.VfoHz = extraRf;
                        changed = true;
                    }
                    if (TryPromoteActiveBandIf(extra.TxVfoHz, active, out long extraTxRf))
                    {
                        extra.TxVfoHz = extraTxRf;
                        changed = true;
                    }
                }

                if (!changed) return null;
                radioLoHz = next.RadioLoHz;
                return next;
            },
            out bool applied);
        if (applied)
        {
            PushPrimaryRadioFrequency(radioLoHz);
            RecomputePaAndPush();
        }
        return applied;
    }

    private static bool TryPromoteActiveBandIf(
        long ifHz,
        TransverterBandDto active,
        out long rfHz)
    {
        if (!TryTranslateNativeIf(ifHz, active, out rfHz)) return false;
        return rfHz >= active.BeginFrequencyHz && rfHz <= active.EndFrequencyHz;
    }

    private static bool TryTranslateNativeIf(
        long ifHz,
        TransverterBandDto active,
        out long rfHz)
    {
        rfHz = 0;
        if (ifHz is < TransverterFrequencyConverter.MinimumRadioFrequencyHz
            or > TransverterFrequencyConverter.MaximumRadioFrequencyHz)
            return false;
        try
        {
            rfHz = checked(ifHz + active.LoOffsetHz - active.LoErrorHz);
            return true;
        }
        catch (OverflowException)
        {
            return false;
        }
    }

    public StateDto SetVfo(long hz) => SetVfo(hz, fromExternal: false);

    public StateDto SetVfoB(long hz)
    {
        long clamped = ClampTuningFrequency(hz);
        lock (_sync) { if (_state.VfoLocked) return Snapshot(); }
        long previousTx;
        lock (_sync) previousTx = TxFrequencyHzLocked(_state);
        Mutate(s => WithRx2(s, r => r with { VfoHz = clamped }));
        if (RuntimeBandKey(previousTx) != RuntimeBandKey(RadioFrequencyResolver.TxFrequencyHz(Snapshot())))
        {
            RecomputePaAndPush();
        }
        return Snapshot();
    }

    /// <summary>
    /// Move a receiver's DDS centre while preserving the receiver VFO's
    /// offset from that centre. TCI DDS changes move the whole captured
    /// spectrum; they do not tune channel A onto the DDS frequency.
    /// </summary>
    public StateDto SetReceiverDds(int rxIndex, long hz)
    {
        if (rxIndex is not (0 or 1))
            throw new ArgumentOutOfRangeException(nameof(rxIndex));

        long ddsHz = ClampTuningFrequency(hz);
        var before = Snapshot();
        var rx2 = before.Rx2();
        if (rxIndex == 1 && !rx2.Enabled)
            return before;

        long receiverVfoHz = rxIndex == 0 ? before.VfoHz : rx2.VfoHz;
        RxMode receiverMode = rxIndex == 0 ? before.Mode : rx2.Mode;

        long currentDdsHz = rxIndex == 0 && before.RadioLoHz > 0
            ? before.RadioLoHz
            : CwOffset.EffectiveLoHz(receiverMode, receiverVfoHz);
        long vfoHz;
        try
        {
            vfoHz = checked(receiverVfoHz + (ddsHz - currentDdsHz));
        }
        catch (OverflowException)
        {
            return before;
        }
        if (!IsExternalFrequencyAvailable(vfoHz))
            return before;

        if (rxIndex == 0)
        {
            Mutate(s =>
            {
                ThrowIfPaCalibrationInvariantMutation("RX1 DDS");
                return s with { VfoHz = vfoHz, RadioLoHz = ddsHz };
            });
            PushPrimaryRadioFrequency(ddsHz);
            if (RuntimeBandKey(before.VfoHz) != RuntimeBandKey(vfoHz))
                RecomputePaAndPush();
            return Snapshot();
        }

        long previousTxHz = RadioFrequencyResolver.TxFrequencyHz(before);
        Mutate(s => WithRx2(s, r => r with { VfoHz = vfoHz }));
        if (RuntimeBandKey(previousTxHz)
            != RuntimeBandKey(RadioFrequencyResolver.TxFrequencyHz(Snapshot())))
        {
            RecomputePaAndPush();
        }
        return Snapshot();
    }

    /// <summary>Receiver-indexed VFO setter for the multi-DDC model.
    /// <paramref name="rxIndex"/> 0 → RX1 (<see cref="SetVfo(long)"/>), 1 → RX2
    /// (<see cref="SetVfoB(long)"/>), ≥ 2 → an extra DDC receiver
    /// (<see cref="SetReceiver"/>). Generalizes the RX1/RX2 A/B split so callers
    /// (the /api/vfo + /api/receivers endpoints, CAT/TCI) can address a receiver
    /// by index.</summary>
    public StateDto SetReceiverVfo(int rxIndex, long hz) => rxIndex switch
    {
        0 => SetVfo(hz),
        1 => SetVfoB(hz),
        _ => SetReceiver(rxIndex, vfoHz: hz),
    };

    /// <summary>
    /// Configure any receiver by index for full multi-DDC operation. Index 0/1
    /// delegate to the RX1/RX2 setters; index ≥ 2 updates the session per-receiver
    /// store (RX3+) and re-projects + broadcasts so DspPipelineService opens the
    /// WDSP channel and the P2 client is told to stream the new DDC. Only the
    /// supplied fields change.
    /// <para>Extra receivers are contiguous (the P2 DDC run has no gaps):
    /// enabling RX(n) implicitly enables RX2..RX(n−1); disabling RX(n) disables
    /// RX(n+1).. . Enabling any extra also turns RX2 on. This never touches the
    /// PureSignal DDC0/1 pair.</para>
    /// </summary>
    public StateDto SetReceiver(
        int index,
        bool? enabled = null,
        long? vfoHz = null,
        byte? adcSource = null,
        RxMode? mode = null,
        int? filterLowHz = null,
        int? filterHighHz = null,
        double? afGainDb = null,
        string? filterPresetName = null,
        int? zoomLevel = null)
    {
        // RX1 (0) and RX2 (1) live on the flat StateDto fields, but the uniform
        // numeric model means /api/receivers/{index} must drive every receiver.
        // Route each supplied field to its canonical RX1/RX2 setter so all their
        // side effects (TX recompute, band-mode memory, filter preset memory)
        // still fire. The legacy A/B endpoints (/api/mode, /api/filter, /api/rx2)
        // remain as thin aliases onto the same setters.
        if (index == 0 || index == 1)
        {
            var receiver = index == 1 ? TxVfo.B : TxVfo.A;
            if (index == 1 && enabled is bool en1)
                SetRx2(new Rx2SetRequest(Enabled: en1));
            if (vfoHz is long v)
            {
                if (index == 1) SetVfoB(v); else SetVfo(v);
            }
            if (mode is RxMode m) SetMode(m, receiver);
            if (filterLowHz is not null || filterHighHz is not null || filterPresetName is not null)
            {
                var cur = Snapshot();
                int lo = filterLowHz ?? (index == 1 ? cur.Rx2().FilterLowHz : cur.FilterLowHz);
                int hi = filterHighHz ?? (index == 1 ? cur.Rx2().FilterHighHz : cur.FilterHighHz);
                SetFilter(lo, hi, filterPresetName, receiver);
            }
            if (afGainDb is double af)
            {
                if (index == 1) SetRx2(new Rx2SetRequest(AfGainDb: af));
                else SetRx1AfGain(af);
            }
            if (adcSource is byte a)
            {
                Mutate(s => WithReceiverAdcSource(s, index, a));
                if (index == 0) ApplyPrimaryAttenuatorToActiveClient(EffectiveAttenDb);
            }
            // RX1's zoom stays on the legacy global StateDto.ZoomLevel (SetZoom /
            // POST /api/rx/zoom) — a zoomLevel here is a no-op for index 0. RX2
            // gets its own independent zoom, stored in Receivers[1] like AdcSource.
            if (index == 1 && zoomLevel is int rx2Zoom)
            {
                int clamped = Math.Clamp(
                    rx2Zoom, SyntheticDspEngine.MinZoomLevel, SyntheticDspEngine.MaxZoomLevel);
                Mutate(s => WithReceiverZoom(s, index, clamped));
            }
            return Snapshot();
        }
        if (index < 2 || index >= _extraReceivers.Length)
            throw new ArgumentOutOfRangeException(
                nameof(index), index, $"receiver index out of range (0..{_extraReceivers.Length - 1})");

        bool enabling = enabled == true;
        lock (_sync)
        {
            // Ordinary P1 currently has no RX3+ ingest path. Ignore an enable
            // request rather than projecting a receiver that can never receive
            // samples. Disabling or pre-configuring a hidden slot remains safe;
            // P2/P3 can use that session-only configuration after reconnect.
            if (_activeClient is not null && enabling)
                return Snapshot();

            var e = _extraReceivers[index];
            if (vfoHz is long v) e.VfoHz = ClampTuningFrequency(v);
            if (adcSource is byte a) e.AdcSource = a;
            if (mode is RxMode m) e.Mode = m;
            if (filterLowHz is int fl) e.FilterLowHz = fl;
            if (filterHighHz is int fh) e.FilterHighHz = fh;
            if (filterPresetName is string fp) e.FilterPresetName = fp;
            if (afGainDb is double af) e.AfGainDb = Math.Clamp(af, -50.0, 20.0);
            if (zoomLevel is int z)
                e.ZoomLevel = Math.Clamp(z, SyntheticDspEngine.MinZoomLevel, SyntheticDspEngine.MaxZoomLevel);
            if (enabled is bool en)
            {
                e.Enabled = en;
                if (en)
                    for (int j = 2; j < index; j++) _extraReceivers[j].Enabled = true;       // contiguity below
                else
                    for (int j = index + 1; j < _extraReceivers.Length; j++) _extraReceivers[j].Enabled = false; // cascade above
            }
        }
        // Re-project + broadcast (StateChanged → DspPipelineService opens channels
        // and pushes SetExtraReceivers/SetExtraReceiverFreqHz to the radio).
        // Enabling an extra DDC requires RX2 (no DDC gap), so turn it on. If the
        // disable cascade just removed the receiver the operator was transmitting
        // on, fall the TX target back to RX1 (never key a receiver that stopped
        // streaming).
        Mutate(s =>
        {
            var next = enabling && !s.Rx2Enabled ? s with { Rx2Enabled = true } : s;
            if (next.TxReceiverIndex >= 2 &&
                (next.TxReceiverIndex >= _extraReceivers.Length
                 || _extraReceivers[next.TxReceiverIndex] is not { Enabled: true }))
            {
                next = next with { TxReceiverIndex = 0, TxVfo = TxVfo.A };
            }
            return next;
        });
        return Snapshot();
    }

    public StateDto SetRx2(Rx2SetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        long previousTx;
        lock (_sync) previousTx = TxFrequencyHzLocked(_state);
        Mutate(s =>
        {
            var rx2 = s.Rx2();
            long nextVfoB = req.VfoBHz.HasValue
                ? ClampTuningFrequency(req.VfoBHz.Value)
                : rx2.VfoHz > 0
                    ? rx2.VfoHz
                    : s.VfoHz;
            var nextMode = req.AudioMode ?? s.Rx2AudioMode;
            double nextGain = req.AfGainDb.HasValue
                ? Math.Clamp(req.AfGainDb.Value, -50.0, 20.0)
                : rx2.AfGainDb;
            bool nextEnabled = req.Enabled ?? s.Rx2Enabled;
            var next = WithRx2(
                s with { Rx2Enabled = nextEnabled, Rx2AudioMode = nextMode },
                r => r with { VfoHz = nextVfoB, AfGainDb = nextGain });
            // A hidden receiver must never remain the TX target after MULTI RX
            // collapses. RX3+ already enforce this in SetReceiver; RX2 needs the
            // same invariant because its legacy setter follows a separate path.
            return !nextEnabled && next.TxReceiverIndex == 1
                ? next with { TxReceiverIndex = 0, TxVfo = TxVfo.A }
                : next;
        });
        var snap = Snapshot();
        PushCwKeyerEnabledToP1(snap);
        if (RuntimeBandKey(previousTx) != RuntimeBandKey(RadioFrequencyResolver.TxFrequencyHz(snap)))
        {
            RecomputePaAndPush();
        }
        return snap;
    }

    public StateDto SetTxVfo(TxVfo txVfo)
    {
        if (!Enum.IsDefined(txVfo))
            throw new ArgumentOutOfRangeException(nameof(txVfo), txVfo, "Unknown TX VFO");
        // Legacy A/B endpoint — funnel through the index-based setter so TxVfo and
        // TxReceiverIndex never diverge.
        return SetTxReceiver(txVfo == TxVfo.B ? 1 : 0);
    }

    /// <summary>Select the transmit target by receiver index (0 = RX1/VFO A,
    /// 1 = RX2/VFO B, >= 2 = an extra DDC). The independent TX DUC and CW/CTUN LO
    /// alignment read <see cref="RadioFrequencyResolver.TxFrequencyHz"/>, so the carrier moves to the
    /// chosen receiver's VFO. An out-of-range or not-exposed index clamps to RX1
    /// (never transmit on a receiver the operator can't see).</summary>
    public StateDto SetTxReceiver(int index)
    {
        long previousTx = 0;
        // Validate and apply under the same mutation lock. A slice can close at
        // the same time its TX button is clicked; separating validation from
        // mutation would let a now-hidden receiver be selected after disable.
        Mutate(s =>
        {
            ThrowIfPaCalibrationInvariantMutation("TX receiver");
            previousTx = TxFrequencyHzLocked(s);
            int target = ClampTxReceiverIndexUnderLock(index);
            // TxVfo stays as the legacy A/B projection (index 1 -> B, else A)
            // so pre-multi-DDC consumers keep working; TxReceiverIndex is
            // authoritative.
            return s.TxReceiverIndex == target
                ? s
                : s with
                {
                    TxReceiverIndex = target,
                    TxVfo = target == 1 ? TxVfo.B : TxVfo.A,
                };
        });
        var snap = Snapshot();
        PushCwKeyerEnabledToP1(snap);
        if (RuntimeBandKey(previousTx) != RuntimeBandKey(RadioFrequencyResolver.TxFrequencyHz(snap)))
        {
            RecomputePaAndPush();
        }
        return snap;
    }

    /// <summary>Enable or disable the independent split-TX dial. The selected
    /// receiver remains the RX/mode context; split only overrides the carrier
    /// frequency, matching Thetis VFO-A RX / VFO-B TX with RX2 disabled.</summary>
    public StateDto SetSplit(int receiverIndex, bool enabled)
    {
        long previousTx = 0;
        Mutate(s =>
        {
            ThrowIfPaCalibrationInvariantMutation("SPLIT");
            // This check runs inside Mutate's _sync critical section so key-on
            // cannot slip between admission and the frequency-state edit.
            if (_mox || _tunActive || _txFrequencyTransition) return null;
            previousTx = TxFrequencyHzLocked(s);
            ValidateReceiverIndexUnderLock(receiverIndex);
            long receiverHz = ReceiverFrequencyHzLocked(s, receiverIndex);
            if (receiverIndex <= 0)
            {
                long txHz = s.SplitTxHz > 0 ? s.SplitTxHz : receiverHz;
                return s with { SplitEnabled = enabled, SplitTxHz = txHz };
            }
            if (receiverIndex == 1)
                return WithRx2(s, r => r with
                {
                    SplitEnabled = enabled,
                    TxVfoHz = r.TxVfoHz > 0 ? r.TxVfoHz : receiverHz,
                });

            var e = _extraReceivers[receiverIndex] ??= new ExtraReceiver();
            e.SplitEnabled = enabled;
            if (e.TxVfoHz <= 0) e.TxVfoHz = receiverHz;
            return s;
        }, out bool applied);
        if (!applied) return Snapshot();
        var snap = Snapshot();
        if (RuntimeBandKey(previousTx) != RuntimeBandKey(RadioFrequencyResolver.TxFrequencyHz(snap)))
            RecomputePaAndPush();
        return snap;
    }

    /// <summary>Move the independent split-TX dial without moving any RX VFO
    /// or hardware receive LO.</summary>
    public StateDto SetSplitFrequency(int receiverIndex, long hz)
    {
        long clamped = ClampTuningFrequency(hz);
        long previousTx = 0;
        Mutate(s =>
        {
            // Atomic with key-on for the same reason as SetSplit above.
            if (_mox || _tunActive || _txFrequencyTransition) return null;
            previousTx = TxFrequencyHzLocked(s);
            ValidateReceiverIndexUnderLock(receiverIndex);
            if (receiverIndex <= 0)
                return s with { SplitTxHz = clamped };
            if (receiverIndex == 1)
                return WithRx2(s, r => r with { TxVfoHz = clamped });

            var e = _extraReceivers[receiverIndex] ??= new ExtraReceiver();
            e.TxVfoHz = clamped;
            return s;
        }, out bool applied);
        if (!applied) return Snapshot();
        var snap = Snapshot();
        if (RuntimeBandKey(previousTx) != RuntimeBandKey(RadioFrequencyResolver.TxFrequencyHz(snap)))
            RecomputePaAndPush();
        return snap;
    }

    // Caller holds _sync. A secondary receiver is a valid TX target only while
    // exposed/enabled — otherwise fall back to RX1 so TX never points at a
    // receiver that isn't streaming.
    private int ClampTxReceiverIndexUnderLock(int index)
    {
        if (index <= 0) return 0;
        if (index == 1) return _state.Rx2Enabled ? 1 : 0;
        if (_activeClient is not null) return 0;
        if (index >= _extraReceivers.Length) return 0;
        return _extraReceivers[index] is { Enabled: true } ? index : 0;
    }

    private void ValidateReceiverIndexUnderLock(int index)
    {
        if (index == 0) return;
        if (ClampTxReceiverIndexUnderLock(index) != index)
            throw new ArgumentOutOfRangeException(
                nameof(index), index, "receiver is not enabled or cannot transmit");
    }

    // Caller holds _sync. Like TxFrequencyHz but resolves an extra-DDC TX target
    // (index >= 2) from _extraReceivers, which the internal _state can't carry
    // (its Receivers list is null until ProjectReceivers runs at Snapshot time).
    private long ReceiverFrequencyHzLocked(StateDto state, int receiverIndex) => receiverIndex switch
    {
        <= 0 => state.VfoHz,
        1 => state.Rx2().VfoHz,
        int i => i < _extraReceivers.Length && _extraReceivers[i] is { } e ? e.VfoHz : state.VfoHz,
    };

    private long TxFrequencyHzLocked(StateDto state)
    {
        int index = state.TxReceiverIndex;
        if (index <= 0)
            return state.SplitEnabled && state.SplitTxHz > 0
                ? state.SplitTxHz
                : state.VfoHz;
        if (index == 1)
        {
            var rx2 = state.Rx2();
            return rx2.SplitEnabled && rx2.TxVfoHz > 0 ? rx2.TxVfoHz : rx2.VfoHz;
        }

        var e = index < _extraReceivers.Length ? _extraReceivers[index] : null;
        return e is { SplitEnabled: true, TxVfoHz: > 0 } ? e.TxVfoHz : e?.VfoHz ?? state.VfoHz;
    }

    /// <summary>TX carrier frequency including any active XIT offset. The
    /// displayed VFO is unchanged; only the transmitted carrier moves (Thetis
    /// XITOn/XITValue, console.cs:22127).</summary>
    public static long TxCarrierHz(StateDto state) =>
        RadioFrequencyResolver.TxFrequencyHz(state) + (state.XitEnabled ? state.XitHz : 0);

    public static long TxEffectiveLoHz(StateDto state) =>
        CwOffset.EffectiveLoHz(RadioFrequencyResolver.TxMode(state), TxCarrierHz(state));

    public StateDto SwapVfos()
    {
        long previousTx = 0;
        long newA = 0;
        RxMode mode = RxMode.USB;
        Mutate(s =>
        {
            previousTx = TxFrequencyHzLocked(s);
            newA = ClampTuningFrequency(s.Rx2().VfoHz);
            mode = s.Mode;
            long oldA = ClampTuningFrequency(s.VfoHz);
            return WithRx2(
                s with
                {
                    VfoHz = newA,
                    RadioLoHz = CwOffset.EffectiveLoHz(mode, newA),
                },
                r => r with { VfoHz = oldA });
        });
        PushPrimaryRadioFrequency(CwOffset.EffectiveLoHz(mode, newA));
        if (RuntimeBandKey(previousTx) != RuntimeBandKey(RadioFrequencyResolver.TxFrequencyHz(Snapshot())))
        {
            RecomputePaAndPush();
        }
        return Snapshot();
    }

    /// <summary>
    /// Set the VFO (dial) frequency.
    ///
    /// <para><b>CTUN on</b> (<see cref="StateDto.CtunEnabled"/>): the operator
    /// click-tunes off the panadapter centre. We move only the dial and leave
    /// the hardware NCO (<c>RadioLoHz</c>) frozen — DspPipelineService
    /// recomputes the WDSP shift stage (= EffectiveLoHz(mode, vfo) − RadioLoHz)
    /// off the StateChanged event so the tuned signal still lands at baseband
    /// for RX. TX retunes the shared VFO register to the dial on key-down
    /// (<see cref="SetMox"/> → <see cref="AlignLoForTx"/>) and restores the
    /// frozen centre on un-key, so the radio transmits on the dial — the fix
    /// for the #470 revert. The frozen NCO is kept only while the dial stays
    /// inside the captured IQ window; once the requested shift would exceed the
    /// IF capacity (≈ ±0.45×sample_rate) the signal is no longer in the
    /// sampled spectrum, so we fall through to a classic recenter.</para>
    ///
    /// <para><b>CTUN off</b>: classic "radio follows the dial" — every tune
    /// retunes the hardware NCO so the clicked frequency becomes the new
    /// centre (RadioLoHz = dial's effective LO, WDSP shift = 0).</para>
    ///
    /// External sources (CAT/TCI/calibration, <paramref name="fromExternal"/>
    /// =true) always recenter regardless of CTUN — they expect "radio follows
    /// the dial" (Thetis <c>CATChangesCenterFreq=true</c>). Mirrors Thetis
    /// <c>ClickTuneDisplay</c> (console.cs:43143).
    /// </summary>
    public StateDto SetVfo(long hz, bool fromExternal) =>
        SetVfoCore(hz, fromExternal, paCalibrationOwner: false);

    internal StateDto SetPaCalibrationVfo(long hz) =>
        SetVfoCore(hz, fromExternal: true, paCalibrationOwner: true);

    private StateDto SetVfoCore(long hz, bool fromExternal, bool paCalibrationOwner)
    {
        long clamped = ClampTuningFrequency(hz);
        // VFO lock guards operator dial tuning only. External sources
        // (CAT/TCI/calibration) tune intentionally and bypass the lock, matching
        // Thetis (chkVFOLock blocks the UI/knob, not CAT). No-op return preserves
        // the current snapshot.
        if (!fromExternal)
        {
            lock (_sync) { if (_state.VfoLocked) return Snapshot(); }
        }
        long previous;
        RxMode currentMode;
        bool ctun;
        long currentLo;
        int sampleRate;
        lock (_sync)
        {
            if (!paCalibrationOwner)
                ThrowIfPaCalibrationInvariantMutation("VFO");
            previous = _state.VfoHz;
            currentMode = _state.Mode;
            ctun = _state.CtunEnabled;
            currentLo = _state.RadioLoHz;
            sampleRate = _state.SampleRate;
        }

        // CTUN: dial roams, NCO frozen — as long as the tuned signal stays
        // inside the captured IQ window (±IF capacity). A panadapter click
        // always resolves to an on-screen frequency, and the visible span is
        // ⊆ the sample window, so clicks never trip the guard; only wheel /
        // keyboard / typed tuning can push the dial out, and that recenters.
        if (ctun && !fromExternal)
        {
            long shiftHz = CwOffset.EffectiveLoHz(currentMode, clamped) - currentLo;
            long ifCapHz = (long)(sampleRate * 0.45);
            if (Math.Abs(shiftHz) <= ifCapHz)
            {
                Mutate(s =>
                {
                    if (!paCalibrationOwner)
                        ThrowIfPaCalibrationInvariantMutation("VFO");
                    return s with { VfoHz = clamped };
                });
                if (RuntimeBandKey(previous) != RuntimeBandKey(clamped))
                {
                    RecomputePaAndPush();
                    if (!fromExternal)
                    {
                        var newBand = RuntimeBandKey(clamped);
                        var afterMode = RestoreBandMode(newBand);
                        var afterFilter = RestoreBandFilter(newBand);
                        var afterZoom = RestoreBandZoom(newBand);
                        var afterDrive = RestoreBandDrive(newBand);
                        if (afterDrive is not null) return afterDrive;
                        if (afterZoom is not null) return afterZoom;
                        if (afterFilter is not null) return afterFilter;
                        if (afterMode is not null) return afterMode;
                    }
                }
                return Snapshot();
            }
            // Dial left the IQ window — fall through to recenter so the radio
            // keeps demodulating (Thetis snaps the display when the click
            // leaves the span). RadioLoHz follows the dial below.
        }

        // Classic recenter (CTUN off, CTUN out-of-window, or external source):
        // retune the hardware NCO to the dial's effective LO (CW: dial ∓
        // pitch), which leaves the WDSP CTUN-shift stage at zero.
        long radioLoNew = CwOffset.EffectiveLoHz(currentMode, clamped);
        Mutate(s =>
        {
            if (!paCalibrationOwner)
                ThrowIfPaCalibrationInvariantMutation("VFO");
            return s with { VfoHz = clamped, RadioLoHz = radioLoNew };
        });
        PushPrimaryRadioFrequency(radioLoNew);
        // Band edge crossed? Per-band PA gain / OC bits may have swapped — push
        // the new snapshot before the next TX frame ships. Cheap when no
        // crossing occurred (same bytes re-pushed). Also recall the new band's
        // last-used demod mode (operator tunes only — fromExternal CAT/TCI keep
        // their own mode).
        if (RuntimeBandKey(previous) != RuntimeBandKey(clamped))
        {
            RecomputePaAndPush();
            if (!fromExternal)
            {
                var newBand = RuntimeBandKey(clamped);
                var afterMode = RestoreBandMode(newBand);
                var afterFilter = RestoreBandFilter(newBand);
                var afterZoom = RestoreBandZoom(newBand);
                var afterDrive = RestoreBandDrive(newBand);
                if (afterDrive is not null) return afterDrive;
                if (afterZoom is not null) return afterZoom;
                if (afterFilter is not null) return afterFilter;
                if (afterMode is not null) return afterMode;
            }
        }
        return Snapshot();
    }

    internal bool RestoreVfoIfCurrent(long hz, long expectedCurrent)
    {
        try
        {
            lock (_sync)
            {
                if (_state.VfoHz != expectedCurrent) return false;
                SetPaCalibrationVfo(hz);
                return true;
            }
        }
        catch (TransmitSafetyRejectedException ex)
        {
            CompleteRejectedStateChange(ex);
            throw;
        }
    }

    /// <summary>
    /// Server-authoritative per-band mode recall. When the operator's dial
    /// crosses into a different band — by ANY means: band buttons, favorites,
    /// the physical front panel, the VFO knob/wheel, a panadapter click, or a
    /// typed frequency — restore that band's last-used demod mode from
    /// <see cref="BandMemoryStore"/> so mode follows the band everywhere, not
    /// just on the band-selector UIs (KB2UKA 2026-06-25). The caller excludes
    /// external sources (CAT/TCI/calibration, <c>fromExternal=true</c>): they
    /// manage their own mode and a recall would fight them.
    ///
    /// <para>When the target band has no remembered entry (first visit) and the
    /// current mode is LSB or USB, snap to the band's SSB convention
    /// (<see cref="BandUtils.DefaultSsbModeForBand"/>) — 160m/80m/40m LSB, else
    /// USB — so an operator moving from 40m LSB to 20m for the first time lands
    /// on USB instead of leaving LSB on a USB band (issue #185). CW / digital /
    /// AM / FM carry over unchanged: an FT8 or CW operator crossing a band
    /// should not be smashed to voice. Never overrides a stored entry — a
    /// deliberate LSB-on-20m only reverts if the operator clears memory.</para>
    ///
    /// <para>No-op (returns null) when band memory is unavailable, the current
    /// mode already matches the remembered / default, or first visit while the
    /// current mode isn't SSB. Recall also yields when a concurrent mode change
    /// lands after observation. RX1 / VFO A only — the per-band entry is a
    /// single register keyed by band name and <see cref="SetVfo"/> owns the
    /// VFO-A dial. Reuses <see cref="SetMode(RxMode, TxVfo)"/> so the per-mode
    /// filter memory, CW dial bump, and LO push all fire exactly as a manual
    /// mode change.</para>
    ///
    /// <para>The band string comes from <see cref="BandUtils.FreqToBand"/>,
    /// which produces the same canonical "40m"-style key the frontend used when
    /// it persisted the entry (web <c>bandOf</c>) for every in-band frequency,
    /// so the lookup matches.</para>
    /// </summary>
    private StateDto? RestoreBandMode(string? newBand)
    {
        if (_bandMemoryStore is null || newBand is null) return null;
        RxMode current;
        lock (_sync) { current = _state.Mode; }
        var mem = _bandMemoryStore.Get(newBand);
        RxMode target;
        if (mem is not null)
        {
            target = mem.Mode;
        }
        else if (current is RxMode.LSB or RxMode.USB)
        {
            target = BandUtils.DefaultSsbModeForBand(newBand);
        }
        else
        {
            return null;
        }
        if (target == current) return null;
        _log.LogInformation("band.mode.recall band={Band} mode={Mode}", newBand, target);
        RecallRaceTestHook?.Invoke();
        return SetModeIfRx1ModeIs(target, current);
    }

    // Sibling of RestoreBandMode / RestoreBandZoom for the TX Drive and Tune
    // sliders (#128). Called from SetVfo after the mode + zoom recall so the
    // last-used Drive % and Tune % for the new band are re-applied on any band
    // crossing. No-op when the new band has no stored value (first visit — the
    // current global slider carries over) or the stored value already matches.
    // Applies Drive then Tune, returning the latest snapshot only if a value
    // was actually changed. Recall uses guarded, non-persisting core paths so
    // concurrent slider changes or TX keying win instead of being overwritten.
    private StateDto? RestoreBandDrive(string? newBand)
    {
        if (newBand is null || IsTxActive()) return null;
        var (drive, tune) = _paStore.GetBandDrive(newBand);
        if (drive is null && tune is null) return null;
        StateDto? last = null;
        int currentDrive, currentTune;
        lock (_sync)
        {
            currentDrive = _state.DrivePct;
            currentTune = _state.TunePct;
        }
        if ((drive is int pendingDrive && pendingDrive != currentDrive) ||
            (tune is int pendingTune && pendingTune != currentTune))
            RecallRaceTestHook?.Invoke();
        if (drive is int d && d != currentDrive)
        {
            _log.LogInformation("band.drive.recall band={Band} drivePct={Drive}", newBand, d);
            if (SetDriveCore(d, persist: false, currentDrive, abortIfTxActive: true))
                last = Snapshot();
        }
        if (tune is int t && t != currentTune)
        {
            _log.LogInformation("band.tune.recall band={Band} tunePct={Tune}", newBand, t);
            if (SetTuneDriveCore(t, persist: false, currentTune, abortIfTxActive: true))
                last = Snapshot();
        }
        return last;
    }

    // Sibling of RestoreBandMode for the RX1 bandpass filter (#179). Called
    // from SetVfo after the mode recall so the last-used filter edges for the
    // new band are re-applied on any band crossing. No-op when band memory is
    // unavailable, the target band has no stored filter yet (first visit — the
    // current filter carries over), the stored edges already match the live
    // ones, or the filter capture mode disagrees with what was just recalled
    // (a saved LSB filter would land wrong on a USB band). Recall yields if any
    // observed filter field or the mode changes before the guarded apply.
    private StateDto? RestoreBandFilter(string? newBand)
    {
        if (_bandMemoryStore is null || newBand is null) return null;
        var mem = _bandMemoryStore.Get(newBand);
        if (mem is null) return null;
        if (mem.FilterLowHz is not int lo || mem.FilterHighHz is not int hi) return null;
        int curLo, curHi;
        RxMode curMode;
        lock (_sync) { curLo = _state.FilterLowHz; curHi = _state.FilterHighHz; curMode = _state.Mode; }
        // Signed filter edges are valid only for the mode they were captured
        // under. FilterMode is intentionally nullable: pre-existing rows did
        // not record this invariant and therefore must not recall a filter.
        if (mem.FilterMode is not RxMode filterMode || filterMode != curMode) return null;
        if (lo == curLo && hi == curHi) return null;
        _log.LogInformation("band.filter.recall band={Band} lo={Lo} hi={Hi}", newBand, lo, hi);
        RecallRaceTestHook?.Invoke();
        return SetFilterIfRx1Is(lo, hi, curLo, curHi, curMode);
    }

    // Sibling of RestoreBandMode for the panadapter scope zoom (#128). Called
    // from SetVfo after the mode recall so the last-used zoom for the new band
    // is re-applied on any band crossing (band buttons, favorites, front panel,
    // dial, typed frequency). No-op when band memory is unavailable, the target
    // band has no stored zoom yet (first visit — the current level carries
    // over), or the stored level already matches. Recall uses the non-persisting
    // core path so the same value is not re-persisted for the new band. Recall
    // yields if a concurrent zoom change lands before the guarded apply.
    private StateDto? RestoreBandZoom(string? newBand)
    {
        if (_bandMemoryStore is null || newBand is null) return null;
        var stored = _bandMemoryStore.GetZoom(newBand);
        if (stored is null) return null;
        int level = stored.Value;
        int maxZoom = CurrentMaxDisplayZoomLevel;
        if (level < MinDisplayZoomLevel || level > maxZoom) return null;
        int current;
        lock (_sync) { current = _state.ZoomLevel; }
        if (level == current) return null;
        _log.LogInformation("band.zoom.recall band={Band} zoom={Zoom}", newBand, level);
        RecallRaceTestHook?.Invoke();
        return SetZoomCore(level, persist: false, current);
    }

    /// <summary>
    /// Set the radio's hardware NCO (LO) centre frequency in Hz, leaving
    /// VfoHz untouched. Returns the updated <see cref="StateDto"/>.
    /// Values remain in external RF and are clamped to the native range or an
    /// enabled transverter profile. Callers wanting strict rejection should
    /// validate before calling. Triggers a P1 client
    /// SetVfoAHz (and the P2 path via DspPipelineService.OnRadioStateChanged
    /// reading the new RadioLoHz), and a PA recompute if the LO crossed a
    /// band edge. WDSP's shift stage is updated by DspPipelineService so the
    /// dial-relative demodulation remains correct.
    /// </summary>
    public StateDto SetRadioLo(long hz)
    {
        // Suppress LO writes while MOX is held. AlignLoForTx snapped the shared
        // NCO to the dial on key-down; a late-arriving push from the frontend
        // keep-in-view autopan, ruler tween, or /api/state reconcile would
        // overwrite that mid-TX and land the carrier on the frozen CTUN centre
        // instead of the dial (issue #1332). RestoreLoAfterTx puts the RX
        // centre back on un-key. Internal alignment callers reach
        // SetRadioLoUnchecked so the key-down snap and un-key restore still land.
        lock (_sync)
        {
            ThrowIfPaCalibrationInvariantMutation("Radio LO");
            if (_mox || _tunActive) return Snapshot();
        }
        return SetRadioLoUnchecked(hz);
    }

    private StateDto SetRadioLoUnchecked(long hz)
    {
        long clamped = ClampTuningFrequency(hz);
        long previous;
        lock (_sync) { previous = _state.RadioLoHz; }
        Mutate(s => s with { RadioLoHz = clamped });
        PushPrimaryRadioFrequency(clamped);
        if (RuntimeBandKey(previous) != RuntimeBandKey(clamped))
        {
            RecomputePaAndPush();
        }
        return Snapshot();
    }

    /// <summary>
    /// Force the hardware LO to the canonical CW-mode offset of the
    /// displayed VFO (LO = VFO − pitch for CWU, LO = VFO + pitch for CWL)
    /// so a CW transmission lands the carrier on the dial. No-op for non-CW
    /// modes and when the LO is already aligned.
    ///
    /// Background: CTUN (centred tuning) lets the operator click-tune the
    /// panadapter without moving the hardware LO; <see cref="SetVfo"/>
    /// updates only the displayed VFO and lets WDSP's shift stage move the
    /// signal to baseband. That trick works fine for RX, but TX shares the
    /// same physical NCO — if we keyed the radio while CTUN was active, the
    /// carrier would land at <c>LO ± pitch</c> in real RF, not at the dial.
    /// The host-side CW engine calls this before each transmission to
    /// guarantee the operator-tuned freq is what reaches the antenna; the
    /// pattern is the TCI-equivalent of "external retune" — see issue
    /// <c>zeus-drf</c> bench notes (2026-05-24).
    ///
    /// Returns true when the LO was actually moved (caller may want to
    /// log it for diagnostics), false on no-op.
    /// </summary>
    // Remembered RX centre while keyed with the LO parked off the dial, so
    // RestoreLoAfterTx() can put the frozen NCO back on un-key.
    // long.MinValue == "not in a TX cycle". Guarded by _sync.
    private long _ctunPreTxLoHz = long.MinValue;

    // Capture the receive centre exactly once per key-down, before TX moves
    // the shared radio LO away from the current RX view (CTUN freeze, TX B,
    // XIT, or an autopan/pure-pan offset). Caller must hold _sync. Call only
    // when a snap is about to move the LO — an unconditional record would
    // make RestoreLoAfterTx write the LO back on every un-key even when TX
    // never moved it.
    private void RememberFrozenLoUnderLock()
    {
        if (_ctunPreTxLoHz == long.MinValue)
            _ctunPreTxLoHz = _state.RadioLoHz;
    }

    public bool AlignLoForCwTx()
    {
        long targetLo;
        lock (_sync)
        {
            // P2 places TX with its independent DUC; keep the RX DDC parked.
            if (_p2Active) return false;
            // Read state, compute the target, decide, and record the frozen
            // centre in ONE critical section: a concurrent state change
            // between read and record would otherwise snap to a stale target
            // or park the restore centre on the wrong frequency.
            var projected = ProjectedStateUnderLock();
            var txMode = RadioFrequencyResolver.TxMode(projected);
            if (txMode != RxMode.CWU && txMode != RxMode.CWL) return false;
            targetLo = CwOffset.EffectiveLoHz(txMode, TxCarrierHz(projected));
            if (targetLo == _state.RadioLoHz) return false;
            RememberFrozenLoUnderLock();
        }
        SetRadioLoUnchecked(targetLo);
        return true;
    }

    /// <summary>
    /// TX LO alignment for all modes (the phone/digi analogue of
    /// <see cref="AlignLoForCwTx"/>). When the hardware NCO sits off the dial
    /// for RX, the shared Protocol 1 VFO register would otherwise transmit on that
    /// centre — the #470 bug (CTUN freeze) and its CTUN-off twin: XIT, and
    /// pure-pan / keep-in-view autopan (/api/radio/lo) deliberately parks the
    /// LO off the dial while VfoHz stays put, so keying in that window would
    /// radiate on the parked centre rather than the dial.
    /// Called from <see cref="SetMox"/> on the key-down edge: snap the
    /// hardware LO to the dial's effective LO so the carrier lands on
    /// frequency, remembering the parked centre for
    /// <see cref="RestoreLoAfterTx"/> to put back on un-key. No-op only when
    /// the LO already sits on the target. Mirrors Thetis, which writes
    /// VFOAFreq to the NCO on MOX and restores CentreFrequency on RX
    /// (console.cs UpdateTXDDSFreq / HdwMOXChanged). Returns true if the LO
    /// moved.
    /// </summary>
    public bool AlignLoForTx()
    {
        long targetLo;
        lock (_sync)
        {
            // P2 places TX with its independent DUC; moving RadioLoHz here
            // retunes the live RX DDC and restarts the DUP waterfall.
            if (_p2Active) return false;
            var projected = ProjectedStateUnderLock();
            // Read state, compute the target, decide, and record the frozen
            // centre in ONE critical section (see AlignLoForCwTx).
            targetLo = TxEffectiveLoHz(projected);
            if (targetLo == _state.RadioLoHz) return false;
            RememberFrozenLoUnderLock();
        }
        SetRadioLoUnchecked(targetLo);
        return true;
    }

    /// <summary>
    /// Restore the frozen RX centre remembered by <see cref="AlignLoForTx"/> /
    /// <see cref="AlignLoForCwTx"/>. Called from <see cref="SetMox"/> on the
    /// un-key edge so the panadapter returns to the same off-centre CTUN view
    /// the operator had before transmitting. No-op when nothing was recorded
    /// (CTUN off, or the LO was already on the dial). Returns true if the LO
    /// moved.
    /// </summary>
    public bool RestoreLoAfterTx()
    {
        long restore;
        lock (_sync)
        {
            if (_ctunPreTxLoHz == long.MinValue) return false;
            restore = _ctunPreTxLoHz;
            _ctunPreTxLoHz = long.MinValue;
        }
        SetRadioLoUnchecked(restore);
        return true;
    }

    /// <summary>
    /// Enable or disable CTUN (click-tune / centred tuning). Enabling simply
    /// freezes the hardware NCO at its current value (which already equals the
    /// dial's effective LO, so nothing moves) — subsequent <see cref="SetVfo"/>
    /// calls leave it put. Disabling snaps the NCO back to the dial so the
    /// panadapter recentres and classic "radio follows the dial" resumes.
    /// Persisted via FlushState.
    /// </summary>
    public StateDto SetCtunEnabled(bool enabled)
    {
        long vfo;
        RxMode mode;
        bool changed;
        lock (_sync)
        {
            changed = _state.CtunEnabled != enabled;
            vfo = _state.VfoHz;
            mode = _state.Mode;
        }
        if (!changed) return Snapshot();
        // Mutate marks the state dirty (so FlushState persists the toggle) and
        // fires StateChanged.
        Mutate(s => s with { CtunEnabled = enabled });
        if (!enabled)
        {
            // Turning CTUN off: recentre the NCO on the dial (mirrors a classic
            // SetVfo). SetRadioLoUnchecked fires StateChanged so the WDSP shift
            // drops to zero and the frontend frames recentre. Bypasses the MOX
            // guard so an operator toggling CTUN off mid-TX still lands the LO
            // on the dial they're transmitting on.
            SetRadioLoUnchecked(CwOffset.EffectiveLoHz(mode, vfo));
        }
        return Snapshot();
    }

    /// <summary>Enable or disable full-duplex RX audio ("DUP" on the
    /// transport bar). Pure software gate — DspPipelineService reads it each
    /// tick to decide whether every unmuted receiver remains in the audio mix
    /// through MOX (true) or every receiver is muted (false, default).
    /// No hardware effect. Persisted via FlushState.</summary>
    public StateDto SetFullDuplexMultiRxEnabled(bool enabled)
    {
        Mutate(s => s.FullDuplexMultiRxEnabled == enabled
            ? s
            : s with { FullDuplexMultiRxEnabled = enabled });
        return Snapshot();
    }

    /// <summary>Lock or unlock the VFO. When locked, operator dial tuning is
    /// rejected (see <see cref="SetVfo(long, bool)"/>); CAT/TCI tuning still
    /// works. Pure software guard — no hardware effect. Persisted via FlushState.
    /// Mirrors Thetis chkVFOLock.</summary>
    public StateDto SetVfoLock(bool locked)
    {
        Mutate(s => s.VfoLocked == locked ? s : s with { VfoLocked = locked });
        return Snapshot();
    }

    // RIT/XIT clamp range — Thetis udRIT/udXIT (±99999 Hz).
    private const long RitXitMaxHz = 99_999;

    /// <summary>Set RIT (Receiver Incremental Tuning). Only the supplied fields
    /// change. The offset is folded into the WDSP shift stage on the next
    /// StateChanged (DspPipelineService), so RX retunes live while the displayed
    /// VFO stays put. Mirrors Thetis chkRIT/udRIT.</summary>
    public StateDto SetRit(bool? enabled, long? hz)
    {
        Mutate(s => s with
        {
            RitEnabled = enabled ?? s.RitEnabled,
            RitHz = hz is long h ? Math.Clamp(h, -RitXitMaxHz, RitXitMaxHz) : s.RitHz,
        });
        return Snapshot();
    }

    /// <summary>Set XIT (Transmit Incremental Tuning). Only the supplied fields
    /// change. The offset moves the transmitted carrier (TxCarrierHz) without
    /// moving the displayed VFO. When already keyed, re-aligns the LO so the
    /// carrier moves immediately. Mirrors Thetis chkXIT/udXIT.</summary>
    public StateDto SetXit(bool? enabled, long? hz)
    {
        bool keyed;
        Mutate(s =>
        {
            ThrowIfPaCalibrationInvariantMutation("XIT");
            return s with
            {
                XitEnabled = enabled ?? s.XitEnabled,
                XitHz = hz is long h ? Math.Clamp(h, -RitXitMaxHz, RitXitMaxHz) : s.XitHz,
            };
        });
        lock (_sync) keyed = _mox;
        // Live update while transmitting (tune/MOX): re-place the carrier now.
        if (keyed) AlignLoForTx();
        return Snapshot();
    }

    /// <summary>Mute or unmute a receiver's audio by index (RX1=0, RX2=1,
    /// RX3+=2..). Muting silences only that receiver's contribution to the audio
    /// mix (DspPipelineService zeroes its drained buffer), leaving the others
    /// audible — distinct from Rx2AudioMode routing. Mirrors Thetis chkMUT /
    /// chkRX2Mute (RXOutputGain=0).</summary>
    public StateDto SetReceiverMuted(int index, bool muted)
    {
        if (index == 0)
        {
            Mutate(s => s.Rx1Muted == muted ? s : s with { Rx1Muted = muted });
            return Snapshot();
        }
        if (index == 1)
        {
            Mutate(s => s.Rx2Muted == muted ? s : s with { Rx2Muted = muted });
            return Snapshot();
        }
        if (index < 2 || index >= _extraReceivers.Length)
            throw new ArgumentOutOfRangeException(
                nameof(index), index, $"receiver index out of range (0..{_extraReceivers.Length - 1})");
        lock (_sync)
        {
            var e = _extraReceivers[index] ??= new ExtraReceiver();
            e.Muted = muted;
        }
        // Re-project + broadcast so the DSP audio loop sees the new mute flag.
        Mutate(s => s);
        return Snapshot();
    }

    /// <summary>Configure the diversity combiner. Only the supplied fields
    /// change. Applied on the next StateChanged: DspPipelineService combines RX0
    /// (ADC0) with the source receiver's IQ (default RX2/ADC1) using a complex
    /// weight (gain·e^{jθ}) in the Protocol-2 ingest. Default-off leaves the
    /// single-ADC RX path byte-identical. Mirrors Thetis DiversityForm.
    /// <para>Protocol-2 / ANAN-class only (needs two phase-synchronous ADCs); a
    /// single-ADC board has no source stream so the combine no-ops. The
    /// gain/phase null point is best dialed in on a live signal — see the
    /// DiversityForm calibration flow.</para>
    /// </summary>
    public StateDto SetDiversity(bool? enabled, double? gain, double? phaseDeg, int? sourceRx,
        int? referenceRx = null, DiversityOutput? output = null)
    {
        Mutate(s =>
        {
            var cur = s.Diversity ?? new DiversityConfig();
            var next = cur with
            {
                Enabled = enabled ?? cur.Enabled,
                Gain = gain is double g && double.IsFinite(g) ? Math.Clamp(g, 0.0, 5.0) : cur.Gain,
                PhaseDeg = phaseDeg is double p && double.IsFinite(p) ? Math.Clamp(p, -180.0, 180.0) : cur.PhaseDeg,
                ReferenceRx = referenceRx is int rr ? Math.Clamp(rr, 0, 1) : cur.ReferenceRx,
                Output = output is DiversityOutput mode && Enum.IsDefined(mode) ? mode : cur.Output,
                // The legacy field is retained on the wire, but diversity always
                // uses the physical ADC pair. ReferenceRx chooses the phase anchor.
                SourceRx = 1,
            };
            return s with { Diversity = next };
        });
        return Snapshot();
    }

    /// <summary>Request an antenna-tuner (ATU) tune cycle. Holds the Apollo/Alex
    /// auto-tune-start bit (Protocol-1 C0=0x12 frame, C2[4] = register 0x09
    /// bit 20 per the HL2 protocol doc) on the wire for <paramref name="durationMs"/>
    /// then auto-clears. No-op when no Protocol-1 client is connected. Mirrors
    /// Thetis ATUTune (NetworkIO ATU_Tune pulse).
    /// <para><b>Bench-verification pending:</b> the tune-request bit is
    /// spec-correct but has not been confirmed against a radio with an ATU.</para>
    /// </summary>
    public StateDto RequestAtuTune(int durationMs)
    {
        int ms = Math.Clamp(durationMs, 100, 30_000);
        (ActiveClient as Zeus.Protocol1.Protocol1Client)?.RequestAtuTune(ms);
        return Snapshot();
    }

    // Per-mode-family remembered filter magnitudes. Mode switching snapshots
    // the current abs-filter into the departing family's slot and restores the
    // target family's slot on entry — so FM→USB brings back the SSB width
    // the user was using, not the 5500-Hz FM stomp the old SignedFilterForMode
    // left behind (FM overrode f_low/f_high to ±5500, and on return to USB
    // the min-abs/max-abs recomputation collapsed the passband to (5500,5500),
    // killing audio).
    private sealed record FamilyFilter(int LoAbs, int HiAbs);
    private FamilyFilter _ssbFilter = new(150, 2850);
    private FamilyFilter _digFilter = new(0, 800);
    private FamilyFilter _amFilter = new(0, 4000);
    private FamilyFilter _fmFilter = new(0, 5500);
    // CW abs values include the cw_pitch offset (Thetis F6 250 Hz preset:
    // pitch=600, half=125 → 475..725). SignedFilterForMode keeps them as
    // (+475,+725) for CWU and mirrors to (-725,-475) for CWL.
    private FamilyFilter _cwFilter = new(475, 725);
    private FamilyFilter _ssbFilterB = new(150, 2850);
    private FamilyFilter _digFilterB = new(0, 800);
    private FamilyFilter _amFilterB = new(0, 4000);
    private FamilyFilter _fmFilterB = new(0, 5500);
    private FamilyFilter _cwFilterB = new(475, 725);

    // TX-side per-family filter memory. Thetis stores a single TX filter Lo/Hi
    // (setup.cs:5029-5066); pihpsdr uses hardcoded per-mode shapes
    // (transmitter.c:2108-2211). Zeus mirrors the RX per-family model so the
    // operator's USB TX width survives an AM round-trip, and LSB/USB share
    // absolute values with sign flipped at apply time. Defaults track Thetis
    // stock: SSB 150-2850, AM/DSB 0-4000, FM 0-3000 (Thetis narrowest FM TX
    // is 3 kHz half-width), CW 475-725 (250 Hz around cw_pitch=600).
    private FamilyFilter _ssbTxFilter = new(150, 2850);
    private FamilyFilter _amTxFilter = new(0, 4000);
    private FamilyFilter _fmTxFilter = new(0, 3000);
    private FamilyFilter _cwTxFilter = new(475, 725);

    // FreeDV runs USB underneath but is spec-locked to a tight bandpass around
    // the 1500 Hz-centred modem — 700C/700D/700E, 1600 and 800XA all fit inside
    // 300..2700 Hz. Kept in its own family slot (RX + TX) so entering/leaving
    // FreeDV saves and restores the operator's SSB widths through the same
    // mode-family memory every other mode uses, instead of stomping the shared
    // SSB slot. Re-seeded to the FreeDV spec passband each session (not
    // persisted) so the digital width can't silently drift across restarts.
    private FamilyFilter _freeDvFilter = new(300, 2700);
    private FamilyFilter _freeDvTxFilter = new(300, 2700);

    public void SetModemAvailability(Func<bool>? provider)
        => Volatile.Write(ref _modemAvailable, provider);

    private bool FreeDvModemAvailable()
    {
        var provider = Volatile.Read(ref _modemAvailable);
        if (provider is null) return false;
        try { return provider(); }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "FreeDV modem availability check failed.");
            return false;
        }
    }

    public StateDto SetMode(RxMode mode) => SetMode(mode, TxVfo.A);

    public StateDto SetMode(RxMode mode, TxVfo receiver)
        => SetModeCore(mode, receiver, onlyIfRx1ModeIs: null, paCalibrationOwner: false)!;

    internal StateDto SetPaCalibrationMode(RxMode mode)
        => SetModeCore(mode, TxVfo.A, onlyIfRx1ModeIs: null, paCalibrationOwner: true)!;

    internal StateDto? SetModeIfRx1ModeIs(RxMode mode, RxMode expectedCurrent)
        => SetModeCore(mode, TxVfo.A, expectedCurrent);

    internal bool RestoreModeIfCurrent(RxMode mode, RxMode expectedCurrent)
    {
        try
        {
            lock (_sync)
            {
                if (_state.Mode != expectedCurrent) return false;
                long preservedVfoHz = _state.VfoHz;
                SetPaCalibrationMode(mode);
                if (_state.VfoHz != preservedVfoHz)
                    SetPaCalibrationVfo(preservedVfoHz);
                return true;
            }
        }
        catch (TransmitSafetyRejectedException ex)
        {
            CompleteRejectedStateChange(ex);
            throw;
        }
    }

    private StateDto? SetModeCore(
        RxMode mode,
        TxVfo receiver,
        RxMode? onlyIfRx1ModeIs,
        bool paCalibrationOwner = false)
    {
        if (!Enum.IsDefined(receiver))
            throw new ArgumentOutOfRangeException(nameof(receiver), receiver, "Unknown VFO receiver");

        if (mode == RxMode.FreeDv && !FreeDvModemAvailable())
        {
            _log.LogWarning("FreeDV plugin not active — falling back to USB");
            mode = RxMode.USB;
        }

        RxMode departingMode = default;
        string? departingPreset = null;
        long newVfoAHz = 0;
        bool targetBAtSet = false;
        Mutate(s =>
        {
            if (!paCalibrationOwner)
                ThrowIfPaCalibrationInvariantMutation("Mode");
            // Close the recall TOCTOU window before touching mode/filter caches.
            if (onlyIfRx1ModeIs is RxMode expected && s.Mode != expected)
                return null;

            bool targetB = receiver == TxVfo.B && s.Rx2Enabled;
            targetBAtSet = targetB;
            var rx2 = s.Rx2();
            var currentMode = targetB ? rx2.Mode : s.Mode;
            var currentPreset = targetB ? rx2.FilterPresetName : s.FilterPresetName;
            var currentFilterLow = targetB ? rx2.FilterLowHz : s.FilterLowHz;
            var currentFilterHigh = targetB ? rx2.FilterHighHz : s.FilterHighHz;

            departingMode = currentMode;
            departingPreset = currentPreset;

            // Save departing mode's preset name to the in-memory cache.
            var presetCache = targetB ? _lastPresetPerModeB : _lastPresetPerMode;
            presetCache[currentMode] = currentPreset;

            // 1) Save current abs-filter into the mode we are LEAVING.
            int curLoAbs = Math.Min(Math.Abs(currentFilterLow), Math.Abs(currentFilterHigh));
            int curHiAbs = Math.Max(Math.Abs(currentFilterLow), Math.Abs(currentFilterHigh));
            StoreFamilyFilter(currentMode, curLoAbs, curHiAbs, targetB ? TxVfo.B : TxVfo.A);
            if (!targetB)
            {
                int curTxLoAbs = Math.Min(Math.Abs(s.TxFilterLowHz), Math.Abs(s.TxFilterHighHz));
                int curTxHiAbs = Math.Max(Math.Abs(s.TxFilterLowHz), Math.Abs(s.TxFilterHighHz));
                StoreTxFamilyFilter(currentMode, curTxLoAbs, curTxHiAbs);
            }

            // 2) Look up the target family's remembered filter (RX + TX).
            var fam = FamilyFilterFor(mode, targetB ? TxVfo.B : TxVfo.A);

            // 3) Re-sign per target mode's sideband convention.
            var (lo, hi) = SignedFilterForMode(mode, fam.LoAbs, fam.HiAbs);

            // 4) Restore the last-known preset name for the incoming mode.
            presetCache.TryGetValue(mode, out var restoredPreset);

            // 5) Thetis-style dial bump on SSB↔CW transitions so the
            //    effective LO doesn't jump under the operator's feet — the
            //    dial absorbs the ±cw_pitch step and the radio stays on the
            //    same physical signal. Within CWU↔CWL the dial stays put
            //    (Thetis console.cs:34037-34052, 34203-34298 mirrored here).
            //    Non-CW↔non-CW transitions return 0, so SSB/AM/FM/DIG
            //    behaviour is unchanged.
            long bump = CwOffset.DialBumpForModeTransition(currentMode, mode);
            long nextVfoA = targetB ? s.VfoHz : ClampTuningFrequency(s.VfoHz + bump);
            long nextVfoB = targetB ? ClampTuningFrequency(rx2.VfoHz + bump) : rx2.VfoHz;
            newVfoAHz = nextVfoA;

            if (targetB)
            {
                return WithRx2(s, r => r with
                {
                    Mode = mode,
                    VfoHz = nextVfoB,
                    FilterLowHz = lo,
                    FilterHighHz = hi,
                    FilterPresetName = restoredPreset,
                });
            }

            var txFam = TxFamilyFilterFor(mode);
            var (txLo, txHi) = SignedFilterForMode(mode, txFam.LoAbs, txFam.HiAbs);
            if (mode is RxMode.AM or RxMode.SAM)
            {
                int amHigh = NormalizeAmTxProfile(s.AmTxProfile).TxFilterHighHz;
                txLo = -amHigh;
                txHi = amHigh;
            }

            // RX2 is untouched on an RX1 mode change — ProjectReceivers carries
            // Receivers[1] forward (nextVfoB == rx2.VfoHz here).
            return s with
            {
                Mode = mode,
                VfoHz = nextVfoA,
                FilterLowHz = lo, FilterHighHz = hi,
                TxFilterLowHz = txLo, TxFilterHighHz = txHi,
                FilterPresetName = restoredPreset,
            };
        }, out bool applied);

        if (!applied) return null;

        // Persist the departing mode's last preset outside the lock.
        if (departingPreset != null && !targetBAtSet)
            _filterPresetStore?.UpsertLastSelectedPreset(departingMode, departingPreset);

        // Push the new effective LO. Even with no dial bump, switching
        // into/out of CW changes EffectiveLoHz by ±cw_pitch and the radio
        // needs the new tuning before the next IQ block arrives. P2 is
        // pushed via DspPipelineService.OnRadioStateChanged.
        if (!targetBAtSet)
        {
            PushPrimaryRadioFrequency(CwOffset.EffectiveLoHz(mode, newVfoAHz));
            // Entering/leaving CW toggles the P2 internal keyer (TxSpecific
            // byte-5 CW-select). Re-push so a paddle keys the radio the moment
            // the operator is in CW, and the bit clears on the way back to
            // SSB/AM/FM. Issue #1032.
            PushCwToP2();
        }
        PushCwKeyerEnabledToP1();

        return Snapshot();
    }

    public AmTxProfile GetAmTxProfile() =>
        Snapshot().AmTxProfile ?? new AmTxProfile();

    public StateDto SetAmTxProfile(AmTxProfile profile)
    {
        var clean = NormalizeAmTxProfile(profile);
        _dspSettingsStore.SetAmTxProfile(clean);
        Mutate(s =>
        {
            if (RadioFrequencyResolver.TxReceiver(s).Mode is RxMode.AM or RxMode.SAM)
            {
                return s with
                {
                    AmTxProfile = clean,
                    TxFilterLowHz = -clean.TxFilterHighHz,
                    TxFilterHighHz = clean.TxFilterHighHz,
                };
            }
            return s with { AmTxProfile = clean };
        });
        return Snapshot();
    }

    public StateDto CaptureCurrentAmTxProfile()
    {
        var s = Snapshot();
        var captured = NormalizeAmTxProfile(new AmTxProfile
        {
            CarrierLevelPercent = s.AmTxProfile?.CarrierLevelPercent ?? 100,
            MicGainDb = s.MicGainDb,
            LevelerMaxGainDb = s.LevelerMaxGainDb,
            TxLeveling = s.TxLeveling ?? new TxLevelingConfig(),
            Cfc = s.Cfc ?? CfcConfig.Default,
            TxPhaseRotator = s.TxPhaseRotator ?? new TxPhaseRotatorConfig(),
            TxFilterHighHz = Math.Max(Math.Abs(s.TxFilterLowHz), Math.Abs(s.TxFilterHighHz)),
        });
        return SetAmTxProfile(captured);
    }

    internal static AmTxProfile NormalizeAmTxProfile(AmTxProfile? profile)
    {
        profile ??= new AmTxProfile();
        double leveler = double.IsFinite(profile.LevelerMaxGainDb)
            ? Math.Clamp(profile.LevelerMaxGainDb, 0.0, 20.0)
            : 8.0;
        var leveling = profile.TxLeveling ?? new TxLevelingConfig();
        leveling = leveling with
        {
            AlcMaxGainDb = double.IsFinite(leveling.AlcMaxGainDb) ? Math.Clamp(leveling.AlcMaxGainDb, 0.0, 120.0) : 3.0,
            AlcDecayMs = Math.Clamp(leveling.AlcDecayMs, 1, 50),
            LevelerDecayMs = Math.Clamp(leveling.LevelerDecayMs, 1, 5000),
            CompressorGainDb = double.IsFinite(leveling.CompressorGainDb) ? Math.Clamp(leveling.CompressorGainDb, 0.0, 20.0) : 0.0,
        };
        var cfc = profile.Cfc is { Bands.Length: 10 } ? profile.Cfc : CfcConfig.Default;
        return profile with
        {
            CarrierLevelPercent = Math.Clamp(profile.CarrierLevelPercent, 0, 125),
            MicGainDb = Math.Clamp(profile.MicGainDb, -40, 10),
            LevelerMaxGainDb = leveler,
            TxLeveling = leveling,
            Cfc = cfc,
            TxPhaseRotator = NormalizeTxPhaseRotator(profile.TxPhaseRotator ?? new TxPhaseRotatorConfig()),
            TxFilterHighHz = Math.Clamp(Math.Abs(profile.TxFilterHighHz), MinFilterWidthHz, MaxFilterEdgeHz),
        };
    }

    public StateDto SetFilter(int lowHz, int highHz, string? presetName = null)
        => SetFilter(lowHz, highHz, presetName, TxVfo.A);

    public StateDto SetFilter(int lowHz, int highHz, string? presetName, TxVfo receiver)
        => SetFilterCore(lowHz, highHz, presetName, receiver, persist: true)!;

    private readonly record struct Rx1FilterCurrent(int LowHz, int HighHz, RxMode Mode);

    internal StateDto? SetFilterIfRx1Is(
        int lowHz,
        int highHz,
        int expectedLowHz,
        int expectedHighHz,
        RxMode expectedMode)
        => SetFilterCore(
            lowHz,
            highHz,
            presetName: null,
            TxVfo.A,
            persist: false,
            new Rx1FilterCurrent(expectedLowHz, expectedHighHz, expectedMode));

    private StateDto? SetFilterCore(
        int lowHz,
        int highHz,
        string? presetName,
        TxVfo receiver,
        bool persist,
        Rx1FilterCurrent? onlyIfRx1Is = null)
    {
        if (!Enum.IsDefined(receiver))
            throw new ArgumentOutOfRangeException(nameof(receiver), receiver, "Unknown VFO receiver");
        if (highHz < lowHz) (lowHz, highHz) = (highHz, lowHz);
        (lowHz, highHz) = ClampMinFilterWidth(lowHz, highHz);
        RxMode modeAtSet = RxMode.USB;
        string? resolvedName = presetName;
        bool targetBAtSet = false;
        Mutate(s =>
        {
            // Close the recall TOCTOU window, including mode-dependent signs.
            if (onlyIfRx1Is is Rx1FilterCurrent expected &&
                (s.FilterLowHz != expected.LowHz ||
                 s.FilterHighHz != expected.HighHz ||
                 s.Mode != expected.Mode))
                return null;

            bool targetB = receiver == TxVfo.B && s.Rx2Enabled;
            targetBAtSet = targetB;
            modeAtSet = targetB ? s.Rx2().Mode : s.Mode;
            // Normalize the slot name: if (low,high) exactly matches a non-VAR
            // preset for this mode, use that slot's name regardless of what the
            // caller passed. Prevents dual selection where a stored VAR happens
            // to equal a standard preset width and edges.
            if (persist)
            {
                var match = FilterPresets.DefaultsForMode(modeAtSet)
                    .FirstOrDefault(e => !e.IsVar && e.LowHz == lowHz && e.HighHz == highHz);
                if (match is not null) resolvedName = match.SlotName;
                if (resolvedName != null)
                {
                    var presetCache = targetB ? _lastPresetPerModeB : _lastPresetPerMode;
                    presetCache[modeAtSet] = resolvedName;
                }
                int loAbs = Math.Min(Math.Abs(lowHz), Math.Abs(highHz));
                int hiAbs = Math.Max(Math.Abs(lowHz), Math.Abs(highHz));
                StoreFamilyFilter(modeAtSet, loAbs, hiAbs, targetB ? TxVfo.B : TxVfo.A);
            }
            if (targetB)
                return WithRx2(s, r => r with { FilterLowHz = lowHz, FilterHighHz = highHz, FilterPresetName = resolvedName });
            return s with { FilterLowHz = lowHz, FilterHighHz = highHz, FilterPresetName = resolvedName };
        }, out bool applied);
        if (!applied) return null;
        if (persist && resolvedName != null && !targetBAtSet)
        {
            _lastUpsertedPreset.TryGetValue(modeAtSet, out var lastName);
            if (!string.Equals(lastName, resolvedName, StringComparison.Ordinal))
            {
                _lastUpsertedPreset[modeAtSet] = resolvedName;
                _filterPresetStore?.UpsertLastSelectedPreset(modeAtSet, resolvedName);
            }
        }
        FlushStateCoalesced();
        return Snapshot();
    }

    // FlushState(), but at most once per FilterFlushCoalesceMs. A skipped flush
    // is not a lost write: Mutate() has already set _stateDirty, so the 1 Hz
    // debounce timer (and the Dispose flush) will persist the settled value.
    private void FlushStateCoalesced()
    {
        long nowMs = Environment.TickCount64;
        long last = Interlocked.Read(ref _lastFilterFlushMs);
        if (last != 0 && nowMs - last < FilterFlushCoalesceMs) return;
        Interlocked.Exchange(ref _lastFilterFlushMs, nowMs);
        FlushState();
    }

    // TX bandpass filter setter. Signed pair like SetFilter — caller is
    // expected to have already re-signed positive (abs) values per the current
    // mode's sideband convention. DspPipelineService picks up the state-change
    // and forwards to the engine via IDspEngine.SetTxFilter.
    public StateDto SetTxFilter(int lowHz, int highHz)
    {
        if (highHz < lowHz) (lowHz, highHz) = (highHz, lowHz);
        (lowHz, highHz) = ClampMinFilterWidth(lowHz, highHz);
        AmTxProfile? updatedAmProfile = null;
        Mutate(s =>
        {
            int loAbs = Math.Min(Math.Abs(lowHz), Math.Abs(highHz));
            int hiAbs = Math.Max(Math.Abs(lowHz), Math.Abs(highHz));
            var txMode = RadioFrequencyResolver.TxReceiver(s).Mode;
            if (txMode is RxMode.AM or RxMode.SAM)
            {
                // AM/SAM always use the dedicated symmetric profile. Route the
                // ordinary TX-filter endpoint here too so every UI/CAT writer
                // edits the value that is actually on air instead of a hidden
                // global field that the AM overlay ignores.
                updatedAmProfile = NormalizeAmTxProfile(s.AmTxProfile) with
                {
                    TxFilterHighHz = Math.Clamp(hiAbs, MinFilterWidthHz, MaxFilterEdgeHz),
                };
                return s with
                {
                    AmTxProfile = updatedAmProfile,
                    TxFilterLowHz = -updatedAmProfile.TxFilterHighHz,
                    TxFilterHighHz = updatedAmProfile.TxFilterHighHz,
                };
            }
            StoreTxFamilyFilter(s.Mode, loAbs, hiAbs);
            return s with { TxFilterLowHz = lowHz, TxFilterHighHz = highHz };
        });
        if (updatedAmProfile is not null)
            _dspSettingsStore.SetAmTxProfile(updatedAmProfile);
        FlushState();
        return Snapshot();
    }

    // Defensive floor for the signed (lo,hi) bandpass pair pushed to the engine
    // — caller is responsible for ordering (hi >= lo). Expands symmetrically
    // about the existing centre so sideband orientation is preserved (LSB stays
    // negative, USB stays positive, CWU/CWL stay on their respective sides).
    // Widths already at or above MinFilterWidthHz pass through unchanged.
    internal static (int low, int high) ClampMinFilterWidth(int lowHz, int highHz)
    {
        if (highHz - lowHz >= MinFilterWidthHz) return (lowHz, highHz);
        int center = (lowHz + highHz) / 2;
        int half = MinFilterWidthHz / 2;
        return (center - half, center + half);
    }

    public IReadOnlyList<FilterPresetDto> GetFilterPresets(RxMode mode)
    {
        var defaults = FilterPresets.DefaultsForMode(mode);
        return defaults.Select(e =>
        {
            var stored = _filterPresetStore?.GetSlotOverride(mode, e.SlotName);
            int lowHz = e.LowHz;
            int highHz = e.HighHz;
            if (stored?.HasWidth == true)
            {
                var normalized = NormalizeLegacyDigitalFilter(
                    mode, stored.LowHz, stored.HighHz);
                lowHz = normalized.low;
                highHz = normalized.high;
            }
            string label = e.IsVar ? e.Label : stored?.Label ?? e.Label;
            bool customized = lowHz != e.LowHz
                || highHz != e.HighHz
                || !string.Equals(label, e.Label, StringComparison.Ordinal);
            return new FilterPresetDto(
                e.SlotName, label, lowHz, highHz, e.IsVar, customized);
        }).ToList();
    }

    public StateDto SetFilterPresetOverride(
        RxMode mode,
        string slotName,
        int loHz,
        int hiHz,
        string? label = null)
    {
        var factory = GetFactoryFilterPreset(mode, slotName);
        if (factory.IsVar && label is not null)
            throw new ArgumentException(
                "VAR1 and VAR2 filter presets cannot be renamed.",
                nameof(label));

        (loHz, hiHz) = ValidateAndNormalizeFilterPresetWidth(mode, loHz, hiHz);
        bool updateLabel = label is not null;
        string? normalizedLabel = updateLabel
            ? NormalizeFilterPresetLabel(label!, factory.Label)
            : null;
        var stored = _filterPresetStore?.GetSlotOverride(mode, factory.SlotName);
        int currentLowHz = factory.LowHz;
        int currentHighHz = factory.HighHz;
        if (stored?.HasWidth == true)
        {
            (currentLowHz, currentHighHz) = NormalizeLegacyDigitalFilter(
                mode, stored.LowHz, stored.HighHz);
        }

        bool updateWidth = loHz != currentLowHz || hiHz != currentHighHz;
        if (updateWidth && updateLabel)
        {
            _filterPresetStore?.UpsertSlotOverride(
                mode, factory.SlotName, loHz, hiHz, updateLabel: true, label: normalizedLabel);
        }
        else if (updateWidth)
        {
            _filterPresetStore?.UpsertSlotWidthOverride(
                mode, factory.SlotName, loHz, hiHz);
        }
        else if (updateLabel)
        {
            _filterPresetStore?.UpsertSlotLabelOverride(
                mode, factory.SlotName, normalizedLabel);
        }
        return Snapshot();
    }

    public StateDto ResetFilterPresetOverride(RxMode mode, string slotName)
    {
        var factory = GetFactoryFilterPreset(mode, slotName);
        _filterPresetStore?.ResetSlotOverride(mode, factory.SlotName);
        return Snapshot();
    }

    private static FilterPresetEntry GetFactoryFilterPreset(RxMode mode, string slotName)
    {
        if (!Enum.IsDefined(mode))
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown RX mode.");
        if (string.IsNullOrWhiteSpace(slotName))
            throw new InvalidOperationException("A filter slot name is required.");
        string candidate = slotName.Trim();
        return FilterPresets.DefaultsForMode(mode).FirstOrDefault(e =>
            string.Equals(e.SlotName, candidate, StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException(
                $"Filter slot '{slotName}' is not available for mode {mode}.");
    }

    private static (int low, int high) ValidateAndNormalizeFilterPresetWidth(
        RxMode mode,
        int lowHz,
        int highHz)
    {
        if (highHz <= lowHz)
            throw new ArgumentException("Filter high edge must be greater than the low edge.");
        if (lowHz < -MaxFilterEdgeHz || highHz > MaxFilterEdgeHz)
            throw new ArgumentOutOfRangeException(
                nameof(lowHz),
                $"Filter edges must be between {-MaxFilterEdgeHz} and {MaxFilterEdgeHz} Hz.");

        (lowHz, highHz) = ClampMinFilterWidth(lowHz, highHz);
        if (lowHz < -MaxFilterEdgeHz)
        {
            highHz += -MaxFilterEdgeHz - lowHz;
            lowHz = -MaxFilterEdgeHz;
        }
        if (highHz > MaxFilterEdgeHz)
        {
            lowHz -= highHz - MaxFilterEdgeHz;
            highHz = MaxFilterEdgeHz;
        }
        return NormalizeLegacyDigitalFilter(mode, lowHz, highHz);
    }

    private static string? NormalizeFilterPresetLabel(string label, string factoryLabel)
    {
        string trimmed = label.Trim();
        if (trimmed.Length == 0 || string.Equals(trimmed, factoryLabel, StringComparison.Ordinal))
            return null;
        if (trimmed.Length > MaxFilterPresetLabelLength)
            throw new ArgumentException(
                $"Filter preset names may contain at most {MaxFilterPresetLabelLength} characters.",
                nameof(label));
        if (trimmed.Any(char.IsControl))
            throw new ArgumentException(
                "Filter preset names cannot contain control characters.",
                nameof(label));
        return trimmed;
    }

    public string[] GetFavoriteFilterSlots(RxMode mode)
    {
        return _filterPresetStore?.GetFavoriteSlots(mode) ?? new[] { "F6", "F5", "F4" };
    }

    public StateDto SetFavoriteFilterSlots(RxMode mode, string[] slotNames)
    {
        if (slotNames.Length > 3)
            throw new ArgumentException("Maximum 3 favorite slots allowed", nameof(slotNames));
        _filterPresetStore?.SetFavoriteSlots(mode, slotNames);
        return Snapshot();
    }

    private void StoreFamilyFilter(RxMode mode, int loAbs, int hiAbs)
        => StoreFamilyFilter(mode, loAbs, hiAbs, TxVfo.A);

    private void StoreFamilyFilter(RxMode mode, int loAbs, int hiAbs, TxVfo receiver)
    {
        var slot = new FamilyFilter(loAbs, hiAbs);
        bool targetB = receiver == TxVfo.B;
        switch (mode)
        {
            case RxMode.USB: case RxMode.LSB:
                if (targetB) _ssbFilterB = slot; else _ssbFilter = slot; break;
            case RxMode.DIGU: case RxMode.DIGL:
                if (targetB) _digFilterB = slot; else _digFilter = slot; break;
            case RxMode.AM: case RxMode.SAM: case RxMode.DSB:
                if (targetB) _amFilterB = slot; else _amFilter = slot; break;
            case RxMode.FM:
                if (targetB) _fmFilterB = slot; else _fmFilter = slot; break;
            case RxMode.CWL: case RxMode.CWU:
                if (targetB) _cwFilterB = slot; else _cwFilter = slot; break;
            case RxMode.FreeDv:
                // FreeDV shares one spec slot across VFOs (no per-VFO digital width).
                _freeDvFilter = slot; break;
        }
    }

    private void StoreTxFamilyFilter(RxMode mode, int loAbs, int hiAbs)
    {
        var slot = new FamilyFilter(loAbs, hiAbs);
        switch (mode)
        {
            case RxMode.USB: case RxMode.LSB: case RxMode.DIGU: case RxMode.DIGL:
                _ssbTxFilter = slot; break;
            case RxMode.AM: case RxMode.SAM: case RxMode.DSB:
                _amTxFilter = slot; break;
            case RxMode.FM:
                _fmTxFilter = slot; break;
            case RxMode.CWL: case RxMode.CWU:
                _cwTxFilter = slot; break;
            case RxMode.FreeDv:
                _freeDvTxFilter = slot; break;
        }
    }

    private FamilyFilter TxFamilyFilterFor(RxMode mode) => mode switch
    {
        RxMode.USB or RxMode.LSB or RxMode.DIGU or RxMode.DIGL => _ssbTxFilter,
        RxMode.AM or RxMode.SAM or RxMode.DSB => _amTxFilter,
        RxMode.FM => _fmTxFilter,
        RxMode.CWL or RxMode.CWU => _cwTxFilter,
        RxMode.FreeDv => _freeDvTxFilter,
        _ => _ssbTxFilter,
    };

    private FamilyFilter FamilyFilterFor(RxMode mode) => mode switch
    {
        RxMode.USB or RxMode.LSB => _ssbFilter,
        RxMode.DIGU or RxMode.DIGL => _digFilter,
        RxMode.AM or RxMode.SAM or RxMode.DSB => _amFilter,
        RxMode.FM => _fmFilter,
        RxMode.CWL or RxMode.CWU => _cwFilter,
        RxMode.FreeDv => _freeDvFilter,
        _ => _ssbFilter,
    };

    private FamilyFilter FamilyFilterFor(RxMode mode, TxVfo receiver)
    {
        if (receiver != TxVfo.B)
            return FamilyFilterFor(mode);

        return mode switch
        {
            RxMode.USB or RxMode.LSB => _ssbFilterB,
            RxMode.DIGU or RxMode.DIGL => _digFilterB,
            RxMode.AM or RxMode.SAM or RxMode.DSB => _amFilterB,
            RxMode.FM => _fmFilterB,
            RxMode.CWL or RxMode.CWU => _cwFilterB,
            RxMode.FreeDv => _freeDvFilter,
            _ => _ssbFilterB,
        };
    }

    // Re-sign a positive (abs) TX/RX bandpass magnitude pair per the mode's
    // sideband convention. WDSP selects the SSB sideband from the SIGN of the
    // bandpass edges (negative = LSB-family, positive = USB-family), so this is
    // the single source of truth for that mapping. Exposed to DspPipelineService
    // so the engine-apply seam can re-derive the sign from the live mode rather
    // than trusting whatever sign happens to be stored in StateDto — see the
    // call site in DspPipelineService for why (legacy DB / split-on-B paths can
    // leave a positive value behind on an LSB mode and transmit the wrong
    // sideband). Idempotent for well-formed state.
    // FreeDV rides on an SSB demod/mod. The FreeDV community adopted the SSB
    // voice-mode convention — LSB below 10 MHz, USB at/above — so every station
    // on a band shares one spectral orientation. Mismatching it mirror-images the
    // OFDM carriers in RF and nothing decodes. WDSP has no FreeDv sideband of its
    // own (WdspDspEngine.MapMode defaults FreeDv → USB, correct only ≥10 MHz), so
    // we resolve the effective engine sideband from the dial at the point the mode
    // and filter are pushed. RxMode.FreeDv stays the radio's mode everywhere else;
    // only the WDSP RXA/TXA orientation + bandpass sign follow this. Non-FreeDv
    // modes pass through unchanged.
    // 60 m is the regulatory exception to "below 10 MHz → LSB": FCC §97.305,
    // Ofcom IR 2002 and every other regulator that permits 60 m amateur
    // operation mandate USB-only on that band, and the FreeDV community follows
    // suit (5,403 kHz USB etc.). The window below covers every 60 m amateur
    // allocation (IARU R1 5,351.5–5,366.5 kHz, FCC channels 5,330.5–5,403.5 kHz,
    // Ofcom 5,258.5–5,406.5 kHz) with a small cushion. Outside the window the
    // 10 MHz rule still applies.
    internal const long FreeDvUsbThresholdHz = 10_000_000;
    internal const long FreeDvSixtyMeterLowHz = 5_250_000;
    internal const long FreeDvSixtyMeterHighHz = 5_450_000;
    internal static RxMode EffectiveEngineMode(RxMode mode, long dialHz)
    {
        if (mode != RxMode.FreeDv) return mode;
        if (dialHz >= FreeDvSixtyMeterLowHz && dialHz <= FreeDvSixtyMeterHighHz) return RxMode.USB;
        return dialHz < FreeDvUsbThresholdHz ? RxMode.LSB : RxMode.USB;
    }

    internal static (int low, int high) SignedFilterForMode(RxMode mode, int loAbs, int hiAbs)
    {
        return mode switch
        {
            RxMode.USB => (+loAbs, +hiAbs),
            RxMode.DIGU => (0, +hiAbs),
            RxMode.LSB => (-hiAbs, -loAbs),
            RxMode.DIGL => (-hiAbs, 0),
            RxMode.AM or RxMode.SAM or RxMode.DSB => (-hiAbs, +hiAbs),
            RxMode.FM => (-hiAbs, +hiAbs),
            // CW is sideband-keyed: CWU sits in the positive baseband around
            // +cw_pitch, CWL in the negative around -cw_pitch. WDSP groups
            // CWU with USB and CWL with LSB inside ApplyBandpassForMode, so
            // the absolute family-filter values already include the cw_pitch
            // offset (see FilterPresets.Cwu/Cwl: low/high = ±(pitch ± half)).
            // A symmetric (-hi,+hi) signing here would collapse the passband
            // to (hi,hi) after WDSP's abs-and-sort, killing CW audio.
            RxMode.CWU => (+loAbs, +hiAbs),
            RxMode.CWL => (-hiAbs, -loAbs),
            _ => (+loAbs, +hiAbs),
        };
    }

    internal static (int low, int high) NormalizeLegacyDigitalFilter(
        RxMode mode, int lowHz, int highHz)
    {
        if (mode is not (RxMode.DIGU or RxMode.DIGL)
            || lowHz != -highHz
            || highHz <= 0)
        {
            return (lowHz, highHz);
        }

        // The legacy symmetric pair's labeled bandwidth was high-low (2N).
        // Preserve that total width when moving it onto a one-sided passband.
        int widthHz = (int)Math.Min((long)highHz * 2, MaxFilterEdgeHz);
        return SignedFilterForMode(mode, 0, widthHz);
    }

    public StateDto SetSampleRate(HpsdrSampleRate rate)
    {
        // Protocol 1 encodes the rate in 2 bits (ControlFrame masks &0x03), so it
        // cannot represent 768/1536 kHz — clamp on the P1 path so a stray request
        // can't silently wrap to 48/96 kHz. Protocol 2 (ActiveClient is null;
        // rate carried as u16 kHz) takes the full 48..1536 kHz ladder.
        bool protocol2;
        bool protocol1;
        lock (_sync)
        {
            protocol1 = _activeClient is not null;
            protocol2 = _p2Active;
        }

        if (protocol1 && rate > HpsdrSampleRate.Rate384k)
        {
            _log.LogWarning(
                "radio.setSampleRate rate={Rate} unsupported on Protocol 1; clamping to 384k", rate);
            rate = HpsdrSampleRate.Rate384k;
        }
        var board = ConnectedBoardKind;
        int maxHz = MaxAllowedSampleRateHz(board, protocol2 && !protocol1);
        if (rate.SampleRateHz() > maxHz)
        {
            _log.LogWarning(
                "radio.setSampleRate rate={Rate} exceeds board={Board} max={Max}; clamping",
                rate, board, maxHz);
            rate = MapSampleRate(maxHz);
        }
        int hz = rate.SampleRateHz();
        Mutate(s => s with
        {
            SampleRate = hz,
            ZoomLevel = protocol2
                ? Math.Min(s.ZoomLevel, P2WidebandZoomPolicy.MaxGlobalZoomForRate(hz))
                : Math.Min(s.ZoomLevel, LegacyMaxDisplayZoomLevel),
        });
        // P1 client owns the rate bits directly. On P2 ActiveClient is null, so
        // the SampleRateChanged event is the only way the new rate reaches the
        // live Protocol2Client and re-rates the WDSP RX channel (issue: live
        // bandwidth change was a no-op on P2 / G2 before this).
        ActiveClient?.SetSampleRate(rate);
        SampleRateChanged?.Invoke(hz);
        if (board != HpsdrBoardKind.Unknown)
            _radioStateStore?.SetBoardSampleRate(board, hz, EffectiveOrionMkIIVariant);
        return Snapshot();
    }

    public StateDto SetPreamp(bool on)
    {
        Mutate(s =>
        {
            _preampOn = on;
            // Fast-attack (#806): a preamp/LNA change steps the noise floor.
            // The pipeline's floor tracker observes the PreampOn edge in the
            // state snapshot and fast-attacks itself (Thetis display.cs:893-906).
            return s with { PreampOn = on };
        });
        // P1 path: Protocol1Client owns the bit; SetPreamp pushes the
        // updated CcState on the next outgoing frame. ActiveClient is
        // null on a P2 connection, so the PreampChanged event below is
        // what carries the bit into Protocol2Client (issue #126).
        ActiveClient?.SetPreamp(on);
        PreampChanged?.Invoke(on);
        FlushState();
        return Snapshot();
    }

    public StateDto SetAttenuator(HpsdrAtten atten)
    {
        Mutate(s =>
        {
            _atten = atten;
            // An automatic offset cannot exceed the remaining 31 dB hardware
            // headroom. Trimming it here prevents a high manual baseline from
            // leaving a long tail of invisible release steps above saturation.
            _attOffsetDb = Math.Min(
                _attOffsetDb,
                Math.Min(_adcProtection.MaxOffsetDb, HpsdrAtten.MaxDb - _atten.ClampedDb));
            return s with { AttenDb = atten.ClampedDb, AttOffsetDb = _attOffsetDb };
        });
        // Honour any active auto-ATT offset when the user adjusts the baseline.
        // _lastAppliedEffectiveDb is invalidated so the new sum reaches the radio
        // even if it happens to equal the previous effective value.
        int effective;
        lock (_sync)
        {
            effective = Math.Clamp(_atten.ClampedDb + _attOffsetDb, HpsdrAtten.MinDb, HpsdrAtten.MaxDb);
            _lastAppliedEffectiveDb = effective;
            // Fast-attack (#806): an operator attenuator step shifts the noise
            // floor by that many dB; the pipeline's floor tracker observes the
            // AttenDb change in the state snapshot and fast-attacks itself. (The
            // gradual auto-ATT ramp pushes effective attenuation through
            // ActiveClient directly, NOT this setter, so it does not trigger a
            // fast-attack and the floor follows it smoothly.)
        }
        ApplyPrimaryAttenuatorToActiveClient(effective);
        return Snapshot();
    }

    private void ApplyPrimaryAttenuatorToActiveClient(int effectiveDb)
    {
        var client = ActiveClient;
        if (client is null) return;

        byte adc;
        lock (_sync) adc = ReceiverAdcSource(_state, 0) == 1 ? (byte)1 : (byte)0;
        var effective = new HpsdrAtten(effectiveDb);
        // Clear the formerly selected physical ADC as part of every route push;
        // this makes a live ADC-source change atomic from the control layer's
        // perspective and prevents stale attenuation on the old antenna input.
        client.SetAdcAttenuator(0, adc == 0 ? effective : HpsdrAtten.Zero);
        client.SetAdcAttenuator(1, adc == 1 ? effective : HpsdrAtten.Zero);
    }

    private static void ApplyPrimaryAttenuatorToProtocol2Client(
        Protocol2Client client,
        byte adc,
        int effectiveDb)
    {
        if (adc == 1)
            client.SetRx1Attenuator(effectiveDb);
        else
            client.SetAttenuator(effectiveDb);
    }

    public StateDto SetAutoAtt(bool enabled)
    {
        bool changed = false;
        lock (_sync)
        {
            if (_state.AutoAttEnabled == enabled) return _state;
            changed = true;
            _adcProtection = _adcProtection with { Enabled = enabled };
            _state = _state with { AutoAttEnabled = enabled };
            if (!enabled)
            {
                // Turning auto off: stop accumulating overload counters so the
                // warning lamp doesn't linger and reset the offset to zero so
                // the hardware comes back to the user's baseline immediately.
                _attOffsetDb = 0;
                _predictiveMagnitudeControlActive = false;
                _adcOverloadLevel = 0;
                ResetAdcProtectionWindowNoLock();
                _lastAttAttackMs = long.MinValue;
                _adcProtectionResumeAfterMs = long.MinValue;
                _lastOverloadMs = long.MinValue;
                _state = _state with { AttOffsetDb = 0, AdcOverloadWarning = false };
                int baseline = _atten.ClampedDb;
                if (_lastAppliedEffectiveDb != baseline)
                {
                    _lastAppliedEffectiveDb = baseline;
                    ApplyPrimaryAttenuatorToActiveClient(baseline);
                }
            }
            else
            {
                _lastTickMs = long.MinValue;
                _lastAttAttackMs = long.MinValue;
                _adcProtectionResumeAfterMs = long.MinValue;
            }
        }
        var snap = Snapshot();
        if (changed)
        {
            _stateDirty = true;
            FlushState();
        }
        StateChanged?.Invoke(snap);
        return snap;
    }

    public AdcProtectionStatusDto GetAdcProtectionStatus()
    {
        lock (_sync) return BuildAdcProtectionStatusNoLock();
    }

    public AdcProtectionStatusDto SetAdcProtection(AdcProtectionSetRequest req)
    {
        int? effectiveToApply = null;
        bool stateBroadcastNeeded = false;
        AdcProtectionStatusDto status;

        lock (_sync)
        {
            var next = NormalizeAdcProtection(new AdcProtectionConfig(
                Enabled: req.Enabled ?? _adcProtection.Enabled,
                AttackMs: req.AttackMs ?? _adcProtection.AttackMs,
                ReleaseMs: req.ReleaseMs ?? _adcProtection.ReleaseMs,
                AttackStepDb: req.AttackStepDb ?? _adcProtection.AttackStepDb,
                ReleaseStepDb: req.ReleaseStepDb ?? _adcProtection.ReleaseStepDb,
                MaxOffsetDb: req.MaxOffsetDb ?? _adcProtection.MaxOffsetDb,
                WarningThreshold: req.WarningThreshold ?? _adcProtection.WarningThreshold,
                MagnitudeSoftLimit: req.MagnitudeSoftLimit ?? _adcProtection.MagnitudeSoftLimit,
                ReleaseHoldMs: req.ReleaseHoldMs ?? _adcProtection.ReleaseHoldMs));

            if (next != _adcProtection)
            {
                _adcProtection = next;
                _lastTickMs = long.MinValue;
                _lastAttAttackMs = long.MinValue;
                _stateDirty = true;
            }

            if (_state.AutoAttEnabled != next.Enabled)
            {
                _state = _state with { AutoAttEnabled = next.Enabled };
                stateBroadcastNeeded = true;
                _stateDirty = true;
            }

            if (!next.Enabled)
            {
                _attOffsetDb = 0;
                _predictiveMagnitudeControlActive = false;
                _adcOverloadLevel = 0;
                ResetAdcProtectionWindowNoLock();
                _lastAttAttackMs = long.MinValue;
                _adcProtectionResumeAfterMs = long.MinValue;
                _lastOverloadMs = long.MinValue;
                if (_state.AttOffsetDb != 0 || _state.AdcOverloadWarning)
                {
                    _state = _state with { AttOffsetDb = 0, AdcOverloadWarning = false };
                    stateBroadcastNeeded = true;
                    _stateDirty = true;
                }
            }
            else
            {
                int maxDynamicOffset = Math.Min(
                    next.MaxOffsetDb,
                    HpsdrAtten.MaxDb - _atten.ClampedDb);
                if (_attOffsetDb > maxDynamicOffset)
                {
                    _attOffsetDb = maxDynamicOffset;
                    _state = _state with { AttOffsetDb = _attOffsetDb };
                    stateBroadcastNeeded = true;
                    _stateDirty = true;
                }
            }

            int effective = Math.Clamp(_atten.ClampedDb + _attOffsetDb, HpsdrAtten.MinDb, HpsdrAtten.MaxDb);
            if (effective != _lastAppliedEffectiveDb)
            {
                _lastAppliedEffectiveDb = effective;
                effectiveToApply = effective;
            }

            status = BuildAdcProtectionStatusNoLock();
        }

        if (effectiveToApply is int eff)
        {
            ApplyPrimaryAttenuatorToActiveClient(eff);
        }

        if (_stateDirty) FlushState();
        if (stateBroadcastNeeded) StateChanged?.Invoke(Snapshot());
        return status;
    }

    private AdcProtectionStatusDto BuildAdcProtectionStatusNoLock()
    {
        int effective = Math.Clamp(_atten.ClampedDb + _attOffsetDb, HpsdrAtten.MinDb, HpsdrAtten.MaxDb);
        return new(
            Config: _adcProtection with { Enabled = _state.AutoAttEnabled },
            AttenDb: _atten.ClampedDb,
            OffsetDb: _attOffsetDb,
            EffectiveDb: effective,
            Warning: _state.AdcOverloadWarning,
            OverloadLevel: _adcOverloadLevel,
            LastOverloadBits: _lastAdcOverloadBits,
            Adc0MaxMagnitude: _lastAdc0MaxMagnitude,
            Adc1MaxMagnitude: _lastAdc1MaxMagnitude,
            Adc0MaxMagnitudeAtOverload: _adc0MaxMagnitudeAtOverload,
            Adc1MaxMagnitudeAtOverload: _adc1MaxMagnitudeAtOverload,
            LastTelemetryUtc: _lastAdcTelemetryUtc);
    }

    private static AdcProtectionConfig NormalizeAdcProtection(AdcProtectionConfig config) => config with
    {
        AttackMs = Math.Clamp(config.AttackMs, 25, 1_000),
        ReleaseMs = Math.Clamp(config.ReleaseMs, 50, 5_000),
        AttackStepDb = Math.Clamp(config.AttackStepDb, 1, 6),
        ReleaseStepDb = Math.Clamp(config.ReleaseStepDb, 1, 6),
        MaxOffsetDb = Math.Clamp(config.MaxOffsetDb, HpsdrAtten.MinDb, HpsdrAtten.MaxDb),
        // The overload counter caps at 5, so the gate (level > threshold) must
        // stay reachable — clamp to 4 so it can never silently disable the ramp.
        WarningThreshold = Math.Clamp(config.WarningThreshold, 0, 4),
        MagnitudeSoftLimit = Math.Clamp(config.MagnitudeSoftLimit, 0, AdcSigned16FullScale),
        ReleaseHoldMs = Math.Clamp(config.ReleaseHoldMs, 0, 10_000),
    };

    private void ResetAdcProtectionWindowNoLock()
    {
        _overloadSeenInWindow = false;
        _hardOverloadSeenInWindow = false;
        _softMagnitudeSeenInWindow = false;
        _validMagnitudeSeenInWindow = false;
        _maxMagnitudeSeenInWindow = 0;
    }

    internal static int MagnitudeAttackStepDb(ushort maxMagnitude, int targetMagnitude, int minimumStepDb)
    {
        int floor = Math.Clamp(minimumStepDb, 1, 6);
        if (targetMagnitude <= 0 || maxMagnitude <= targetMagnitude) return floor;
        double excessDb = 20.0 * Math.Log10(maxMagnitude / (double)targetMagnitude);
        return Math.Max(floor, (int)Math.Ceiling(excessDb));
    }

    private static (int Attack, int Target, int Release) AdcMagnitudeZones(int configuredSoftLimit)
    {
        if (configuredSoftLimit <= 0)
            return (AdaptiveAttackMagnitude, AdaptiveTargetMagnitude, AdaptiveReleaseMagnitude);

        // An explicit threshold remains an operator override. Hysteresis is
        // expressed in dB so it scales correctly over the complete ADC range.
        int attack = configuredSoftLimit;
        // Round upward so integer quantization never turns an exact 2 dB or
        // 5 dB boundary into a spurious extra whole-dB attenuation step.
        int target = Math.Max(1, (int)Math.Ceiling(attack * Math.Pow(10.0, -2.0 / 20.0)));
        int release = Math.Max(1, (int)Math.Ceiling(attack * Math.Pow(10.0, -5.0 / 20.0)));
        return (attack, target, release);
    }

    public StateDto SetAutoAgc(bool enabled)
    {
        bool changed = false;
        lock (_sync)
        {
            if (_state.AutoAgcEnabled == enabled) return _state;
            changed = true;
            _state = _state with { AutoAgcEnabled = enabled };
            _agcServoSeatedTopDb = double.NaN;
            if (!enabled)
            {
                // Turning auto off: reset the offset to zero so AGC-T returns
                // to the user's baseline immediately.
                _agcOffsetDb = 0.0;
                _lastAgcTickMs = long.MinValue;
                _state = _state with { AgcOffsetDb = 0.0 };
            }
            else
            {
                // Turning auto on: reset the tick timer so we recalibrate. The
                // pipeline's floor tracker fast-attacks on the enabled edge it
                // observes in the state snapshot, so the loop re-seeds from the
                // current band (Thetis fast-attack semantics).
                _lastAgcTickMs = long.MinValue;
            }
        }
        var snap = Snapshot();
        if (changed)
        {
            _stateDirty = true;
            FlushState();
        }
        StateChanged?.Invoke(snap);
        return snap;
    }

    public StateDto SetRxLeveler(bool enabled)
    {
        bool changed = false;
        lock (_sync)
        {
            if (_state.RxLevelerEnabled == enabled) return _state;
            changed = true;
            _state = _state with { RxLevelerEnabled = enabled };
        }
        var snap = Snapshot();
        if (changed)
        {
            _stateDirty = true;
            FlushState();
        }
        StateChanged?.Invoke(snap);
        return snap;
    }

    public StateDto SetRxLevelerConfig(bool enabled, RxLevelerConfig config)
    {
        var normalized = NormalizeRxLevelerConfig(config);
        bool changed = false;
        lock (_sync)
        {
            if (_state.RxLevelerEnabled == enabled && _state.RxLeveler == normalized)
                return _state;
            changed = true;
            _state = _state with
            {
                RxLevelerEnabled = enabled,
                RxLeveler = normalized,
            };
        }
        var snap = Snapshot();
        if (changed)
        {
            _stateDirty = true;
            FlushState();
        }
        StateChanged?.Invoke(snap);
        return snap;
    }

    internal static RxLevelerConfig NormalizeRxLevelerConfig(RxLevelerConfig? config)
    {
        var value = config ?? new RxLevelerConfig();
        var mode = Enum.IsDefined(value.Mode) ? value.Mode : RxLevelerMode.Auto;
        double target = double.IsFinite(value.TargetRmsDb)
            ? Math.Clamp(value.TargetRmsDb, RxLevelerConfig.MinTargetRmsDb, RxLevelerConfig.MaxTargetRmsDb)
            : RxLevelerConfig.DefaultTargetRmsDb;
        double maxBoost = double.IsFinite(value.MaxBoostDb)
            ? Math.Clamp(value.MaxBoostDb, RxLevelerConfig.MinBoostDb, RxLevelerConfig.MaxBoostLimitDb)
            : RxLevelerConfig.DefaultMaxBoostDb;
        return new RxLevelerConfig(
            mode,
            target,
            maxBoost,
            Math.Clamp(value.AttackMs, RxLevelerConfig.MinAttackMs, RxLevelerConfig.MaxAttackMs),
            Math.Clamp(value.ReleaseMs, RxLevelerConfig.MinReleaseMs, RxLevelerConfig.MaxReleaseMs),
            Math.Clamp(value.HangMs, RxLevelerConfig.MinHangMs, RxLevelerConfig.MaxHangMs));
    }

    /// <summary>
    /// Set by DspPipelineService at construction: the RX display-analyzer FFT
    /// size (DisplayPerformanceOptions.RxAnalyzerFftSize, 16384 stock / 8192
    /// low-power) that produced the panadapter bins the floor is measured from.
    /// WDSP's threshold→max-gain conversion is only self-consistent when this
    /// matches that FFT (wcpAGC.c:482).
    /// </summary>
    internal void SetAutoAgcAnalyzerFftSize(double fftSize)
    {
        if (fftSize > 0) _autoAgcAnalyzerFftSize = fftSize;
    }

    /// <summary>
    /// Thetis auto-AGC-T servo math: given the estimated band noise floor (dBm),
    /// return the WDSP AGC max-gain ("AGC-T top", dB) that seats the AGC knee at
    /// that floor. This is WDSP's own SetRXAAGCThresh→GetRXAAGCTop conversion
    /// (wcpAGC.c:477-495) computed in-process, so SetRXAAGCTop(top) reproduces the
    /// exact max_gain SetRXAAGCThresh(floor) would — bit-faithful to Thetis with
    /// no extra engine round-trip. Inputs (filter width, sample rate, FFT size,
    /// slope=0 for canned modes) are the same WDSP uses, so the only free term is
    /// <see cref="AgcThreshCalOffsetDb"/>.
    /// </summary>
    internal double AutoAgcTopFromNoiseFloor(double noiseFloorDbm) =>
        Math.Clamp(
            Math.Round(AutoAgcRawTopFromNoiseFloor(noiseFloorDbm)),
            AgcTopMinDb,
            AgcTopMaxDb);

    private double AutoAgcRawTopFromNoiseFloor(double noiseFloorDbm)
    {
        var agc = _state.Agc ?? new AgcConfig(AgcMode.Med);
        double calOffsetDb = agc.Mode == AgcMode.Fixed ? 0.0 : AgcThreshCalOffsetDb;
        // 1) Desired AGC threshold (Thetis: floor + userOffset − cal), clamped.
        double thresh = Math.Clamp(
            noiseFloorDbm + AutoAgcOffsetDb - calOffsetDb,
            AgcThreshMinDbm, AgcThreshMaxDbm);
        // 2) WDSP bandwidth/FFT term: 10·log10((fhigh−flow)·size/rate) (wcpAGC.c:482).
        //    fhigh−flow is the RX passband width WDSP runs (our _state filter edges).
        double bwHz = Math.Max(1.0, Math.Abs(_state.FilterHighHz - _state.FilterLowHz));
        double rate = Math.Max(1.0, _state.SampleRate);
        double noiseOffset = 10.0 * Math.Log10(bwHz * _autoAgcAnalyzerFftSize / rate);
        // 3) top = 20·log10(max_gain) = 20·log10(out_target) − 20·log10(var_gain)
        //    − (thresh + noiseOffset). WDSP sets var_gain=10^(slope/200),
        //    while Zeus/Thetis send Custom's UI slope multiplied by 10. Its dB
        //    contribution is therefore exactly the UI slope. Canned modes use 0.
        double slopeDb = agc.Mode == AgcMode.Custom
            ? Math.Clamp(agc.Slope ?? 0, 0, 20)
            : 0.0;
        return AgcOutTargetDb - slopeDb - (thresh + noiseOffset);
    }

    /// <summary>
    /// Auto-AGC-T control loop. Consumes the settled band noise floor estimated
    /// upstream by DspPipelineService's AutoAgcNoiseFloorTracker (the Thetis
    /// display.cs processNoiseFloor port) and, via
    /// <see cref="AutoAgcTopFromNoiseFloor"/>, seats the AGC knee at Thetis's
    /// configured floor shift exactly as its 500 ms tmrAutoAGC_Tick does — carrying the result as
    /// AgcOffsetDb on top of the operator baseline. A deadband suppresses
    /// sub-dB dither. When no settled floor is available (tracker fast-
    /// attacking after a band change, or no spectrum at all) the loop HOLDS
    /// the current offset, exactly as Thetis's timer skips a tick whose
    /// IsNoiseFloorGood is false.
    /// </summary>
    internal void HandleRxMeterForAutoAgc(double signalDbm, long nowMs) =>
        HandleRxMetersForAutoAgc(signalDbm, double.NaN, double.NaN, double.NaN, nowMs);

    // Back-compat overload (no spectrum floor): callers/tests that only have the
    // S-meter delegate here. spectrumFloorDbm = NaN means "no settled floor",
    // so the loop holds — the S-meter fallback lives in the pipeline's tracker.
    internal void HandleRxMetersForAutoAgc(double signalDbm, double adcPkDbfs, double agcGainDb, long nowMs) =>
        HandleRxMetersForAutoAgc(signalDbm, double.NaN, adcPkDbfs, agcGainDb, nowMs);

    // Primary overload (issue #806). spectrumFloorDbm is the tracker's SETTLED
    // per-band noise floor — the same physical quantity Thetis tracks
    // (displayed-dBm scale, RX cal offset already applied by the pipeline).
    // signalDbm (post-AGC audio-RMS S-meter) is unused here — it moves with
    // the signal, not the floor; the pipeline's tracker owns the only
    // S-meter fallback path (sustained spectrum outage), gated and smoothed so
    // it cannot pump. adcPkDbfs and agcGainDb are retained for call-site
    // back-compat and diagnostics only.
    internal void HandleRxMetersForAutoAgc(double signalDbm, double spectrumFloorDbm, double adcPkDbfs, double agcGainDb, long nowMs)
    {
        bool changedOffset = false;
        double newOffset = 0.0;

        lock (_sync)
        {
            if (!_state.AutoAgcEnabled) return;
            if (_mox) return;   // Pause during TX (Thetis: tmrAutoAGC_Tick skips on _mox)
            // No settled floor this tick → hold (Thetis: timer skips when
            // IsNoiseFloorGood is false, e.g. during fast-attack settle).
            if (!double.IsFinite(spectrumFloorDbm) || spectrumFloorDbm <= -250.0) return;

            // Thetis's auto-AGC timer runs at 500 ms.
            if (_lastAgcTickMs != long.MinValue && nowMs - _lastAgcTickMs < 500)
                return;
            _lastAgcTickMs = nowMs;

            // Thetis auto-AGC-T: drive the AGC *threshold* (knee) to the noise
            // floor and let WDSP derive the max-gain ("AGC-T top"). That top
            // becomes the effective AGC-T; AgcOffsetDb carries it relative to
            // the operator baseline so state/slider reflect it and the existing
            // SetAgcTop(AgcTopDb+AgcOffsetDb) apply path pushes the same
            // max_gain SetRXAAGCThresh would have. Manual AGC-T is untouched:
            // SetAgcTop zeroes the offset and disables auto, so this never runs
            // in manual mode.
            double noiseFloor = spectrumFloorDbm;
            double rawAutoTop = AutoAgcRawTopFromNoiseFloor(noiseFloor);

            // Thetis's auto-AGC-T tick (console.cs tmrAutoAGC_Tick:46066) does ONE
            // thing: seat the AGC threshold at the SETTLED noise floor. It never
            // reacts to instantaneous signal level or ADC peak. Recovering from
            // a genuine ADC overload is the operator's RF-gain / attenuation
            // call (auto-ATT owns that path) — it is not the audio AGC loop's
            // job, and post-ADC AGC cannot un-clip an already-overdriven sample
            // anyway.
            // Compare unrounded servo targets: comparing already-quantized
            // offsets makes every nonzero delta at least 1 dB and leaves a
            // sub-dB deadband inert. The 2 dB seated-target band follows
            // Thetis's other noise-floor follower and suppresses ordinary floor
            // ripple while still jumping immediately on a real band change.
            bool holdSeatedTop = double.IsFinite(_agcServoSeatedTopDb)
                && Math.Abs(rawAutoTop - _agcServoSeatedTopDb) < AgcDeadbandDb;
            double seatedTopToApply;
            if (holdSeatedTop)
            {
                seatedTopToApply = _agcServoSeatedTopDb;
            }
            else
            {
                _agcServoSeatedTopDb = rawAutoTop;
                seatedTopToApply = rawAutoTop;
            }

            // Preserve Thetis/WDSP whole-dB application semantics. A raw-target
            // re-seat at a clamp rail can produce the same applied top; record
            // the new seat above, but do not churn state or notify subscribers.
            // On every servo tick, re-derive the offset against the current
            // baseline. After a baseline change, the effective AGC-T therefore
            // converges to this seated rounded top on the next eligible tick.
            double autoTop = Math.Clamp(
                Math.Round(seatedTopToApply),
                AgcTopMinDb,
                AgcTopMaxDb);
            double desiredOffset = autoTop - AgcBaseline(_state);
            if (desiredOffset == _agcOffsetDb) return;

            _agcOffsetDb = desiredOffset;

            _state = _state with { AgcOffsetDb = _agcOffsetDb };
            newOffset = _agcOffsetDb;
            changedOffset = true;
        }

        if (changedOffset)
        {
            StateChanged?.Invoke(Snapshot());
            _log.LogDebug("auto-agc offset={Offset}dB noisefloor={Floor}dBm", newOffset, spectrumFloorDbm);
        }
    }

    // MOX is transient — it belongs on the wire (CcState.Mox → C0 LSB), not in
    // the persisted RX StateDto. TxService owns the latched bool that the UI
    // reads back; this method is the P1-side fan-out only. We also stash the
    // bit locally so the auto-ATT loop can pause itself during TX (Thetis
    // console.cs:22188 — TX uses its own TxAttenData path, not the RX ramp).
    internal void SetMox(bool on) => SetMoxCore(on, Environment.TickCount64);

    // Distinctly named test seam preserves the single SetMox method required
    // by the TX safety reflection audit while making the boundary deterministic.
    internal void SetMoxAtForTest(bool on, long nowMs) => SetMoxCore(on, nowMs);

    private void SetMoxCore(bool on, long nowMs)
    {
        // The hardware NCO can sit off the dial for RX (CTUN freeze, or an
        // autopan/pure-pan offset). Snap it to the dial before the wire MOX
        // bit flips so TX lands on frequency, and restore the parked centre
        // after un-key so the RX view returns. This is the universal keying
        // chokepoint — MOX, TUN, CW, and two-tone all route through here — so
        // every TX path is covered. Both helpers are no-ops when the LO
        // already sits on the dial. (CW pre-aligns in CwEngine for its
        // baseband calc; AlignLoForTx then finds the LO already on the dial
        // and is a no-op, but the frozen centre it recorded is still restored
        // below.)
        // Latch _mox before AlignLoForTx so a concurrent guarded SetRadioLo
        // (the frontend LO heartbeat) cannot slip between the snap and guard.
        bool moxFell = false;
        lock (_sync)
        {
            if (_mox != on)
            {
                moxFell = _mox && !on;
                _mox = on;
                // Telemetry arriving around a TX/RX transition can describe
                // the old signal path. Start a fresh sample window on both
                // MOX edges so it can neither attack nor release RX S-ATT.
                // A one-status-cycle guard after unkey rejects queued software-
                // MOX packets; the P2 physical-PTT bit is an additional guard.
                ResetAdcProtectionWindowNoLock();
                _lastTickMs = long.MinValue;
                _lastAttAttackMs = long.MinValue;
                _adcProtectionResumeAfterMs = on
                    ? long.MinValue
                    : nowMs + PostMoxTelemetryGuardMs;
            }
        }
        if (on) AlignLoForTx();
        if (on && Interlocked.Read(ref _productPluginNativeFrequencyHz) > 0)
            PushPrimaryRadioFrequency(Interlocked.Read(ref _productPluginNativeFrequencyHz));
        ActiveClient?.SetMox(on);
        MoxChanged?.Invoke(on);
        if (!on) RestoreLoAfterTx();
        if (moxFell) ApplyBoardKindToActiveClientIfSafe();
    }

    // Drive is transient like MOX — latched on the Protocol1Client so the
    // DriveFilter register on the next outgoing frame carries it. We clamp
    // here rather than at the endpoint so every entry point (REST, future
    // CAT bridge, tests) gets the same range guarantee.
    public void SetDrive(int percent)
        => SetDriveCore(percent, persist: true);

    internal int? ProductPluginDriveCapPct
    {
        get
        {
            int value = Volatile.Read(ref _productPluginDriveCapPct);
            return value < 0 ? null : value;
        }
    }

    internal void SetProductPluginDriveCap(long generation, int? percent)
    {
        lock (_productPluginDriveCapGate)
        {
            if (generation < _productPluginDriveCapGeneration) return;
            int ceiling = Math.Clamp(Snapshot().DriveMaxPct, 1, 100);
            int value = percent is int requested ? Math.Clamp(requested, 0, ceiling) : -1;
            _productPluginDriveCapGeneration = generation;
            if (Interlocked.Exchange(ref _productPluginDriveCapPct, value) != value)
                RecomputePaAndPush();
        }
    }

    internal void ClearProductPluginDriveCap(long generation)
    {
        lock (_productPluginDriveCapGate)
        {
            if (_productPluginDriveCapGeneration == generation)
            {
                _productPluginDriveCapGeneration = 0;
                if (Interlocked.Exchange(ref _productPluginDriveCapPct, -1) != -1)
                    RecomputePaAndPush();
            }
        }
        ProductPluginDriveCapClearAttemptedForTest?.Invoke(generation);
    }

    internal bool SetDriveIfCurrent(int percent, int expectedCurrent)
        => SetDriveCore(percent, persist: false, expectedCurrent, abortIfTxActive: true);

    private bool SetDriveCore(
        int percent,
        bool persist,
        int? onlyIfDrivePctIs = null,
        bool abortIfTxActive = false)
    {
        int requested = Math.Clamp(percent, 0, 100);
        int clamped = 0;
        // Mutate() broadcasts the new StateDto to subscribed clients and
        // flips _stateDirty so the debounce flush persists to LiteDB. Without
        // the broadcast a fresh client connect would not see the hydrated
        // value until something else dirtied the state.
        Mutate(s =>
        {
            // Close recall's value and key-up TOCTOU windows atomically.
            if ((onlyIfDrivePctIs is int expected && s.DrivePct != expected) ||
                (abortIfTxActive && (_mox || _tunActive)))
                return null;

            clamped = Math.Min(requested, Math.Clamp(s.DriveMaxPct, 1, 100));
            Interlocked.Exchange(ref _drivePct, clamped);
            return s with { DrivePct = clamped };
        }, out bool applied);
        if (!applied) return false;
        RecomputePaAndPush();
        // Per-band Drive recall (#128). Persist public slider changes to the
        // current band; recall calls this core with persist=false.
        if (persist)
        {
            long vfoHz;
            lock (_sync) { vfoHz = _state.VfoHz; }
            var band = RuntimeBandKey(vfoHz);
            if (band is not null)
                _paStore.SetBandDrive(band, clamped);
        }
        return true;
    }

    // ---- TX pre-key (MOX) delay (issue #630) -----------------------------
    // Max operator-settable pre-key delay. Thetis RF-Delay parity range.
    internal const int MaxPreKeyDelayMs = 500;
    // Safety margin the pre-key window must stay below the PS MOX hold-off by,
    // so the IQ mute is fully open before WDSP calcc can leave LMOXDELAY and
    // start binning feedback samples. A pre-key window that outlasts the PS
    // hold-off would let PS collect zero-envelope samples → COLLECT never
    // completes → the documented PS calibration stall. 50 ms is comfortably
    // longer than one WDSP TX block at any supported rate.
    private const int PsPreKeyMarginMs = 50;

    // Clamp a requested pre-key delay to [0, MaxPreKeyDelayMs] AND strictly
    // below (psMoxDelaySec*1000 - margin). With the default PS hold-off of
    // 200 ms this caps the pre-key at 150 ms — ample for amp T/R sequencing
    // (the #630 reporter needs ~30 ms) while keeping PS safe by construction.
    private static int ClampPreKeyToPs(int requestedMs, double psMoxDelaySec)
    {
        int ceiling = (int)(psMoxDelaySec * 1000.0) - PsPreKeyMarginMs;
        if (ceiling < 0) ceiling = 0;
        if (ceiling > MaxPreKeyDelayMs) ceiling = MaxPreKeyDelayMs;
        return Math.Clamp(requestedMs, 0, ceiling);
    }

    /// <summary>
    /// Set the TX pre-key (MOX) delay in milliseconds. Clamped to
    /// [0, <see cref="MaxPreKeyDelayMs"/>] and hard-clamped strictly below the
    /// current PureSignal MOX hold-off (bidirectional invariant — the PS setter
    /// re-clamps this downward too). Returns the updated snapshot so the caller
    /// can surface the actually-applied value (which may be lower than asked).
    /// </summary>
    public StateDto SetTxMoxPreKeyDelayMs(int ms)
    {
        int clamped = ClampPreKeyToPs(ms, Snapshot().PsMoxDelaySec);
        Interlocked.Exchange(ref _txMoxPreKeyDelayMs, clamped);
        Mutate(s => s with { TxMoxPreKeyDelayMs = clamped });
        return Snapshot();
    }

    /// <summary>
    /// Enable/disable the old-school end-of-over roger beep. Persisted with the
    /// radio-state snapshot; default OFF preserves existing TX behaviour.
    /// </summary>
    public StateDto SetRogerBeepEnabled(bool enabled)
    {
        Mutate(s => s with { RogerBeepEnabled = enabled });
        return Snapshot();
    }

    public bool RogerBeepEnabled
    {
        get { lock (_sync) return _state.RogerBeepEnabled; }
    }

    /// <summary>Authoritative pre-key delay (ms) read by TxService on the MOX
    /// rising edge. Already PS-clamped.</summary>
    public int TxMoxPreKeyDelayMs => Volatile.Read(ref _txMoxPreKeyDelayMs);

    // ---- TX tail (MOX hang) delay (issue #1294) --------------------------
    // Thetis exposes PTT Delay up to 5000 ms. Zeus uses this specifically as a
    // browser/WebSocket TX audio drain before the software MOX bit drops.
    internal const int MaxTailDelayMs = 5000;

    /// <summary>
    /// Set the TX tail (MOX hang) delay in milliseconds. Clamped to
    /// [0, <see cref="MaxTailDelayMs"/>]. Returns the updated snapshot so the
    /// caller can surface the actually-applied value.
    /// </summary>
    public StateDto SetTxMoxTailDelayMs(int ms)
    {
        int clamped = Math.Clamp(ms, 0, MaxTailDelayMs);
        Interlocked.Exchange(ref _txMoxTailDelayMs, clamped);
        Mutate(s => s with { TxMoxTailDelayMs = clamped });
        return Snapshot();
    }

    /// <summary>Authoritative tail delay (ms) read by TxService on the MOX
    /// falling edge to hold the wire MOX bit asserted while audio in flight
    /// finishes draining.</summary>
    public int TxMoxTailDelayMs => Volatile.Read(ref _txMoxTailDelayMs);

    // ---- RX resume delay after TX ----------------------------------------
    internal const int DefaultPostTxRxMuteDelayMs = 200;
    internal const int MaxPostTxRxMuteDelayMs = 5000;

    /// <summary>
    /// Set the post-TX RX resume mute delay in milliseconds. Clamped to
    /// [0, <see cref="MaxPostTxRxMuteDelayMs"/>]. Returns the updated snapshot
    /// so the UI can surface the actually-applied value.
    /// </summary>
    public StateDto SetTxPostTxRxMuteDelayMs(int ms)
    {
        int clamped = Math.Clamp(ms, 0, MaxPostTxRxMuteDelayMs);
        Interlocked.Exchange(ref _txPostTxRxMuteDelayMs, clamped);
        Mutate(s => s with { TxPostTxRxMuteDelayMs = clamped });
        return Snapshot();
    }

    /// <summary>Authoritative post-TX RX mute delay read by DspPipelineService
    /// when MOX falls.</summary>
    public int TxPostTxRxMuteDelayMs => Volatile.Read(ref _txPostTxRxMuteDelayMs);

    // ---- TX timeout (issue #1270) ---------------------------------------
    // 0 = disabled, but every server caller must explicitly acknowledge the
    // disconnect/stuck-key risk before applying that value. Otherwise minimum
    // 30 s so an operator can shorten the guard for CW/digital ops while still
    // leaving a safety window; maximum 600 s = 10 min so a very long QSO tail
    // can't defeat PA protection unless the operator explicitly disables it.
    // Default preserves the historical FR-6 120 s value.
    internal const int DisabledTxTimeoutSec = 0;
    internal const int MinTxTimeoutSec = 30;
    internal const int MaxTxTimeoutSec = 600;
    internal const int DefaultTxTimeoutSec = 120;

    /// <summary>Normalise a requested TX-timeout to the stored form: 0 (or any
    /// non-positive value) means "disabled"; anything else is clamped to
    /// [<see cref="MinTxTimeoutSec"/>, <see cref="MaxTxTimeoutSec"/>].</summary>
    internal static int ClampTxTimeoutSec(int seconds)
        => seconds <= 0 ? DisabledTxTimeoutSec : Math.Clamp(seconds, MinTxTimeoutSec, MaxTxTimeoutSec);

    /// <summary>
    /// Set the maximum single-transmission length in seconds. A value &lt;= 0
    /// disables the guard entirely, but only when
    /// <paramref name="acknowledgeDisconnectRisk"/> is true; otherwise it is clamped to
    /// [<see cref="MinTxTimeoutSec"/>, <see cref="MaxTxTimeoutSec"/>].
    /// Returns the updated snapshot so the caller can surface the applied
    /// value (which may be clamped or 0 = disabled).
    /// </summary>
    /// <exception cref="InvalidOperationException">The caller requested a
    /// disabled timeout without acknowledging that a disconnect during
    /// transmission may leave the radio keyed.</exception>
    public StateDto SetTxTimeoutSec(int seconds, bool acknowledgeDisconnectRisk = false)
    {
        int clamped = ClampTxTimeoutSec(seconds);
        if (clamped == DisabledTxTimeoutSec && !acknowledgeDisconnectRisk)
        {
            throw new InvalidOperationException(
                "Disabling the TX timeout requires acknowledgement that a disconnect during transmission may leave the radio keyed.");
        }

        Interlocked.Exchange(ref _txTimeoutSec, clamped);
        Mutate(s => s with { TxTimeoutSec = clamped });
        return Snapshot();
    }

    /// <summary>Authoritative TX timeout in seconds read by TxMetersService on
    /// every meter tick to evaluate the protection trip. 0 = disabled (no
    /// trip). Issue #1270.</summary>
    public int TxTimeoutSec => Volatile.Read(ref _txTimeoutSec);

    // Re-clamp the stored pre-key delay after the PS MOX hold-off changed, so
    // lowering PsMoxDelaySec can never leave a now-too-large pre-key window in
    // place. Called from SetPsAdvanced after the PS mutate commits.
    private void ReclampPreKeyToPs()
    {
        int current = Volatile.Read(ref _txMoxPreKeyDelayMs);
        int reclamped = ClampPreKeyToPs(current, Snapshot().PsMoxDelaySec);
        if (reclamped != current)
        {
            Interlocked.Exchange(ref _txMoxPreKeyDelayMs, reclamped);
            Mutate(s => s with { TxMoxPreKeyDelayMs = reclamped });
        }
    }

    /// <summary>
    /// Forward the on-board CW keyer config to the connected radio and
    /// remember it so a reconnect re-applies it. Called by the CW settings
    /// endpoint whenever the operator changes a persisted CW setting.
    /// No-op (cached only) when no radio is connected. See zeus-bks.
    /// <list type="bullet">
    /// <item>P1: speed, mode, weight, and paddle direction go to C&amp;C register 0x0B.</item>
    /// <item>P2: all values arm the radio's internal keyer via
    /// the TxSpecific packet so a paddle on the rear KEY jack keys the
    /// transmitter — issue #1032.</item>
    /// </list>
    /// </summary>
    public void SetCwKeyerConfig(
        int wpm,
        CwKeyerMode mode,
        int sidetoneHz,
        double sidetoneGainDb,
        bool breakIn,
        int hangMs,
        int weight,
        bool paddleReverse)
    {
        Volatile.Write(ref _cwKeyerWpm, wpm);
        Volatile.Write(ref _cwKeyerMode, (int)mode);
        lock (_sync) { _cwSidetoneHz = sidetoneHz; _cwSidetoneGainDb = sidetoneGainDb; }
        Volatile.Write(ref _cwBreakIn, breakIn ? 1 : 0);
        Volatile.Write(ref _cwHangMs, hangMs);
        Volatile.Write(ref _cwWeight, weight);
        Volatile.Write(ref _cwPaddleReverse, paddleReverse ? 1 : 0);
        ActiveClient?.SetCwKeyerConfig(wpm, mode, weight, paddleReverse);
        ActiveClient?.SetCwSidetone(HardwareSidetoneLevel(sidetoneGainDb), sidetoneHz);
        PushCwKeyerEnabledToP1();
        PushCwToP2();
    }

    // Map the operator's host sidetone gain (dBFS, -60..0) to the legacy P1
    // firmware sidetone_level (0..100). The FPGA multiplies its sine table by
    // this level (anan-100d_angelia sidetone.v:97), so a linear-amplitude
    // mapping keeps the hardware headphone sidetone tracking the same volume
    // the host-generated sidetone uses. A muted (very low dB) setting rounds to
    // 0 = hardware sidetone off. Issue #1783.
    internal static int HardwareSidetoneLevel(double gainDb)
    {
        double linear = Math.Pow(10.0, Math.Clamp(gainDb, -60.0, 0.0) / 20.0);
        return Math.Clamp((int)Math.Round(linear * 100.0), 0, 100);
    }

    private static bool IsCwMode(RxMode mode) => mode is RxMode.CWU or RxMode.CWL;

    private void PushCwKeyerEnabledToP1(StateDto? state = null)
    {
        var effectiveState = state ?? Snapshot();
        ActiveClient?.SetCwKeyerEnabled(
            IsCwMode(RadioFrequencyResolver.TxMode(effectiveState)));
    }

    // 1 while a host-driven CW source (CwEngine / MoxSource.Cwx — keyboard,
    // macros, TCI/CAT keying) is keying. The P2 internal (FPGA) keyer must NOT
    // be armed at the same time: doing so would put two T/R masters on the air
    // (host MOX + carrier IQ vs. the gateware self-keying with break-in). This
    // mirrors pihpsdr, which only arms the internal keyer when
    // !CAT_cw && !MIDI_cw (new_protocol.c:1463-1465). Gating on the host CW
    // source (not host MOX) is deliberate: a paddle-driven internal-keyer TX
    // can raise host MOX via an opt-in PTT-IN→MOX setting, and gating on MOX
    // there would oscillate (disarm→drop→re-arm). Volatile int for lock-free
    // cross-thread reads in PushCwToP2.
    private int _hostCwKeying;
    private readonly object _cwPushSync = new();
    private Func<StateDto, bool>? _hardwareCwArmEvaluator;
    private int _hardwareCwSafetyBlocked;
    // Socketless observer of the exact config handed to the P2 client.
    internal Action<Zeus.Protocol2.CwKeyerWireConfig>? HardwareCwConfigSinkForTests { get; set; }

    internal void ConfigureHardwareCwArmSafety(Func<StateDto, bool> evaluator)
    {
        _hardwareCwArmEvaluator = evaluator ?? throw new ArgumentNullException(nameof(evaluator));
        PushCwToP2();
    }

    internal void SetHardwareCwSafetyBlocked(bool blocked)
    {
        Volatile.Write(ref _hardwareCwSafetyBlocked, blocked ? 1 : 0);
        PushCwToP2();
    }

    internal void RefreshHardwareCwArmPermission() => PushCwToP2();

    /// <summary>
    /// Mark the host CW sender (CwEngine, <see cref="MoxSource.Cwx"/>) as
    /// keying or idle. While keying, the Protocol-2 internal keyer is disarmed
    /// (TxSpecific byte-5 cleared) so host-keyed and FPGA-keyed CW are mutually
    /// exclusive — the pihpsdr model. Re-pushes immediately so the arm state
    /// tracks the host sender edge. No-op on P1 (no <c>_p2Client</c>).
    /// </summary>
    public void SetHostCwKeying(bool active)
    {
        Volatile.Write(ref _hostCwKeying, active ? 1 : 0);
        PushCwToP2();
    }

    /// <summary>
    /// Map the operator's CW sidetone gain (dB) onto the Protocol-2 radio
    /// sidetone level (0-127, TxSpecific byte 6). A muted sidetone (gain at or
    /// below the floor) sends level 0, which clears the sidetone bit.
    /// </summary>
    private static byte CwSidetoneLevelFromGainDb(double gainDb)
    {
        if (gainDb <= -60.0) return 0;
        double level = 100.0 * Math.Pow(10.0, gainDb / 20.0);
        return (byte)Math.Clamp((int)Math.Round(level), 0, 127);
    }

    /// <summary>
    /// Push the internal-keyer config to the Protocol-2 client (no-op on P1,
    /// whose keyer is driven via <c>ActiveClient</c>). The radio's internal
    /// keyer is armed only in CW mode (byte-5 bit-1), so this is called on
    /// connect, on a CW-settings change, and on every mode change so byte 5
    /// toggles as the operator enters/leaves CW.
    /// </summary>
    private void PushCwToP2()
    {
        var p2 = _p2Client;
        if (p2 is null) return;
        // Capture every RadioService-owned value before taking the transport
        // serialization lock. State mutations can call back through TxService
        // while holding _sync, so the reverse _cwPushSync -> _sync ordering
        // would deadlock a simultaneous safety trip.
        RxMode mode;
        int sidetoneHz;
        double sidetoneGainDb;
        StateDto state;
        lock (_sync)
        {
            mode = _state.Mode;
            sidetoneHz = _cwSidetoneHz;
            sidetoneGainDb = _cwSidetoneGainDb;
            state = _state;
        }
        bool requestedActive = mode is RxMode.CWU or RxMode.CWL
            && Volatile.Read(ref _hostCwKeying) == 0
            && Volatile.Read(ref _hardwareCwSafetyBlocked) == 0;
        bool active = requestedActive
            && (_hardwareCwArmEvaluator?.Invoke(state) ?? true);

        lock (_cwPushSync)
        {
            // Arm only in CW mode AND when the host CW sender is idle (see
            // SetHostCwKeying) — never two T/R masters at once. Production
            // additionally routes the standing permission through the engine
            // safety module; isolated RadioService tests retain the legacy
            // allow when no evaluator has been installed.
            var config = new Zeus.Protocol2.CwKeyerWireConfig
            {
                Active = active,
                Mode = (CwKeyerMode)Volatile.Read(ref _cwKeyerMode),
                SpeedWpm = Volatile.Read(ref _cwKeyerWpm),
                SidetoneHz = sidetoneHz,
                SidetoneLevel = CwSidetoneLevelFromGainDb(sidetoneGainDb),
                BreakIn = Volatile.Read(ref _cwBreakIn) != 0,
                HangMs = Volatile.Read(ref _cwHangMs),
                Weight = Volatile.Read(ref _cwWeight),
                PaddleReverse = Volatile.Read(ref _cwPaddleReverse) != 0,
            };
            p2.SetCwKeyerConfig(config);
            HardwareCwConfigSinkForTests?.Invoke(config);
        }
    }

    // Independent TUN drive %. Applies on the very next frame if TUN is already
    // keyed; otherwise it sits until TxService flips _tunActive.
    public void SetTuneDrive(int percent)
        => SetTuneDriveCore(percent, persist: true);

    internal bool SetTuneDriveIfCurrent(int percent, int expectedCurrent)
        => SetTuneDriveCore(percent, persist: false, expectedCurrent, abortIfTxActive: true);

    internal bool SetPaCalibrationTuneDriveIfCurrent(int percent, int expectedCurrent)
        => SetTuneDriveCore(
            percent,
            persist: false,
            expectedCurrent,
            // Calibration owns TUN and deliberately changes its drive between
            // the 10, 25, and 50 W checks while staying keyed on a band.
            abortIfTxActive: false,
            paCalibrationOwner: true);

    private bool SetTuneDriveCore(
        int percent,
        bool persist,
        int? onlyIfTunePctIs = null,
        bool abortIfTxActive = false,
        bool paCalibrationOwner = false)
    {
        int requested = Math.Clamp(percent, 0, 100);
        int clamped = 0;
        Mutate(s =>
        {
            if (!paCalibrationOwner)
                ThrowIfPaCalibrationInvariantMutation("TUN power");
            // Close recall's value and key-up TOCTOU windows atomically.
            if ((onlyIfTunePctIs is int expected && s.TunePct != expected) ||
                (abortIfTxActive && (_mox || _tunActive)))
                return null;

            clamped = Math.Min(requested, Math.Clamp(s.DriveMaxPct, 1, 100));
            Interlocked.Exchange(ref _tunePct, clamped);
            return s with { TunePct = clamped };
        }, out bool applied);
        if (!applied) return false;
        RecomputePaAndPush();
        // Per-band Tune recall (#128); see SetDriveCore for the shape.
        if (persist)
        {
            long vfoHz;
            lock (_sync) { vfoHz = _state.VfoHz; }
            var band = RuntimeBandKey(vfoHz);
            if (band is not null)
                _paStore.SetBandTune(band, clamped);
        }
        return true;
    }

    // TxService calls this on every MOX/TUN edge. Runs the same recompute the
    // drive-slider path uses so the drive byte on the wire always reflects the
    // just-applied keying state (Thetis PreviousPWR swap, `console.cs:30094`).
    internal void NotifyTunActive(bool on)
    {
        bool tuneFell;
        lock (_sync)
        {
            tuneFell = _tunActive && !on;
            _tunActive = on;
            _txFrequencyTransition = false;
        }
        // Latch the TUN flag on the P1 client so its ControlFrame OC composition
        // ORs the OcTune mask on top of OcTx only during TUN (issue #1325). P1's
        // wire MOX bit rises for both TUN and regular TX, so the client needs a
        // separate signal to decide. The P2 client picks up TUN through
        // DspPipelineService's TunActiveChanged subscription.
        (ActiveClient as Zeus.Protocol1.Protocol1Client)?.SetTune(on);
        RecomputePaAndPush();
        TunActiveChanged?.Invoke(on);
        if (tuneFell) ApplyBoardKindToActiveClientIfSafe();
    }

    internal void ClearTunActiveForSafety()
    {
        bool tuneFell;
        lock (_sync)
        {
            tuneFell = _tunActive;
            _tunActive = false;
            _txFrequencyTransition = false;
        }
        (ActiveClient as Zeus.Protocol1.Protocol1Client)?.SetTune(false);
        TunActiveChanged?.Invoke(false);
        if (tuneFell) ApplyBoardKindToActiveClientIfSafe();
    }

    internal void BeginTxFrequencyTransition()
    {
        lock (_sync) _txFrequencyTransition = true;
    }

    internal void SetTxSafetyAuthority(bool granted) =>
        Volatile.Write(ref _txSafetyAuthority, granted ? 1 : 0);

    internal void ConfigureTxDriveSafety(Func<int, TransmitSafetyDecision> evaluator) =>
        _txDriveSafetyEvaluator = evaluator ?? throw new ArgumentNullException(nameof(evaluator));

    // DspPipelineService calls this right after a P2 client is created so the
    // fresh connection sees the current PA snapshot without waiting for the
    // next state change.
    public void ReplayPaSnapshot() => RecomputePaAndPush();
    internal void ApplyTxSafetyInhibit() => RecomputePaAndPushCore(safetyInhibit: true);

    public RfFilterSettingsDto GetRfFilterSettings()
    {
        if (_rfFilterStore is null)
            throw new InvalidOperationException("RF filter settings store is not configured.");
        var snap = Snapshot();
        return _rfFilterStore.GetDto(EffectiveBoardKind, snap, IsTxActive(), snap.PsEnabled);
    }

    public RfFilterSettingsDto SetRfFilterSettings(RfFilterSettingsSetRequest req)
    {
        if (_rfFilterStore is null)
            throw new InvalidOperationException("RF filter settings store is not configured.");
        var snap = Snapshot();
        return _rfFilterStore.Set(req, EffectiveBoardKind, snap, IsTxActive(), snap.PsEnabled);
    }

    public RfFilterSettingsDto ResetRfFilterSettings()
    {
        if (_rfFilterStore is null)
            throw new InvalidOperationException("RF filter settings store is not configured.");
        var snap = Snapshot();
        return _rfFilterStore.Reset(EffectiveBoardKind, snap, IsTxActive(), snap.PsEnabled);
    }

    internal bool IsTxActive()
    {
        lock (_sync) return _mox || _tunActive;
    }

    // Global audio front-end push (external-audio-jacks re-port). Server-
    // authoritative: read AudioSettingsStore, clamp per-board, push to the P1
    // client directly and fire AudioFrontEndChanged for the P2 forwarder + the
    // radio-mic STREAM gate. Called on store edit and on connect (P1 + P2).
    // Mirrors the PA RecomputePaAndPush discipline but for the GLOBAL (not
    // per-band) audio state. Defaults (Host, no boost/bias, gain 0) reproduce
    // today's wire output bit-for-bit on every board.
    internal BoardCapabilities AudioCapabilities => BoardCapabilitiesTable.ForAudio(
        EffectiveBoardKind, EffectiveOrionMkIIVariant, _audioStore?.Hl2PlusCodecEnabled == true);

    private void PushAudioFrontEnd()
    {
        var sel = _audioStore?.Get() ?? AudioSourceSelection.Default;
        var board = EffectiveBoardKind;
        var variant = EffectiveOrionMkIIVariant;
        var caps = AudioCapabilities;
        if (ActiveClient is { } audioClient)
            audioClient.Hl2CodecInstalled = board == HpsdrBoardKind.HermesLite2 && caps.HasOnboardCodec;

        // CLAMP the persisted source against the connected board's capabilities
        // (external-audio-jacks re-port, safety invariant 5). Any source the
        // board can't honour falls back to Host so the wire is never handed a
        // jack the hardware lacks:
        //   - HL2 (no onboard codec) is Host-only — any radio source → Host.
        //   - RadioLineIn requires HasRadioLineIn (10E + 100D/200D + 0x0A).
        //   - RadioBalancedXlr requires HasBalancedXlr (G2 / G2-1K only).
        //   - RadioMic requires the stream codec.
        // The mic-bias param is additionally suppressed on boards without
        // HasMicBias, so a stale bias bit can never reach a non-bias board.
        var resolved = ClampAudioSource(sel, caps);

        // Encode the wire bytes as PURE FUNCTIONS of the resolved source. Host →
        // literal zero on every surface, byte-identical to today; no param
        // fallthrough.
        //
        // Dispatch the encoder by the LIVE transport, not the board's default
        // protocol. ANAN-10E (HermesII) is a P1 board by default, but its
        // firmware can also run Protocol 2 — the operator flashes one or the
        // other. Without this override, ExternalPortEncoders.For defaults
        // HermesII to the Protocol-1 encoder, whose P2 byte-50/51 surfaces are
        // zero, so the TxSpecific "switch to line-in" bit is never set and the
        // radio's codec stays on its default mic input (issue #1053). On a P2
        // connection force the Protocol-2 encoder; otherwise let dispatch fall
        // back to the board's default transport.
        bool p2Active;
        lock (_sync) p2Active = _p2Active;
        RadioProtocol? liveProtocol = p2Active ? RadioProtocol.Protocol2 : null;
        var encoder = ExternalPortEncoders.For(board, variant, liveProtocol);
        var portState = new ExternalPortState(
            Source: resolved.Source,
            MicBoost: resolved.MicBoost,
            MicBias: resolved.MicBias,
            LineInGain: resolved.LineInGain);

        // P1 codec boards (Hermes-class): mic_boost / mic_linein on the 0x12
        // frame. The optional HL2+ AK4951 uses the same mic-boost bit,
        // selected only after the installed-hardware capability clamp.
        // Its microphone bias is hardware-configured; the legacy 0x14
        // mic_trs / mic_bias / line_in_gain fields remain clear.
        var (p1Boost, p1LineIn) = encoder.EncodeP1CodecAudioBits(in portState);
        if (board == HpsdrBoardKind.HermesLite2 && caps.HasOnboardCodec)
            (p1Boost, p1LineIn) = ExternalPortAudio.P1CodecAudioBits(resolved.Source, resolved.MicBoost);
        // ANAN-10E line-in (issue #667): when the encoder selected line-in
        // (HermesII only — p1LineIn is false on every other P1 board), forward
        // the 0..31 gain so ControlFrame can place it on the 0x14 frame. The gain
        // stays 0 for all other sources/boards → byte-identical to today.
        //
        // mic_bias (external-port parity audit, GAP-AUD-1): forward the RESOLVED
        // bias. ClampAudioSource already dropped it to false on any board without
        // HasMicBias and on any source other than RadioMic/XLR, so on P1 it is
        // true only for ANAN-100D/200D with the radio mic selected — and
        // ControlFrame writes C1[5] only when set. Default Host → false →
        // byte-identical to today. micTrs stays false (HL2-only tip/ring jack; HL2
        // is Host-only in v1).
        ActiveClient?.SetAudioFrontEnd(
            micBoost: p1Boost,
            micLineIn: p1LineIn,
            micTrs: false,
            micBias: resolved.MicBias,
            lineInGain: p1LineIn ? resolved.LineInGain : 0);

        // P2 (0x0A Saturn family): forward the resolved literal byte-50
        // mic_control + byte-51 line_in_gain to the live Protocol2Client via
        // DspPipelineService. The forwarder does no source interpretation. The
        // same event drives the radio-mic STREAM (1026) single-select gate.
        byte micControl = encoder.EncodeP2MicControlByte(in portState);
        byte lineInGain = encoder.EncodeP2LineInGainByte(in portState);
        AudioFrontEndChanged?.Invoke(new AudioFrontEndPush(
            Source: resolved.Source,
            MicControlByte: micControl,
            LineInGain: lineInGain));

        // Mirror the RESOLVED source into StateDto so the frontend hydrates from
        // the server and never clobbers it on connect (PR #359/#360 pattern).
        Mutate(s => s.TxAudioSource == resolved.Source ? s : s with { TxAudioSource = resolved.Source });
    }

    /// <summary>
    /// Clamp a persisted <see cref="AudioSourceSelection"/> against a board's
    /// capabilities (external-audio-jacks re-port). Returns Host (with no params)
    /// whenever the board cannot honour the requested jack, and drops the
    /// mic-bias param on boards without <c>HasMicBias</c>. Pure — no side
    /// effects — so the endpoint and the push share one definition.
    /// </summary>
    internal static AudioSourceSelection ClampAudioSource(AudioSourceSelection sel, BoardCapabilities caps)
    {
        // A board with neither the stream codec nor the HL2 mic front-end has no
        // audio jacks at all → Host.
        bool audioCapable = caps.HasOnboardCodec || caps.HermesLite2MicFrontEnd;
        if (!audioCapable) return AudioSourceSelection.Default;

        switch (sel.Source)
        {
            case TxAudioSource.RadioMic:
                // RadioMic needs a stream codec, including an explicitly
                // configured HL2+ companion. Drop bias on non-bias boards.
                if (!caps.HasOnboardCodec) return AudioSourceSelection.Default;
                return sel with { MicBias = sel.MicBias && caps.HasMicBias };

            case TxAudioSource.RadioLineIn:
                if (!caps.HasOnboardCodec || !caps.HasRadioLineIn)
                    return AudioSourceSelection.Default;
                // Line-in carries gain, not mic params.
                return new AudioSourceSelection(TxAudioSource.RadioLineIn, MicBoost: false, MicBias: false, LineInGain: sel.LineInGain);

            case TxAudioSource.RadioBalancedXlr:
                if (!caps.HasOnboardCodec || !caps.HasBalancedXlr)
                    return AudioSourceSelection.Default;
                return sel with { MicBias = sel.MicBias && caps.HasMicBias, LineInGain = 0 };

            case TxAudioSource.Host:
            default:
                return AudioSourceSelection.Default;
        }
    }

    // DspPipelineService calls this right after a P2 client is created so the
    // fresh connection picks up the persisted audio front-end without waiting
    // for a store edit. Public so the P2-connect hook can drive it.
    public void ReplayAudioFrontEnd() => PushAudioFrontEnd();

    /// <summary>
    /// Push the persisted HL2 user-GPIO mask to the live client (external-ports
    /// plan, Phase 5; re-ported in the external-port parity audit). Gated on the
    /// connected board's <c>HasHl2UserGpio</c> capability: a non-HL2 board (or one
    /// without the store) is handed mask 0, so its 0x14-frame C3 stays clear and
    /// byte-identical to today. ControlFrame only writes C3[3:0] on HermesLite2,
    /// so this is a belt-and-braces second gate at the service layer.
    /// </summary>
    private void PushHl2Gpio()
    {
        var caps = BoardCapabilitiesTable.For(EffectiveBoardKind, EffectiveOrionMkIIVariant);
        byte mask = (caps.HasHl2UserGpio && _hl2GpioStore is not null) ? _hl2GpioStore.Get() : (byte)0;
        ActiveClient?.SetUserDigOut(mask);
    }

    /// <summary>Re-push the persisted HL2 GPIO mask after a fresh P1 connect, so a
    /// reconnect re-applies the operator's selection without a store edit.</summary>
    public void ReplayHl2Gpio() => PushHl2Gpio();

    /// <summary>The persisted HL2 user-GPIO mask (0..15), or 0 when no store.</summary>
    public byte GetHl2GpioMask() => _hl2GpioStore?.Get() ?? 0;

    /// <summary>Persist a new HL2 user-GPIO mask (low nibble). The store's Changed
    /// event fans out to <see cref="PushHl2Gpio"/> so the live client updates on
    /// the next outgoing frame. No-op when no store is configured.</summary>
    public void SetHl2GpioMask(byte mask) => _hl2GpioStore?.Set((byte)(mask & 0x0F));

    /// <summary>
    /// HL2 Band Volts PWM enable (issue #279). Updates the persisted
    /// per-radio preference AND any live Protocol-1 client so the next
    /// outgoing Config frame carries the new bit. Honoured on HL2 only;
    /// non-HL2 boards never see this bit on the wire because Zeus' UI gate
    /// (<c>HasHl2OptionalToggles</c>) hides the control there. Returns the
    /// effective value (echoes the input — present for symmetry with other
    /// state setters that may sanitize).
    /// </summary>
    public bool SetHl2BandVolts(bool enabled)
    {
        // Write-through to persistent storage first so a crash between the
        // store write and the live-client push can't lose the preference.
        _preferredRadioStore?.SetEnableHl2BandVolts(enabled);
        // Then push to the live client (if any) so the bit lands on the wire
        // immediately. Safe on non-HL2 boards: SnapshotState's C3-bit-3
        // encoding fires regardless of board, but the gate at the UI /
        // capability level keeps non-HL2 operators from ever flipping it.
        if (_activeClient is not null)
        {
            _activeClient.EnableHl2BandVolts = enabled;
        }
        return enabled;
    }

    /// <summary>
    /// Reads the persisted HL2 Band Volts preference. Surfaced for the
    /// <c>/api/radio/hl2-options</c> GET endpoint. Returns <c>false</c> when
    /// no preferences store is wired (test factories) or no row exists yet.
    /// </summary>
    public bool GetHl2BandVolts() =>
        _preferredRadioStore?.GetEnableHl2BandVolts() ?? false;

    private bool SupportsG2AdcOptions(HpsdrBoardKind board, OrionMkIIVariant variant) =>
        BoardCapabilitiesTable.For(board, variant).SupportsG2AdcOptions;

    private (bool Supported, bool DitherEnabled, bool RandomEnabled, bool Rx1AttenuatorSupported, int Rx1AttenuatorDb)
        ResolveG2AdcOptionsFor(
        HpsdrBoardKind board,
        OrionMkIIVariant variant)
    {
        var caps = BoardCapabilitiesTable.For(board, variant);
        bool supported = caps.SupportsG2AdcOptions;
        bool dither = supported && (_preferredRadioStore?.GetG2AdcDitherEnabled() ?? true);
        bool random = supported && (_preferredRadioStore?.GetG2AdcRandomEnabled() ?? true);
        bool rx1AttenuatorSupported = supported && caps.HasSteppedAttenuationRx2;
        int rx1AttenuatorDb = rx1AttenuatorSupported
            ? Math.Clamp(_preferredRadioStore?.GetG2Rx1AttenuatorDb() ?? 0, 0, 31)
            : 0;
        return (supported, dither, random, rx1AttenuatorSupported, rx1AttenuatorDb);
    }

    public (bool Supported, bool DitherEnabled, bool RandomEnabled, bool Rx1AttenuatorSupported, int Rx1AttenuatorDb)
        ResolveG2AdcOptionsForWire(
        HpsdrBoardKind connectedBoard)
    {
        var board = connectedBoard != HpsdrBoardKind.Unknown
            ? connectedBoard
            : EffectiveBoardKind;
        return ResolveG2AdcOptionsFor(board, EffectiveOrionMkIIVariant);
    }

    public G2OptionsDto GetG2Options()
    {
        var options = ResolveG2AdcOptionsFor(EffectiveBoardKind, EffectiveOrionMkIIVariant);
        // Protocol-1 LT2208 boards expose the same ADC dither/random controls,
        // but the Thetis default is OFF (netInterface.c) and there is no RX1
        // stepped attenuator on this path. Surfaced only when a non-HL2 P1 board
        // is the live connection, so the shipped P2/G2 reporting is untouched.
        if (!options.Supported && ConnectedP1SupportsAdcDitherRandom)
        {
            var (p1Dither, p1Random) = ResolveP1AdcOptions();
            return new G2OptionsDto(
                DitherEnabled: p1Dither,
                RandomEnabled: p1Random,
                MaxRxFreqMHz: 60.0,
                Supported: true,
                Rx1AttenuatorDb: 0,
                Rx1AttenuatorMinDb: 0,
                Rx1AttenuatorMaxDb: 31,
                Rx1AttenuatorSupported: false);
        }
        return new G2OptionsDto(
            DitherEnabled: _preferredRadioStore?.GetG2AdcDitherEnabled() ?? true,
            RandomEnabled: _preferredRadioStore?.GetG2AdcRandomEnabled() ?? true,
            MaxRxFreqMHz: 60.0,
            Supported: options.Supported,
            Rx1AttenuatorDb: _preferredRadioStore?.GetG2Rx1AttenuatorDb() ?? 0,
            Rx1AttenuatorMinDb: 0,
            Rx1AttenuatorMaxDb: 31,
            Rx1AttenuatorSupported: options.Rx1AttenuatorSupported);
    }

    public G2OptionsDto SetG2Options(G2OptionsSetRequest req)
    {
        _preferredRadioStore?.SetG2AdcOptions(req.DitherEnabled, req.RandomEnabled, req.Rx1AttenuatorDb);
        var options = GetG2Options();
        // Push to whichever protocol is live. ActiveClient is non-null only for
        // Protocol 1; _p2Client only for Protocol 2 — so at most one of these
        // does real work, and each no-ops on the board kinds it does not apply
        // to (HL2 for P1; non-G2 for P2).
        ApplyG2AdcOptionsToP2Client(_p2Client, ConnectedBoardKind);
        ApplyAdcOptionsToP1Client(_activeClient, ConnectedBoardKind);
        return options;
    }

    public void ApplyG2AdcOptionsToP2Client(Protocol2Client? client, HpsdrBoardKind connectedBoard)
    {
        if (client is null) return;
        var options = ResolveG2AdcOptionsForWire(connectedBoard);
        client.SetAdcDitherRandom(options.DitherEnabled, options.RandomEnabled);
        int adc1Attenuation;
        lock (_sync)
        {
            // ADC1's standalone G2 preference owns byte 1442 unless the
            // primary receiver is explicitly sourced from ADC1. In that case
            // the front-panel S-ATT baseline/Auto offset owns the same hardware
            // attenuator and must remain authoritative across option replays.
            adc1Attenuation = ReceiverAdcSource(_state, 0) == 1
                ? Math.Clamp(_atten.ClampedDb + _attOffsetDb, 0, 31)
                : options.Rx1AttenuatorSupported ? options.Rx1AttenuatorDb : 0;
        }
        client.SetRx1Attenuator(adc1Attenuation);
    }

    /// <summary>
    /// True when a non-HL2 Protocol-1 board is the live connection. Protocol 1
    /// is the only protocol with a live <see cref="IProtocol1Client"/>
    /// (<see cref="ActiveClient"/>); HL2's AD9866 has no LT2208 dither/random
    /// (its C3 bit 3 is Band Volts), so it is excluded.
    /// </summary>
    private bool ConnectedP1SupportsAdcDitherRandom =>
        _activeClient is not null
        && ConnectedBoardKind != HpsdrBoardKind.HermesLite2;

    /// <summary>
    /// Resolve the persisted LT2208 ADC dither/random with the Thetis Protocol-1
    /// default of OFF (netInterface.c <c>adc[i].dither = adc[i].random = 0</c>).
    /// Uses the nullable raw store getters so an operator who has only ever set
    /// the P2/G2 controls does not inherit the P2 default-on here.
    /// </summary>
    private (bool DitherEnabled, bool RandomEnabled) ResolveP1AdcOptions()
    {
        bool dither = _preferredRadioStore?.GetG2AdcDitherEnabledRaw() ?? false;
        bool random = _preferredRadioStore?.GetG2AdcRandomEnabledRaw() ?? false;
        return (dither, random);
    }

    /// <summary>
    /// Push the persisted LT2208 ADC dither/random to a live Protocol-1 client.
    /// No-op on HL2 (no LT2208) and when no client is connected. The bits ride
    /// the next periodic Config frame — the Config register is on the TX-tick
    /// round-robin, so no explicit send is required. Mirrors
    /// <see cref="ApplyG2AdcOptionsToP2Client"/> for the P1 path.
    /// </summary>
    public void ApplyAdcOptionsToP1Client(IProtocol1Client? client, HpsdrBoardKind connectedBoard)
    {
        if (client is null || connectedBoard == HpsdrBoardKind.HermesLite2) return;
        var (dither, random) = ResolveP1AdcOptions();
        client.SetAdcDitherRandom(dither, random);
    }

    /// <summary>
    /// Raised after the operator's frequency-correction factor (issue #325)
    /// has been persisted and pushed to the active P1 client. P2 subscribers
    /// (<c>DspPipelineService</c>) use this to forward the new factor to the
    /// live <see cref="Zeus.Protocol2.Protocol2Client"/>, since
    /// <see cref="ActiveClient"/> is always null in P2 mode.
    /// </summary>
    public event Action<double>? FrequencyCorrectionFactorChanged;

    /// <summary>
    /// Reads the per-radio frequency-correction factor (issue #325). 1.0
    /// when no store is wired or no calibration has been run.
    /// </summary>
    public double GetFrequencyCorrectionFactor() =>
        _preferredRadioStore?.GetFrequencyCorrectionFactor() ?? 1.0;

    /// <summary>
    /// Persists the frequency-correction factor, pushes it to the live P1
    /// client (if any), raises <see cref="FrequencyCorrectionFactorChanged"/>
    /// for the P2 listener, and re-pushes the current dial VFO so the new
    /// factor reaches the wire immediately. Clamps to ±100 ppm
    /// (factor ∈ [0.9999, 1.0001]) — matches piHPSDR's range and is far
    /// wider than any crystal-stabilised HPSDR board needs.
    /// </summary>
    public double SetFrequencyCorrectionFactor(double factor)
    {
        // Clock calibration changes the NCO even though no dial/mode state
        // changes. Serialize the entire write-through with fixed-channel
        // admission, and refuse before touching storage or either protocol.
        lock (_productPluginFrequencyConfigSync)
        {
            if (Interlocked.Read(ref _productPluginNativeFrequencyHz) > 0)
                throw new TransmitSafetyRejectedException(
                    "Frequency calibration cannot change during a fixed-channel audio transmission.");
            return SetFrequencyCorrectionFactorCore(factor);
        }
    }

    private double SetFrequencyCorrectionFactorCore(double factor)
    {
        if (double.IsNaN(factor) || double.IsInfinity(factor))
            throw new ArgumentException("factor must be a finite real number", nameof(factor));
        double clamped = Math.Clamp(factor, 0.9999, 1.0001);

        // Write-through to persistent storage first so a crash between the
        // store write and the live-client push can't lose the calibration.
        _preferredRadioStore?.SetFrequencyCorrectionFactor(clamped);
        _activeClient?.SetFrequencyCorrectionFactor(clamped);
        FrequencyCorrectionFactorChanged?.Invoke(clamped);

        // Re-push the current hardware LO so the new factor lands on the wire.
        // Re-pushing the LO rather than re-tuning the dial matters in two
        // states where a SetVfo(dial) round-trip never reaches the radio: under
        // CTUN it only mutates VfoHz and leaves the frozen NCO alone, and under
        // VFO lock it returns early — either way the fresh factor would sit
        // unused until the operator's next tune. SetRadioLoUnchecked re-writes
        // the same LO through ToHardwareFrequencyHz, so the corrected value
        // ships immediately without disturbing dial, CTUN centre, or lock. The
        // FrequencyCorrectionFactorChanged event above covers the P2 client.
        var pushSnap = Snapshot();
        if (pushSnap.RadioLoHz > 0) SetRadioLoUnchecked(pushSnap.RadioLoHz);
        else SetVfo(pushSnap.VfoHz, fromExternal: true);

        return clamped;
    }

    // Compute the current drive byte + OC masks + PA enable from _drivePct,
    // PaSettingsStore, and the current VFO band. Push to the active P1 client
    // and fire PaSnapshotChanged for the P2 forwarder. Called on:
    //   - SetDrive (slider moved)
    //   - SetVfo when the band changes
    //   - PaSettingsStore.Changed (user edited PA Settings)
    //   - Connected (push current snapshot to fresh client)
    private void RecomputePaAndPush() => RecomputePaAndPushCore(safetyInhibit: false);

    public StateDto SetDriveMaximum(int percent)
    {
        int maximum = Math.Clamp(percent, 1, 100);
        Mutate(s =>
        {
            ThrowIfPaCalibrationInvariantMutation("Drive maximum");
            int drive = Math.Min(s.DrivePct, maximum);
            int tune = Math.Min(s.TunePct, maximum);
            Interlocked.Exchange(ref _drivePct, drive);
            Interlocked.Exchange(ref _tunePct, tune);
            return s with { DrivePct = drive, DriveMaxPct = maximum, TunePct = tune };
        });
        lock (_productPluginDriveCapGate)
        {
            int productCap = Volatile.Read(ref _productPluginDriveCapPct);
            if (productCap > maximum)
                Interlocked.Exchange(ref _productPluginDriveCapPct, maximum);
        }
        // Reduce live RF before touching storage, then persist this infrequent
        // hardware-protection setting before acknowledging the change so an
        // abrupt exit cannot restore an older, higher ceiling on next launch.
        RecomputePaAndPush();
        FlushState();
        return Snapshot();
    }

    private void RecomputePaAndPushCore(bool safetyInhibit)
    {
        var stateSnap = Snapshot();
        // PA config uses the effective board so the operator can pre-stage
        // PA Settings for a radio not yet connected; once a radio IS on the
        // wire, EffectiveBoardKind == ConnectedBoardKind (discovery wins).
        var cfg = _paStore.GetAll(EffectiveBoardKind, EffectiveOrionMkIIVariant);
        var txHz = RadioFrequencyResolver.TxFrequencyHz(stateSnap);
        bool txThroughTransverter = TryResolveTransverterBand(txHz, out var txXvtrBand);
        bool rxThroughTransverter = TryResolveTransverterBand(stateSnap.VfoHz, out _);
        bool xvtrOutputEnabled = txThroughTransverter && txXvtrBand.DisablePa;
        long hardwareTxHz = ToHardwareFrequencyHz(txHz);
        var hardwareBandName = BandUtils.FreqToBand(hardwareTxHz);
        var paBandName = txThroughTransverter
            ? BandUtils.XvtrBandKey(txXvtrBand.Id)
            : hardwareBandName;
        var paBandCfg = paBandName is not null
            ? cfg.Bands.FirstOrDefault(b => b.Band == paBandName) ?? new PaBandSettingsDto(paBandName)
            : new PaBandSettingsDto("unknown");
        // Transverter PA calibration follows its stable profile slot. Relay
        // masks, RF filtering, and antenna routing remain tied to the
        // translated hardware IF band so existing safe routing is unchanged.
        var bandCfg = hardwareBandName is not null
            ? cfg.Bands.FirstOrDefault(b => b.Band == hardwareBandName) ?? new PaBandSettingsDto(hardwareBandName)
            : new PaBandSettingsDto("unknown");

        bool tunActive;
        lock (_sync) tunActive = _tunActive;
        int requestedPct = tunActive
            ? Volatile.Read(ref _tunePct)
            : Volatile.Read(ref _drivePct);
        // Belt-and-suspenders enforcement at the final TX-chain seam. Normal
        // setters and the PA-change callback already clamp state, but this
        // prevents any racing legacy/internal source from producing a drive
        // byte above the persisted amplifier ceiling.
        int activePct = Math.Min(requestedPct, Math.Clamp(stateSnap.DriveMaxPct, 1, 100));
        int productCap = Volatile.Read(ref _productPluginDriveCapPct);
        if (!tunActive && productCap >= 0)
            activePct = Math.Min(activePct, productCap);
        if (txThroughTransverter && txXvtrBand.MaxPowerDbm is null)
            activePct = Math.Min(activePct, txXvtrBand.Power);
        // Route through the per-board drive-profile so HL2's 4-bit drive
        // register is respected (bottom nibble ignored by gateware). See
        // Zeus.Server.RadioDriveProfile + docs/lessons/hl2-drive-byte-
        // quantization.md. Non-HL2 boards get the straight 8-bit math via
        // FullByteDriveProfile.
        var connectedBoard = ConnectedBoardKind;
        var variant = EffectiveOrionMkIIVariant;
        // A2/C3/D3: the engine module owns board/variant policy and today's
        // unchanged 0..100 domain. RadioService only executes its result.
        var decision = _txDriveSafetyEvaluator?.Invoke(activePct)
            ?? EngineTransmitSafetyModule.ResolveEffectiveDrive(activePct, connectedBoard, variant);
        bool safetyAuthorized = Volatile.Read(ref _txSafetyAuthority) != 0;
        var driveProfile = RadioDriveProfiles.For(connectedBoard);
        // Xvtr ratings are low-level RF drive specifications. At 100%, target
        // the selected profile's configured dBm rating rather than the radio
        // PA's full-scale wattage, preserving the entire slider range for the
        // Xvtr. Missing values retain the legacy PA-target behaviour.
        double maxPowerWatts = txThroughTransverter && txXvtrBand.MaxPowerDbm is double maxPowerDbm
            ? DriveByteMath.WattsFromDbm(maxPowerDbm)
            : cfg.Global.PaMaxPowerWatts;
        double calibratedGain = paBandName is null
            ? paBandCfg.PaGainDb
            : _paStore.ResolveCalibrationGain(
                paBandName,
                decision.EffectiveDrivePercent,
                cfg.Global.PaMaxPowerWatts,
                connectedBoard,
                variant,
                paBandCfg.PaGainDb);
        double driveGain = txThroughTransverter && txXvtrBand.MaxPowerDbm is not null
            ? driveProfile.ResolveXvtrGain(calibratedGain, maxPowerWatts, cfg.Global.PaMaxPowerWatts)
            : calibratedGain;
        byte driveByte = decision.Allowed && safetyAuthorized && !safetyInhibit
            ? driveProfile.EncodeDriveByte(
                decision.EffectiveDrivePercent,
                driveGain,
                maxPowerWatts)
            : (byte)0;
        bool paEnabled = decision.Allowed && safetyAuthorized && !safetyInhibit
            && cfg.Global.PaEnabled && !bandCfg.DisablePa
            && (!txThroughTransverter || !txXvtrBand.DisablePa);

        _log.LogInformation(
            "pa.recompute tunActive={Tun} requestedPct={RequestedPct} pct={Pct} driveMaxPct={DriveMaxPct} txVfo={TxVfo} txHz={TxHz} band={Band} gainDb={Gain:F2} maxW={Max} profile={Profile} -> byte={Byte} paEn={PaEn} ocTx=0x{OcTx:X2} ocRx=0x{OcRx:X2} ocTune=0x{OcTune:X2} ocDxTx=0x{OcDxTx:X2} ocDxRx=0x{OcDxRx:X2}",
            tunActive, requestedPct, activePct, stateSnap.DriveMaxPct, stateSnap.TxVfo, txHz, paBandName ?? "?", driveGain, maxPowerWatts, driveProfile.BoardLabel, driveByte, paEnabled,
            bandCfg.OcTx, bandCfg.OcRx, bandCfg.OcTune, bandCfg.OcDxTx, bandCfg.OcDxRx);

        ActiveClient?.SetDriveByte(driveByte);
        ActiveClient?.SetOcMasks(bandCfg.OcTx, bandCfg.OcRx, bandCfg.OcTune);
        ActiveClient?.SetPaEnabled(paEnabled);
        ActiveClient?.SetXvtrEnabled(xvtrOutputEnabled);

        // ---- External-antenna resolution (antenna slice — #804) ----
        // Server-authoritative: resolve the active HF band's or shared
        // transverter route's persisted TX/RX antenna + RX-aux, gate the aux against the connected board's
        // capability set (HL2's None collapses any stale value), and push.
        // P1 RX-antenna goes straight to the active client; P2 (TX antenna +
        // RX-aux state-mux) rides the PaRuntimeSnapshot into
        // DspPipelineService.SetAntennas. The wire layer clamps HL2 RX to ANT1
        // and defers any mid-key relay change to the unkey edge; PS owns the
        // K36/BYPASS relay while armed regardless of an aux=BYPASS pick.
        var caps = BoardCapabilitiesTable.For(ConnectedBoardKind, EffectiveOrionMkIIVariant);
        var defaultAntSel = new AntennaBandSelection(
            hardwareBandName ?? "unknown", HpsdrAntenna.Ant1, HpsdrAntenna.Ant1, RxAuxInputSel.None);
        var xvtrAntSel = _antennaStore?.GetBand(AntennaSettingsStore.XvtrBand) ?? defaultAntSel;
        var txAntSel = txThroughTransverter
            ? xvtrAntSel
            : (_antennaStore is not null && hardwareBandName is not null
                ? _antennaStore.GetBand(hardwareBandName)
                : defaultAntSel);
        long hardwareRxHz = ToHardwareFrequencyHz(stateSnap.VfoHz);
        string? rxBandName = BandUtils.FreqToBand(hardwareRxHz);
        var rxAntSel = rxThroughTransverter
            ? xvtrAntSel
            : (_antennaStore is not null && rxBandName is not null
                ? _antennaStore.GetBand(rxBandName)
                : new AntennaBandSelection(
                    rxBandName ?? "unknown", HpsdrAntenna.Ant1, HpsdrAntenna.Ant1, RxAuxInputSel.None));
        var antSel = txAntSel with
        {
            RxAnt = rxAntSel.RxAnt,
            RxAux = rxAntSel.RxAux,
        };
        int rxAuxWire = GateRxAux(antSel.RxAux, caps.RxAuxInputs);
        // P1: RX-antenna relay (C3[7:5], HL2-clamped at the wire). ActiveClient
        // is null on P2 — the P2 RX-antenna rides the SetAntennas path below.
        ActiveClient?.SetAntennaRx(antSel.RxAnt);
        ActiveClient?.SetRxAuxInput(rxAuxWire);
        // P1: TX-antenna relay (Config-frame C4[1:0]) — external-port parity audit
        // (GAP-P1-1). Clamped to ANT1 at the wire for boards without full Alex TX
        // relays (ControlFrame.EncodeTxAntennaC4Bits → only ANAN-100D/200D emit
        // it), and deferred to the unkey edge while keyed. P2 TX antenna rides the
        // alex0[26:24] path in the SetAntennas snapshot below.
        ActiveClient?.SetAntennaTx(antSel.TxAnt);

        PaSnapshotChanged?.Invoke(new PaRuntimeSnapshot(
            DriveByte: driveByte,
            OcTxMask: bandCfg.OcTx,
            OcRxMask: bandCfg.OcRx,
            OcTuneMask: bandCfg.OcTune,
            PaEnabled: paEnabled,
            // Anvelina-PRO3 DX OC masks (issue #407) — always emitted in
            // the snapshot so DspPipelineService can forward them to the
            // Protocol2Client. The wire-encode in SendCmdHighPriority is
            // gated by board+variant, so non-Anvelina radios receive a
            // SetOcDxMasks call but the bytes never reach the wire.
            OcDxTxMask: bandCfg.OcDxTx,
            OcDxRxMask: bandCfg.OcDxRx,
            // External antenna (#804). HasTxAntennaRelays gates the alex0[26:24]
            // emission on the P2 client; RxAuxInput/MkiiBpfRxSelect drive the
            // operator RX-aux ORs (composed strictly before the PS coupler).
            TxAntenna: antSel.TxAnt,
            RxAntenna: antSel.RxAnt,
            HasTxAntennaRelays: caps.HasTxAntennaRelays,
            RxAuxInput: rxAuxWire,
            MkiiBpfRxSelect: caps.MkiiBpf,
            RfFilters: _rfFilterStore?.GetRuntime(ConnectedBoardKind),
            XvtrEnabled: xvtrOutputEnabled));
    }

    // Gate a persisted per-band RX-aux pick against the connected board's
    // capability set (antenna slice — #804). Band rows are board-agnostic (no
    // board column), so a stale aux persisted on an ANAN must collapse to None
    // (base ANT relay) on a board that does not expose it — notably HL2, whose
    // RxAuxInputs is None. Returns the 1-based wire selector the P2 client uses
    // (0=None .. 4=BYPASS); the RxAuxInputSel byte already maps 1:1.
    private static int GateRxAux(RxAuxInputSel sel, RxAuxInputs available) => sel switch
    {
        RxAuxInputSel.Ext1   => available.HasFlag(RxAuxInputs.Ext1)   ? (int)sel : 0,
        RxAuxInputSel.Ext2   => available.HasFlag(RxAuxInputs.Ext2)   ? (int)sel : 0,
        RxAuxInputSel.Xvtr   => available.HasFlag(RxAuxInputs.Xvtr)   ? (int)sel : 0,
        RxAuxInputSel.Bypass => available.HasFlag(RxAuxInputs.Bypass) ? (int)sel : 0,
        _                    => 0,
    };

    // Back-compat shim for callers/tests that predate IRadioDriveProfile.
    // Runtime RecomputePaAndPush no longer goes through here — it uses the
    // per-board RadioDriveProfiles.For(board) dispatch so HL2's 4-bit drive
    // is quantised correctly. Keep this method as the 8-bit/full-byte math
    // for tests and anything else that wants the raw value.
    internal static byte ComputeDriveByte(int drivePct, double paGainDb, int maxWatts)
        => DriveByteMath.ComputeFullByte(drivePct, paGainDb, maxWatts);

    // "AGC Top" slider — max post-AGC gain in dB. In Fixed mode Thetis routes
    // this same front-panel RF control to RXFixedAGC (-20..120) while retaining
    // the normal max-gain baseline for the next non-Fixed mode. Keep those two
    // baselines separate by storing Fixed in AgcConfig.FixedGainDb.
    public StateDto SetAgcTop(double topDb)
    {
        bool fixedMode;
        lock (_sync) fixedMode = (_state.Agc?.Mode ?? AgcMode.Med) == AgcMode.Fixed;
        double clamped = Math.Clamp(
            topDb,
            fixedMode ? MinAgcFixedGainDb : MinAgcTopDb,
            MaxAgcTopDb);
        // Grabbing the AGC-T slider takes MANUAL control. The value pushed to
        // WDSP is the EFFECTIVE AGC-T = AgcTopDb + AgcOffsetDb, where the offset
        // is the Auto-AGC control-loop accumulator. If Auto-AGC kept running,
        // that offset would stack on the new baseline (a momentary "blast" the
        // instant you adjust) and the loop would then re-target away from the
        // slider ("sits too low/high vs where the slider is") — issue #733. So
        // disable Auto-AGC and zero its offset + recalibration window here, all
        // under _sync (Mutate invokes fn exactly once inside the lock). Net
        // effect: the effective AGC-T equals the slider EXACTLY. Auto-AGC is a
        // deliberate mode the operator re-enables from its own toggle.
        AgcConfig? fixedConfigToPersist = null;
        Mutate(s =>
        {
            _agcOffsetDb = 0.0;
            _agcServoSeatedTopDb = double.NaN;
            _lastAgcTickMs = long.MinValue;
            if ((s.Agc?.Mode ?? AgcMode.Med) == AgcMode.Fixed)
            {
                fixedConfigToPersist = NormalizeAgcConfig(
                    (s.Agc ?? new AgcConfig(AgcMode.Fixed)) with { FixedGainDb = clamped });
                return s with
                {
                    Agc = fixedConfigToPersist,
                    AgcOffsetDb = 0.0,
                    AutoAgcEnabled = false,
                };
            }
            return s with
            {
                AgcTopDb = clamped,
                AgcOffsetDb = 0.0,
                AutoAgcEnabled = false,
            };
        });
        // Persist only the active user baseline; the offset is live-recomputed.
        if (fixedConfigToPersist is not null)
            _dspSettingsStore.SetAgc(fixedConfigToPersist);
        else
            _dspSettingsStore.SetAgcTopDb(clamped);
        return Snapshot();
    }

    // (Removed: the manual AGC "knee" / threshold control. In WDSP the threshold
    // and AGC-T are the SAME register (max_gain); exposing both as independent
    // operator controls made them clobber each other and made AGC-T hair-trigger.
    // AGC-T (SetAgcTop) is now the single manual AGC control; Auto-AGC tracks the
    // noise floor on top of it.)

    // Master RX AF gain in dB. −50 dB is effectively silent (0.003 linear),
    // 0 dB matches the fresh-open default, +20 dB is a 10× linear boost for
    // quiet signals. Range mirrors Thetis's ptbAF (console.cs:4312-4313:
    // tbAF.Minimum = -50, Maximum = 20).
    public const double MinRxAfGainDb = -50.0;
    public const double MaxRxAfGainDb = 20.0;

    /// <summary>Coerces a persisted AF-gain field into the operator range.
    /// A missing, non-finite or out-of-range stored value must never reach the
    /// dB to linear conversion at the engine seam, and the fold below adds
    /// two of these together.</summary>
    private static double SanitizeAfGainDb(double? db) =>
        db is double v && double.IsFinite(v)
            ? Math.Clamp(v, MinRxAfGainDb, MaxRxAfGainDb)
            : 0.0;

    public StateDto SetRxAfGain(double db)
    {
        double clamped = Math.Clamp(db, MinRxAfGainDb, MaxRxAfGainDb);
        Mutate(s => s with { RxAfGainDb = clamped });
        return Snapshot();
    }

    /// <summary>Sets RX1's AF trim independently of the station-wide master.</summary>
    public StateDto SetRx1AfGain(double db)
    {
        double clamped = Math.Clamp(db, MinRxAfGainDb, MaxRxAfGainDb);
        Mutate(s => s with { Rx1AfGainDb = clamped });
        return Snapshot();
    }

    // TX mic gain in dB. Server-clamped to [-40, +10] to match the endpoint
    // contract and Thetis's MicGainMin/Max defaults. The dB → linear (10^(db/20))
    // conversion happens at the engine seam in DspPipelineService so the wire
    // and persisted form is the operator-friendly integer.
    public StateDto SetTxMicGain(int db)
    {
        int clamped = Math.Clamp(db, -40, 10);
        Mutate(s => s with { MicGainDb = clamped });
        return Snapshot();
    }

    // TX Leveler max-gain ceiling in dB. Server-clamped to [0, 20] for Thetis
    // parity (radio.cs leveler top range 0..20); previously 0..15.
    public StateDto SetTxLevelerMaxGain(double db)
    {
        double clamped = Math.Clamp(db, 0.0, 20.0);
        Mutate(s => s with { LevelerMaxGainDb = clamped });
        return Snapshot();
    }

    public StateDto SetNr(NrConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        var normalized = NormalizeNrConfig(cfg);
        Mutate(s => s with { Nr = normalized });

        // Persist the new DSP settings to the store
        _dspSettingsStore.Upsert(normalized);

        return Snapshot();
    }

    // ---- NR3 (RNNoise) model management ----
    // The operator installs an RNNoise weights file (Zeus ships none). The model
    // store persists the file to disk and raises Changed, which the DSP pipeline
    // observes to (re)load it into libwdsp (process-global RNNRloadModel). We
    // mirror the active model name into StateDto so the UI can reveal NR3 once a
    // model is present.
    public StateDto InstallNr3Model(byte[] content, string fileName)
    {
        ArgumentNullException.ThrowIfNull(content);
        if (_nr3ModelStore is null)
            throw new InvalidOperationException("NR3 model store is not configured.");
        // Install writes the file and synchronously fires Changed, which the DSP
        // pipeline observes to (re)load it into a live engine. After that returns,
        // the store carries the load outcome — reject a model the native RNNoise
        // loader couldn't parse instead of silently leaving NR3 inert. When no
        // engine is live the result is null (unverified) and we accept it; the
        // load is re-attempted on the next connect.
        _nr3ModelStore.Install(content, fileName);
        if (_nr3ModelStore.LastLoadResult == Zeus.Dsp.Nr3ModelLoadResult.LoadFailed)
        {
            _nr3ModelStore.Remove(); // drop the bad file — reverts to the bundled default
            throw new ArgumentException(
                "That file isn't a compatible RNNoise model — it failed to load. " +
                "Use an RNNoise weights file (DNNw format) matching the bundled model's architecture.");
        }
        var name = _nr3ModelStore.GetActiveModelName();
        Mutate(s => s with { Nr3ModelName = name, Nr3UsingBundledDefault = false });
        return Snapshot();
    }

    public StateDto RemoveNr3Model()
    {
        if (_nr3ModelStore is null || !_nr3ModelStore.Remove())
            return Snapshot();
        // Removing the operator model reverts to the bundled default (if shipped),
        // not to inert. Mirror the now-active model (default name, or null when no
        // default exists) into StateDto.
        Mutate(s => s with
        {
            Nr3ModelName = _nr3ModelStore.GetActiveModelName(),
            Nr3UsingBundledDefault = _nr3ModelStore.UsingBundledDefault(),
        });
        // Only strand-proof the NR mode when NO model remains active (no bundled
        // default). With a default still active, NR3 stays valid — leave it be.
        if (!_nr3ModelStore.UsingBundledDefault())
        {
            var cur = Snapshot().Nr;
            if (cur?.NrMode == NrMode.Rnnr)
                return SetNr(cur with { NrMode = NrMode.Off });
        }
        return Snapshot();
    }

    private static NrConfig NormalizeNrConfig(NrConfig cfg) =>
        IsSupportedNrMode(cfg.NrMode) ? cfg : cfg with { NrMode = NrMode.Off };

    private static bool IsSupportedNrMode(NrMode mode) =>
        mode is NrMode.Off or NrMode.Anr or NrMode.Emnr or NrMode.Sbnr or NrMode.Rnnr or NrMode.Nnr;

    // AGC mode + custom/fixed params. Replace-style like SetNr; the engine apply
    // happens in DspPipelineService via the _appliedAgc latch. The separate AGC
    // max-gain path (SetAgcTop) is untouched.
    public StateDto SetAgc(AgcConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        var normalized = NormalizeAgcConfig(cfg);
        Mutate(s =>
        {
            var current = s.Agc ?? new AgcConfig(AgcMode.Med);
            bool servoConfigChanged = current.Mode != normalized.Mode
                || AutoAgcEffectiveSlopeDb(current) != AutoAgcEffectiveSlopeDb(normalized)
                || AutoAgcEffectiveFixedGainDb(current) != AutoAgcEffectiveFixedGainDb(normalized);
            if (servoConfigChanged)
            {
                // Only parameters used by the servo math invalidate its seat.
                // Decay/hang edits and redundant config writes must preserve
                // hysteresis memory and the 500 ms rate gate.
                _agcServoSeatedTopDb = double.NaN;
                _lastAgcTickMs = long.MinValue;
            }
            return s with { Agc = normalized };
        });
        _dspSettingsStore.SetAgc(normalized);
        return Snapshot();
    }

    // Match the servo's mode gates and null defaults when deciding whether an
    // AGC edit changes its math. Inactive fields are deliberately canonical 0.
    private static int AutoAgcEffectiveSlopeDb(AgcConfig cfg) =>
        cfg.Mode == AgcMode.Custom
            ? Math.Clamp(cfg.Slope ?? 0, 0, 20)
            : 0;

    private static double AutoAgcEffectiveFixedGainDb(AgcConfig cfg) =>
        cfg.Mode == AgcMode.Fixed
            ? Math.Clamp(cfg.FixedGainDb ?? 20.0, MinAgcFixedGainDb, MaxAgcTopDb)
            : 0.0;

    internal static AgcConfig NormalizeAgcConfig(AgcConfig cfg) => cfg with
    {
        Slope = cfg.Slope is int slope ? Math.Clamp(slope, 0, 20) : null,
        DecayMs = cfg.DecayMs is int decay ? Math.Clamp(decay, 1, 5_000) : null,
        HangMs = cfg.HangMs is int hang ? Math.Clamp(hang, 1, 5_000) : null,
        HangThreshold = cfg.HangThreshold is int threshold
            ? Math.Clamp(threshold, 0, 100)
            : null,
        FixedGainDb = cfg.FixedGainDb is double fixedGain
            ? Math.Clamp(fixedGain, MinAgcFixedGainDb, MaxAgcTopDb)
            : null,
    };

    internal static double AgcBaseline(StateDto state) =>
        state.Agc?.Mode == AgcMode.Fixed
            ? Math.Clamp(state.Agc.FixedGainDb ?? 20.0, MinAgcFixedGainDb, MaxAgcTopDb)
            : Math.Clamp(state.AgcTopDb, MinAgcTopDb, MaxAgcTopDb);

    // RX squelch (mode-aware single control). Replace-style like SetAgc; the
    // engine apply happens in DspPipelineService via the _appliedSquelch latch.
    // Level and fixed-mode sensitivity are clamped to 0..100 here so a
    // persisted/echoed value is always sane. Adaptive defaults in
    // SquelchConfig keep older clients dynamic.
    public StateDto SetSquelch(SquelchConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        var clamped = cfg with
        {
            Level = Math.Clamp(cfg.Level, 0, 100),
            FixedSensitivity = Math.Clamp(
                cfg.FixedSensitivity,
                SquelchConfig.MinFixedSensitivity,
                SquelchConfig.MaxFixedSensitivity),
        };
        Mutate(s => s with { Squelch = clamped });
        _dspSettingsStore.SetSquelch(clamped);
        return Snapshot();
    }

    // TX leveling — ALC (max-gain/decay), Leveler (on/off/decay), Compressor
    // (on/off/gain), and CESSB (on/off + 3/4 kHz bandwidth). Replace-style like
    // SetSquelch; the engine apply happens in DspPipelineService via the
    // _appliedTxLeveling latch. All ranges are
    // clamped here so a persisted/echoed value is always sane (Thetis parity:
    // AlcMaxGainDb 0..120, AlcDecayMs 1..50, LevelerDecayMs 1..5000,
    // CompressorGainDb 0..20). The Leveler max-gain stays on the separate
    // SetTxLevelerMaxGain path and is never duplicated here.
    public StateDto SetTxLeveling(TxLevelingConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        var clamped = cfg with
        {
            AlcMaxGainDb = Math.Clamp(cfg.AlcMaxGainDb, 0.0, 120.0),
            AlcDecayMs = Math.Clamp(cfg.AlcDecayMs, 1, 50),
            LevelerDecayMs = Math.Clamp(cfg.LevelerDecayMs, 1, 5000),
            CompressorGainDb = Math.Clamp(cfg.CompressorGainDb, 0.0, 20.0),
            CessbBandwidthHz = cfg.CessbBandwidthHz is 4000 ? 4000 : 3000,
        };
        Mutate(s => s with { TxLeveling = clamped });
        _dspSettingsStore.SetTxLeveling(clamped);
        return Snapshot();
    }

    // TX phase rotator — WDSP all-pass phase redistribution plus explicit
    // microphone polarity reverse. Replace-style like SetTxLeveling; the DSP
    // apply happens in DspPipelineService so rapid Auto Tune edits are live and
    // the final state is persisted as the locked-in optimized value.
    public StateDto SetTxPhaseRotator(TxPhaseRotatorConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        var clamped = NormalizeTxPhaseRotator(cfg);
        Mutate(s => s with { TxPhaseRotator = clamped });
        _dspSettingsStore.SetTxPhaseRotator(clamped);
        return Snapshot();
    }

    private static TxPhaseRotatorConfig NormalizeTxPhaseRotator(TxPhaseRotatorConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        return cfg with
        {
            CornerHz = Math.Clamp(
                cfg.CornerHz,
                TxPhaseRotatorConfig.MinCornerHz,
                TxPhaseRotatorConfig.MaxCornerHz),
            Stages = Math.Clamp(
                cfg.Stages,
                TxPhaseRotatorConfig.MinStages,
                TxPhaseRotatorConfig.MaxStages),
        };
    }

    // RX/TX bandpass resolution — both selectors extend beyond the Thetis tap
    // ladder through Zeus WDSP 2.0's 262144 planning ceiling. The pipeline
    // applies the enum live and the store preserves it across restarts.
    public StateDto SetRxBandpassWindow(BandpassWindow window)
    {
        Mutate(s => s with { RxFilterWindow = window });
        _dspSettingsStore.SetRxFilterWindow(window);
        return Snapshot();
    }

    public StateDto SetTxBandpassWindow(BandpassWindow window)
    {
        Mutate(s => s with { TxFilterWindow = window });
        _dspSettingsStore.SetTxFilterWindow(window);
        return Snapshot();
    }

    public StateDto SetRxFilterPhase(FilterPhaseMode phase)
    {
        Mutate(s => s with { RxFilterPhase = phase });
        _dspSettingsStore.SetRxFilterPhase(phase);
        return Snapshot();
    }

    public StateDto SetTxFilterPhase(FilterPhaseMode phase)
    {
        Mutate(s => s with { TxFilterPhase = phase });
        _dspSettingsStore.SetTxFilterPhase(phase);
        return Snapshot();
    }

    // Replace the full manual-notch set. The client posts the whole list on
    // every change (and on connect), so there's nothing to merge — store it and
    // raise NotchesChanged for DspPipelineService to push to the engine. Notch
    // centre/width are validated as finite, positive-width, and clamped to a
    // sane count so a malformed client can't flood the WDSP notch database.
    public void SetNotches(IReadOnlyList<NotchDto> notches)
    {
        ArgumentNullException.ThrowIfNull(notches);
        var cleaned = new List<NotchDto>(Math.Min(notches.Count, MaxNotches));
        foreach (var n in notches)
        {
            if (!double.IsFinite(n.CenterHz) || !double.IsFinite(n.WidthHz)) continue;
            if (n.WidthHz < MinNotchWidthHz || n.WidthHz > MaxNotchWidthHz) continue;
            if (n.CenterHz <= 0) continue;
            cleaned.Add(new NotchDto(n.CenterHz, n.WidthHz, n.Active, NormalizeNotchSource(n.Source)));
            if (cleaned.Count >= MaxNotches) break;
        }

        IReadOnlyList<NotchDto> snapshot;
        lock (_sync)
        {
            _notches = cleaned;
            snapshot = cleaned.ToArray();
        }
        _stateDirty = true;
        FlushState();
        NotchesChanged?.Invoke(snapshot);
    }

    // WDSP's notch database is bounded; keep well under it and reject absurd
    // widths so the panadapter paint gesture can't push garbage into the DSP.
    private const int MaxNotches = 64;
    private const double MinNotchWidthHz = 1.0;
    private const double MaxNotchWidthHz = 50_000.0;

    private static string? NormalizeNotchSource(string? source) =>
        string.Equals(source, "auto", StringComparison.OrdinalIgnoreCase) ? "auto" : null;

    // Right-click popover save for NR2 (EMNR) post2 tunables. Merges only
    // the non-null fields onto the current NrConfig so the operator can edit
    // a single knob without disturbing siblings, then re-pushes the whole
    // block through SetNr to keep persistence and engine state in lock-step.
    public StateDto SetNr2Post2(Nr2Post2ConfigSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        var current = Snapshot().Nr ?? new NrConfig();
        var merged = current with
        {
            EmnrPost2Run = req.Post2Run ?? current.EmnrPost2Run,
            EmnrPost2Factor = req.Post2Factor ?? current.EmnrPost2Factor,
            EmnrPost2Nlevel = req.Post2Nlevel ?? current.EmnrPost2Nlevel,
            EmnrPost2Rate = req.Post2Rate ?? current.EmnrPost2Rate,
            EmnrPost2Taper = req.Post2Taper ?? current.EmnrPost2Taper,
        };
        return SetNr(merged);
    }

    // NR2 (EMNR) core algorithm selectors + Trained-method T1/T2. Same
    // null-merge pattern as SetNr2Post2: each absent field leaves the
    // persisted value untouched. Range-checks the enum-shaped fields so
    // an out-of-range value can't push WDSP into an undefined branch.
    public StateDto SetNr2Core(Nr2CoreConfigSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        if (req.GainMethod is int gm && (gm < 0 || gm > 3))
            throw new ArgumentException($"GainMethod must be 0..3, got {gm}", nameof(req));
        if (req.NpeMethod is int npm && (npm < 0 || npm > 2))
            throw new ArgumentException($"NpeMethod must be 0..2, got {npm}", nameof(req));

        var current = Snapshot().Nr ?? new NrConfig();
        var merged = current with
        {
            EmnrGainMethod = req.GainMethod ?? current.EmnrGainMethod,
            EmnrNpeMethod = req.NpeMethod ?? current.EmnrNpeMethod,
            EmnrAeRun = req.AeRun ?? current.EmnrAeRun,
            EmnrTrainT1 = req.TrainT1 ?? current.EmnrTrainT1,
            EmnrTrainT2 = req.TrainT2 ?? current.EmnrTrainT2,
        };
        return SetNr(merged);
    }

    // Right-click popover save for NR4 (SBNR) tunables — same merge-and-
    // re-push pattern as SetNr2Post2.
    public StateDto SetNr4(Nr4ConfigSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        var current = Snapshot().Nr ?? new NrConfig();
        var merged = current with
        {
            Nr4ReductionAmount = req.ReductionAmount ?? current.Nr4ReductionAmount,
            Nr4SmoothingFactor = req.SmoothingFactor ?? current.Nr4SmoothingFactor,
            Nr4WhiteningFactor = req.WhiteningFactor ?? current.Nr4WhiteningFactor,
            Nr4NoiseRescale = req.NoiseRescale ?? current.Nr4NoiseRescale,
            Nr4PostFilterThreshold = req.PostFilterThreshold ?? current.Nr4PostFilterThreshold,
            Nr4NoiseScalingType = req.NoiseScalingType ?? current.Nr4NoiseScalingType,
            Nr4Position = req.Position ?? current.Nr4Position,
        };
        return SetNr(merged);
    }

    // CFC (Continuous Frequency Compressor) — issue #123. The whole 10-band
    // config travels in one POST because the operator edits the panel as a
    // single table; the engine then re-pushes the whole profile to WDSP.
    // Mirrors the SetNr shape: validate, mutate state, persist, return
    // snapshot. DspPipelineService picks up the change-detect on the next
    // OnRadioStateChanged tick and pushes through to the engine.
    public StateDto SetCfc(CfcSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        var cfg = req.Config ?? throw new ArgumentException("Config required", nameof(req));
        if (cfg.Bands is null || cfg.Bands.Length != 10)
            throw new ArgumentException($"Bands must have exactly 10 entries; got {cfg.Bands?.Length ?? 0}", nameof(req));

        Mutate(s => s with { Cfc = cfg });
        _dspSettingsStore.Upsert(cfg);
        _log.LogInformation(
            "radio.setCfc enabled={Enabled} peq={Peq} preComp={Pre:F1}dB prePeq={PrePeq:F1}dB",
            cfg.Enabled, cfg.PostEqEnabled, cfg.PreCompDb, cfg.PrePeqDb);
        return Snapshot();
    }

    /* ---- TX Audio Suite: equalizers and the TX noise gate ----------
     *
     * Same shape as SetCfc above: mutate the StateDto, persist, and let
     * DspPipelineService.OnRadioStateChanged carry it to WDSP. Each stage
     * is independent and each is optional — nothing here turns anything on
     * that the operator did not ask for.
     */

    public StateDto SetTxEq(GraphicEqConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!cfg.IsWellFormed)
            throw new ArgumentException("EQ config out of range", nameof(cfg));
        Mutate(s => s with { TxEq = cfg });
        _dspSettingsStore.UpsertEq(cfg, transmit: true);
        _log.LogInformation(
            "radio.setTxEq enabled={On} preamp={Preamp}dB", cfg.Enabled, cfg.PreampDb);
        return Snapshot();
    }

    public StateDto SetRxEq(GraphicEqConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!cfg.IsWellFormed)
            throw new ArgumentException("EQ config out of range", nameof(cfg));
        Mutate(s => s with { RxEq = cfg });
        _dspSettingsStore.UpsertEq(cfg, transmit: false);
        _log.LogInformation(
            "radio.setRxEq enabled={On} preamp={Preamp}dB", cfg.Enabled, cfg.PreampDb);
        return Snapshot();
    }

    public StateDto SetTxGate(TxGateConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!cfg.IsWellFormed)
            throw new ArgumentException("gate config out of range", nameof(cfg));
        Mutate(s => s with { TxGate = cfg });
        _dspSettingsStore.Upsert(cfg);
        _log.LogInformation(
            "radio.setTxGate enabled={On} thresh={Thresh:F1}dB",
            cfg.Enabled, cfg.ThresholdDb);
        return Snapshot();
    }

    public StateDto SetTxDexp(TxDexpConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!cfg.IsWellFormed)
            throw new ArgumentException("expander config out of range", nameof(cfg));
        Mutate(s => s with { TxDexp = cfg });
        _dspSettingsStore.Upsert(cfg);
        _log.LogInformation(
            "radio.setTxDexp enabled={On} thresh={Thresh:F1}dB ratio={Ratio:F1}dB",
            cfg.Enabled, cfg.ThresholdDb, cfg.ExpansionRatioDb);
        return Snapshot();
    }

    public StateDto SetTxEqParametric(ParametricEqConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!cfg.IsWellFormed) throw new ArgumentException("parametric EQ out of range", nameof(cfg));
        Mutate(s => s with { TxEqParametric = cfg });
        _dspSettingsStore.UpsertParametricEq(cfg, transmit: true);
        _log.LogInformation("radio.setTxEqParametric enabled={On} points={N}", cfg.Enabled, cfg.Points.Length);
        return Snapshot();
    }

    public StateDto SetRxEqParametric(ParametricEqConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!cfg.IsWellFormed) throw new ArgumentException("parametric EQ out of range", nameof(cfg));
        Mutate(s => s with { RxEqParametric = cfg });
        _dspSettingsStore.UpsertParametricEq(cfg, transmit: false);
        _log.LogInformation("radio.setRxEqParametric enabled={On} points={N}", cfg.Enabled, cfg.Points.Length);
        return Snapshot();
    }

    public StateDto SetCfcParametric(ParametricCfcConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!cfg.IsWellFormed)
            throw new ArgumentException("parametric CFC out of range, or its two curves differ in length", nameof(cfg));
        Mutate(s => s with { CfcParametric = cfg });
        _dspSettingsStore.Upsert(cfg);
        _log.LogInformation(
            "radio.setCfcParametric enabled={On} peq={Peq} points={N}",
            cfg.Enabled, cfg.PostEqEnabled, cfg.Compression.Points.Length);
        return Snapshot();
    }

    // ---------------- PureSignal ----------------
    // SetPs flips master arm and cal-mode in a single mutate so the engine
    // sees a consistent state when DspPipelineService.OnRadioStateChanged
    // fires.
    public StateDto SetPs(PsControlSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        bool psDisarmed = false;
        Mutate(s =>
        {
            if (req.Enabled)
                ThrowIfPaCalibrationInvariantMutation("PureSignal");
            // This is the sole writer of persisted arm intent. Runtime safety
            // disarms (watchdog, disconnect, sanitize) use Mutate directly and
            // therefore cannot erase the operator's decision.
            _psArmIntent = req.Enabled;
            psDisarmed = s.PsEnabled && !req.Enabled;
            return s with
            {
                PsEnabled = req.Enabled,
                PsAuto = req.Auto,
                PsSingle = req.Single,
            };
        });
        // Persist the explicit operator arm decision plus calibration/tuning.
        // Runtime PsEnabled remains separate and always starts false in a new
        // process until the connect-time sanitize applies ArmIntent.
        PersistPsState();
        if (psDisarmed) ApplyBoardKindToActiveClientIfSafe();
        return Snapshot();
    }

    /// <summary>
    /// Clear only the live PureSignal arm after a transport/provider safety
    /// failure. The persisted operator decision deliberately survives, matching
    /// the existing feedback-watchdog and disconnect safety semantics.
    /// </summary>
    internal StateDto FailClosedPureSignalRuntime()
    {
        Mutate(s => s.PsEnabled ? s with { PsEnabled = false } : s);
        return Snapshot();
    }

    /// <summary>
    /// Disarm PureSignal for PA calibration without changing the operator's
    /// persisted arm intent. The unconditional state broadcast also re-drives
    /// the hardware transition if runtime state was already fail-closed.
    /// </summary>
    internal StateDto DisarmPureSignalForPaCalibration()
    {
        Mutate(s => s with { PsEnabled = false });
        return Snapshot();
    }

    public StateDto SetPsAdvanced(PsAdvancedSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        Mutate(s => s with
        {
            PsAutoAttenuate = req.AutoAttenuate ?? s.PsAutoAttenuate,
            PsMoxDelaySec = req.MoxDelaySec is double moxDelaySec
                ? PsTimingLimits.ClampMoxDelaySec(moxDelaySec)
                : s.PsMoxDelaySec,
            PsLoopDelaySec = req.LoopDelaySec is double loopDelaySec
                ? PsTimingLimits.ClampLoopDelaySec(loopDelaySec)
                : s.PsLoopDelaySec,
            PsAmpDelayNs = req.AmpDelayNs is double ampDelayNs
                ? PsTimingLimits.ClampAmpDelayNs(ampDelayNs)
                : s.PsAmpDelayNs,
            PsHwPeak = req.HwPeak ?? s.PsHwPeak,
        });
        // If the PS MOX hold-off just dropped, shrink the pre-key window so the
        // pre-key < PS-hold-off invariant holds regardless of setter ordering.
        ReclampPreKeyToPs();
        PersistPsState();
        return Snapshot();
    }

    /// <summary>
    /// Choose Internal vs External feedback antenna for PureSignal.
    /// Mutates StateDto; DspPipelineService.OnRadioStateChanged forwards
    /// the bool into the active Protocol2Client where it flips one alex0
    /// bit on the next CmdHighPriority. WDSP cal/iqc are unaffected — the
    /// HW-Peak slider stays shared across sources (matches pihpsdr/Thetis).
    /// </summary>
    public StateDto SetPsFeedbackSource(PsFeedbackSourceSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        Mutate(s => s with { PsFeedbackSource = req.Source });
        PersistPsState();
        return Snapshot();
    }

    /// <summary>
    /// Toggle the "Monitor PA output" view (issue #121). When on, AND PS is
    /// armed, AND PS has converged, DspPipelineService.Tick reads pixels
    /// from the PS-feedback analyzer instead of the post-CFIR TX analyzer
    /// so the operator sees the actual on-air RF rather than the
    /// predistorted baseband. Operator viewing preference — NOT persisted
    /// across sessions.
    /// </summary>
    public StateDto SetPsMonitor(PsMonitorSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        _log.LogInformation("setPsMonitor enabled={Enabled}", req.Enabled);
        Mutate(s => s with { PsMonitorEnabled = req.Enabled });
        return Snapshot();
    }

    /// <summary>TX-monitor toggle (preview path). Mutates StateDto so the
    /// next DspPipelineService.UpdateState tick latches the value into
    /// engine.SetTxMonitorEnabled. Mirrors PsMonitor's lifecycle — operator
    /// preference, not persisted across sessions; resets to off on each new
    /// connect so the radio doesn't come up previewing unintentionally.</summary>
    public StateDto SetTxMonitor(TxMonitorSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        _log.LogInformation("setTxMonitor enabled={Enabled}", req.Enabled);
        Mutate(s => s with { TxMonitorEnabled = req.Enabled });
        return Snapshot();
    }

    public StateDto SetTwoTone(TwoToneSetRequest req)
    {
        ArgumentNullException.ThrowIfNull(req);
        Mutate(s => s with
        {
            TwoToneEnabled = req.Enabled,
            TwoToneFreq1 = req.Freq1 ?? s.TwoToneFreq1,
            TwoToneFreq2 = req.Freq2 ?? s.TwoToneFreq2,
            TwoToneMag = req.Mag ?? s.TwoToneMag,
        });
        // Persist freq1/freq2/mag — operator tunings survive restart.
        // TwoToneEnabled (master arm) is NOT persisted; same operator-action
        // discipline as MOX/TUN.
        PersistPsState();
        return Snapshot();
    }

    // Safety rollback/trip seam: stop PostGen without rewriting the operator's
    // persisted tone frequencies/magnitude or touching any PureSignal state.
    internal void SetTwoToneRuntimeEnabled(bool enabled) =>
        Mutate(s => s.TwoToneEnabled == enabled ? s : s with { TwoToneEnabled = enabled });

    // Update the live state to track PS read-back from the engine. Called by
    // TxMetersService at 10 Hz while PS is armed.
    public void UpdatePsLiveReadout(double feedbackLevel, byte calState, bool correcting)
    {
        Mutate(s => s with
        {
            PsFeedbackLevel = feedbackLevel,
            PsCalState = calState,
            PsCorrecting = correcting,
        });
    }

    // Surface calcc-stall state to the frontend. PsAutoAttenuateService raises
    // this when info5 stays at 0 for >5s while keyed; the frontend renders a
    // banner pointing operator at HW peak. No-ops if the flag isn't changing.
    public void SetPsCalibrationStalled(bool stalled)
    {
        if (Snapshot().PsCalibrationStalled == stalled) return;
        Mutate(s => s with { PsCalibrationStalled = stalled });
    }

    /// <summary>
    /// Resolves the operator-correct PS hardware-peak default for the given
    /// protocol + board kind. Sources:
    ///   - P1 Hermes / ANAN-10/100/100D/200D / Hermes-II / 10E / 100B → 0.4072
    ///   - P2 OrionMkII (G2 / Saturn) → 0.6121
    ///   - P2 ANAN-7000 / 8000 (default P2) → 0.2899
    ///   - HermesLite2 (either protocol) → 0.233 (MI0BOT special, but only if
    ///     someone connects an HL2 — the reference Hermes is original, not HL2)
    /// Source authority: Thetis clsHardwareSpecific.cs:295-318 +
    /// pihpsdr transmitter.c:1166-1179 NEW_DEVICE_SATURN.
    /// </summary>
    public static double ResolvePsHwPeak(bool isProtocol2, HpsdrBoardKind board) =>
        ResolvePsHwPeak(isProtocol2, board, OrionMkIIVariant.G2);

    /// <summary>
    /// Variant-aware overload (issue #218 Phase 6). When
    /// <paramref name="board"/> is <see cref="HpsdrBoardKind.OrionMkII"/>
    /// on Protocol 2, the variant disambiguates the Saturn-FPGA family
    /// (G2 / G2-1K → 0.6121) from the OrionMkII-class family (7000DLE /
    /// 8000DLE / Apache OrionMkII original / ANVELINA-PRO3 / Red Pitaya
    /// → 0.2899). Pre-#218 the dispatch returned 0.6121 for every 0x0A
    /// board, which over-scaled the PS curve on non-Saturn variants.
    /// </summary>
    public static double ResolvePsHwPeak(bool isProtocol2, HpsdrBoardKind board, OrionMkIIVariant variant) =>
        // Per-protocol switch shaped so future P1 boards can wire HW-peak
        // per-board too. On P1, PS is live on HermesLite2 and HermesC10
        // (ANAN-G2E); the remaining P1 boards keep the engine-arm guard in
        // DspPipelineService (GH #426) but still receive the right number
        // on connect — keeps Synthetic + tests deterministic.
        (isProtocol2, board) switch
        {
            // HL2: mi0bot clsHardwareSpecific.cs:312 PSDefaultPeak = 0.233.
            // Same value regardless of protocol — HL2 hardware peak does
            // not change between P1 and P2.
            (false, HpsdrBoardKind.HermesLite2)              => 0.233,
            (false, _)                                        => 0.4072,
            // 0x0A wire byte: Saturn FPGA (G2 / G2-1K) reports the high
            // peak per Thetis clsHardwareSpecific.cs:313; everything else
            // sharing the byte (7000DLE / 8000DLE / Apache OrionMkII /
            // ANVELINA-PRO3 / Red Pitaya) takes the default. Default
            // variant G2 preserves Zeus' pre-#218 P2 behaviour.
            (true,  HpsdrBoardKind.OrionMkII)                 => variant switch
            {
                OrionMkIIVariant.G2     => 0.6121,
                OrionMkIIVariant.G2_1K  => 0.6121,
                _                        => 0.2899,
            },
            (true,  HpsdrBoardKind.HermesLite2)               => 0.233,
            (true,  _)                                        => 0.2899,
        };

    /// <summary>
    /// Apply a per-radio PS hardware-peak to the StateDto. Called by
    /// DspPipelineService after a successful connect (P1 or P2) so the
    /// engine sees the correct curve scale before the operator arms PS.
    ///
    /// Resolution order:
    ///   1. Operator-calibrated value from PsSettingsStore.HwPeakByBoard
    ///      (set by SetPsAdvanced or the auto-cal control loop) — wins when
    ///      present so chains that don't match the factory default
    ///      (external amp sample taps, non-stock attenuator pads) keep
    ///      their hard-won calibration across reconnects.
    ///   2. Per-board factory default from ResolvePsHwPeak.
    ///
    /// PsHwPeakDefault always tracks (2) so the frontend can render a
    /// "differs from factory default" hint when the operator value is
    /// active. Doesn't fire StateChanged unless something actually moves.
    /// </summary>
    public void ApplyPsHwPeakForConnection(bool isProtocol2, HpsdrBoardKind board)
    {
        var variant = EffectiveOrionMkIIVariant;
        string boardKey = GetPsBoardKey(isProtocol2, board, variant);
        double factoryDefault = ResolvePsHwPeak(isProtocol2, board, variant);
        // Prefer a persisted operator-calibrated value for this exact
        // board / variant. Missing entry → fall through to the factory
        // default (first connect on a new board, or operator hasn't tuned).
        var persisted = _psStore?.Get();
        bool usingPersisted = persisted?.HwPeakByBoard is { } map
            && map.TryGetValue(boardKey, out double saved)
            && saved > 0.0;
        double peak = usingPersisted ? persisted!.HwPeakByBoard[boardKey] : factoryDefault;
        // Cache the board key so PersistPsState routes future SetPsAdvanced
        // writes into the right slot.
        _currentPsBoardKey = boardKey;
        // Surface the TX feedback attenuation for the PURESIGNAL panel's manual
        // control: the per-board floor (HL2 reaches -28, others 0) and the
        // persisted value for this board (0 when none saved). GetPersistedPsTxAttnDb
        // also seeds _currentPsTxAttnDb so PersistPsState keeps the slot.
        int attnMin = board == HpsdrBoardKind.HermesLite2 ? -28 : 0;
        int? persistedAttn = GetPersistedPsTxAttnDb();
        int attn = persistedAttn
            ?? (!isProtocol2 && board is HpsdrBoardKind.HermesC10 or HpsdrBoardKind.HermesII
                ? ActiveClient?.PsTxAttenOnTxDb ?? 0
                : 0);
        Mutate(s =>
            s.PsHwPeak == peak && s.PsHwPeakDefault == factoryDefault
            && s.PsTxFeedbackAttenuationDb == attn && s.PsTxFeedbackAttenuationDbMin == attnMin
                ? s
                : s with
                {
                    PsHwPeak = peak,
                    PsHwPeakDefault = factoryDefault,
                    PsTxFeedbackAttenuationDb = attn,
                    PsTxFeedbackAttenuationDbMin = attnMin,
                });
        _log.LogInformation(
            "radio.applyPsHwPeak proto={Proto} board={Board} variant={Variant} key={Key} peak={Peak:F4} default={Default:F4} source={Source}",
            isProtocol2 ? "P2" : "P1", board, variant, boardKey, peak, factoryDefault,
            usingPersisted ? "persisted" : "factory");
    }

    /// <summary>
    /// Apply a persisted operator arm decision after a radio connection has
    /// restored PS HW peak, feedback attenuation, and replayed the disarmed
    /// state into the fresh engine. The explicit disarm mutation preserves a
    /// genuine falling edge on an in-process reconnect; the conditional arm
    /// then enters DspPipelineService's existing PS FIFO worker.
    /// </summary>
    internal void ApplyPsArmIntentForConnection(bool isProtocol2, HpsdrBoardKind board)
    {
        bool armIntent;
        bool txActive;
        lock (_sync)
        {
            armIntent = _psArmIntent;
            txActive = _mox || _tunActive || _state.TwoToneEnabled;
        }

        if (!armIntent) return;

        bool armSupported = DspPipelineService.P1PsEngineArmSupported(
            p1Connected: !isProtocol2,
            board);
        if (!armSupported)
        {
            _log.LogInformation(
                "ps.rearm intent=armed board={Board} skipped=unsupported",
                board);
            return;
        }

        if (txActive)
        {
            _log.LogInformation(
                "ps.rearm intent=armed board={Board} skipped=tx-active",
                board);
            return;
        }

        Mutate(s => s with { PsEnabled = false });

        string? skipped = null;
        Mutate(s =>
        {
            // Re-check under _sync: an operator SetPs or a TX edge during the
            // sanitize window must win over the stale connect-time read.
            if (!_psArmIntent)
            {
                skipped = "intent-cleared";
                return null;
            }
            if (_mox || _tunActive || s.TwoToneEnabled)
            {
                skipped = "tx-active";
                return null;
            }
            return s with { PsEnabled = true };
        }, out bool applied);

        if (applied)
        {
            _log.LogInformation(
                "ps.rearm intent=armed board={Board} sanitize=disarm→arm",
                board);
        }
        else
        {
            _log.LogInformation(
                "ps.rearm intent=armed board={Board} skipped={Reason}",
                board,
                skipped);
        }
    }

    internal bool PsArmIntent
    {
        get { lock (_sync) return _psArmIntent; }
    }

    public StateDto SetZoom(int level)
        => SetZoomCore(level, persist: true)!;

    internal StateDto? SetZoomIfCurrent(int level, int expectedCurrent)
        => SetZoomCore(level, persist: false, expectedCurrent);

    private StateDto? SetZoomCore(int level, bool persist, int? onlyIfZoomLevelIs = null)
    {
        // Accepts the full display range; DDC/WDSP application clamps to the
        // engine's stable analyzer range while wideband display consumes the
        // deeper values directly. A prior powers-of-two guard here silently
        // rejected 3/5/6/7 with a 500, causing the frontend slider to appear
        // stuck after valid steps.
        int maxZoom = CurrentMaxDisplayZoomLevel;
        if (level < MinDisplayZoomLevel || level > maxZoom)
            throw new ArgumentException(
                $"zoom level must be in [{MinDisplayZoomLevel},{maxZoom}]; got {level}",
                nameof(level));
        Mutate(s =>
        {
            // Close the recall TOCTOU window before applying the stored zoom.
            if (onlyIfZoomLevelIs is int expected && s.ZoomLevel != expected)
                return null;
            return s with { ZoomLevel = level };
        }, out bool applied);
        if (!applied) return null;
        // Persist public zoom changes to the current band's memory row so a
        // later band change can recall them (#128). Recall calls this core with
        // persist=false. Off-band dials have no row to key on.
        if (_bandMemoryStore is not null && persist)
        {
            long vfoHz;
            lock (_sync) { vfoHz = _state.VfoHz; }
            var band = RuntimeBandKey(vfoHz);
            if (band is not null)
                _bandMemoryStore.SetZoom(band, level);
        }
        return Snapshot();
    }

    // Workspace UI zoom (cell-pitch scale, see StateDto.WorkspaceZoomPct). Pure
    // frontend display value — persisted + rebroadcast here, no DSP side effect.
    public const int MinWorkspaceZoomPct = 50;
    public const int MaxWorkspaceZoomPct = 200;
    public const int DefaultWorkspaceZoomPct = 100;

    private static int ClampWorkspaceZoomPct(int pct) =>
        Math.Clamp(pct, MinWorkspaceZoomPct, MaxWorkspaceZoomPct);

    public StateDto SetWorkspaceZoom(int pct)
    {
        var clamped = ClampWorkspaceZoomPct(pct);
        Mutate(s => s with { WorkspaceZoomPct = clamped });
        return Snapshot();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _paStore.Changed -= RecomputePaAndPush;
        if (_antennaStore is not null)
            _antennaStore.Changed -= RecomputePaAndPush;
        if (_rfFilterStore is not null)
            _rfFilterStore.Changed -= RecomputePaAndPush;
        if (_audioStore is not null)
            _audioStore.Changed -= PushAudioFrontEnd;
        if (_hl2GpioStore is not null)
            _hl2GpioStore.Changed -= PushHl2Gpio;
        if (_transverterSettingsStore is not null)
            _transverterSettingsStore.Changed -= OnTransverterSettingsChanged;
        if (_layoutStore is not null)
            _layoutStore.ActiveTransverterEnabledChanged -= OnActiveTransverterSelectionChanged;
        try { DisconnectAsync(CancellationToken.None).GetAwaiter().GetResult(); }
        catch { /* best-effort */ }
        try { _operatorConnectionActionCts.Cancel(); }
        catch (ObjectDisposedException) { }
        _operatorConnectionActionCts.Dispose();
        if (_stateFlushTimer is not null)
        {
            // Parameterless Dispose does not wait for in-flight callbacks; that
            // races host teardown while a timer flush may still be using DI
            // stores that are about to be disposed (issue #1342).
            var flushed = new ManualResetEvent(false);
            if (_stateFlushTimer.Dispose(flushed))
            {
                var completed = flushed.WaitOne(TimeSpan.FromSeconds(5));
                // If the wait times out, Timer may still Set() this handle when
                // the callback unwinds; disposing it here would move the crash
                // from the flush path into Timer internals.
                if (completed) flushed.Dispose();
            }
        }
        // Final flush so the last operator actions survive a clean shutdown.
        _stateDirty = true;
        FlushState();
    }

    private void Mutate(Func<StateDto, StateDto> fn)
        => Mutate(s => (StateDto?)fn(s), out _);

    // Conditional mutation seam for compare-and-swap style recalls. A null
    // result aborts without dirtying or broadcasting the unchanged state.
    // Unconditional Mutate delegates here so the Receivers/MaxReceivers/
    // ConnectedProtocol projection lives in exactly one place.
    private void Mutate(Func<StateDto, StateDto?> fn, out bool applied)
    {
        StateDto? next;
        try
        {
            lock (_sync)
            {
                next = fn(_state);
                if (next is null)
                {
                    applied = false;
                    return;
                }
                // Project the canonical per-receiver array from the flat RX1/RX2
                // fields on every mutation so StateChanged subscribers and the
                // SignalR broadcast always carry an up-to-date Receivers[] (wire
                // v2). Pure function of the flat fields — cheap (1–2 elements).
                next = next with
                {
                    Receivers = ProjectReceivers(next),
                    MaxReceivers = EffectiveMaxReceivers,
                    ConnectedProtocol = ConnectedProtocolLocked(),
                };
                TransmitSafetyStateChanging?.Invoke(_state, next);
                _state = next;
                _currentMode = (int)next.Mode;
            }
        }
        catch (TransmitSafetyRejectedException ex)
        {
            CompleteRejectedStateChange(ex);
            throw;
        }
        applied = true;
        _stateDirty = true;
        StateChanged?.Invoke(next);
    }

    private void CompleteRejectedStateChange(TransmitSafetyRejectedException rejection)
    {
        // Conditional VFO/mode restores hold an outer state lock around their
        // setters. Let that outer caller finish unwinding before running DSP
        // teardown; completion is consumed once at the outermost boundary.
        if (!Monitor.IsEntered(_sync)) rejection.CompleteAfterStateUnlock();
    }

    // Patch the authoritative RX2 (index 1) entry inside a StateDto's Receivers
    // array, returning a new StateDto. Callers set RX2's receiver-array fields
    // (VFO / mode / filter / AF gain / ADC source) here — the subsequent Mutate /
    // ProjectReceivers pass re-overlays the flat control fields. This is the
    // write counterpart to ReceiverProjection.Rx2 (the read accessor). Off the
    // audio thread (operator setters), so the small list copy is fine.
    private static StateDto WithRx2(StateDto s, Func<ReceiverDto, ReceiverDto> patch)
    {
        var next = patch(s.Rx2());
        var src = s.Receivers;
        if (src is null)
            return s with { Receivers = new[] { next } };
        var list = new List<ReceiverDto>(src.Count);
        bool replaced = false;
        foreach (var r in src)
        {
            if (r.Index == 1) { list.Add(next); replaced = true; }
            else list.Add(r);
        }
        if (!replaced) list.Add(next);
        return s with { Receivers = list };
    }

    private static ReceiverDto Rx1ProjectionSeed(StateDto s) => new(
        Index: 0, Enabled: true, AdcSource: ReceiverAdcSource(s, 0),
        VfoHz: s.VfoHz, Mode: s.Mode,
        FilterLowHz: s.FilterLowHz, FilterHighHz: s.FilterHighHz,
        FilterPresetName: s.FilterPresetName,
        AfGainDb: s.Rx1AfGainDb, SampleRateHz: s.SampleRate,
        Muted: s.Rx1Muted, SplitEnabled: s.SplitEnabled, TxVfoHz: s.SplitTxHz,
        // Read-only mirror of the legacy global zoom — RX1 is still SET only via
        // StateDto.ZoomLevel / SetZoom, never through this projection.
        ZoomLevel: s.ZoomLevel);

    private static StateDto WithReceiverAdcSource(StateDto s, int index, byte adcSource)
    {
        var seed = index == 0
            ? Rx1ProjectionSeed(s)
            : index == 1
                ? s.Rx2()
                : throw new ArgumentOutOfRangeException(nameof(index), index, "only RX1/RX2 are stored in StateDto.Receivers");
        var next = seed with { AdcSource = adcSource };
        var src = s.Receivers;
        if (src is null)
            return s with { Receivers = new[] { next } };

        var list = new List<ReceiverDto>(src.Count);
        bool replaced = false;
        foreach (var r in src)
        {
            if (r.Index == index) { list.Add(next); replaced = true; }
            else list.Add(r);
        }
        if (!replaced) list.Add(next);
        return s with { Receivers = list };
    }

    // RX2's independent zoom, stored in Receivers[1] like AdcSource (see
    // WithReceiverAdcSource above). RX1 never routes here — its zoom stays on
    // the legacy global StateDto.ZoomLevel.
    private static StateDto WithReceiverZoom(StateDto s, int index, int zoomLevel)
    {
        if (index != 1)
            throw new ArgumentOutOfRangeException(nameof(index), index, "only RX2 zoom is stored in StateDto.Receivers");
        var next = s.Rx2() with { ZoomLevel = zoomLevel };
        var src = s.Receivers;
        if (src is null)
            return s with { Receivers = new[] { next } };

        var list = new List<ReceiverDto>(src.Count);
        bool replaced = false;
        foreach (var r in src)
        {
            if (r.Index == index) { list.Add(next); replaced = true; }
            else list.Add(r);
        }
        if (!replaced) list.Add(next);
        return s with { Receivers = list };
    }

    // The zoom level RX2+ receiver `index` last had applied — falls back to
    // 1x before the array is seeded. Used by DspPipelineService to drive each
    // secondary channel's independent SetZoom instead of the global
    // StateDto.ZoomLevel every receiver used to share.
    internal static int ReceiverZoomLevel(StateDto s, int index)
    {
        if (s.Receivers is { } receivers)
        {
            for (int i = 0; i < receivers.Count; i++)
                if (receivers[i].Index == index)
                    return receivers[i].ZoomLevel;
        }
        return 1;
    }

    internal static byte ReceiverAdcSource(StateDto s, int index)
    {
        if (s.Receivers is { } receivers)
        {
            for (int i = 0; i < receivers.Count; i++)
                if (receivers[i].Index == index)
                    return receivers[i].AdcSource;
        }
        return 0;
    }

    // Build the canonical per-receiver array (wire v2). Index 0 = RX1 is rebuilt
    // from the flat RX1 fields; index 1 = RX2 is carried forward from the array
    // (authoritative) with Enabled
    // tracking Rx2Enabled so the frontend has the VFO-B config even when RX2 is
    // off. AdcSource defaults to ADC0 until the multi-DDC UI assigns per-DDC
    // ADCs; SampleRateHz is the shared capture rate. Additional DDCs (index ≥ 2)
    // are appended here once DDC 2..N control exists. Pure + allocation-light
    // (called on every Mutate / Snapshot).
    // Caller holds _sync (Mutate / Snapshot) — reads _extraReceivers.
    private IReadOnlyList<ReceiverDto> ProjectReceivers(StateDto s)
    {
        var list = new List<ReceiverDto>(2)
        {
            new ReceiverDto(
                Index: 0, Enabled: true, AdcSource: ReceiverAdcSource(s, 0),
                VfoHz: s.VfoHz, Mode: s.Mode,
                FilterLowHz: s.FilterLowHz, FilterHighHz: s.FilterHighHz,
                FilterPresetName: s.FilterPresetName,
                AfGainDb: s.Rx1AfGainDb, SampleRateHz: s.SampleRate,
                Muted: s.Rx1Muted, SplitEnabled: s.SplitEnabled,
                TxVfoHz: s.SplitTxHz, ZoomLevel: s.ZoomLevel),
            // index 1 = RX2: its VFO / mode / filter / AF gain are authoritative
            // in the array itself (the flat VFO-B fields are gone). Carry the
            // existing tuning forward and overlay the flat RX2 control fields
            // (Enabled / Muted) + the shared capture rate. RX2 writes patch
            // Receivers[1] (see WithRx2) before this runs, so the latest tuning
            // is what gets carried.
            s.Rx2() with
            {
                Index = 1,
                Enabled = s.Rx2Enabled,
                AdcSource = ReceiverAdcSource(s, 1),
                SampleRateHz = s.SampleRate,
                Muted = s.Rx2Muted,
            },
        };
        // Extra DDC receivers (RX3+). Appended contiguously while enabled — the
        // P2 multi-DDC path requires no DDC gaps, so the first disabled slot
        // ends the run. Ordinary P1 deliberately projects no extras because its
        // current ingest path only supplies RX1/RX2. SampleRate is the shared
        // capture rate for now.
        for (int i = 2; _activeClient is null && i < _extraReceivers.Length; i++)
        {
            var e = _extraReceivers[i];
            if (e is null || !e.Enabled) break;
            list.Add(new ReceiverDto(
                Index: i, Enabled: true, AdcSource: e.AdcSource,
                VfoHz: e.VfoHz, Mode: e.Mode,
                FilterLowHz: e.FilterLowHz, FilterHighHz: e.FilterHighHz,
                FilterPresetName: e.FilterPresetName,
                AfGainDb: e.AfGainDb, SampleRateHz: s.SampleRate,
                Muted: e.Muted, SplitEnabled: e.SplitEnabled,
                TxVfoHz: e.TxVfoHz, ZoomLevel: e.ZoomLevel));
        }
        // Non-hardware KiwiSDR slice (reserved index KiwiReceiverIndex). Appended
        // out of the contiguous DDC run — it is a remote receiver, not a DDC, so
        // it never participates in the no-gap cascade above. Null when disabled.
        var kiwi = _externalReceiverSource.GetReceiver();
        if (kiwi is not null)
            list.Add(kiwi);
        return list;
    }

    // Debounce flush: called by _stateFlushTimer every 1 s.
    // Captures the latest StateDto + family-filter memory under _sync and
    // writes to LiteDB. No-op when nothing has mutated since the last flush.
    private void FlushState()
    {
        if (!_stateDirty || _radioStateStore is null) return;
        _stateDirty = false;

        StateDto snap;
        AdcProtectionConfig adcProtection;
        FamilyFilter ssb, dig, am, fm, cw, ssbTx, amTx, fmTx, cwTx;
        List<NotchDto> notches;
        lock (_sync)
        {
            snap = _state;
            adcProtection = _adcProtection;
            ssb = _ssbFilter; dig = _digFilter; am = _amFilter; fm = _fmFilter; cw = _cwFilter;
            ssbTx = _ssbTxFilter; amTx = _amTxFilter; fmTx = _fmTxFilter; cwTx = _cwTxFilter;
            notches = _notches.ToList();
        }

        var rx2Snap = snap.Rx2();
        var rxLeveler = snap.RxLeveler ?? new RxLevelerConfig();
        try
        {
            _radioStateStore.Save(new RadioStateEntry
            {
                VfoHz = snap.VfoHz,
                Mode = snap.Mode,
                FilterLowHz = snap.FilterLowHz,
                FilterHighHz = snap.FilterHighHz,
                TxFilterLowHz = snap.TxFilterLowHz,
                TxFilterHighHz = snap.TxFilterHighHz,
                FilterPresetName = snap.FilterPresetName,
                AutoAttEnabled = snap.AutoAttEnabled,
                AdcProtectionAttackMs = adcProtection.AttackMs,
                AdcProtectionReleaseMs = adcProtection.ReleaseMs,
                AdcProtectionAttackStepDb = adcProtection.AttackStepDb,
                AdcProtectionReleaseStepDb = adcProtection.ReleaseStepDb,
                AdcProtectionMaxOffsetDb = adcProtection.MaxOffsetDb,
                AdcProtectionWarningThreshold = adcProtection.WarningThreshold,
                AdcProtectionMagnitudeSoftLimit = adcProtection.MagnitudeSoftLimit,
                AdcProtectionReleaseHoldMs = adcProtection.ReleaseHoldMs,
                AttenDb = snap.AttenDb,
                AutoAgcEnabled = snap.AutoAgcEnabled,
                RxLevelerEnabled = snap.RxLevelerEnabled,
                RxLevelerMode = rxLeveler.Mode,
                RxLevelerTargetRmsDb = rxLeveler.TargetRmsDb,
                RxLevelerMaxBoostDb = rxLeveler.MaxBoostDb,
                RxLevelerAttackMs = rxLeveler.AttackMs,
                RxLevelerReleaseMs = rxLeveler.ReleaseMs,
                RxLevelerHangMs = rxLeveler.HangMs,
                PreampOn = snap.PreampOn,
                RxAfGainDb = snap.RxAfGainDb,
                Rx1AfGainDb = snap.Rx1AfGainDb,
                // Always true once this build has written the row: the fold is a
                // one-shot upgrade step, and a master the operator sets after it
                // must survive the next start untouched.
                AfMasterSplitMigrated = true,
                MicGainDb = snap.MicGainDb,
                LevelerMaxGainDb = snap.LevelerMaxGainDb,
                ZoomLevel = snap.ZoomLevel,
                WorkspaceZoomPct = snap.WorkspaceZoomPct,
                SsbFilterLoAbs = ssb.LoAbs,   SsbFilterHiAbs = ssb.HiAbs,
                DigFilterLoAbs = dig.LoAbs,   DigFilterHiAbs = dig.HiAbs,
                AmFilterLoAbs = am.LoAbs,     AmFilterHiAbs = am.HiAbs,
                FmFilterLoAbs = fm.LoAbs,     FmFilterHiAbs = fm.HiAbs,
                CwFilterLoAbs = cw.LoAbs,     CwFilterHiAbs = cw.HiAbs,
                SsbTxFilterLoAbs = ssbTx.LoAbs, SsbTxFilterHiAbs = ssbTx.HiAbs,
                AmTxFilterLoAbs = amTx.LoAbs,   AmTxFilterHiAbs = amTx.HiAbs,
                FmTxFilterLoAbs = fmTx.LoAbs,   FmTxFilterHiAbs = fmTx.HiAbs,
                CwTxFilterLoAbs = cwTx.LoAbs,   CwTxFilterHiAbs = cwTx.HiAbs,
                DrivePct = snap.DrivePct,
                DriveMaxPct = snap.DriveMaxPct,
                TunePct = snap.TunePct,
                TxMoxPreKeyDelayMs = snap.TxMoxPreKeyDelayMs,
                TxMoxTailDelayMs = snap.TxMoxTailDelayMs,
                TxPostTxRxMuteDelayMs = snap.TxPostTxRxMuteDelayMs,
                RogerBeepEnabled = snap.RogerBeepEnabled,
                TxTimeoutSec = snap.TxTimeoutSec,
                RadioLoHz = snap.RadioLoHz,
                // RX2 tuning persists from the canonical Receivers[1] entry (the
                // flat VFO-B StateDto fields are gone); the RadioStateEntry schema
                // is unchanged so older/newer DBs round-trip identically.
                Rx2Enabled = snap.Rx2Enabled,
                VfoBHz = rx2Snap.VfoHz,
                ModeB = rx2Snap.Mode,
                FilterLowHzB = rx2Snap.FilterLowHz,
                FilterHighHzB = rx2Snap.FilterHighHz,
                FilterPresetNameB = rx2Snap.FilterPresetName,
                Rx2AudioMode = snap.Rx2AudioMode,
                Rx2AfGainDb = rx2Snap.AfGainDb,
                TxVfo = snap.TxVfo,
                CtunEnabled = snap.CtunEnabled,
                FullDuplexMultiRxEnabled = snap.FullDuplexMultiRxEnabled,
                Notches = notches.Select(n => new RadioStateNotchEntry
                {
                    CenterHz = n.CenterHz,
                    WidthHz = n.WidthHz,
                    Active = n.Active,
                    Source = NormalizeNotchSource(n.Source),
                }).ToList(),
                UpdatedUtc = DateTime.UtcNow,
            });
        }
        catch (Exception ex)
        {
            try { _log.LogWarning(ex, "radio.state.flush failed"); }
            catch { /* logger may be disposed during shutdown */ }
        }
    }

    // Used by DspPipelineService when a Protocol 2 radio connects or
    // disconnects. RadioService's _activeClient is P1-only; this is how
    // the shared state (Status, Endpoint, SampleRate) stays coherent for
    // the UI without growing a P2 client slot here.
    //
    // The optional <paramref name="client"/> wires the freshly-opened
    // Protocol2Client to subscribers of <see cref="P2Connected"/>; passing
    // null keeps the signature backward-compatible for tests that don't
    // need the telemetry surface (issue #174).
    public void MarkProtocol2Connected(
        string endpoint,
        int sampleRateHz,
        Protocol2Client? client = null,
        HpsdrBoardKind boardKind = HpsdrBoardKind.Unknown,
        string? firmware = null)
    {
        // Claiming the hardware for P2 supersedes any pending P1 start-failure
        // auto-retry, whichever path marked the connection.
        NotifyOperatorConnectionAction();
        Protocol2Client? previous;
        lock (_sync)
        {
            previous = _p2Client;
            _p2Client = client;
            _p2Active = true;
            _p2BoardKind = boardKind;
            _p3Active = false;
            _p3MaxReceivers = Zeus.Contracts.WireContract.MaxReceivers;
            // Record the discovered firmware for the diagnostics snapshot.
            _connectedFirmware = firmware;
            _attOffsetDb = 0;
            _predictiveMagnitudeControlActive = false;
            _adcOverloadLevel = 0;
            ResetAdcProtectionWindowNoLock();
            _lastTickMs = long.MinValue;
            _lastAttAttackMs = long.MinValue;
            _adcProtectionResumeAfterMs = long.MinValue;
            _lastOverloadMs = long.MinValue;
            _lastAppliedEffectiveDb = -1;
            _lastAdcOverloadBits = 0;
            _lastAdc0MaxMagnitude = null;
            _lastAdc1MaxMagnitude = null;
            _adc0MaxMagnitudeAtOverload = 0;
            _adc1MaxMagnitudeAtOverload = 0;
            _lastAdcTelemetryUtc = null;
        }
        if (previous is not null) previous.TelemetryReceived -= OnP2Telemetry;
        if (client is not null) client.TelemetryReceived += OnP2Telemetry;
        Mutate(s => s with
        {
            Status = ConnectionStatus.Connected,
            Endpoint = endpoint,
            SampleRate = sampleRateHz,
            // The fresh P2 client and engine must receive a disarmed replay
            // before persisted intent is sanitized at the connect tail.
            PsEnabled = false,
            ZoomLevel = Math.Min(
                s.ZoomLevel,
                P2WidebandZoomPolicy.MaxGlobalZoomForRate(sampleRateHz)),
            AttOffsetDb = 0,
            AdcOverloadWarning = false,
        });
        // P2 is alive — PA defaults should reflect G2 / Orion class so the
        // operator sees realistic numbers when they open the PA panel.
        RecomputePaAndPush();
        ApplyG2AdcOptionsToP2Client(client, ConnectedBoardKind);
        // Arm the internal CW keyer with the operator's persisted WPM / mode /
        // sidetone so a paddle on the rear KEY jack works on first connect
        // without touching the CW panel — mirrors the P1 connect push. Byte-5
        // CW-select only actually engages once the radio is in CW mode. #1032.
        PushCwToP2();
        // Fire AFTER the state mutation + PA recompute so subscribers see a
        // fully-coherent RadioService when they read board kind / snapshot.
        if (client is not null) P2Connected?.Invoke(client);
    }

    /// <summary>Test seam: mark a non-Protocol-2 (P1-style) connection — Status
    /// Connected with <c>_p2Active</c> left false — so the P2 speaker sink's
    /// protocol gate can be exercised without a live Protocol1Client. This is
    /// the issue-#1122 cross-fire case: a connected codec radio that is NOT on
    /// Protocol 2 must not open the UDP→1028 path.</summary>
    internal void MarkConnectedNonP2ForTest(string endpoint) =>
        Mutate(s => s with { Status = ConnectionStatus.Connected, Endpoint = endpoint });

    /// <summary>Test seam: inject a socketless
    /// <see cref="IProtocol1Client"/> as the active P1 client, so
    /// <see cref="ActiveClient"/> / <see cref="ConnectedBoardKind"/> /
    /// <see cref="IsConnected"/> behave as a live P1 session without any
    /// socket I/O (real network in unit tests crashes the Windows CI test
    /// host). The caller owns the client; nothing here starts RX/TX loops.
    /// Real-client tests exercise board plumbing, while an interface proxy can
    /// capture pipeline writes without opening sockets.</summary>
    internal void SetActiveClientForTest(IProtocol1Client? client)
    {
        lock (_sync)
        {
            _activeClient = client as Protocol1Client;
            _activeClientForTest = client is Protocol1Client ? null : client;
        }
    }

    public void MarkProtocol2Disconnected()
    {
        Protocol2Client? previous;
        lock (_sync)
        {
            previous = _p2Client;
            _p2Client = null;
            _p2Active = false;
            _p2BoardKind = HpsdrBoardKind.Unknown;
            _connectedFirmware = null;
            _attOffsetDb = 0;
            _predictiveMagnitudeControlActive = false;
            _adcOverloadLevel = 0;
            ResetAdcProtectionWindowNoLock();
            _lastTickMs = long.MinValue;
            _lastAttAttackMs = long.MinValue;
            _adcProtectionResumeAfterMs = long.MinValue;
            _lastOverloadMs = long.MinValue;
            _lastAppliedEffectiveDb = -1;
        }
        if (previous is not null) previous.TelemetryReceived -= OnP2Telemetry;
        Mutate(s => s with
        {
            Status = ConnectionStatus.Disconnected,
            Endpoint = null,
            PsEnabled = false,
            ZoomLevel = Math.Min(s.ZoomLevel, LegacyMaxDisplayZoomLevel),
            AttOffsetDb = 0,
            AdcOverloadWarning = false,
        });
        // Same reasoning as P1 DisconnectAsync — clear the board key so
        // disconnected SetPsAdvanced writes don't leak into the previous
        // radio's per-board HW Peak slot.
        _currentPsBoardKey = string.Empty;
        _currentPsTxAttnDb = -1;
        P2Disconnected?.Invoke();
    }

    /// <summary>
    /// Sends one radio-speaker packet through the active Protocol-2 transport.
    /// Commands, TX IQ, and speaker audio must share its bound source port so
    /// radios that learn the host reply endpoint cannot redirect RX IQ to a
    /// second audio-only socket (issue #1457).
    /// </summary>
    internal int SendProtocol2SpeakerAudio(ReadOnlySpan<byte> packet) =>
        Volatile.Read(ref _p2Client)?.SendSpeakerAudio(packet) ?? 0;

    public void MarkProtocol3Connected(
        string endpoint,
        int sampleRateHz,
        int maxReceivers,
        string? firmware = null)
    {
        // Claiming the hardware for P3 supersedes any pending P1 start-failure
        // auto-retry, whichever path marked the connection.
        NotifyOperatorConnectionAction();
        Protocol2Client? previous;
        lock (_sync)
        {
            previous = _p2Client;
            _p2Client = null;
            _p2Active = false;
            _p2BoardKind = HpsdrBoardKind.Unknown;
            _p3Active = true;
            _p3MaxReceivers = Math.Clamp(maxReceivers, 1, Zeus.Contracts.WireContract.MaxReceivers);
            _connectedFirmware = firmware;
            _attOffsetDb = 0;
            _predictiveMagnitudeControlActive = false;
            _adcOverloadLevel = 0;
            ResetAdcProtectionWindowNoLock();
            _lastTickMs = long.MinValue;
            _lastAttAttackMs = long.MinValue;
            _adcProtectionResumeAfterMs = long.MinValue;
            _lastOverloadMs = long.MinValue;
            _lastAppliedEffectiveDb = -1;
            _lastAdcOverloadBits = 0;
            _lastAdc0MaxMagnitude = null;
            _lastAdc1MaxMagnitude = null;
            _adc0MaxMagnitudeAtOverload = 0;
            _adc1MaxMagnitudeAtOverload = 0;
            _lastAdcTelemetryUtc = null;
        }
        if (previous is not null) previous.TelemetryReceived -= OnP2Telemetry;
        Mutate(s => s with
        {
            Status = ConnectionStatus.Connected,
            Endpoint = endpoint,
            SampleRate = sampleRateHz,
            // P3 always starts disarmed. An optional external provider path is
            // capability-gated later, but it still requires an explicit
            // operator arm in this server session. Persisted intent is never
            // applied automatically here.
            PsEnabled = false,
            ZoomLevel = Math.Min(s.ZoomLevel, LegacyMaxDisplayZoomLevel),
            AttOffsetDb = 0,
            AdcOverloadWarning = false,
        });
        RecomputePaAndPush();
    }

    public void MarkProtocol3Disconnected()
    {
        lock (_sync)
        {
            _p3Active = false;
            _p3MaxReceivers = Zeus.Contracts.WireContract.MaxReceivers;
            _connectedFirmware = null;
            _attOffsetDb = 0;
            _predictiveMagnitudeControlActive = false;
            _adcOverloadLevel = 0;
            ResetAdcProtectionWindowNoLock();
            _lastTickMs = long.MinValue;
            _lastAttAttackMs = long.MinValue;
            _adcProtectionResumeAfterMs = long.MinValue;
            _lastOverloadMs = long.MinValue;
            _lastAppliedEffectiveDb = -1;
        }
        Mutate(s => s with
        {
            Status = ConnectionStatus.Disconnected,
            Endpoint = null,
            PsEnabled = false,
            AttOffsetDb = 0,
            AdcOverloadWarning = false,
        });
        _currentPsBoardKey = string.Empty;
        _currentPsTxAttnDb = -1;
    }

    // Resolves the board class for ALL board-specific behavior: PA settings,
    // drive-byte encoding, ATT behavior, filter switching. Normally returns
    // the board ID from discovery for P1 or P2. When the
    // operator has enabled "Override Detection" in PreferredRadioStore, returns
    // the preferred board instead — use this for hardware combinations that
    // report incorrect board IDs or need different behavior (e.g., Anvelina SDR
    // + ANAN 200D PA detected as OrionMkII but needs Orion behavior).
    public HpsdrBoardKind ConnectedBoardKind
    {
        get
        {
            lock (_sync)
            {
                // Protocol1Client.BoardKind is the board actually driving the
                // live P1 wire. It already includes any safely-applied override;
                // when a change is deferred, keep reporting the still-active
                // board rather than claiming the pending preference is live.
                var protocol1 = _activeClientForTest ?? _activeClient;
                if (protocol1 is not null) return protocol1.BoardKind;

                // Check if operator has explicitly enabled board override.
                // This allows forcing specific board behavior when auto-detection
                // is wrong or incomplete (different hardware with same board ID).
                if (_preferredRadioStore?.GetOverrideDetection() == true)
                {
                    var preferred = _preferredRadioStore.Get();
                    if (preferred.HasValue && preferred.Value != HpsdrBoardKind.Unknown)
                    {
                        return preferred.Value;
                    }
                }

                // Normal path: use discovery result.
                if (_p2Active)
                {
                    // Brick2 announces as Hermes (0x01) on P2; older Zeus
                    // assumed every P2 radio was OrionMkII because the connect
                    // API did not carry discovery identity (issue #171).
                    // Decision 4 makes an absent identity fail-safe Unknown.
                    return ResolveProtocol2BoardKind(_p2BoardKind);
                }
                if (_p3Active)
                {
                    return HpsdrBoardKind.OrionMkII;
                }
                return HpsdrBoardKind.Unknown;
            }
        }
    }

    /// <summary>
    /// Raw board identity supplied by discovery for the active connection,
    /// before any operator override is applied. Diagnostics use this to keep
    /// the radio-reported identity distinct from live effective behavior.
    /// </summary>
    public HpsdrBoardKind DiscoveredBoardKind
    {
        get
        {
            lock (_sync)
            {
                if (_activeClient is not null)
                    return _activeP1ConnectionAttempt?.DiscoveredKind
                        ?? HpsdrBoardKind.Unknown;
                if (_p2Active)
                    return _p2BoardKind;
                return HpsdrBoardKind.Unknown;
            }
        }
    }

    internal static HpsdrBoardKind ResolveProtocol2BoardKind(HpsdrBoardKind boardKind)
        => boardKind;

    // Board used to seed PA defaults / power-math tables. When a radio is
    // connected, ConnectedBoardKind wins (which may be overridden by the
    // operator). Before first connect, the stored preference takes over so
    // the PA panel shows sane values for the radio the operator is about to
    // plug in.
    public HpsdrBoardKind EffectiveBoardKind
    {
        get
        {
            var connected = ConnectedBoardKind;
            if (connected != HpsdrBoardKind.Unknown) return connected;
            return _preferredRadioStore?.Get() ?? HpsdrBoardKind.Unknown;
        }
    }

    private string? ConnectedProtocolLocked()
    {
        if (_p2Active) return "P2";
        if (_p3Active) return "P3";
        if (_activeClient is not null) return "P1";
        return null;
    }

    // Board-aware count of user-visible receivers the connected radio can
    // actually expose, advertised to the frontend via StateDto.MaxReceivers so
    // the Receivers menu renders exactly the reachable slots. On Protocol-2 the
    // standard DDC enable byte addresses DDC0..DDC7 (MaxRxDdc = 8), but
    // Orion-family boards reserve the first RxBaseDdc slots (DDC0/1) for the
    // PureSignal feedback pair, leaving user RX = MaxRxDdc - RxBaseDdc(board):
    // 8 on Hermes-class, 6 on G2/Orion. Ordinary Protocol-1 is capped to the two
    // receivers Zeus currently feeds; this is a host-ingest capability limit,
    // not a claim about the P1 wire or board gateware. Disconnected state keeps
    // the flat wire ceiling so a later P2/P3 connection can be preconfigured.
    public int EffectiveMaxReceivers
    {
        get
        {
            lock (_sync)
            {
                if (_p2Active)
                    return Zeus.Protocol2.Protocol2Client.MaxRxDdc
                         - Zeus.Protocol2.Protocol2Client.RxBaseDdc(ConnectedBoardKind);
                if (_p3Active)
                    return _p3MaxReceivers;
                if (_activeClient is not null)
                    return Protocol1OrdinaryMaxReceivers;
                return Zeus.Contracts.WireContract.MaxReceivers;
            }
        }
    }

    // Variant override for the 0x0A wire-byte alias family (issue #218).
    // Read by dispatch helpers (RadioCalibrations.For / PaDefaults.* /
    // BoardCapabilitiesTable.For) when EffectiveBoardKind == OrionMkII;
    // ignored for every other board. Default OrionMkIIVariant.G2 preserves
    // Zeus' pre-#218 behaviour for operators who never touch this setting.
    public OrionMkIIVariant EffectiveOrionMkIIVariant =>
        _preferredRadioStore?.GetOrionMkIIVariant() ?? OrionMkIIVariant.G2;

    // Fires from the Protocol1 RX thread when consecutive receive timeouts exhaust
    // the threshold — the radio stopped sending. Runs teardown and any initial-start
    // post-mortem on the thread pool so StopAsync's _rxThread.Join() cannot deadlock.
    private void OnClientDisconnected(
        Protocol1Client disconnectedClient,
        P1ConnectionAttempt attempt) =>
        _ = Task.Run(async () =>
        {
            try
            {
                await HandleClientDisconnectedAsync(disconnectedClient, attempt).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "p1.disconnect handling failed");
            }
        });

    private async Task HandleClientDisconnectedAsync(
        Protocol1Client disconnectedClient,
        P1ConnectionAttempt attempt)
    {
        await RadioLifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            lock (_sync)
            {
                if (!ReferenceEquals(_activeClient, disconnectedClient))
                    return;
            }

            bool startHandshakeFailed = disconnectedClient.StartHandshakeFailed;
            long framesDelivered = disconnectedClient.TotalFrames;
            await DisconnectClientCoreAsync(disconnectedClient, CancellationToken.None).ConfigureAwait(false);

            if (_p1StartFailureRecovery is null)
                return;

            await _p1StartFailureRecovery.HandleAsync(
                startHandshakeFailed,
                framesDelivered,
                attempt,
                async (retryAttempt, retryCt) =>
                {
                    await ConnectCoreAsync(retryAttempt, retryCt).ConfigureAwait(false);
                },
                () => IsCurrentOperatorConnectionAction(attempt),
                attempt.OperatorActionToken).ConfigureAwait(false);
        }
        finally
        {
            RadioLifecycleGate.Release();
        }
    }

    private (long Generation, CancellationToken Token) BeginOperatorConnectionAction()
    {
        var next = new CancellationTokenSource();
        CancellationTokenSource previous;
        long generation;
        lock (_sync)
        {
            previous = _operatorConnectionActionCts;
            _operatorConnectionActionCts = next;
            generation = ++_operatorConnectionGeneration;
        }

        try { previous.Cancel(); }
        catch (ObjectDisposedException) { }
        previous.Dispose();
        return (generation, next.Token);
    }

    internal void NotifyOperatorConnectionAction() => BeginOperatorConnectionAction();

    internal async Task DisconnectSupersededP1AutomaticRetryAsync()
    {
        Protocol1Client? supersededRetry;
        lock (_sync)
        {
            supersededRetry = _activeP1ConnectionAttempt is
                {
                    IsAutomaticRetry: true,
                } activeAttempt
                && activeAttempt.Generation != _operatorConnectionGeneration
                    ? _activeClient
                    : null;
        }

        if (supersededRetry is not null)
        {
            await DisconnectClientCoreAsync(supersededRetry, CancellationToken.None)
                .ConfigureAwait(false);
        }
    }

    private bool IsCurrentOperatorConnectionAction(P1ConnectionAttempt attempt)
    {
        lock (_sync)
        {
            return _operatorConnectionGeneration == attempt.Generation
                   && !attempt.OperatorActionToken.IsCancellationRequested;
        }
    }

    private void ThrowIfSuperseded(P1ConnectionAttempt attempt)
    {
        if (!IsCurrentOperatorConnectionAction(attempt))
            throw new OperationCanceledException(attempt.OperatorActionToken);
    }

    // #1302 F4: PS feedback watchdog fired — an armed HermesC10 stream went
    // 2 s with zero parseable 4-DDC packets while datagrams kept arriving
    // (misframed EP6). Auto-disarm PS via the normal Mutate → StateChanged
    // flow: DspPipelineService picks up PsEnabled=false and routes the wire
    // change through the client's safe stop/drain/restart transition, which
    // also realigns the radio's EP6 framing. Runs off-thread — the event
    // fires on the RX thread, which must never block on the state pipeline.
    // This watchdog disarm is a runtime safety action, not an operator
    // decision. ArmIntent deliberately survives so the next connect retries
    // through a fresh disarm/rearm sanitize cycle; no store write is needed.
    private void OnPsFeedbackStalled()
    {
        _log.LogWarning(
            "p1.ps.watchdog auto-disarming PureSignal — feedback unavailable: stalled stream or receive geometry never converged");
        _ = Task.Run(() =>
        {
            try { Mutate(s => s with { PsEnabled = false }); }
            catch (Exception ex) { _log.LogWarning(ex, "p1.ps.watchdog auto-disarm failed"); }
        });
    }

    // Protocol1 → RadioService bridge. Runs on the RX thread at ~1.2 kHz;
    // hands off to HandleAdcOverload for the logic the tests can drive.
    private void OnAdcOverload(AdcOverloadStatus status) =>
        HandleAdcOverload(status, Environment.TickCount64);

    private void OnP2Telemetry(P2TelemetryReading reading) =>
        HandleP2AdcTelemetry(reading, Environment.TickCount64);

    /// <summary>
    /// Protocol-1 compatibility entrypoint for tests and the P1 overload
    /// event. Uses the same configurable primary-RX protection core as
    /// Protocol 2.
    /// </summary>
    internal void HandleAdcOverload(AdcOverloadStatus status, long nowMs)
    {
        byte bits = (byte)((status.Adc0 ? 0x01 : 0) | (status.Adc1 ? 0x02 : 0));
        HandleAdcProtection(bits, null, null, transmitterActive: false, nowMs: nowMs);
    }

    /// <summary>
    /// Protocol-2 hi-priority telemetry path. Consumes overload bits and, when
    /// configured, ADC max-magnitude words so the G2/Orion board can protect
    /// before the overload bit latches.
    /// </summary>
    internal void HandleP2AdcTelemetry(P2TelemetryReading reading, long nowMs)
    {
        HandleAdcProtection(
            reading.AdcOverloadBits,
            reading.Adc0MaxMagnitude,
            reading.Adc1MaxMagnitude,
            reading.PttIn,
            nowMs);
    }

    /// <summary>
    /// Adaptive three-zone ADC protection. Protocol 2 magnitude telemetry
    /// provides predictive attack/hold/release control; Protocol 1 and missing
    /// or zero P2 magnitude data retain hard-overload-only protection.
    /// </summary>
    private void HandleAdcProtection(
        byte overloadBits,
        ushort? adc0MaxMagnitude,
        ushort? adc1MaxMagnitude,
        bool transmitterActive,
        long nowMs)
    {
        bool changedWarning = false;
        int? effectiveToApply = null;
        bool newWarning = false;
        int newOffset = 0;

        lock (_sync)
        {
            _lastAdcOverloadBits = overloadBits;
            _lastAdc0MaxMagnitude = adc0MaxMagnitude;
            _lastAdc1MaxMagnitude = adc1MaxMagnitude;
            _lastAdcTelemetryUtc = DateTimeOffset.UtcNow;
            if ((overloadBits & 0x01) != 0 && adc0MaxMagnitude is ushort adc0)
                _adc0MaxMagnitudeAtOverload = adc0;
            if ((overloadBits & 0x02) != 0 && adc1MaxMagnitude is ushort adc1)
                _adc1MaxMagnitudeAtOverload = adc1;

            if (!_state.AutoAttEnabled) return;
            // Local MOX and the radio-reported PTT bit are both authoritative.
            // The latter rejects already-queued TX telemetry after local
            // unkeying, before the first confirmed RX status packet arrives.
            if (_mox
                || transmitterActive
                || (_adcProtectionResumeAfterMs != long.MinValue
                    && nowMs < _adcProtectionResumeAfterMs))
            {
                ResetAdcProtectionWindowNoLock();
                _lastTickMs = long.MinValue;
                return;
            }
            _adcProtectionResumeAfterMs = long.MinValue;

            var cfg = _adcProtection;
            // The single front-panel S-ATT control belongs to the primary RX.
            // Route protection telemetry through that receiver's physical ADC
            // instead of OR-ing unrelated ADCs and attenuating the wrong input.
            byte protectedAdc = ReceiverAdcSource(_state, 0) == 1 ? (byte)1 : (byte)0;
            bool protectedHardOverload = (overloadBits & (1 << protectedAdc)) != 0;
            ushort? protectedMagnitude = protectedAdc == 1
                ? adc1MaxMagnitude
                : adc0MaxMagnitude;
            bool validMagnitude = protectedMagnitude is > 0;
            var zones = AdcMagnitudeZones(cfg.MagnitudeSoftLimit);
            bool magnitudeSoftHit = validMagnitude
                && protectedMagnitude!.Value >= zones.Attack;

            if (protectedHardOverload)
            {
                _overloadSeenInWindow = true;
                _hardOverloadSeenInWindow = true;
            }
            if (magnitudeSoftHit)
            {
                _overloadSeenInWindow = true;
                _softMagnitudeSeenInWindow = true;
            }
            if (validMagnitude)
                _validMagnitudeSeenInWindow = true;
            _maxMagnitudeSeenInWindow = Math.Max(
                _maxMagnitudeSeenInWindow,
                protectedMagnitude ?? 0);

            // The first threat after a quiet period is handled immediately,
            // but every actual attack step shares one cooldown. This prevents
            // threshold chatter (high/low/high packets) from masquerading as
            // repeated first observations and railing the attenuator.
            bool attackCooldownElapsed = _lastAttAttackMs == long.MinValue
                || nowMs - _lastAttAttackMs >= cfg.AttackMs;
            bool immediateAttackDue = (protectedHardOverload || magnitudeSoftHit)
                && attackCooldownElapsed;
            if (_lastTickMs == long.MinValue)
            {
                _lastTickMs = nowMs;
                if (!immediateAttackDue) return;
            }
            else if (!immediateAttackDue)
            {
                int intervalMs = _overloadSeenInWindow ? cfg.AttackMs : cfg.ReleaseMs;
                if (nowMs - _lastTickMs < intervalMs) return;
                _lastTickMs = nowMs;
            }
            else
            {
                _lastTickMs = nowMs;
            }

            bool hardSeen = _hardOverloadSeenInWindow;
            bool softSeen = _softMagnitudeSeenInWindow;
            bool validMagnitudeSeen = _validMagnitudeSeenInWindow;
            ushort maxMagnitudeSeen = _maxMagnitudeSeenInWindow;
            ResetAdcProtectionWindowNoLock();

            bool attackZone = hardSeen || softSeen;
            bool holdZone = !attackZone
                && validMagnitudeSeen
                && maxMagnitudeSeen >= zones.Release;

            if (attackZone)
            {
                _lastOverloadMs = nowMs;
                if (validMagnitudeSeen)
                    _predictiveMagnitudeControlActive = true;
                // Thetis counts +1 per overload poll (console.cs:22107), capped
                // at 5, and decays -1 per clean poll. _adcOverloadLevel mirrors
                // that counter exactly so the gate below matches its >3 timing.
                _adcOverloadLevel = Math.Min(5, _adcOverloadLevel + 1);

                int maxDynamicOffset = Math.Min(
                    cfg.MaxOffsetDb,
                    HpsdrAtten.MaxDb - _atten.ClampedDb);
                if (_attOffsetDb < maxDynamicOffset)
                {
                    int previousOffset = _attOffsetDb;
                    int attackDb = cfg.AttackStepDb;
                    if (validMagnitudeSeen)
                        attackDb = MagnitudeAttackStepDb(
                            maxMagnitudeSeen,
                            zones.Target,
                            attackDb);
                    if (hardSeen)
                        attackDb = Math.Max(4, attackDb);
                    _attOffsetDb = Math.Min(maxDynamicOffset, _attOffsetDb + attackDb);
                    if (_attOffsetDb > previousOffset)
                        _lastAttAttackMs = nowMs;
                }
            }
            else if (holdZone)
            {
                // Hysteresis band: do not pump the attenuator around the attack
                // point. Every in-band observation restarts the release hold.
                _lastOverloadMs = nowMs;
                if (_attOffsetDb > 0)
                    _predictiveMagnitudeControlActive = true;
                if (_adcOverloadLevel > 0) _adcOverloadLevel--;
            }
            else if (!validMagnitudeSeen
                && _predictiveMagnitudeControlActive
                && _attOffsetDb > 0)
            {
                // Once valid P2 magnitude established predictive control, a
                // zero/missing word is loss of telemetry, not evidence of a
                // quiet ADC. Freeze the offset until a valid below-release
                // sample permits controlled release. P1/hard-only offsets do
                // not enter this state and retain their clean-bit fallback.
                if (_adcOverloadLevel > 0) _adcOverloadLevel--;
            }
            else
            {
                if (_adcOverloadLevel > 0) _adcOverloadLevel--;

                // Hold the applied attenuation for ReleaseHoldMs after the last
                // overload before unwinding — Thetis' nudAutoAttHold delay
                // (console.cs:21569). This stops the offset pumping up and down
                // on a signal that hovers right at the ADC ceiling. The level
                // counter still decays above so the lamp clears on schedule.
                bool holdElapsed = _lastOverloadMs == long.MinValue
                    || (nowMs - _lastOverloadMs) >= cfg.ReleaseHoldMs;
                if (holdElapsed && _attOffsetDb > 0)
                    _attOffsetDb = Math.Max(0, _attOffsetDb - cfg.ReleaseStepDb);
                if (_attOffsetDb == 0)
                    _predictiveMagnitudeControlActive = false;
            }

            int effective = Math.Clamp(_atten.ClampedDb + _attOffsetDb, HpsdrAtten.MinDb, HpsdrAtten.MaxDb);
            if (effective != _lastAppliedEffectiveDb)
            {
                _lastAppliedEffectiveDb = effective;
                effectiveToApply = effective;
                // P2 auto-ATT is a protection path, not an ordinary UI state
                // update. Emit the hardware command while the decision and its
                // physical-ADC route are still serialized by _sync. Releasing
                // the lock first would let a newer manual attenuation,
                // auto-off, or ADC-route mutation reach the wire before this
                // stale protection write. Waiting for StateChanged would also
                // route through generic engine work before CmdHighPriority.
                if (_p2Client is { } p2Client)
                {
                    try
                    {
                        ApplyPrimaryAttenuatorToProtocol2Client(
                            p2Client,
                            protectedAdc,
                            effective);
                    }
                    catch (SocketException ex)
                    {
                        // A concurrent disconnect may tear down the UDP socket.
                        // The canonical state and telemetry broadcasts must
                        // still complete; reconnect replay will restore it.
                        _log.LogDebug(ex, "p2.auto_att immediate send failed during socket teardown");
                    }
                    catch (ObjectDisposedException ex)
                    {
                        _log.LogDebug(ex, "p2.auto_att immediate send raced client disposal");
                    }
                }
            }

            // Protection is operator-visible for as long as automatic
            // attenuation remains applied, even after the diagnostic leaky
            // overload counter has decayed.
            bool warn = _attOffsetDb > 0 || _adcOverloadLevel > cfg.WarningThreshold;
            if (warn != _state.AdcOverloadWarning || _attOffsetDb != _state.AttOffsetDb)
            {
                _state = _state with { AttOffsetDb = _attOffsetDb, AdcOverloadWarning = warn };
                changedWarning = true;
                newWarning = warn;
                newOffset = _attOffsetDb;
            }
        }

        if (effectiveToApply is int eff)
        {
            // Protocol 1 retains its existing atomic-state/EP2-rotation path.
            ApplyPrimaryAttenuatorToActiveClient(eff);
        }
        if (changedWarning)
        {
            StateChanged?.Invoke(Snapshot());
            // Debug-level — at 10 Hz this would flood logs if promoted.
            _log.LogDebug("auto-att offset={Offset}dB warn={Warn}", newOffset, newWarning);
        }
    }

    private static async Task TearDownClientAsync(Protocol1Client client, CancellationToken ct = default)
    {
        try { await client.StopAsync(ct).ConfigureAwait(false); } catch { /* best-effort */ }
        try { await client.DisconnectAsync(ct).ConfigureAwait(false); } catch { /* best-effort */ }
        client.Dispose();
    }

    internal static bool TryParseEndpoint(string endpoint, out IPEndPoint result)
    {
        result = null!;
        if (string.IsNullOrWhiteSpace(endpoint)) return false;

        if (IPEndPoint.TryParse(endpoint, out var parsed))
        {
            result = parsed.Port == 0
                ? new IPEndPoint(parsed.Address, DefaultHpsdrPort)
                : parsed;
            return true;
        }

        if (IPAddress.TryParse(endpoint, out var addr))
        {
            result = new IPEndPoint(addr, DefaultHpsdrPort);
            return true;
        }

        return false;
    }

    private static HpsdrSampleRate MapSampleRate(int hz) => hz switch
    {
        48_000 => HpsdrSampleRate.Rate48k,
        96_000 => HpsdrSampleRate.Rate96k,
        192_000 => HpsdrSampleRate.Rate192k,
        384_000 => HpsdrSampleRate.Rate384k,
        768_000 => HpsdrSampleRate.Rate768k,     // P2 only (ANAN G2)
        1_536_000 => HpsdrSampleRate.Rate1536k,  // P2 only (ANAN G2)
        _ => throw new ArgumentException($"Unsupported sample rate {hz}.", nameof(hz)),
    };
}

internal static class HpsdrSampleRateExtensions
{
    public static int SampleRateHz(this HpsdrSampleRate rate) => rate switch
    {
        HpsdrSampleRate.Rate48k => 48_000,
        HpsdrSampleRate.Rate96k => 96_000,
        HpsdrSampleRate.Rate192k => 192_000,
        HpsdrSampleRate.Rate384k => 384_000,
        HpsdrSampleRate.Rate768k => 768_000,
        HpsdrSampleRate.Rate1536k => 1_536_000,
        _ => throw new ArgumentOutOfRangeException(nameof(rate), rate, null),
    };
}
