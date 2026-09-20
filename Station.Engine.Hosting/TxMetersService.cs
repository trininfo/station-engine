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

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Zeus.Contracts;
using Zeus.Dsp;

namespace Zeus.Server;

/// <summary>
/// Consumes raw FWD/REF ADC readings from Protocol1, smooths them with an
/// exponential low-pass, converts to watts + SWR per the HermesLite2
/// calibration, and broadcasts a <see cref="TxMetersV2Frame"/> over the
/// StreamingHub at 10 Hz while MOX is on / 2 Hz when idle.
///
/// PRD FR-6: If SWR > 2.5 sustained for ≥500 ms while MOX or TUN is on,
/// auto-drop MOX/TUN and emit an AlertFrame. Protects the HL2 finals if
/// the antenna goes out of match mid-transmission.
///
/// Math provenance: Thetis <c>console.cs:25008-25072</c> (watts) and
/// <c>console.cs:25972-25978</c> (SWR). Smoothing α matches
/// <c>console.cs:25011,25931</c>.
/// </summary>
public sealed class TxMetersService : BackgroundService
{
    // Thetis uses a 90/10 split on the raw ADC (console.cs:25011).
    private const double SmoothAlpha = 0.90;

    /// <summary>
    /// Raised when TX meter values are updated (approximately 10 Hz during MOX).
    /// Arguments: (fwdWatts, refWatts, swr, alcPk, alcGr)
    /// </summary>
    public event Action<float, float, float, float, float>? TxMetersUpdated;
    /// <summary>Unsmoothened calibrated-bridge ADC pair for bounded analyzer
    /// capture. Handlers run on the radio RX thread and must not block.</summary>
    public event Action<ushort, ushort>? RawPowerTelemetryUpdated;

    // Wire FWD ≤ 2 W as a floor for SWR; below the bridge noise dominates
    // and the ratio is meaningless (Thetis does the same in console.cs:25974).
    private const double SwrMinFwdWatts = 2.0;
    private const double SwrMax = 9.0;

    // PRD FR-6: a single MOX/TUN transmission may not exceed the operator-set
    // limit (default 120 s, range 30..600 — see RadioService.SetTxTimeoutSec).
    // Catches stuck spacebars, jammed buttons, or a client that forgot to
    // unkey. The read is Volatile via the RadioService accessor so an in-flight
    // change from the settings panel takes effect on the next 10 Hz tick.
    internal TimeSpan TxTimeout => TimeSpan.FromSeconds(_radio.TxTimeoutSec);
    private static readonly TimeSpan MoxTick = TimeSpan.FromMilliseconds(100); // 10 Hz
    private static readonly TimeSpan IdleTick = TimeSpan.FromMilliseconds(500); // 2 Hz

    // PA temperature broadcast cadence: 2 Hz regardless of MOX. Temperature
    // is a protection signal (HL2 auto-disables TX at 55 °C) and moves on
    // a seconds timescale, so piggybacking on the 10 Hz MOX tick would be
    // wasted wire. When MOX is off the outer loop already ticks at 500 ms;
    // when MOX is on the outer loop ticks at 100 ms and we throttle the
    // PA broadcast with a last-sent timestamp.
    private static readonly TimeSpan PaTempTick = TimeSpan.FromMilliseconds(500);

    // MCP9700 / TMP36-style sensor on the HL2 Q6 position. Datasheet:
    // V_out = 500 mV + 10 mV/°C * T; ADC is 12-bit against a 3.26 V ref.
    // Derived: tempC = (3.26 * raw / 4096 - 0.5) * 100. See Steve Haynal's
    // hermes-lite wiki for the board-level mapping and reference voltage.
    private const double PaTempAdcRefVolts = 3.26;
    private const int PaTempAdcFullScale = 4096;
    private const double PaTempSensorOffsetVolts = 0.5;
    private const double PaTempSensorVoltsPerDegC = 0.01;

    // Clamp range for the conversion output. Below this the sensor is
    // either unplugged or reading noise floor; above this the reading is
    // well beyond the HL2 gateware's 55 °C shutdown. Broadcasting a
    // clamped value keeps the UI from flashing red during boot while a
    // floating ADC settles.
    private const float PaTempMinC = -40f;
    private const float PaTempMaxC = 125f;
    private const double PaTempWarningC = 50.0;
    private const double PaTempCriticalC = 55.0;
    private static readonly TimeSpan PaTempFreshWindow = TimeSpan.FromSeconds(5);

    private readonly StreamingHub _hub;
    private readonly RadioService _radio;
    private readonly TxService _tx;
    private readonly DspPipelineService _pipe;
    private readonly ILogger<TxMetersService> _log;
    // Per-board calibration is resolved at sample time via
    // RadioCalibrations.For(_radio.ConnectedBoardKind). Mirrors the
    // PaDefaults.GetPaGainDb dispatch seam (per CLAUDE.md, do not
    // special-case inside ComputeMeters). See RadioCalibrations for the
    // dispatch table and the OrionMkII / ANAN-8000D caveat.

    private readonly object _sync = new();
    private double _fwdAdc;
    private double _refAdc;
    // Peak-hold ADC tracking between publish ticks. The radio sends
    // hi-priority status at hundreds of Hz on P2 (G2 MkII observed at ~820
    // pkts/s) but we publish to the UI at only 10 Hz; voice peaks lasting
    // 100-200 ms fall between publish ticks and the bar reads the
    // inter-syllable average (~3× low vs an analog peak-reading wattmeter
    // like an LP-100A). Tracking max ADC seen since last publish gives the
    // UI a meter that catches transients the way a hardware peak-reading
    // wattmeter does. Reset to 0 every publish tick.
    private ushort _fwdAdcPeak;
    private ushort _refAdcPeak;
    // PA temperature smoothed ADC and first-sample flag — separate from
    // _seenSample because temperature arrives on a different slot and may
    // show up before / after the FWD-REF pair on any given packet.
    private double _paTempAdc;
    private bool _seenPaTempSample;
    // Last info[5] seen, so a new calcc fit can be told from a repeat and
    // the PS curve frame only goes out when there is a new curve behind it.
    // -1 rather than 0 so the first fit of a session counts as an edge.
    private int _lastPsCalibrationAttempts = -1;
    private uint _psCurveSeq;
    private DateTimeOffset? _paTempUpdatedUtc;
    private bool _seenSample;
    private ushort _latestRawFwd;
    private ushort _latestRawRef;
    private bool _seenRawFwd;
    private bool _seenRawRef;
    // Last time a PaTempFrame was broadcast, so the 10 Hz MOX loop can
    // throttle itself down to the 2 Hz PA cadence without a separate timer.
    private DateTime _lastPaTempBroadcastAtUtc = DateTime.MinValue;

    // Diagnostic counters for the 1 Hz tx.meters.diag log emitted while
    // MOX/TUN is on. Bumped under _sync from OnTelemetry / OnTelemetryRaw /
    // ApplyPaTempSmoothed; reset under _sync after each emit. The "last"
    // values capture the most recent raw sample on each axis so a quiet
    // tick (count=0) still shows the last value the radio sent.
    private uint _diagFwdSlotCount;
    private uint _diagRefSlotCount;
    private uint _diagPaTempSlotCount;
    private ushort _diagLastFwdAin1;
    private ushort _diagLastFwdAin0;
    private ushort _diagLastRefAin0;
    private DateTime _lastDiagLogAtUtc = DateTime.MinValue;

    public TxMetersService(StreamingHub hub, RadioService radio, TxService tx, DspPipelineService pipe, ILogger<TxMetersService> log)
    {
        _hub = hub;
        _radio = radio;
        _tx = tx;
        _pipe = pipe;
        _log = log;
        // Bind to the radio's connection lifecycle so we subscribe to telemetry
        // on every fresh Protocol1Client instance. The event is one-way
        // (Protocol1 → Server) and carries only AIN readings.
        radio.Connected += OnConnected;
        radio.Disconnected += OnDisconnected;
        // Mirror the same subscribe/detach dance for Protocol-2 so
        // hi-priority status (UDP 1025) feeds the same FWD/REF smoothing
        // path as P1 alex telemetry. Issue #174 — without this hook, a
        // G2 / ANAN-class radio's TX power meter sits at zero.
        radio.P2Connected += OnP2Connected;
        radio.P2Disconnected += OnP2Disconnected;
    }

    // Holds the last subscribed client so OnDisconnected can detach the
    // TelemetryReceived handler once the Protocol1 surface lands.
    private Zeus.Protocol1.IProtocol1Client? _subscribedClient;
    // Same idea, P2 side. Tracked separately so a P1 disconnect doesn't
    // accidentally detach a P2 handler (the two protocols can't be live at
    // the same time, but the events are independent and this keeps the
    // coupling clean).
    private Zeus.Protocol2.Protocol2Client? _subscribedP2Client;

    // HL2 C&C-echo addresses that carry the alex FWD/REF ADCs and the PA
    // temperature (see TelemetryReading docs in Zeus.Protocol1).
    // addr=1 (C0=0x08): Ain0 = HL2 PA temperature;   Ain1 = alex_forward_power
    // addr=2 (C0=0x10): Ain0 = alex_reverse_power;   Ain1 = ADC0 bias
    // Match on bits 4:1 only — C0[0] is the PTT/MOX echo (so a live TX packet
    // arrives as 0x09/0x11), and C0[7] is the HL2 IOB ACK
    // marker which PacketParser already filters out via the addr==1|2|3 gate.
    private const byte C0AddrMask = 0x7E;
    private const byte C0AddrAlexFwd = 0x08;
    private const byte C0AddrAlexRef = 0x10;

    /// <summary>
    /// Entry point for telemetry consumers. FWD+temperature share the 0x08
    /// slot (Ain1 / Ain0 respectively); REF arrives on 0x10 (Ain0). One
    /// <see cref="Zeus.Protocol1.TelemetryReading"/> may update multiple axes
    /// on the 0x08 slot (FWD + temperature) but never more than one slot's
    /// worth per call — packets carry the echo-slot map, not a combined blob.
    /// </summary>
    public void OnTelemetry(Zeus.Protocol1.TelemetryReading reading)
    {
        switch (reading.C0Address & C0AddrMask)
        {
            case C0AddrAlexFwd:
                _latestRawFwd = reading.Ain1;
                _seenRawFwd = true;
                ApplySmoothed(ref _fwdAdc, reading.Ain1);
                TrackPeak(ref _fwdAdcPeak, reading.Ain1);
                // Ain0 on this slot is the HL2 Q6 temperature ADC. Smooth
                // with the same α as FWD/REF so the UI sees a stable reading
                // instead of ADC jitter.
                ApplyPaTempSmoothed(reading.Ain0);
                lock (_sync)
                {
                    _diagFwdSlotCount++;
                    _diagPaTempSlotCount++;
                    _diagLastFwdAin1 = reading.Ain1;
                    _diagLastFwdAin0 = reading.Ain0;
                }
                break;
            case C0AddrAlexRef:
                _latestRawRef = reading.Ain0;
                _seenRawRef = true;
                ApplySmoothed(ref _refAdc, reading.Ain0);
                TrackPeak(ref _refAdcPeak, reading.Ain0);
                lock (_sync)
                {
                    _diagRefSlotCount++;
                    _diagLastRefAin0 = reading.Ain0;
                }
                break;
            default:
                // Other echo slots (ADC bias, exciter/temp) aren't part of the
                // FWD/REF meter pair. Silently ignored — protection/alerts
                // are a later slice.
                break;
        }
        if (_seenRawFwd && _seenRawRef)
        {
            ushort fwd = _latestRawFwd;
            ushort reverse = _latestRawRef;
            _seenRawFwd = false;
            _seenRawRef = false;
            RawPowerTelemetryUpdated?.Invoke(fwd, reverse);
        }
    }

    // Overload kept for unit tests that want to drive both axes simultaneously
    // without constructing two TelemetryReading structs. Also the P2 ingress
    // point — both axes arrive in the same hi-priority status packet.
    internal void OnTelemetryRaw(ushort fwdAdc, ushort refAdc)
    {
        ApplySmoothed(ref _fwdAdc, fwdAdc);
        ApplySmoothed(ref _refAdc, refAdc);
        TrackPeak(ref _fwdAdcPeak, fwdAdc);
        TrackPeak(ref _refAdcPeak, refAdc);
        lock (_sync)
        {
            _diagFwdSlotCount++;
            _diagRefSlotCount++;
            _diagLastFwdAin1 = fwdAdc;
            _diagLastRefAin0 = refAdc;
        }
        RawPowerTelemetryUpdated?.Invoke(fwdAdc, refAdc);
    }

    // Hold the highest raw ADC seen since the last publish-tick reset. Called
    // on every incoming telemetry sample. Lock-free against the publish reader
    // because the writer side runs only on the radio RX thread; cross-thread
    // visibility is taken care of by the _sync lock used at publish time.
    private void TrackPeak(ref ushort state, ushort raw)
    {
        if (raw > state) state = raw;
    }

    private void ApplySmoothed(ref double state, ushort raw)
    {
        lock (_sync)
        {
            if (!_seenSample)
            {
                // First-sample fast path matches Thetis console.cs:25011 — seed
                // both axes so the UI doesn't ramp up from zero across the
                // ~2 s alpha=0.90 settling time.
                _fwdAdc = raw;
                _refAdc = raw;
                state = raw;
                _seenSample = true;
                return;
            }
            state = SmoothAlpha * state + (1.0 - SmoothAlpha) * raw;
        }
    }

    // Same α as FWD/REF (0.90 / 0.10) so the temperature reading settles at
    // the same timescale the operator is already used to for protection
    // signals. Tracked separately from _seenSample because the temperature
    // arrives on the same slot as FWD but via a different AIN pair; seeding
    // it on the first sample avoids a ~2 s ramp from zero.
    internal void ApplyPaTempSmoothed(ushort raw)
    {
        lock (_sync)
        {
            if (!_seenPaTempSample)
            {
                _paTempAdc = raw;
                _seenPaTempSample = true;
                _paTempUpdatedUtc = DateTimeOffset.UtcNow;
                return;
            }
            _paTempAdc = SmoothAlpha * _paTempAdc + (1.0 - SmoothAlpha) * raw;
            _paTempUpdatedUtc = DateTimeOffset.UtcNow;
        }
    }

    /// <summary>
    /// Convert a raw HL2 Q6-sensor ADC reading to °C, clamped into the
    /// plausible physical range so a floating ADC or disconnected sensor
    /// can't trip the UI's 55 °C red zone at boot. Pure function — exposed
    /// <c>internal</c> for unit tests. Formula derivation:
    /// MCP9700-class sensor, V_out = 500 mV + 10 mV/°C · T, 12-bit ADC
    /// against a 3.26 V reference; see the hermes-lite wiki / Steve
    /// Haynal's HL2 docs for the board-level mapping.
    /// </summary>
    internal static float ConvertPaTempAdcToCelsius(double rawAdc)
    {
        double volts = rawAdc * PaTempAdcRefVolts / PaTempAdcFullScale;
        double tempC = (volts - PaTempSensorOffsetVolts) / PaTempSensorVoltsPerDegC;
        if (tempC < PaTempMinC) tempC = PaTempMinC;
        if (tempC > PaTempMaxC) tempC = PaTempMaxC;
        return (float)tempC;
    }

    public RadioPaThermalDiagnosticsDto PaThermalSnapshot()
    {
        var state = _radio.Snapshot();
        var connected = _radio.ConnectedBoardKind;
        var effective = _radio.EffectiveBoardKind;
        var variant = _radio.EffectiveOrionMkIIVariant;
        bool p1Active = _subscribedClient is not null || _radio.ActiveClient is not null;
        bool p2Active = IsProtocol2Active(state, connected);
        bool g2Class = effective == HpsdrBoardKind.OrionMkII && variant == OrionMkIIVariant.G2;
        bool hl2Class = effective == HpsdrBoardKind.HermesLite2 || connected == HpsdrBoardKind.HermesLite2;

        bool seen;
        double rawAdc;
        DateTimeOffset? updatedUtc;
        lock (_sync)
        {
            seen = _seenPaTempSample;
            rawAdc = _paTempAdc;
            updatedUtc = _paTempUpdatedUtc;
        }

        var now = DateTimeOffset.UtcNow;
        long? ageMs = updatedUtc is { } ts
            ? Math.Max(0L, (long)(now - ts).TotalMilliseconds)
            : null;
        double? tempC = seen ? Round(ConvertPaTempAdcToCelsius(rawAdc), 1) : null;

        bool decoded = seen || hl2Class;
        bool supported = seen || hl2Class || g2Class;
        bool available = seen;
        string? activeProtocol = p1Active ? "P1" : p2Active ? "P2" : state.Status == ConnectionStatus.Connected ? "unknown" : null;
        string source;
        string status;
        string recommendation;

        if (g2Class && p2Active)
        {
            source = "p2-g2-temperature-slot-unmapped";
            status = "p2-g2-temp-unmapped";
            decoded = false;
            available = false;
            tempC = null;
            ageMs = null;
            updatedUtc = null;
            recommendation = "The G2 manual confirms current, voltage, temperature sensors and fan cooling, but Zeus has not mapped the G2 Protocol-2 PA-temperature word yet. Capture P2 diagnostics markers during TX warm-up and fan transitions before arming thermal inhibit; use RF, SWR, supply, and duty-cycle telemetry as live protection evidence meanwhile.";
        }
        else if (seen)
        {
            source = "p1-hl2-c0-0x08-ain0";
            status = ThermalStatus(tempC.GetValueOrDefault(), ageMs);
            recommendation = status == "stale"
                ? "PA temperature was decoded from the Protocol-1 HL2 C&C echo slot, but the sample is stale; verify the telemetry stream before relying on thermal guidance."
                : "PA temperature is decoded from the Protocol-1 HL2 C&C echo slot and can be correlated with SWR, duty cycle, and fan behavior during TX tests.";
        }
        else if (g2Class)
        {
            source = "p2-g2-temperature-slot-unmapped";
            status = "p2-g2-temp-unmapped";
            decoded = false;
            recommendation = "The G2 manual confirms current, voltage, temperature sensors and fan cooling, but Zeus has not mapped the G2 Protocol-2 PA-temperature word yet. Capture P2 diagnostics markers during TX warm-up and fan transitions before arming thermal inhibit; use RF, SWR, supply, and duty-cycle telemetry as live protection evidence meanwhile.";
        }
        else if (hl2Class)
        {
            source = "p1-hl2-c0-0x08-ain0";
            status = "waiting-for-p1-temp";
            recommendation = "HL2 PA temperature decoding is available, but no C&C temperature sample has arrived yet; keep the radio connected and verify the P1 telemetry stream.";
        }
        else
        {
            source = "unavailable";
            status = "unsupported";
            decoded = false;
            recommendation = "This board does not currently have a mapped PA-temperature telemetry path in Zeus; PA protection should rely on SWR, timeout, RF power, supply, and duty-cycle diagnostics.";
        }

        return new(
            SchemaVersion: 1,
            ActiveProtocol: activeProtocol,
            ConnectedBoard: connected.ToString(),
            EffectiveBoard: effective.ToString(),
            OrionMkIIVariant: variant.ToString(),
            SupportsTemperatureTelemetry: supported,
            TemperatureDecoded: decoded,
            TemperatureAvailable: available,
            Source: source,
            Status: status,
            TempC: tempC,
            RawAdc: available ? Round(rawAdc, 1) : null,
            AgeMs: ageMs,
            LastUpdatedUtc: updatedUtc,
            WarningTempC: PaTempWarningC,
            CriticalTempC: PaTempCriticalC,
            ManualReference: "ANAN G2 manual V1.4: PA/supply feature list, specifications, and cooling guidance document current, voltage, temperature sensors, thermally compensated bias, and internal/external fan support.",
            DiagnosticRecommendation: recommendation,
            GeneratedUtc: now);
    }

    private bool IsProtocol2Active(StateDto state, HpsdrBoardKind connected)
        => _subscribedP2Client is not null
           || (state.Status == ConnectionStatus.Connected
               && _radio.ActiveClient is null
               && connected != HpsdrBoardKind.Unknown);

    private static string ThermalStatus(double tempC, long? ageMs)
    {
        if (ageMs is null || ageMs.Value > PaTempFreshWindow.TotalMilliseconds) return "stale";
        if (tempC >= PaTempCriticalC) return "critical";
        if (tempC >= PaTempWarningC) return "warm";
        return "fresh";
    }

    private static double Round(double value, int digits) =>
        double.IsFinite(value) ? Math.Round(value, digits) : 0.0;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // TX timeout guard: PRD FR-6 caps a single TX at the operator-
                // set limit (default 120 s) to catch stuck spacebar / jammed
                // PTT. TxService hands off the trip via the same AlertFrame
                // path as SWR so the client only needs one protection-event
                // listener.
                var timeoutNow = DateTime.UtcNow;
                if (EvaluateTimeoutTrip(timeoutNow) is { } timeoutReason)
                {
                    _tx.TryTripForAlert(AlertKind.TxTimeout, timeoutReason);
                }
                else if (EvaluateTimeoutWarning(timeoutNow) is { } warning)
                {
                    _hub.Broadcast(new AlertFrame(AlertKind.TxTimeoutWarning, warning));
                }

                // Meter during MOX *or* TUN — both drive the PA, both need live
                // FWD/SWR readouts. Idle frame is only for fully-unkeyed RX.
                bool mox = _tx.IsMoxOn || _tx.IsTunOn;
                TxMetersV2Frame frame;
                double swr = 1.0;
                if (mox)
                {
                    double fwdAdc, refAdc, fwdAdcSmoothed, refAdcSmoothed;
                    ushort fwdAdcPeak, refAdcPeak;
                    lock (_sync)
                    {
                        // Use the peak ADC seen since the previous publish
                        // tick rather than the smoothed value — voice peaks
                        // are 100-200 ms but our publish cadence is 100 ms,
                        // and a hardware peak-reading wattmeter (LP-100A) is
                        // what the operator compares against. Fall back to
                        // the smoothed value if no new sample arrived in
                        // this tick (radio quiescence) so the bar doesn't
                        // collapse to zero between hi-pri packets.
                        fwdAdcPeak = _fwdAdcPeak;
                        refAdcPeak = _refAdcPeak;
                        fwdAdcSmoothed = _fwdAdc;
                        refAdcSmoothed = _refAdc;
                        fwdAdc = fwdAdcPeak > 0 ? fwdAdcPeak : _fwdAdc;
                        refAdc = refAdcPeak > 0 ? refAdcPeak : _refAdc;
                        _fwdAdcPeak = 0;
                        _refAdcPeak = 0;
                    }
                    var cal = RadioCalibrations.For(_radio.ConnectedBoardKind, _radio.EffectiveOrionMkIIVariant);
                    long hardwareTxHz = _radio.ToHardwareFrequencyHz(
                        RadioFrequencyResolver.TxFrequencyHz(_radio.Snapshot()));
                    bool sixMeters = BandUtils.FreqToBand(hardwareTxHz) == "6m";
                    // Watts use peak-hold (matches LP-100A peak-reading
                    // wattmeter). SWR uses the smoothed ADC pair instead:
                    // peak(FWD) vs peak(REF) are two uncorrelated transient
                    // maxima and their ratio is not a physical standing-wave
                    // ratio — on HL2 both rails can saturate together during
                    // PA-on / LPF-relay transients, with REF clamping a few
                    // counts above FWD, which the trip logic reads as 9:1.
                    var (fwdW, refW, _) = ComputeMeters(fwdAdc, refAdc, cal, sixMeters);
                    var (_, _, swrVal) = ComputeMeters(fwdAdcSmoothed, refAdcSmoothed, cal, sixMeters);
                    swr = swrVal;
                    // Stage meters are published by WdspDspEngine.ProcessTxBlock;
                    // may lag the first TX block by a few ticks at MOX-on, which
                    // reads as "Silent" (−∞ level / 0 GR) — UI treats as empty.
                    var stage = _pipe.CurrentEngine?.GetTxStageMeters() ?? TxStageMeters.Silent;
                    frame = BuildFrame((float)fwdW, (float)refW, (float)swr, stage);

                    EmitTxMetersDiag(
                        DateTime.UtcNow,
                        fwdAdc, refAdc,
                        fwdAdcSmoothed, refAdcSmoothed,
                        fwdAdcPeak, refAdcPeak,
                        fwdW, refW, swr,
                        cal);

                    bool isTun = _tx.IsTunOn;
                    DateTime keyedAt = (isTun ? _tx.TunStartedAt : _tx.MoxStartedAt) ?? DateTime.UtcNow;
                    // Tune drive gates the low-power TUN SWR bypass; irrelevant
                    // to MOX so only read it while tuning.
                    int tuneDrivePct = isTun ? _radio.Snapshot().TunePct : 100;
                    // P3 display-only (2026-07): under Protocol 3 the FWD/REF ADCs
                    // feeding this meter arrive from the sidecar board-health
                    // telemetry (Protocol3SidecarFrameForwarder), whose raw scale
                    // has NOT yet been validated against the G2 Protocol-2
                    // calibration that ComputeMeters assumes. Show the watts/SWR
                    // readout, but do NOT let an unvalidated SWR figure auto-drop
                    // the PA — a miscalibrated ratio could nuisance-trip or, worse,
                    // fail to protect. The TX-timeout guard above still runs under
                    // P3. Re-enable this trip once the P3 raw→watts calibration is
                    // confirmed against a known load.
                    if (!_radio.IsProtocol3Active
                        && EvaluateSwrTrip(swr, DateTime.UtcNow, isTun, keyedAt, fwdW, tuneDrivePct) is { } tripReason)
                    {
                        // Log the inputs the trip decision was actually made on.
                        // The operator-facing alert text carries only the SWR and
                        // the sustain window, and TxService's tx.trip line adds
                        // only kind/reason/faultEpoch — so when issue #1659 and
                        // then #2275 arrived from the same operator on the same
                        // radio, neither report could say which guard rejected the
                        // tune cycle, and the root cause had to be inferred twice.
                        // These are the four values that decide it. Keep them on
                        // the WARN ring so they survive to the next bug report.
                        _log.LogWarning(
                            "tx.trip.swr.inputs intent={Intent} swr={Swr:F2} fwdW={FwdWatts:F2} " +
                            "refW={RefWatts:F2} tunePct={TuneDrivePercent} board={Board} " +
                            "bypassMaxW={BypassMax:F1} bypassMaxDrive={BypassDrive}",
                            isTun ? "TUN" : "MOX",
                            swr,
                            fwdW,
                            refW,
                            tuneDrivePct,
                            _radio.ConnectedBoardKind,
                            EngineTransmitSafetyModule.SwrTripTunBypassMaxFwdWatts,
                            EngineTransmitSafetyModule.SwrTripTunBypassMaxDrivePercent);

                        // TryTripForAlert is idempotent — a second caller on the
                        // same tick (e.g. timeout firing concurrently) finds MOX
                        // already off and no-ops.
                        _tx.TryTripForAlert(AlertKind.SwrTrip, tripReason);
                    }
                }
                else
                {
                    // Zero the TX fields while idle so the UI doesn't latch a
                    // stale pre-unkey reading. Stage meters go to Silent (−∞)
                    // so the diagnostic strip renders empty instead of latching
                    // last-during-TX values.
                    frame = BuildFrame(0f, 0f, 1.0f, TxStageMeters.Silent);
                    // Clear the trip timer when not keyed so a brief spike doesn't
                    // carry over into the next TX.
                    _tx.Safety.ObserveConfirmedIdle();
                }

                _hub.Broadcast(frame);

                // Raise TCI meter event (FWD, REF, SWR, ALC peak, ALC GR)
                TxMetersUpdated?.Invoke(frame.FwdWatts, frame.RefWatts, frame.Swr, frame.AlcPk, frame.AlcGr);

                // PureSignal stage meters — broadcast only while PsEnabled is
                // armed so idle wire stays quiet. The engine returns
                // `PsStageMeters.Silent` when PS is off, so we double-gate on
                // both the StateDto bit and the engine view to avoid emitting
                // a frame between the operator arming PS and the engine
                // applying it.
                var snap = _radio.Snapshot();
                if (snap.PsEnabled && _pipe.CurrentEngine is IDspEngine ps)
                {
                    var psm = ps.GetPsStageMeters();
                    var psFrame = new PsMetersFrame(
                        FeedbackLevel: psm.FeedbackLevel,
                        CorrectionDb: psm.CorrectionDb,
                        CalState: psm.CalState,
                        Correcting: psm.Correcting,
                        MaxTxEnvelope: psm.MaxTxEnvelope);
                    _hub.Broadcast(psFrame);
                    // Mirror the live read-out into the StateDto so REST/state
                    // pollers see it too — same pattern PA/Mic meters use.
                    _radio.UpdatePsLiveReadout(psm.FeedbackLevel, psm.CalState, psm.Correcting);

                    // Correction curves, on a fit edge only. calcc rewrites
                    // its display buffers at the end of an accepted calc()
                    // pass and at no other time, so info[5] moving is exactly
                    // the signal that there is something new to send. A
                    // 10 KB frame on a timer would be the same curve over
                    // and over.
                    if (psm.CalibrationAttempts != _lastPsCalibrationAttempts)
                    {
                        _lastPsCalibrationAttempts = psm.CalibrationAttempts;
                        try
                        {
                            if (ps.GetPsCurve() is { } curve)
                            {
                                _hub.Broadcast(new PsCurveFrame(
                                    Seq: unchecked(_psCurveSeq++),
                                    TsUnixMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                                    Points: checked((ushort)curve.Points),
                                    PhsRefDeg: curve.PhsRefDeg,
                                    CalibrationAttempts: (uint)Math.Max(0, curve.CalibrationAttempts),
                                    MagCorrection: curve.MagCorrection,
                                    PhaseDeg: curve.PhaseDeg,
                                    ScatterX: curve.ScatterX,
                                    ScatterMag: curve.ScatterMag,
                                    ScatterPhaseDeg: curve.ScatterPhaseDeg));
                            }
                        }
                        catch (Exception ex)
                        {
                            // A curve is a nicety; the meter loop is not. Never
                            // let a display buffer take the 10 Hz tick down.
                            _log.LogDebug(ex, "ps.curve broadcast failed");
                        }
                    }
                }

                // PA temperature broadcast — 2 Hz always, throttled against
                // wall-clock so the 10 Hz MOX loop emits it every 5th tick
                // and the 2 Hz idle loop emits it every tick. Suppressed
                // until at least one telemetry sample has landed; a fresh
                // client would otherwise see a garbage ADC of 0 mapped to
                // the -40 °C clamp floor.
                var nowUtc = DateTime.UtcNow;
                bool paSeen;
                double paAdc;
                lock (_sync) { paSeen = _seenPaTempSample; paAdc = _paTempAdc; }
                if (paSeen && nowUtc - _lastPaTempBroadcastAtUtc >= PaTempTick)
                {
                    _lastPaTempBroadcastAtUtc = nowUtc;
                    _hub.Broadcast(new PaTempFrame(ConvertPaTempAdcToCelsius(paAdc)));
                }

                try { await Task.Delay(mox ? MoxTick : IdleTick, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
            }
        }
        catch (OperationCanceledException) { /* shutdown */ }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "tx.meters broadcast loop exited with error");
        }
    }

    // Once-per-second diagnostic line emitted while MOX/TUN is on. Captures
    // raw + smoothed + peak ADC for both axes, the per-second telemetry slot
    // counts, and the watts/SWR the meter just published. Purpose: tell us
    // whether the radio is sending FWD/REF samples at all, what raw ADC
    // values they carry, and whether the calibration math is what's
    // collapsing the reading to ~0 W. INFO level so operators can capture it
    // without enabling debug logging; silent during RX.
    private void EmitTxMetersDiag(
        DateTime nowUtc,
        double fwdAdc, double refAdc,
        double fwdAdcSmoothed, double refAdcSmoothed,
        ushort fwdAdcPeak, ushort refAdcPeak,
        double fwdW, double refW, double swr,
        RadioCalibration cal)
    {
        if (nowUtc - _lastDiagLogAtUtc < TimeSpan.FromSeconds(1)) return;
        uint fwdCount, refCount, paTempCount;
        ushort lastFwdAin1, lastFwdAin0, lastRefAin0;
        bool seenFwdRef, seenPaTemp;
        double paTempAdcLast;
        lock (_sync)
        {
            fwdCount = _diagFwdSlotCount;
            refCount = _diagRefSlotCount;
            paTempCount = _diagPaTempSlotCount;
            lastFwdAin1 = _diagLastFwdAin1;
            lastFwdAin0 = _diagLastFwdAin0;
            lastRefAin0 = _diagLastRefAin0;
            seenFwdRef = _seenSample;
            seenPaTemp = _seenPaTempSample;
            paTempAdcLast = _paTempAdc;
            _diagFwdSlotCount = 0;
            _diagRefSlotCount = 0;
            _diagPaTempSlotCount = 0;
        }
        _lastDiagLogAtUtc = nowUtc;
        _log.LogInformation(
            "tx.meters.diag board={Board} cal=fwdBridge{FwdBridge:F2}/refBridge{RefBridge:F2}/ref{Ref:F1}/fwdOff{FwdOff}/refOff{RefOff} " +
            "fwdSlot/s={FwdCount} refSlot/s={RefCount} paTempSlot/s={PaTempCount} " +
            "lastFwdAin1={LastFwdAin1} lastFwdAin0={LastFwdAin0} lastRefAin0={LastRefAin0} " +
            "fwdAdcSm={FwdSm:F0} refAdcSm={RefSm:F0} fwdAdcPk={FwdPk} refAdcPk={RefPk} " +
            "fwdAdcUsed={FwdUsed:F0} refAdcUsed={RefUsed:F0} " +
            "fwdW={FwdW:F2} refW={RefW:F2} swr={Swr:F2} " +
            "seenFwdRef={SeenFR} seenPaTemp={SeenPT} paTempAdc={PaTempAdc:F0}",
            _radio.ConnectedBoardKind,
            cal.BridgeVolt, cal.ReverseBridgeVolt, cal.RefVoltage,
            cal.AdcCalOffset, cal.ReverseAdcCalOffset,
            fwdCount, refCount, paTempCount,
            lastFwdAin1, lastFwdAin0, lastRefAin0,
            fwdAdcSmoothed, refAdcSmoothed, fwdAdcPeak, refAdcPeak,
            fwdAdc, refAdc,
            fwdW, refW, swr,
            seenFwdRef, seenPaTemp, paTempAdcLast);
    }

    /// <summary>
    /// Evaluate the per-mode SWR sustain window and return the operator-facing
    /// trip message when the active-mode threshold has been exceeded for the
    /// active-mode sustain duration, or null if not yet. Honours the per-mode
    /// startup-grace window after the keying edge so a bridge-settle transient
    /// at MOX/TUN-on does not arm the trip before the load has stabilised.
    /// Exposed as <c>internal</c> so unit tests can drive synthetic timestamps
    /// without a <see cref="DateTime"/> abstraction — the production caller
    /// passes <see cref="DateTime.UtcNow"/>. Firing resets the timer so the
    /// caller gets exactly one trip per sustained excursion.
    /// </summary>
    // fwdWatts/tuneDrivePercent feed the low-power TUN SWR bypass in
    // EngineTransmitSafetyModule. They default to fail-safe "high power"
    // sentinels so a caller that omits them never accidentally suppresses
    // protection; the live meter loop always supplies the measured values.
    internal string? EvaluateSwrTrip(
        double swr,
        DateTime now,
        bool isTun,
        DateTime keyedAt,
        double fwdWatts = double.MaxValue,
        int tuneDrivePercent = 100)
    {
        var decision = _tx.Safety.ObserveProtection(new ProtectionSample(
            ProtectionEvidenceState.Available,
            swr,
            now,
            isTun ? TransmitIntent.Tun : TransmitIntent.Mox,
            keyedAt,
            TimeoutSeconds: 0,
            FwdWatts: fwdWatts,
            TuneDrivePercent: tuneDrivePercent));
        return decision.Trip ? decision.OperatorText : null;
    }

    /// <summary>
    /// PRD FR-6 TX timeout: returns a trip reason if MOX or TUN has been
    /// continuously on for ≥ <see cref="TxTimeout"/>, else null. Reads the
    /// keyed-at timestamps from <see cref="TxService"/> (which records them
    /// on state transitions) so the check is stateless in this class.
    /// </summary>
    internal string? EvaluateTimeoutTrip(DateTime now)
    {
        // 0 = operator disabled the guard entirely (issue #1270). Bail before
        // any comparison — a zero TxTimeout would otherwise satisfy the
        // >= check on the first tick and trip instantly.
        if (_radio.TxTimeoutSec <= 0) return null;
        var tunStart = _tx.TunStartedAt;
        var moxStart = _tx.MoxStartedAt;
        DateTime? keyedAt = tunStart ?? moxStart;
        if (keyedAt is null) return null;
        var intent = tunStart is not null ? TransmitIntent.Tun : TransmitIntent.Mox;
        var decision = _tx.Safety.ObserveTimeout(
            now,
            intent,
            keyedAt.Value,
            _radio.TxTimeoutSec);
        return decision.Trip ? decision.OperatorText : null;
    }

    /// <summary>
    /// Issue #1270 TX-timeout pre-warning: returns a heads-up message when the
    /// current MOX/TUN transmission is within the warning lead window of the
    /// trip, else null. Fires exactly once per keyed transmission — the fired-
    /// for timestamp is remembered until a new keyed edge (or a trip) resets it
    /// so the operator sees the amber banner once and can un-key or dismiss.
    /// </summary>
    internal string? EvaluateTimeoutWarning(DateTime now)
    {
        var moxStart = _tx.MoxStartedAt;
        var tunStart = _tx.TunStartedAt;
        DateTime? keyedAt = tunStart ?? moxStart;
        TransmitIntent? intent = tunStart is not null
            ? TransmitIntent.Tun
            : moxStart is not null ? TransmitIntent.Mox : null;
        return _tx.Safety.ObserveTimeoutWarning(now, keyedAt, intent, _radio.TxTimeoutSec);
    }

    /// <summary>
    /// Compose a <see cref="TxMetersV2Frame"/> from the protection readings
    /// (FWD/REF/SWR) and the latest stage-meter snapshot. Kept as a small
    /// helper so the MOX and idle branches in <see cref="ExecuteAsync"/>
    /// stay symmetric and a future v3 frame is a one-line change. Pure
    /// function — no instance state.
    /// </summary>
    internal static TxMetersV2Frame BuildFrame(float fwdW, float refW, float swr, TxStageMeters stage)
        => new(
            FwdWatts: fwdW,
            RefWatts: refW,
            Swr: swr,
            MicPk: stage.MicPk,
            MicAv: stage.MicAv,
            EqPk: stage.EqPk,
            EqAv: stage.EqAv,
            LvlrPk: stage.LvlrPk,
            LvlrAv: stage.LvlrAv,
            LvlrGr: stage.LvlrGr,
            CfcPk: stage.CfcPk,
            CfcAv: stage.CfcAv,
            CfcGr: stage.CfcGr,
            CompPk: stage.CompPk,
            CompAv: stage.CompAv,
            AlcPk: stage.AlcPk,
            AlcAv: stage.AlcAv,
            AlcGr: stage.AlcGr,
            OutPk: stage.OutPk,
            OutAv: stage.OutAv);

    /// <summary>
    /// Port of Thetis <c>console.cs:25008-25072</c> watts math plus the
    /// <c>console.cs:25972-25978</c> SWR ratio. Exposed for unit tests —
    /// pure function, no state.
    /// </summary>
    public static (double FwdWatts, double RefWatts, double Swr) ComputeMeters(
        double fwdAdc, double refAdc, RadioCalibration cal, bool sixMeters = false)
    {
        double fwdV = (fwdAdc - cal.AdcCalOffset) / 4095.0 * cal.RefVoltage;
        double refV = (refAdc - cal.ReverseAdcCalOffset) / 4095.0 * cal.RefVoltage;
        double fwdW = fwdV * fwdV / cal.BridgeVolt;
        double reverseBridgeVolt = sixMeters
            ? cal.SixMeterReverseBridgeVolt
            : cal.ReverseBridgeVolt;
        double refW = refV * refV / reverseBridgeVolt;
        if (fwdW < 0 || double.IsNaN(fwdW)) fwdW = 0;
        if (refW < 0 || double.IsNaN(refW)) refW = 0;

        double swr;
        if (fwdW <= SwrMinFwdWatts)
        {
            swr = 1.0;
        }
        else
        {
            double ratio = refW / fwdW;
            if (ratio < 0) ratio = 0;
            if (ratio >= 1.0)
            {
                swr = SwrMax;
            }
            else
            {
                double rho = Math.Sqrt(ratio);
                double s = (1.0 + rho) / (1.0 - rho);
                if (double.IsNaN(s) || double.IsInfinity(s)) swr = SwrMax;
                else swr = Math.Min(s, SwrMax);
            }
        }

        return (fwdW, refW, swr);
    }

    private void OnConnected(Zeus.Protocol1.IProtocol1Client client)
    {
        _subscribedClient = client;
        client.TelemetryReceived += OnTelemetry;
    }

    private void OnDisconnected()
    {
        var client = _subscribedClient;
        _subscribedClient = null;
        if (client is not null) client.TelemetryReceived -= OnTelemetry;
        lock (_sync)
        {
            _fwdAdc = 0;
            _refAdc = 0;
            _fwdAdcPeak = 0;
            _refAdcPeak = 0;
            _paTempAdc = 0;
            _seenPaTempSample = false;
            _paTempUpdatedUtc = null;
            _seenSample = false;
            _latestRawFwd = 0;
            _latestRawRef = 0;
            _seenRawFwd = false;
            _seenRawRef = false;
            _diagFwdSlotCount = 0;
            _diagRefSlotCount = 0;
            _diagPaTempSlotCount = 0;
            _diagLastFwdAin1 = 0;
            _diagLastFwdAin0 = 0;
            _diagLastRefAin0 = 0;
        }
        // Reset the broadcast throttle so the next connection's first
        // sample fires a PaTempFrame immediately instead of waiting out
        // the previous session's 500 ms window.
        _lastPaTempBroadcastAtUtc = DateTime.MinValue;
        _lastDiagLogAtUtc = DateTime.MinValue;
        _tx.Safety.ObserveConfirmedIdle();
    }

    /// <summary>
    /// Hi-priority status (UDP 1025) handler for Protocol 2. The packet
    /// already carries FWD/REF as 16-bit ADC values matched to the same
    /// per-board RadioCalibration tables the P1 path uses, so we route both
    /// axes through OnTelemetryRaw. PA temperature on G2 lives on a
    /// different ADC slot and isn't decoded yet — separate task.
    ///
    /// Runs on the Protocol2Client RX thread; OnTelemetryRaw takes _sync.
    /// </summary>
    public void OnP2Telemetry(Zeus.Protocol2.P2TelemetryReading reading)
    {
        OnTelemetryRaw(reading.FwdAdc, reading.RevAdc);
    }

    private void OnP2Connected(Zeus.Protocol2.Protocol2Client client)
    {
        _subscribedP2Client = client;
        client.TelemetryReceived += OnP2Telemetry;
        lock (_sync)
        {
            _paTempAdc = 0;
            _seenPaTempSample = false;
            _paTempUpdatedUtc = null;
            _diagPaTempSlotCount = 0;
        }
        _log.LogInformation("tx.meters subscribed to p2 hi-priority telemetry");
    }

    private void OnP2Disconnected()
    {
        var client = _subscribedP2Client;
        _subscribedP2Client = null;
        if (client is not null) client.TelemetryReceived -= OnP2Telemetry;
        lock (_sync)
        {
            _fwdAdc = 0;
            _refAdc = 0;
            _fwdAdcPeak = 0;
            _refAdcPeak = 0;
            _paTempAdc = 0;
            _seenPaTempSample = false;
            _paTempUpdatedUtc = null;
            _seenSample = false;
            _latestRawFwd = 0;
            _latestRawRef = 0;
            _seenRawFwd = false;
            _seenRawRef = false;
            _diagFwdSlotCount = 0;
            _diagRefSlotCount = 0;
            _diagLastFwdAin1 = 0;
            _diagLastRefAin0 = 0;
        }
        _lastDiagLogAtUtc = DateTime.MinValue;
        _tx.Safety.ObserveConfirmedIdle();
    }
}
