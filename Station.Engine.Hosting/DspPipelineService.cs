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

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Zeus.Contracts;
using Zeus.Dsp;
using Zeus.Dsp.Wdsp;
using Zeus.Protocol1;

namespace Zeus.Server;

public sealed record Protocol3TxEngineConnection(
    string Engine,
    int EngineRateHz,
    int TxIqRateHz,
    bool External,
    bool PureSignal);

public class DspPipelineService : BackgroundService,
    Zeus.Protocol1.IRxPacketSink,
    Zeus.Protocol2.IRxPacketSink
{
    private const int SyntheticSampleRateHz = 192_000;
    private const int OfflinePreviewTxOutputRateHz = 192_000;
    public const int AudioOutputRateHz = 48_000;
    private const int AudioDrainCapacity = 2048;
    private const float DisplayInvalidBinDb = -200f;
    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(1000.0 / 30.0);

    private readonly RadioService _radio;
    private readonly SMeterCalibrationStore? _sMeterCalibrationStore;
    private readonly object _sMeterCalibrationSync = new();
    private HpsdrBoardKind _cachedSMeterBoard;
    private OrionMkIIVariant _cachedSMeterVariant;
    private double _operatorSMeterOffsetDb;
    private readonly StreamingHub _hub;
    private readonly IRxAudioSink[] _audioSinks;
    private readonly TxIqRing? _txIqRing;
    private readonly TransmitEgressGate _txEgressGate = new();
    internal TransmitEgressGate TxEgressGate => _txEgressGate;
    // Operator RX master mute (desktop "Mute" button, issue #1252). Read once per
    // tick so the local-monitor lane (Recorder playback) can stay audible while
    // real RX audio is muted. Null in unit tests / non-desktop hosts => never muted.
    private readonly RxAudioMuteState? _rxAudioMute;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<DspPipelineService> _log;
    private readonly ExternalDspEngineProviderLoader _externalDspProvider;
    private Protocol3TxEngineConnection? _protocol3TxEngineConnection;
    private readonly int _rxAnalyzerFftSize;
    private readonly int _panadapterWidth;
    private double _displayMaxFrameRateHz;
    private int _displayDecimation = DisplayPerformanceOptions.DefaultDisplayDecimation;
    private int _waterfallUpdatePeriod = DisplayPerformanceOptions.DefaultWaterfallUpdatePeriod;

    /// <summary>
    /// Raised when an RX S-meter reading is available (approximately 5 Hz).
    /// Arguments: (channelId, dBm)
    /// </summary>
    public event Action<int, double>? RxMeterUpdated;

    /// <summary>
    /// Raised on every decoded RX IQ frame, after it has been fed to WDSP.
    /// Arguments: (receiver, sampleRateHz, interleavedIQ).
    /// The memory references a pooled buffer and is only valid for the
    /// duration of the synchronous handler — copy if retention is needed.
    /// </summary>
    public event Action<int, int, ReadOnlyMemory<double>>? RxIqAvailable;

    /// <summary>
    /// Raised when demodulated RX audio samples are available (~30 Hz ticks,
    /// 48 kHz mono FLOAT32). Arguments: (receiver, sampleRateHz, samples).
    /// The memory references a local buffer and is only valid for the
    /// duration of the synchronous handler — copy if retention is needed.
    /// </summary>
    public event Action<int, int, ReadOnlyMemory<float>>? RxAudioAvailable;

    /// <summary>
    /// Raised when TX-monitor audio is available — the processed transmit audio
    /// demodulated back from the TX IQ (post EQ / compressor / leveler / CFC),
    /// i.e. what actually goes on the air. Only fires while the TX monitor /
    /// preview path is running. 48 kHz mono float32; args (receiver,
    /// sampleRateHz, samples). Memory valid only for the synchronous handler.
    /// </summary>
    public event Action<int, int, ReadOnlyMemory<float>>? TxMonitorAudioAvailable;

    /// <summary>
    /// Delegate for the RX audio plugin insert seam (<c>rx.post-demod</c> slot).
    /// Invoked once per <see cref="Tick"/> over the demodulated 48 kHz mono RX
    /// audio block, IN PLACE, after the MOX fade ramp and BEFORE the CW
    /// sidetone is mixed in and the block is published to sinks — so a filter
    /// shapes the received band audio without touching the locally-generated
    /// sidetone. The <c>audio</c> span is both input and output.
    /// </summary>
    public delegate void RxAudioBlockHandler(Span<float> audio, int frames, int sampleRate);

    // RX audio plugin insert handler. Wired by AudioPluginBridge when an
    // rx.post-demod audio plugin is attached; null (the default) makes the RX
    // path bit-identical to before this seam existed — single volatile read in
    // the Tick hot path, no cost when no RX plugin is loaded. The handler runs
    // on the DSP pipeline thread and MUST be realtime-disciplined (no alloc /
    // lock / IO) — AudioChain.Process honours that contract.
    private volatile RxAudioBlockHandler? _rxAudioPluginHandler;

    /// <summary>Install (or clear, with <c>null</c>) the RX audio plugin
    /// insert handler. Single volatile write; safe from the control thread.</summary>
    public void SetRxAudioPluginHandler(RxAudioBlockHandler? handler) => _rxAudioPluginHandler = handler;

    /// <summary>Engage (or release) meter-only TX monitor for Auto Tune. When
    /// engaged together with TX Monitor, the TXA chain runs (stage meters
    /// animate) but the demodulated monitor audio is not broadcast, so the
    /// operator hears nothing while Auto Tune samples. Single volatile write;
    /// safe from the request thread.</summary>
    public void SetTxMonitorMeterOnly(bool on) => _txMonitorMeterOnly = on;
    public bool TxMonitorMeterOnly => _txMonitorMeterOnly;
    public const double MinTxMonitorVolumeDb = TxMonitorSettingsStore.MinVolumeDb;
    public const double MaxTxMonitorVolumeDb = TxMonitorSettingsStore.MaxVolumeDb;
    private readonly TxMonitorSettingsStore? _txMonitorSettingsStore;
    private readonly object _txMonitorVolumeSync = new();
    private double _txMonitorVolumeDb;
    public double TxMonitorVolumeDb => Volatile.Read(ref _txMonitorVolumeDb);

    public double SetTxMonitorVolumeDb(double db)
    {
        lock (_txMonitorVolumeSync)
        {
            if (!double.IsFinite(db)) db = TxMonitorSettingsStore.DefaultVolumeDb;
            double applied = Math.Clamp(db, MinTxMonitorVolumeDb, MaxTxMonitorVolumeDb);
            _txMonitorSettingsStore?.SetVolumeDb(applied);
            Volatile.Write(ref _txMonitorVolumeDb, applied);
            return applied;
        }
    }

    internal struct RxAudioLevelerState
    {
        public double GainDb;
        public bool DiagnosticsValid;
        public double InputRmsDbfs;
        public double InputPeakDbfs;
        public double OutputRmsDbfs;
        public double OutputPeakDbfs;
        public double DesiredGainDb;
        public double AppliedGainDb;
        public double GainDeltaDb;
        public double PeakHeadroomDb;
        public double PreLimitPeakDbfs;
        public double OutputLimitReductionDb;
        public int OutputLimitSampleCount;
        public int PauseHoldBlocks;
        public bool BoostSlewLimited;
        public bool PeakLimited;
        public bool OutputLimited;
        public bool ReleaseToUnity;
    }

    private RxAudioLevelerState _rxAudioLeveler;

    internal sealed class AdaptiveSquelchState
    {
        internal readonly double[] Window = new double[AdaptiveSquelchWindowSamples];
        internal readonly double[] Scratch = new double[AdaptiveSquelchWindowSamples];
        public int WindowIndex;
        public int WindowFill;
        public double NoiseFloorDbm = double.NaN;
        public double LastSignalDbm = double.NaN;
        public bool Open;
        public int CloseHoldBlocks;
        public double Gain;
    }

    private AdaptiveSquelchState _adaptiveSquelch = new();

    private const double RxLevelerTargetRmsDb = -18.0;
    // Softened (issue #733): raise the gate so near-noise-floor signals pass
    // through unprocessed (clean static), and cap the boost so the leveler
    // gently lifts weak audio instead of pumping it ~36 dB toward target — the
    // big boost + fast slews were the "crackle on anything above the noise
    // floor" zipper. The CUT/peak-guard safety below is unchanged, so loud
    // signals are still caught.
    private const double RxLevelerGateRmsDb = -50.0;
    // Soft-gate window above the hard gate: the upward boost ramps 0 -> full
    // across [GateRms, GateRms + GateSoftWindow] instead of snapping on at the
    // gate. Without this, a hair-trigger AGC-T move that nudges a weak signal
    // across the gate toggled the full boost on/off — an ~18 dB output step
    // heard as crackle / "audio fell off the planet". WDSP's own AGC-T is 1:1
    // and Thetis-faithful; this always-on leveler (which Thetis lacks) was the
    // amplifier. The boost ramp is continuous with the belowGate=0 region.
    private const double RxLevelerGateSoftWindowDb = 8.0;
    // Cap the upward boost low (was 10 dB) so the leveler no longer fights the
    // operator's AGC-T: AGC-T sets weak-signal loudness (Thetis-like) and the
    // leveler only nudges. The downward CUT below is unchanged — it stays the
    // blast-guard that catches a sudden strong signal.
    private const double RxLevelerMaxBoostDb = 3.0;
    // The production constant-loudness path may use more makeup than the old
    // audio-only helper, but only while an independent passband measurement
    // says RX0 contains a resolved RF signal.  Noise/unavailable telemetry
    // fails closed to zero boost; downward gain and the peak guard stay live.
    private const double RxConstantLoudnessMaxBoostDb = 24.0;
    private static readonly RxLevelerConfig DefaultRxLevelerConfig = new();
    private const double RxConstantLoudnessMinConfidence = 0.10;
    // Current pre-AGC S+N power above the estimator's integrated noise power.
    // This is deliberately a total-power excess, not signal-only SNR.
    private const double RxConstantLoudnessMinCurrentTotalExcessDb = 1.5;
    private const double RxConstantLoudnessReleaseTotalExcessDb = 0.25;
    // The 5 Hz stage meter must agree for ~0.6 s to open; once open, require
    // ~1.2 s of consecutive misses so ordinary fades cannot chatter the gate.
    private const int RxConstantLoudnessAcquireFrames = 3;
    private const int RxConstantLoudnessReleaseFrames = 6;
    private const double RxConstantLoudnessAdcVetoDbfs = -3.0;
    private const double RxConstantLoudnessAdcUnavailableDbfs = -200.0;
    private const double RxConstantLoudnessMeterUnavailableDbm = -200.0;
    private const long RxConstantLoudnessEvidenceMaxAgeMs = 750;
    private const double RxLevelerMaxCutDb = -24.0;
    private const double RxLevelerBoostSlewDbPerBlock = 2.0;
    private const double RxLevelerFastBoostSlewDbPerBlock = 2.5;
    private const double RxLevelerFastBoostHeadroomDb = 6.0;
    private const double RxLevelerVeryFastBoostSlewDbPerBlock = 3.0;
    private const double RxLevelerVeryFastBoostHeadroomDb = 10.0;
    private const double RxLevelerVeryFastBoostGateRmsDb = -45.0;
    private const double RxLevelerCrestCatchupBoostSlewDbPerBlock = 3.0;
    private const double RxLevelerCrestCatchupHeadroomDb = 16.0;
    private const double RxLevelerCrestCatchupMinCrestDb = 8.0;
    private const double RxLevelerCrestCatchupMaxRmsDb = -28.0;
    private const double RxLevelerCrestCatchupMinPeakDb = -52.0;
    private const double RxLevelerCrestCatchupMinGainGapDb = 6.0;
    private const double RxLevelerMemoryCatchupGateRmsDb = -66.0;
    private const double RxLevelerMemoryCatchupGatePeakDb = -56.0;
    private const double RxLevelerMemoryCatchupMinGainDb = 3.0;
    private const double RxLevelerSmoothCutDb = 6.0;
    // Match the ordinary smooth-cut rate: +24 dB then clears in four blocks
    // (~130 ms at 30 Hz), fast enough not to hold noise but never in one block.
    internal const double RxLevelerEvidenceLossReleaseDbPerBlock = 6.0;
    private const int RxLevelerPauseHoldBlocks = 18;
    private const double RxLevelerPauseMemoryDecayDbPerBlock = 4.5;
    private const int RxLevelerGainRampMaxSamples = 256;
    private const double RxLevelerLargeTransitionDb = 6.0;
    private const int RxLevelerEmergencyCutRampSamples = 64;
    private const double RxLevelerPeakTarget = 0.74;
    private const double RxLevelerOutputSoftKnee = 0.74;
    private const double RxLevelerOutputPeakCeiling = 0.84;
    private const int AdaptiveSquelchWindowSamples = 12;
    private const int AdaptiveSquelchMinSamples = 2;
    private const double AdaptiveSquelchFloorPercentile = 0.20;
    private const double AdaptiveSquelchFloorRiseSlewDb = 0.25;
    private const double AdaptiveSquelchFloorFallSlewDb = 7.0;
    private const int AdaptiveSquelchCloseHoldBlocks = 12;
    private const double AdaptiveSquelchAttackPerBlock = 1.0;
    private const double AdaptiveSquelchReleasePerBlock = 0.14;
    private const double AdaptiveSquelchOpenMarginDb = 2.5;
    private const double AdaptiveSquelchOpenInitialGain = 0.35;

    internal static float SanitizeAudioSample(float sample)
    {
        if (!float.IsFinite(sample)) return 0f;
        return Math.Clamp(sample, -1f, 1f);
    }

    internal static void SanitizeAudioBuffer(Span<float> samples)
    {
        for (int i = 0; i < samples.Length; i++)
        {
            samples[i] = SanitizeAudioSample(samples[i]);
        }
    }

    // Multi-receiver summing deliberately retains unity gain and may exceed
    // full scale before the final-bus soft limiter. Remove only non-finite
    // values here; hard-clamping would flatten coincident peaks before that
    // limiter has a chance to preserve their shape.
    internal static void SanitizeMixedRxAudioBuffer(Span<float> samples)
    {
        for (int i = 0; i < samples.Length; i++)
        {
            if (!float.IsFinite(samples[i])) samples[i] = 0f;
        }
    }

    internal static void LimitRxAudioBuffer(Span<float> samples)
    {
        for (int i = 0; i < samples.Length; i++)
        {
            float s = samples[i];
            samples[i] = float.IsFinite(s) ? SoftLimitRxAudioSample(s) : 0f;
        }
    }

    internal static void SanitizeDisplayBuffer(Span<float> dbBins)
    {
        for (int i = 0; i < dbBins.Length; i++)
        {
            if (!float.IsFinite(dbBins[i]))
                dbBins[i] = DisplayInvalidBinDb;
        }
    }

    private static double Rms(ReadOnlySpan<float> samples)
    {
        if (samples.Length == 0) return 0.0;
        double sumSq = 0.0;
        for (int i = 0; i < samples.Length; i++)
        {
            double s = samples[i];
            sumSq += s * s;
        }
        return Math.Sqrt(sumSq / samples.Length);
    }

    private static double PeakAbs(ReadOnlySpan<float> samples)
    {
        double peak = 0.0;
        for (int i = 0; i < samples.Length; i++)
        {
            double value = Math.Abs(samples[i]);
            if (double.IsFinite(value) && value > peak) peak = value;
        }
        return peak;
    }

    private static double ClampUnit(double value) =>
        double.IsFinite(value) ? Math.Clamp(value, 0.0, 1.0) : 0.0;

    private static double DbToLinear(double db) =>
        double.IsFinite(db) ? Math.Pow(10.0, db / 20.0) : 1.0;

    internal static void ApplyRxAudioLeveler(
        Span<float> samples,
        ref RxAudioLevelerState state)
        => ApplyRxAudioLevelerCore(
            samples, ref state, allowBoost: true, RxLevelerMaxBoostDb);

    internal static void ApplyRxAudioLeveler(
        Span<float> samples,
        ref RxAudioLevelerState state,
        bool rfSignalResolved,
        bool adcOverloadRisk,
        double levelReferenceOffsetDb = 0.0)
        => ApplyRxAudioLevelerCore(
            samples,
            ref state,
            allowBoost: rfSignalResolved && !adcOverloadRisk,
            RxConstantLoudnessMaxBoostDb,
            levelReferenceOffsetDb);

    internal static void ApplyRxAudioLeveler(
        Span<float> samples,
        ref RxAudioLevelerState state,
        bool enabled,
        bool rfSignalResolved,
        bool adcOverloadRisk,
        double levelReferenceOffsetDb = 0.0,
        RxLevelerConfig? config = null)
    {
        var profile = config ?? DefaultRxLevelerConfig;
        bool custom = profile.Mode == RxLevelerMode.Custom;
        if (!enabled && (state.GainDb > 0.0 || state.AppliedGainDb > 0.0))
            state.ReleaseToUnity = true;

        ApplyRxAudioLevelerCore(
            samples,
            ref state,
            allowBoost: enabled && !state.ReleaseToUnity &&
                rfSignalResolved && !adcOverloadRisk,
            maxBoostDb: custom ? profile.MaxBoostDb : RxConstantLoudnessMaxBoostDb,
            levelReferenceOffsetDb,
            targetRmsDb: custom ? profile.TargetRmsDb : RxLevelerTargetRmsDb,
            customAttackMs: custom ? profile.AttackMs : null,
            customReleaseMs: custom ? profile.ReleaseMs : null,
            customHangMs: custom ? profile.HangMs : null,
            forceReleaseToUnity: state.ReleaseToUnity);

        if (state.ReleaseToUnity && state.GainDb <= 0.0 && state.AppliedGainDb <= 0.0)
            state.ReleaseToUnity = false;
    }

    private static void ApplyRxAudioLevelerCore(
        Span<float> samples,
        ref RxAudioLevelerState state,
        bool allowBoost,
        double maxBoostDb,
        double levelReferenceOffsetDb = 0.0,
        double targetRmsDb = RxLevelerTargetRmsDb,
        int? customAttackMs = null,
        int? customReleaseMs = null,
        int? customHangMs = null,
        bool forceReleaseToUnity = false)
    {
        if (samples.Length == 0) return;

        double blockDurationMs = samples.Length * 1000.0 / AudioOutputRateHz;
        double customBoostSlewDb = customAttackMs.HasValue
            ? maxBoostDb * blockDurationMs / customAttackMs.Value
            : double.NaN;
        // Release time is defined against the fixed +24 dB safety-bounded
        // makeup range, so reducing Max Boost never accidentally makes release
        // slower. Peak-urgent cuts continue to bypass this audible timing.
        double customReleaseSlewDb = customReleaseMs.HasValue
            ? RxConstantLoudnessMaxBoostDb * blockDurationMs / customReleaseMs.Value
            : double.NaN;
        int customPauseHoldBlocks = customHangMs.HasValue
            ? (int)Math.Ceiling(customHangMs.Value / blockDurationMs)
            : -1;

        double sumSq = 0.0;
        double peak = 0.0;
        for (int i = 0; i < samples.Length; i++)
        {
            // The final RX bus deliberately permits finite values beyond unity
            // until its soft limiter.  Preserve that waveform here so summed
            // receivers are gain-reduced as a whole instead of hard-clipped
            // sample-by-sample before the peak guard can act.
            float s = float.IsFinite(samples[i]) ? samples[i] : 0f;
            double a = Math.Abs(s);
            if (a > peak) peak = a;
            sumSq += (double)s * s;
        }

        double rms = Math.Sqrt(sumSq / samples.Length);
        double inputRmsDbfs = AudioLinearToDbfsRaw(rms);
        double inputPeakDbfs = AudioLinearToDbfsRaw(peak);
        targetRmsDb += levelReferenceOffsetDb;
        double gateRmsDb = RxLevelerGateRmsDb + levelReferenceOffsetDb;
        bool belowGate = rms <= 0.0 || !double.IsFinite(inputRmsDbfs) || inputRmsDbfs < gateRmsDb;

        double desiredDb = belowGate
            ? 0.0
            : targetRmsDb - inputRmsDbfs;
        desiredDb = Math.Clamp(desiredDb, RxLevelerMaxCutDb, maxBoostDb);

        // Audio amplitude cannot distinguish weak speech from receiver noise.
        // Positive makeup therefore requires independent, fresh RF evidence.
        // Cuts remain unconditional so a sudden loud block is still contained.
        if (desiredDb > 0.0 && !allowBoost)
            desiredDb = 0.0;

        // Soft gate: taper the upward BOOST to zero as the input approaches the
        // floor, so crossing the gate can never snap the full boost on/off (the
        // AGC-T hair-trigger). Cuts (desiredDb < 0, the loud-signal blast-guard)
        // are NEVER tapered. Continuous with belowGate: at the gate the factor is
        // 0, reaching full boost RxLevelerGateSoftWindowDb above it.
        if (!belowGate && desiredDb > 0.0)
        {
            double gateFactor = Math.Clamp(
                (inputRmsDbfs - gateRmsDb) / RxLevelerGateSoftWindowDb,
                0.0, 1.0);
            desiredDb *= gateFactor;
        }

        double peakHeadroomDb = double.NaN;
        bool peakLimited = false;
        if (peak > 1.0e-9)
        {
            peakHeadroomDb = 20.0 * Math.Log10(Math.Max(RxLevelerPeakTarget, 1.0e-9) / peak);
            if (double.IsFinite(peakHeadroomDb) && desiredDb > peakHeadroomDb)
            {
                desiredDb = Math.Clamp(peakHeadroomDb, RxLevelerMaxCutDb, maxBoostDb);
                peakLimited = true;
            }
        }

        double currentDb = double.IsFinite(state.GainDb) ? state.GainDb : 0.0;
        currentDb = Math.Clamp(currentDb, RxLevelerMaxCutDb, maxBoostDb);
        // GainDb is the controller's pause-memory value. AppliedGainDb is the
        // gain that actually reached the final sample of the previous block.
        // They intentionally differ while audio is below the gate: retain the
        // controller memory, but fade the applied gain to unity so noise is not
        // held up. Keeping both prevents a hard gain step at word boundaries.
        double appliedStartDb = state.DiagnosticsValid && double.IsFinite(state.AppliedGainDb)
            ? Math.Clamp(state.AppliedGainDb, RxLevelerMaxCutDb, maxBoostDb)
            : currentDb;

        // Evidence loss bypasses pause memory so averaged-SNR hangover cannot
        // hold makeup on noise, but releases it over several audio blocks. Peak
        // safety below may still cut faster when the held gain is genuinely unsafe.
        bool evidenceLossRelease = !allowBoost &&
            (currentDb > 0.0 || forceReleaseToUnity);

        // True when the gain we are currently holding would, on its own, drive
        // this block's peak past the limiter target — i.e. a gain "held" high
        // across a quiet gap (pause memory / catch-up) is about to be dumped onto
        // a louder block. The raw input-peak gate below (peak > target) misses
        // this case because the danger is gain × peak, not peak alone: a signal
        // whose input peak is modest still blasts the speaker if the held gain is
        // large. When this is set we cut as urgently as a clipping peak would.
        bool currentGainOverdrivesPeak =
            double.IsFinite(peakHeadroomDb) && appliedStartDb > peakHeadroomDb;

        double nextDb = currentDb;
        if (evidenceLossRelease)
        {
            state.PauseHoldBlocks = 0;
        }
        else if (belowGate)
        {
            bool holdMemory = state.PauseHoldBlocks > 0;
            if (holdMemory)
                state.PauseHoldBlocks--;

            double releaseStep = customReleaseMs.HasValue
                ? customReleaseSlewDb
                : RxLevelerPauseMemoryDecayDbPerBlock;
            if (holdMemory)
            {
                nextDb = currentDb;
            }
            else if (Math.Abs(currentDb) <= releaseStep)
            {
                nextDb = 0.0;
            }
            else
            {
                nextDb = currentDb + (currentDb > 0.0 ? -releaseStep : releaseStep);
            }
        }
        else
        {
            state.PauseHoldBlocks = customHangMs.HasValue
                ? customPauseHoldBlocks
                : RxLevelerPauseHoldBlocks;
        }

        bool boostSlewLimited = false;
        if (!evidenceLossRelease && !belowGate && desiredDb > currentDb)
        {
            double boostSlewDb = customAttackMs.HasValue
                ? customBoostSlewDb
                : RxLevelerBoostSlewDbPerBlock;
            if (!customAttackMs.HasValue &&
                double.IsFinite(peakHeadroomDb) && peakHeadroomDb >= RxLevelerFastBoostHeadroomDb)
            {
                boostSlewDb = Math.Max(boostSlewDb, RxLevelerFastBoostSlewDbPerBlock);
            }
            if (!customAttackMs.HasValue &&
                double.IsFinite(peakHeadroomDb) &&
                peakHeadroomDb >= RxLevelerVeryFastBoostHeadroomDb &&
                inputRmsDbfs >= RxLevelerVeryFastBoostGateRmsDb + levelReferenceOffsetDb)
            {
                boostSlewDb = Math.Max(boostSlewDb, RxLevelerVeryFastBoostSlewDbPerBlock);
            }

            double crestDb = inputPeakDbfs - inputRmsDbfs;
            if (!customAttackMs.HasValue &&
                double.IsFinite(crestDb) &&
                crestDb >= RxLevelerCrestCatchupMinCrestDb &&
                inputRmsDbfs <= RxLevelerCrestCatchupMaxRmsDb + levelReferenceOffsetDb &&
                inputPeakDbfs >= RxLevelerCrestCatchupMinPeakDb + levelReferenceOffsetDb &&
                desiredDb - currentDb >= RxLevelerCrestCatchupMinGainGapDb)
            {
                boostSlewDb = Math.Max(boostSlewDb, RxLevelerCrestCatchupBoostSlewDbPerBlock);
            }
            if (!customAttackMs.HasValue &&
                state.GainDb >= RxLevelerMemoryCatchupMinGainDb &&
                inputRmsDbfs >= RxLevelerMemoryCatchupGateRmsDb + levelReferenceOffsetDb &&
                inputPeakDbfs >= RxLevelerMemoryCatchupGatePeakDb + levelReferenceOffsetDb)
            {
                boostSlewDb = Math.Max(boostSlewDb, RxLevelerFastBoostSlewDbPerBlock);
            }

            nextDb = Math.Min(desiredDb, currentDb + boostSlewDb);
            boostSlewLimited = nextDb + 1.0e-9 < desiredDb;
        }
        else if (!belowGate || evidenceLossRelease)
        {
            bool peakUrgency = peak > RxLevelerPeakTarget || peakLimited || currentGainOverdrivesPeak;
            // Pause memory may exceed the gain actually applied before evidence
            // closed. Discard that hidden makeup instead of replaying it.
            double cutStartDb = evidenceLossRelease
                ? Math.Min(currentDb, appliedStartDb)
                : currentDb;
            double smoothCutDb = customReleaseMs.HasValue
                ? customReleaseSlewDb
                : RxLevelerSmoothCutDb;
            double cutSlewDb = peakUrgency
                ? Math.Max(smoothCutDb, cutStartDb - desiredDb)
                : evidenceLossRelease
                    ? RxLevelerEvidenceLossReleaseDbPerBlock
                    : smoothCutDb;
            double cutFloorDb = evidenceLossRelease ? Math.Min(desiredDb, 0.0) : desiredDb;
            nextDb = Math.Max(cutFloorDb, cutStartDb - cutSlewDb);
        }
        nextDb = Math.Clamp(nextDb, RxLevelerMaxCutDb, maxBoostDb);

        // Hard per-block peak guard. The smooth boost/cut slews above track
        // loudness gently; this is the safety floor that stops a held-high gain
        // from ever being *applied* to a block whose peak would then exceed the
        // limiter target. Without it, gain banked across a quiet gap gets dumped
        // onto the first loud-ish block of a new signal and rides the soft-limit
        // ceiling for several blocks before the slew catches up — the "sudden
        // strong signal blasts the speaker" failure this leveler exists to stop.
        // Cutting straight to the peak-safe gain is inaudible next to that blast.
        if (!belowGate && double.IsFinite(peakHeadroomDb) && nextDb > peakHeadroomDb)
            nextDb = Math.Clamp(peakHeadroomDb, RxLevelerMaxCutDb, maxBoostDb);

        double appliedEndDb = belowGate ? 0.0 : nextDb;
        double appliedTransitionDb = Math.Abs(appliedEndDb - appliedStartDb);
        int requestedRampSamples = (int)Math.Ceiling(
            RxLevelerGainRampMaxSamples *
            Math.Max(1.0, appliedTransitionDb / RxLevelerLargeTransitionDb));
        int rampSamples = Math.Clamp(
            Math.Min(samples.Length, requestedRampSamples), 1, Math.Max(1, samples.Length));
        double preLimitPeak = 0.0;
        int outputLimitSampleCount = 0;
        bool emergencyCut = !belowGate && appliedEndDb < appliedStartDb &&
            (peak > RxLevelerPeakTarget || peakLimited || currentGainOverdrivesPeak);
        if (emergencyCut)
        {
            double heldGain = DbToLinear(appliedStartDb);
            int firstUnsafeSample = 0;
            while (firstUnsafeSample < samples.Length &&
                Math.Abs(samples[firstUnsafeSample] * heldGain) <= RxLevelerPeakTarget)
            {
                firstUnsafeSample++;
            }

            // Complete the safety cut no later than the first sample that the
            // previously applied gain would overdrive. A block loud from sample
            // zero still cuts immediately; a later crest gets a short dezipper
            // ramp instead of forcing an unrelated block-boundary step.
            rampSamples = firstUnsafeSample < samples.Length
                ? Math.Clamp(firstUnsafeSample + 1, 1, RxLevelerEmergencyCutRampSamples)
                : Math.Min(rampSamples, RxLevelerEmergencyCutRampSamples);
        }
        for (int i = 0; i < samples.Length; i++)
        {
            float clean = float.IsFinite(samples[i]) ? samples[i] : 0f;
            double ramp = i < rampSamples
                ? (i + 1) / (double)rampSamples
                : 1.0;
            double gainDb = appliedStartDb + (appliedEndDb - appliedStartDb) * ramp;
            double scaled = clean * DbToLinear(gainDb);
            double absScaled = Math.Abs(scaled);
            if (absScaled > preLimitPeak) preLimitPeak = absScaled;

            float limited = SoftLimitRxAudioSample((float)scaled);
            if (Math.Abs(limited) + 1.0e-6 < absScaled) outputLimitSampleCount++;
            samples[i] = limited;
        }

        double outputRms = Rms(samples);
        double outputPeak = PeakAbs(samples);
        double outputRmsDbfs = AudioLinearToDbfsRaw(outputRms);
        double outputPeakDbfs = AudioLinearToDbfsRaw(outputPeak);
        double preLimitPeakDbfs = AudioLinearToDbfsRaw(preLimitPeak);
        double outputLimitReductionDb = preLimitPeak > outputPeak && outputPeak > 0.0
            ? 20.0 * Math.Log10(preLimitPeak / outputPeak)
            : 0.0;

        state.GainDb = nextDb;
        state.DiagnosticsValid = true;
        state.InputRmsDbfs = inputRmsDbfs;
        state.InputPeakDbfs = inputPeakDbfs;
        state.OutputRmsDbfs = outputRmsDbfs;
        state.OutputPeakDbfs = outputPeakDbfs;
        state.DesiredGainDb = desiredDb;
        state.AppliedGainDb = appliedEndDb;
        state.GainDeltaDb = appliedEndDb - appliedStartDb;
        state.PeakHeadroomDb = peakHeadroomDb;
        state.PreLimitPeakDbfs = preLimitPeakDbfs;
        state.OutputLimitReductionDb = outputLimitReductionDb;
        state.OutputLimitSampleCount = outputLimitSampleCount;
        state.BoostSlewLimited = boostSlewLimited;
        state.PeakLimited = peakLimited;
        state.OutputLimited = outputLimitSampleCount > 0;
    }


    internal static double AudioRmsToFallbackDbm(double rms)
    {
        if (!double.IsFinite(rms)) return double.NaN;
        double dbfs = 20.0 * Math.Log10(Math.Max(rms, 1e-10));
        return dbfs - 50.0;
    }

    internal static double AudioRmsToFallbackDbm(double rms, double meterCorrectionDb)
    {
        double dbm = AudioRmsToFallbackDbm(rms);
        return double.IsFinite(dbm) ? dbm + meterCorrectionDb : dbm;
    }

    internal static double AdaptiveSquelchMarginDb() => AdaptiveSquelchOpenMarginDb;

    // The exact config the managed adaptive gate acts on for a channel. FM must
    // never run the adaptive audio-RMS gate — open FM hiss reads as high power,
    // so the gate opens/closes backwards (issue #2101) — so ForMode neutralizes
    // Adaptive in FM, leaving the managed gate fully open for WDSP's FMSQ to own
    // the squelch. Exposed internally so this resolution is unit-testable.
    internal static SquelchConfig ResolveManagedSquelchForMode(SquelchConfig? cfg, RxMode mode) =>
        (cfg ?? new SquelchConfig()).ForMode(mode);

    private static double AdaptiveSquelchCloseHysteresisDb(double marginDb) =>
        Math.Clamp(marginDb * 0.5, 1.5, 4.0);

    internal static void UpdateAdaptiveSquelchMeter(
        AdaptiveSquelchState state,
        SquelchConfig cfg,
        double signalDbm)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(cfg);
        if (!double.IsFinite(signalDbm) || signalDbm <= -250.0) return;

        signalDbm = Math.Clamp(signalDbm, -200.0, 60.0);
        state.LastSignalDbm = signalDbm;
        state.Window[state.WindowIndex] = signalDbm;
        state.WindowIndex = (state.WindowIndex + 1) % state.Window.Length;
        if (state.WindowFill < state.Window.Length) state.WindowFill++;

        if (state.WindowFill >= AdaptiveSquelchMinSamples)
        {
            Array.Copy(state.Window, state.Scratch, state.WindowFill);
            Array.Sort(state.Scratch, 0, state.WindowFill);
            int floorIndex = Math.Clamp(
                (int)Math.Round((state.WindowFill - 1) * AdaptiveSquelchFloorPercentile),
                0,
                state.WindowFill - 1);
            double candidateFloor = state.Scratch[floorIndex];
            if (!double.IsFinite(state.NoiseFloorDbm))
            {
                state.NoiseFloorDbm = candidateFloor;
            }
            else if (candidateFloor > state.NoiseFloorDbm)
            {
                state.NoiseFloorDbm = Math.Min(candidateFloor, state.NoiseFloorDbm + AdaptiveSquelchFloorRiseSlewDb);
            }
            else
            {
                state.NoiseFloorDbm = Math.Max(candidateFloor, state.NoiseFloorDbm - AdaptiveSquelchFloorFallSlewDb);
            }
        }

        if (!cfg.Enabled || !cfg.Adaptive || state.WindowFill < AdaptiveSquelchMinSamples
            || !double.IsFinite(state.NoiseFloorDbm))
        {
            state.Open = false;
            state.CloseHoldBlocks = 0;
            return;
        }

        double marginDb = AdaptiveSquelchMarginDb();
        double openThreshold = state.NoiseFloorDbm + marginDb;
        double closeThreshold = openThreshold - AdaptiveSquelchCloseHysteresisDb(marginDb);

        if (signalDbm >= openThreshold)
        {
            state.Open = true;
            state.CloseHoldBlocks = AdaptiveSquelchCloseHoldBlocks;
        }
        else if (state.Open)
        {
            if (signalDbm >= closeThreshold)
            {
                state.CloseHoldBlocks = AdaptiveSquelchCloseHoldBlocks;
            }
            else if (state.CloseHoldBlocks > 0)
            {
                state.CloseHoldBlocks--;
            }
            else
            {
                state.Open = false;
            }
        }
    }

    internal static void ApplyAdaptiveSquelch(
        Span<float> samples,
        SquelchConfig cfg,
        AdaptiveSquelchState state)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        ArgumentNullException.ThrowIfNull(state);
        if (samples.Length == 0 || !cfg.Enabled || !cfg.Adaptive) return;

        double target = state.WindowFill >= AdaptiveSquelchMinSamples && state.Open ? 1.0 : 0.0;
        double start = Math.Clamp(double.IsFinite(state.Gain) ? state.Gain : 0.0, 0.0, 1.0);
        if (target > 0.0 && start < AdaptiveSquelchOpenInitialGain)
        {
            start = AdaptiveSquelchOpenInitialGain;
        }
        double end = target > start
            ? Math.Min(target, start + AdaptiveSquelchAttackPerBlock)
            : Math.Max(target, start - AdaptiveSquelchReleasePerBlock);
        double delta = end - start;
        double denom = samples.Length;

        for (int i = 0; i < samples.Length; i++)
        {
            double gain = start + delta * ((i + 1) / denom);
            samples[i] *= (float)gain;
        }

        if (end <= 0.0001 && target == 0.0)
        {
            samples.Clear();
            end = 0.0;
        }
        state.Gain = end;
    }

    private static float SoftLimitRxAudioSample(float sample)
    {
        float a = Math.Abs(sample);
        if (a <= RxLevelerOutputSoftKnee) return sample;

        double kneeWidth = Math.Max(1.0e-6, RxLevelerOutputPeakCeiling - RxLevelerOutputSoftKnee);
        double over = (a - RxLevelerOutputSoftKnee) / kneeWidth;
        double limited = RxLevelerOutputSoftKnee + kneeWidth * Math.Tanh(over);
        return MathF.CopySign((float)limited, sample);
    }

    // Local-playback monitor inject (e.g. the Recorder plugin playing a clip
    // back locally). SPSC ring: producer = plugin playback thread via
    // EnqueueMonitorAudio, consumer = Tick. Mixed into the RX audio block so a
    // clip is audible on EVERY sink (browser WS + native) in any host mode —
    // unlike the desktop-only preview path. Power-of-two capacity for masking.
    private const int MonitorInjectCapacity = 1 << 14; // 16384 floats (~340 ms @ 48 kHz)
    private const int MonitorInjectMask = MonitorInjectCapacity - 1;
    private readonly float[] _monitorInject = new float[MonitorInjectCapacity];
    private long _monInjW;
    private long _monInjR;

    // Output samples per Tick at the 30 Hz cadence (~1600 @ 48 kHz). When RX
    // produces no audio this tick (RX1 muted / no band audio) but a local clip
    // is mid-playback through the monitor-inject ring, we synthesize a silent RX
    // block this size, mix the queued playback into it, and publish — otherwise
    // local WAV playback is inaudible while RX is silent (FIX 4). Sized to drain
    // the ring at ~real time so playback stays glitch-free.
    private static readonly int MonitorInjectSilentBlockSamples =
        Math.Min(AudioDrainCapacity, (int)Math.Round(AudioOutputRateHz * TickPeriod.TotalSeconds));

    /// <summary>
    /// Enqueue mono float32 samples to be mixed into the local RX audio output
    /// (the operator's monitor) on the next ticks. Realtime-safe, lock-free.
    /// Returns <c>false</c> (writing nothing) when the ring can't fit the block
    /// — the caller should retry rather than drop, so the consumer (RX tick
    /// clock) paces the producer and the playback stays glitch-free. Used by
    /// <see cref="Zeus.Plugins.Contracts.Audio.IAudioPlaybackSink.PlayLocal"/>.
    /// </summary>
    public bool EnqueueMonitorAudio(ReadOnlySpan<float> samples)
    {
        if (samples.Length == 0) return true;
        long w = _monInjW;
        long r = Volatile.Read(ref _monInjR);
        if (MonitorInjectCapacity - (w - r) < samples.Length) return false; // full — caller retries
        int start = (int)(w & MonitorInjectMask);
        int first = Math.Min(samples.Length, MonitorInjectCapacity - start);
        samples[..first].CopyTo(_monitorInject.AsSpan(start, first));
        if (first < samples.Length)
            samples[first..].CopyTo(_monitorInject.AsSpan(0, samples.Length - first));
        Volatile.Write(ref _monInjW, w + samples.Length);
        return true;
    }

    /// <summary>Samples still queued in the monitor-inject ring (for a player
    /// to wait out the tail before declaring playback finished).</summary>
    public long MonitorBacklog => Volatile.Read(ref _monInjW) - Volatile.Read(ref _monInjR);

    // Mix any queued monitor-inject audio into the RX block (consumer side).
    private void MixMonitorInject(Span<float> dest)
    {
        long w = Volatile.Read(ref _monInjW);
        long r = _monInjR;
        long avail = w - r;
        if (avail <= 0) return;
        int n = (int)Math.Min(avail, dest.Length);
        for (int i = 0; i < n; i++)
            dest[i] += _monitorInject[(int)((r + i) & MonitorInjectMask)];
        Volatile.Write(ref _monInjR, r + n);
    }

    // _engineLock serialises CONCURRENT WRITERS to _engine / _channelId /
    // _sampleRateHz on the rare connect/disconnect path. After iter5 the
    // hot path (OnIqFrame / OnPsFeedbackFrame / Tick) reads these fields
    // LOCK-FREE via Volatile.Read — the lock is here only because multiple
    // writer threads (RadioService.Connected / Disconnected events,
    // ConnectP2Async / DisconnectP2Async HTTP handlers) can race against
    // each other, and we want the swap to be atomic from the writer side.
    //
    // Single-thread WDSP ownership on the hot path is now provided by:
    //   (a) AttachRxSink AFTER the engine swap is committed, so the sink
    //       only ever observes the freshly-installed engine,
    //   (b) Volatile.Read inside the sink callbacks (acquire fence pairs
    //       with the release fence on lock release),
    //   (c) cross-thread mutators (SetMox / SetTxTune) routing through
    //       PostDspCommand instead of touching the engine directly.
    //
    // OnRadioStateChanged still calls engine.* methods under _engineLock —
    // documented at the call site; that's a rare operator-edge path, not the
    // per-packet hot path. CurrentEngine and the IDspEngine endpoint setters
    // (e.g. /api/mic-gain) also fall outside the hot path and keep the lock.
    private readonly object _engineLock = new();
    private IDspEngine? _engine;
    private int _channelId;

    // Secondary receivers (RX2..RXn). RX1 is the clock-master in _channelId; each
    // secondary is a slaved DDC broadcast every tick with its own RxId. Indexed by
    // RECEIVER INDEX — slot [1] is the historical RX2; slot [0] is unused (RX1 lives
    // in _channelId). Sized to the protocol DDC ceiling so the pipeline can drive
    // every DDC once B3/B4 wire the per-receiver state + UI. Today only slot 1 is
    // ever activated (Rx2Enabled), so behaviour is identical to the previous
    // hardcoded RX1/RX2 pair. ChannelId is read/written via Volatile on the hot
    // path exactly as the old _rx2ChannelId was. Size to the shared hardware
    // receiver contract (10) rather than the P2 wire ceiling (8) so Protocol 3
    // can expose every G2 receiver without another DSP array refactor.
    internal const int MaxReceivers = Zeus.Contracts.WireContract.MaxReceivers;
    private sealed class SecondaryRx
    {
        public SecondaryRx(int width)
        {
            PanBuf = new float[width];
            WfBuf = new float[width];
        }

        public int ChannelId = -1; // -1 = inactive; Volatile.Read/Write(ref ChannelId)
        // Raised-cosine fade-in samples remaining on a freshly opened channel.
        // Armed to SecondaryRxOpenFadeInSamples when EnsureSecondaryRxChannel
        // opens a new WDSP channel and consumed by the audio tick; 0 once the
        // ramp completes or the channel is closed. Cross-thread (armed on the
        // state thread, consumed on the DSP tick thread) — Volatile.Read/Write.
        public int FadeInSamplesRemaining;
        // Hardware DDC centre (this receiver's analogue of RX1's RadioLoHz). Under
        // CTUN it stays frozen while the dial roams within the window; with CTUN off
        // it follows the dial. Recentres if the dial would leave the captured DDC
        // bandwidth. Server-side only — not in StateDto. See UpdateRxLo.
        public long LoHz;
        public bool LoInit;
        public readonly float[] PanBuf;
        public readonly float[] WfBuf;
        public readonly float[] AudioBuf = new float[AudioDrainCapacity];
        public int PanCnt;  // 1 Hz panadapter freshness tally (display probe)
        public int WfCnt;   // 1 Hz waterfall freshness tally
        public long IqRmsLogMs; // 1 Hz IQ RMS/peak probe gate
        public long RoutedFrames; // diag: IQ frames routed to this receiver index
        public long FedFrames;    // diag: IQ frames actually FeedIq'd (channel open)
        // Last values applied to this WDSP channel. Radio state notifications are
        // whole-state snapshots, so a VFO-B drag otherwise re-sends mode, filter,
        // AGC, NR, and squelch on every mouse move. In particular, SetMode clears
        // WDSP's queued demodulated audio. RX1 already change-latches these values;
        // keep the same behavior for every secondary receiver.
        public RxMode? AppliedMode;
        public int AppliedFilterLowHz = int.MinValue;
        public int AppliedFilterHighHz = int.MinValue;
        public long AppliedVfoHz = long.MinValue;
        public int AppliedCtunShiftHz = int.MinValue;
        public double AppliedAgcTopDb = double.NaN;
        public NrConfig? AppliedNr;
        public AgcConfig? AppliedAgc;
        public SquelchConfig? AppliedSquelch;
        public BandpassWindow? AppliedBandpassWindow;
        public FilterPhaseMode? AppliedFilterPhase;
        public int AppliedZoom = int.MinValue;
        // Slewed AF-gain dB last pushed to this secondary's WDSP channel.
        // NaN sentinel = "no value applied yet" — used at channel-open so
        // the freshly-opened channel snaps to the operator's target instead
        // of dragging from a stale 0 dB. See AfGainSlewMaxDbPerTick.
        public double AppliedAfGainDb = double.NaN;

        public void ResetAppliedState()
        {
            AppliedMode = null;
            AppliedFilterLowHz = int.MinValue;
            AppliedFilterHighHz = int.MinValue;
            AppliedVfoHz = long.MinValue;
            AppliedCtunShiftHz = int.MinValue;
            AppliedAgcTopDb = double.NaN;
            AppliedAfGainDb = double.NaN;
            AppliedNr = null;
            AppliedAgc = null;
            AppliedSquelch = null;
            AppliedBandpassWindow = null;
            AppliedFilterPhase = null;
            AppliedZoom = int.MinValue;
        }
    }
    private readonly SecondaryRx[] _secondaryRx;
    private int _sampleRateHz;

    // One secondary-receiver audio block read during a Tick, paired with its
    // sample count. Used to feed the N-receiver "Both" mixer without per-tick
    // allocation (the scratch span below is reused every tick).
    internal readonly record struct RxAudioSlice(float[] Buffer, int Count);
    // Reused each tick to collect the enabled secondary RX audio blocks for the
    // mixer. Sized for every secondary slot (1..N-1) PLUS the Kiwi slice, which
    // is mixed in on the same bus; only the populated prefix is passed to
    // MixRxAudioN.
    private readonly RxAudioSlice[] _mixSlices = new RxAudioSlice[MaxReceivers + 1];

    // An external slice receiver feeds demodulated audio onto the SAME RX mix
    // bus as the hardware receivers. The null port produces no samples.
    private readonly IExternalRxAudioSource _externalRxAudioSource;
    private readonly float[] _kiwiMixBuf = new float[AudioDrainCapacity];

    // Protocol 2 path (parallel to the RadioService-owned P1 path). Held
    // directly here because RadioService is Protocol1Client-shaped and
    // growing a P2 variant there would require a larger refactor; for now
    // keeping it isolated avoids touching any P1 behavior.
    private Zeus.Protocol2.Protocol2Client? _p2Client;
    // Serialises the complete P2 ownership transition.  _engineLock protects
    // only the DSP-engine pointer swap; it deliberately does not cover awaits,
    // so it cannot prevent two HTTP connect/disconnect requests from opening
    // competing radio sessions.
    // Visible only while the lifecycle gate is held.  Keeping the not-yet-
    // published client here lets the wrapper reliably stop/dispose it when any
    // connect stage throws or is cancelled before _p2Client is committed.
    private Zeus.Protocol2.Protocol2Client? _p2ConnectingClient;
    private Action? _p2DisconnectedHandler;
    private long _p2ConnectionGeneration;
    private readonly IExternalRadioSidecar _externalRadioSidecar;
    private readonly bool _hasExternalRadioSidecar;

    // Wideband display mode. P2 uses bounded ADC snapshots; P3 consumes a
    // sidecar-projected full-span DisplayFrame when available. The radio/sidecar
    // transport is enabled only when this user setting is on AND at least one
    // display client is mounted. The P2 RX socket thread only copies the latest
    // assembled ADC snapshot into _widebandPendingSamples; FFT/binning runs on
    // RunWidebandDisplayAnalyzerAsync so display work cannot stall packet
    // receive or audio.
    private int _widebandDisplayEnabled;
    private int _widebandTransportEnabled;
    private int _p2WidebandTransportEnabled;
    // Hybrid P2 detail source. The target is display-only: it never mutates
    // RadioLo/VFO or any operator receiver. It uses a physical hidden DDC;
    // at full capacity the request stays on the honest raw-snapshot ceiling.
    private long _widebandTargetCenterHz = long.MinValue;
    private long _widebandSourceGeneration;
    private int _widebandDetailChannelId = -1;
    private int _widebandDetailDdcIndex = -1;
    private int _widebandDetailSourceReceiverIndex = int.MinValue;
    private long _widebandDetailSourceCenterHz;
    private int _widebandDetailZoomLevel = 1;
    private int _widebandDetailReady;
    private long _widebandDetailLastIqMs = long.MinValue;
    // Delay-compensated centre history for wideband-detail frames — same
    // pixel_ref emulation as _loHistory (issue #597 Phase 2), but fed by
    // wideband viewport pans. Lets a mid-drag frame carry the centre its
    // pixels were actually captured at, so panning glides instead of
    // stick-then-jump. Mirrors the detail channel's live analyzer FFT size
    // so the lookback tracks the true aperture (65,536 pts ≈ 341 ms at
    // 192 kHz, vs the engine default's ~85 ms).
    private readonly LoHistoryRing _widebandCenterHistory = new();
    private int _widebandDetailAnalyzerFftSize = P2WidebandZoomPolicy.MinDetailAnalyzerFftSize;
    private readonly object _widebandZoomRequestLock = new();
    internal const long WidebandDetailIqStaleMs = 750;
    private readonly WidebandSpectrumAnalyzer _widebandAnalyzer = new();
    // Deterministic wideband signal detection (opt-in via display settings).
    // Runs inside the analyzer loop on the same DC-suppressed, averaged
    // spectrum the display renders; results are published as a snapshot under
    // _widebandSignalSnapshotLock for the REST signals endpoint. Display-only:
    // markers never touch the binary DisplayFrame stream or any RX/TX path.
    private readonly WidebandSignalDetector _widebandSignalDetector =
        new((WidebandSpectrumAnalyzer.AnalysisFftSize / 2) + 1);
    private readonly WidebandSignalMarker[] _widebandMarkersBuf =
        new WidebandSignalMarker[WidebandSignalDetector.MaxTrackedSignals];
    private int _widebandSignalMarkersEnabled;
    private readonly object _widebandSignalSnapshotLock = new();
    private WidebandSignalMarker[] _widebandSignalSnapshot =
        new WidebandSignalMarker[WidebandSignalDetector.MaxTrackedSignals];
    private int _widebandSignalSnapshotCount;
    private long _widebandSignalSnapshotMs;
    private long _widebandSignalSnapshotCenterHz;
    private float _widebandSignalSnapshotHzPerPixel;
    // Analyzer-loop fault containment: a bad frame must never kill the
    // display worker. Counted and log-throttled for diagnostics.
    private long _widebandAnalyzerErrorCount;
    private long _widebandAnalyzerErrorLogMs;
    private readonly object _widebandFrameLock = new();
    private readonly SemaphoreSlim _widebandFrameSignal = new(0, int.MaxValue);
    private readonly short[] _widebandPendingSamples =
        new short[Zeus.Protocol2.Protocol2Client.WidebandMaxFrameSamples];
    private readonly short[] _widebandAnalysisSamples =
        new short[Zeus.Protocol2.Protocol2Client.WidebandMaxFrameSamples];
    private readonly float[] _widebandPanBuf = new float[WidebandSpectrumAnalyzer.DisplayWidth];
    private readonly float[] _widebandWfBuf = new float[WidebandSpectrumAnalyzer.DisplayWidth];
    private readonly float[] _widebandPanDecimatedBuf = new float[WidebandSpectrumAnalyzer.DisplayWidth];
    private readonly float[] _widebandWfDecimatedBuf = new float[WidebandSpectrumAnalyzer.DisplayWidth];
    private bool _widebandFramePending;
    private int _widebandPendingSampleCount;
    private int _widebandPendingSampleRateHz = Zeus.Protocol2.Protocol2Client.WidebandAdcSampleRateHz;
    private long _p3WidebandDisplayMissingLogMs;
    private long _p3WidebandDisplayErrorLogMs;

    // Radio-mic (UDP 1026) routing — external-audio-jacks re-port. The
    // re-blocker buffers 64-sample 1026 packets into the 960-sample mic blocks
    // TxAudioIngest consumes; it forwards into OnMicPcmBytesFromRadioMic, whose
    // in-lock _activeSource gate drops them unless a radio jack is armed. The
    // 1026 handler is attached to the live P2 client ONLY while a radio source
    // is selected, so Host-default has zero added RX cost. Both are lazily wired
    // (TxAudioIngest depends on this service, so it's resolved through a factory
    // to avoid a DI cycle — feedback_di_cycle_iservice_provider pattern).
    private readonly Func<TxAudioIngest?>? _txIngestFactory;
    private TxAudioIngest? _txIngest;
    private RadioMicReceiver? _radioMicReceiver;
    private bool _radioMicAttached;
    // P1 codec mic / line-in re-blocker (issue #992). EP6 frames carry the
    // codec samples inline; the receiver decimates to 48 kHz and re-blocks
    // into the same 960-sample mic blocks TxAudioIngest consumes for the
    // Saturn 1026 path. Wired through the same in-lock _activeSource gate, so
    // a stale radio source can't leak onto the air after a switch back to Host.
    private P1RadioMicReceiver? _p1RadioMicReceiver;
    private bool _p1RadioMicAttached;

    private RxMode _appliedMode = RxMode.USB;
    // Sideband actually pushed to WDSP. Equals _appliedMode for every mode except
    // FreeDv, where it follows the FreeDV band convention (LSB < 10 MHz, USB ≥) so
    // a dial crossing 10 MHz while staying in FreeDv re-flips the demod/mod
    // orientation. See RadioService.EffectiveEngineMode.
    private RxMode _appliedEngineMode = RxMode.USB;
    private RxMode _appliedTxMode = RxMode.USB;
    private RxMode _appliedTxEngineMode = RxMode.USB;
    private int _appliedLowHz;
    private int _appliedHighHz;
    // WDSP RX filter shift currently applied (Hz). Equals
    // (EffectiveLoHz(VfoHz) - RadioLoHz) — the always-frozen-NCO model.
    // Tracked separately from FilterLowHz/HighHz so re-pushing the filter
    // when the dial moves doesn't require Mutate-ing the StateDto.
    // See docs/prd/panfall_behavior.md.
    private int _appliedCtunOffsetHz;
    private int _appliedTxLowHz;
    private int _appliedTxHighHz;

    // Derive the TX bandpass edges WDSP should use from the live mode, ignoring
    // the sign already stored in StateDto. WDSP selects the SSB sideband from
    // the sign of the bandpass (negative = LSB-family, positive = USB-family),
    // so the sign MUST track the current mode — not whatever a stale prefs DB or
    // a mode writer that forgot to re-sign happened to leave behind. Re-deriving
    // from the magnitudes via the single source of truth (SignedFilterForMode)
    // is idempotent for well-formed state, so this never overrides an operator's
    // deliberate width edit.
    internal sealed record EffectiveTxAudioProfile(
        int MicGainDb,
        double LevelerMaxGainDb,
        TxLevelingConfig TxLeveling,
        CfcConfig Cfc,
        TxPhaseRotatorConfig TxPhaseRotator,
        int FilterLowHz,
        int FilterHighHz,
        double CarrierCoefficient);

    internal static EffectiveTxAudioProfile ResolveEffectiveTxAudioProfile(
        StateDto s,
        RxMode txEngineMode)
    {
        var txMode = RadioFrequencyResolver.TxReceiver(s).Mode;
        var am = RadioService.NormalizeAmTxProfile(s.AmTxProfile);
        if (txMode is RxMode.AM or RxMode.SAM)
        {
            int high = am.TxFilterHighHz;
            return new(
                am.MicGainDb,
                am.LevelerMaxGainDb,
                am.TxLeveling,
                am.Cfc,
                am.TxPhaseRotator,
                -high,
                high,
                0.5 * Math.Sqrt(am.CarrierLevelPercent / 100.0));
        }
        int loAbs = Math.Min(Math.Abs(s.TxFilterLowHz), Math.Abs(s.TxFilterHighHz));
        int hiAbs = Math.Max(Math.Abs(s.TxFilterLowHz), Math.Abs(s.TxFilterHighHz));
        // EffectiveEngineMode is a no-op for every mode except FreeDv, where the
        // TX bandpass must follow the same band-convention sideband as the RX/TXA
        // demod so the transmitted OFDM carriers land on the band's shared
        // orientation (LSB < 10 MHz, USB ≥). See RadioService.EffectiveEngineMode.
        var (low, highNormal) = RadioService.SignedFilterForMode(txEngineMode, loAbs, hiAbs);
        return new(
            s.MicGainDb,
            s.LevelerMaxGainDb,
            s.TxLeveling ?? new TxLevelingConfig(),
            s.Cfc ?? CfcConfig.Default,
            s.TxPhaseRotator ?? new TxPhaseRotatorConfig(),
            low,
            highNormal,
            0.5 * Math.Sqrt(am.CarrierLevelPercent / 100.0));
    }

    private static (int low, int high) SignedTxFilterFor(StateDto s, RxMode txEngineMode)
    {
        var effective = ResolveEffectiveTxAudioProfile(s, txEngineMode);
        return (effective.FilterLowHz, effective.FilterHighHz);
    }

    internal static bool IsDigitalTxZeusMode(RxMode mode) =>
        mode is RxMode.DIGU or RxMode.DIGL or RxMode.FreeDv;

    internal static (RxMode engineMode, int low, int high) SecondaryEngineFilterFor(
        RxMode mode, long vfoHz, int lowHz, int highHz)
    {
        var engineMode = RadioService.EffectiveEngineMode(mode, vfoHz);
        if (mode == RxMode.FreeDv)
        {
            int loAbs = Math.Min(Math.Abs(lowHz), Math.Abs(highHz));
            int hiAbs = Math.Max(Math.Abs(lowHz), Math.Abs(highHz));
            (lowHz, highHz) = RadioService.SignedFilterForMode(engineMode, loAbs, hiAbs);
        }
        else
        {
            (lowHz, highHz) = RadioService.NormalizeLegacyDigitalFilter(mode, lowHz, highHz);
        }

        return (engineMode, lowHz, highHz);
    }

    // RX bandpass signed for the effective engine sideband. For FreeDv this
    // re-signs the stored (USB-positive) FreeDV passband to the convention
    // sideband so an LSB sub-10 MHz demod gets a negative-frequency passband
    // rather than a dead positive one. Other modes pass their already-signed
    // stored width through, except for the legacy symmetric DIG migration.
    internal static (int low, int high) SignedRxFilterFor(StateDto s, RxMode engineMode)
    {
        if (s.Mode == RxMode.FreeDv)
        {
            int loAbs = Math.Min(Math.Abs(s.FilterLowHz), Math.Abs(s.FilterHighHz));
            int hiAbs = Math.Max(Math.Abs(s.FilterLowHz), Math.Abs(s.FilterHighHz));
            return RadioService.SignedFilterForMode(engineMode, loAbs, hiAbs);
        }
        return RadioService.NormalizeLegacyDigitalFilter(
            s.Mode, s.FilterLowHz, s.FilterHighHz);
    }
    // Effective AGC-T ceiling (s.AgcTopDb + s.AgcOffsetDb) last actually
    // pushed to WDSP. Slewed by AgcTopSlewMaxDbPerTick per pipeline tick so
    // a slider drag (or an Auto-AGC servo jump) doesn't stair-step
    // wcpAGC.max_gain into a click train; the value snaps to the target
    // once within one cap, so the steady-state push is bit-identical to a
    // direct write — WDSP remains the gain authority. Computed once per
    // OnRadioStateChanged so primary + every secondary fan the same dB.
    private double _appliedAgcCeilingDb;
    // Slewed AF-gain dB last pushed to RX1 — same rate-cap rationale as
    // _appliedAgcCeilingDb, applied to the WDSP panel.gain1 register.
    // Secondary receivers keep their own per-RX slew state on
    // SecondaryRx.AppliedAfGainDb because each receiver carries its own
    // AfGainDb in Receivers[i].
    private double _appliedRxAfGainDb;

    // FreeDV decoded-speech AF gain (linear), slewed per audio tick. FreeDV's
    // vocoder output level is independent of the pre-decode modem amplitude, so
    // the WDSP panel gain (forced to unity in FreeDV) can't set the listening
    // volume — the operator's AF is re-applied to the decoded speech here. Starts
    // at unity so a non-FreeDV channel (ApplyFreeDvAfGain never runs) is
    // byte-identical. See ApplyFreeDvAfGain and the FreeDV AF note in
    // OnRadioStateChanged.
    private double _freeDvAfGainLinear = 1.0;

    // Per-tick caps (dB) for the AF-gain and AGC-T register pushes to WDSP.
    // Both registers (panel.gain1, agc.max_gain) are applied by WDSP as
    // instant scalar multiplies — without a per-tick rate cap, a slider
    // drag at the 30 Hz tick rate becomes a stair-step of audible clicks.
    //
    // CONSERVATIVE PLACEHOLDERS pending bench tuning on a G2 with 3 RX
    // (issue #939). Smaller cap = quieter individual step but more
    // perceived lag; larger cap = snappier feel but louder per-step
    // residual click. AGC-T cap MUST exceed Auto-AGC's noise-floor servo
    // step so auto-tracking isn't throttled — Auto-AGC moves the offset
    // by up to ~30 dB per ~500 ms eval (~60 dB/s peak burst). 6 dB/tick at
    // the 30 Hz pipeline tick = 180 dB/s ≈ 3× headroom over that peak.
    private const double AfGainSlewMaxDbPerTick = 2.0;
    private const double AgcTopSlewMaxDbPerTick = 6.0;

    private static double StepTowardCappedDb(double current, double target, double maxStep)
    {
        double delta = target - current;
        return Math.Abs(delta) <= maxStep ? target : current + Math.Sign(delta) * maxStep;
    }

    // Apply the FreeDV decoded-speech AF gain in place. The WDSP panel gain is
    // forced to unity in FreeDV (it would otherwise act on the pre-decode modem
    // audio ProcessRx discards), so this is the only seam that sets FreeDV
    // listening volume. Ramps from the previous block's gain to a rate-capped
    // (AfGainSlewMaxDbPerTick) target across the block, mirroring the WDSP
    // panel-gain slew, so a slider drag fades click-free instead of stair-
    // stepping. Updates _freeDvAfGainLinear to the end-of-block gain.
    private void ApplyFreeDvAfGain(Span<float> block, double targetDb)
    {
        double startGain = _freeDvAfGainLinear;
        double startDb = 20.0 * Math.Log10(Math.Max(startGain, 1e-9));
        double endDb = StepTowardCappedDb(startDb, targetDb, AfGainSlewMaxDbPerTick);
        double endGain = Math.Pow(10.0, endDb / 20.0);
        int n = block.Length;
        if (n == 0) { _freeDvAfGainLinear = endGain; return; }
        for (int i = 0; i < n; i++)
        {
            double t = (i + 1) / (double)n;
            double g = startGain + (endGain - startGain) * t;
            block[i] = (float)(block[i] * g);
        }
        _freeDvAfGainLinear = endGain;
    }
    // TX mic gain change-detect cache. NaN sentinel forces the first apply
    // even when the persisted value happens to equal 0 dB (the engine seam
    // expects an explicit unity SetTxPanelGain call so the TX chain leaves
    // its uninitialised state in a known place after channel-open).
    private double _appliedTxMicGainLinear = double.NaN;
    // Same NaN-first-apply sentinel for the Leveler ceiling so a channel-open
    // with the persisted value matching the 8 dB default still re-pushes it.
    private double _appliedTxLevelerMaxGainDb = double.NaN;
    private double _appliedTxAmCarrierLevel = double.NaN;
    private NrConfig _appliedNr = new();
    // Diversity-combiner latch. Null seed so the first state push always applies
    // (mirrors _appliedNr's change-detect). Global, not per-channel.
    private DiversityConfig? _appliedDiversity;
    // ---- Diversity combiner (managed, P2-only) ----
    // Combines simultaneous ADC samples from one synchronized P2 packet with
    // gain·e^{jθ} before RX0 demodulation. No source survives its paired frame.
    // Config and IQ processing are serialized through the engine lock.
    // Protocol 1 ingest does not use this combiner.
    private volatile bool _divEnabled;
    private int _divReferenceRx;
    private DiversityOutput _divOutput;
    private double _divWeightI = 1.0, _divWeightQ;
    private double[] _divCombineBuf = [];
    // AGC mode + custom params latch (issue: DSP controls Thetis parity §4).
    // Same change-detect pattern as _appliedNr — SetAgc only fires when the
    // config actually moves. Seeded to Med so a connect landing on the Med
    // default still matches what ApplyStateToNewChannel force-pushed.
    private AgcConfig _appliedAgc = new(AgcMode.Med);
    // RX squelch latch (issue: DSP controls Thetis parity §5). Same
    // change-detect pattern as _appliedAgc — SetSquelch only fires when the
    // config actually moves. Seeded to the off default so a connect landing on
    // squelch-off still matches what ApplyStateToNewChannel force-pushed.
    private SquelchConfig _appliedSquelch = new();
    // TX leveling latch (issue: DSP controls Thetis parity §6.1-6.3). Same
    // change-detect pattern as _appliedAgc/_appliedSquelch — SetTxLeveling only
    // fires when the config actually moves. Seeded to the TxLevelingConfig
    // defaults so a connect landing on defaults still matches what
    // ApplyStateToNewChannel force-pushed.
    private TxLevelingConfig _appliedTxLeveling = new();
    // TX phase rotator latch (Thetis DSP->CFC->PhaseRot parity). Default-OFF
    // matches the TXA-open baseline; every operator/Auto Tune edit is pushed
    // live and replayed on reconnect.
    private TxPhaseRotatorConfig _appliedTxPhaseRotator = new();
    // RX/TX bandpass "rectangularity" latches — issue #871. Seeded to
    // BandpassWindow.Normal (= byte 1), which resolves to the WDSP open-time tap
    // count, so a connect landing on the default matches what
    // ApplyStateToNewChannel force-pushed. Same change-detect pattern as the
    // other _applied* siblings.
    private BandpassWindow _appliedRxBandpassWindow = BandpassWindow.Normal;
    private BandpassWindow _appliedTxBandpassWindow = BandpassWindow.Normal;
    private FilterPhaseMode _appliedRxFilterPhase = FilterPhaseMode.Linear;
    private FilterPhaseMode _appliedTxFilterPhase = FilterPhaseMode.Linear;
    private int _appliedZoomLevel = 1;
    // PureSignal latched values — same change-detect pattern as the others
    // so OnRadioStateChanged only fires PS setters when values move.
    private bool _appliedPsEnabled;
    private bool _appliedPsAuto = true;
    private bool _appliedPsSingle;
    private double _appliedPsMoxDelaySec = 0.2;
    private double _appliedPsLoopDelaySec;
    private double _appliedPsAmpDelayNs = 150.0;
    private double _appliedPsHwPeak = 0.4072;
    private PsFeedbackSource _appliedPsFeedbackSource = PsFeedbackSource.Internal;
    private int _appliedExternalPsFeedbackAttenuationDb = -1;
    private int _appliedExternalPsCorrectionMode = -1;
    private int _externalPsFailCloseLatched;
    // PS-Monitor toggle (issue #121). Pure source-routing flag — Tick reads
    // it on each tick to choose between the TX analyzer (predistorted IQ)
    // and the PS-feedback analyzer (post-PA loopback IQ). volatile because
    // OnRadioStateChanged writes from the state-handler thread and Tick
    // reads from the pipeline thread — no compound mutation, just a bool.
    private volatile bool _psMonitorEnabled;
    private long _psMonitorTickCount;
    // TX Monitor latch (issue #106 follow-up). Same change-detect pattern as
    // _psMonitorEnabled — UpdateState writes when StateDto.TxMonitorEnabled
    // flips, and the latch fires engine.SetTxMonitorEnabled exactly once per
    // edge so we don't spam the engine on every tick with the same value.
    private bool _appliedTxMonitorEnabled;
    // Meter-only TX monitor (Auto Tune). When true AND the monitor is on, the
    // TXA chain still runs (so the stage meters from ProcessTxBlock animate and
    // Auto Tune can sample), but the demodulated monitor audio is NOT broadcast
    // to the operator's playback path — the metering "happens in the background"
    // with no audible preview. volatile: written from the preview-endpoint
    // request thread, read on the pipeline tick thread. Self-clears whenever the
    // monitor latch turns off (below).
    private volatile bool _txMonitorMeterOnly;
    // Set by DisconnectP2Async so the next OnRadioStateChanged after a
    // fresh ConnectP2Async re-pushes every PS field regardless of equality
    // — necessary because the new WdspDspEngine instance starts with field
    // defaults that don't match the cached `_appliedPs*` state.
    private bool _psResyncRequired;
    // TwoTone latched fields (protocol-agnostic, drives PostGen mode=1).
    private bool _appliedTwoToneEnabled;
    private double _appliedTwoToneFreq1 = 700.0;
    private double _appliedTwoToneFreq2 = 1900.0;
    private double _appliedTwoToneMag = 0.49;
    // CFC (Continuous Frequency Compressor) — issue #123. Default-OFF so a
    // fresh state-change push (no Cfc field on the wire) doesn't flip the
    // engine into a partial config. _psResyncRequired piggybacks: when a P2
    // reconnect tears down the engine, we re-push the CFC profile too so the
    // new WdspDspEngine instance picks up the operator's persisted config.
    private CfcConfig _appliedCfc = CfcConfig.Default;
    // Last values pushed to the engine, so a state change that does not
    // touch these does not re-push a 16384-tap FIR rebuild on every tick.
    private GraphicEqConfig _appliedTxEq = GraphicEqConfig.Default;
    private GraphicEqConfig _appliedRxEq = GraphicEqConfig.Default;
    private TxGateConfig _appliedTxGate = TxGateConfig.Default;
    private ParametricEqConfig? _appliedTxEqP;
    private ParametricEqConfig? _appliedRxEqP;
    private ParametricCfcConfig? _appliedCfcP;
    // A record of scalars, so plain equality is the right comparison here —
    // unlike the EQ configs, which hold arrays.
    private TxDexpConfig _appliedTxDexp = TxDexpConfig.Default;

    // RX front-end (step attenuator + Mercury preamp). Mirrored to a live
    // Protocol2Client when the value moves; on P1 these go through
    // RadioService.ActiveClient directly. Issue #126 — without this
    // forwarding the S-ATT slider and PRE button were inert on Angelia /
    // ANAN-100D. Effective atten = StateDto.AttenDb + AttOffsetDb (auto-ATT
    // offset), so the existing overload control loop continues to drive the
    // radio on P2. Sentinel -1 forces the first push regardless of value.
    private int _appliedEffectiveAttDb = -1;
    private int _appliedAttenuatorAdc = -1;
    private bool _appliedPreampOn;

    private int _seq;
    private uint _audioSeq;
    // Latched from MoxChanged so Tick can select ordinary RX display-DUP or
    // PureSignal's established keyed TX/feedback display path without
    // snapshotting RadioService. TUN also flips MOX on, so this single flag
    // covers both paths. volatile because MoxChanged fires on the caller's
    // thread and Tick reads from the pipeline thread.
    private volatile bool _keyed;
    // Measurement-only TX-turnaround latency observer (MOX→first-IQ-at-egress
    // and PTT-release→egress-drain). Pure instrumentation: never gates, delays,
    // or mutates the TX IQ path and never touches RX. See TxTurnaroundTelemetry.
    private TxTurnaroundTelemetry? _txTurnaround;
    // Issue #597 Phase 0: display-EMA fast-attack latch. OnRadioStateChanged
    // arms it when RadioLoHz moves (the operator is tuning) and Tick restores
    // the default tau once the LO has been quiet for FastAttackRestoreMs.
    // Debounced by design: one arm P/Invoke at gesture start, one restore
    // P/Invoke at gesture end — NOT per wheel notch. Skipped entirely while
    // _keyed so RX analyzer smoothing is never retuned mid-over (PS safety;
    // the engine method is additionally scoped to the RX analyzer). long.MinValue
    // sentinel suppresses the arm on the first state callback after connect.
    // _fastAttackLastLoHz is only touched on the state-handler thread;
    // _fastAttackLoChangedAt crosses to the RX thread via Interlocked.
    private long _fastAttackLastLoHz = long.MinValue;
    private long _fastAttackLoChangedAt;
    private volatile bool _fastAttackActive;
    // Last VfoState pushed to clients (RX0). Broadcast a VfoStateFrame whenever
    // the dial or the display centre moves so the frontend tracks a HARDWARE
    // tune immediately, instead of waiting for the 1 Hz state poll (dial) and
    // the display-frame echo (pan). State-handler thread only. long.MinValue =
    // nothing pushed yet (suppresses a spurious edge on the connect snapshot).
    private long _lastPushedVfoHz = long.MinValue;
    private long _lastPushedRadioLoHz = long.MinValue;
    // Same, for RX2 / VFO B (Receiver: 1). Reset to the sentinel whenever RX2 is
    // disabled so re-enabling it pushes a fresh edge.
    private long _lastPushedRx2VfoHz = long.MinValue;
    private long _lastPushedRx2LoHz = long.MinValue;
    private const int FastAttackRestoreMs = 250;
    private static readonly long FastAttackRestoreTicks =
        (long)(FastAttackRestoreMs / 1000.0 * Stopwatch.Frequency);
    // Issue #597 Phase 2: delay-compensated CenterHz stamp (Thetis pixel_ref
    // emulation). The display pixels broadcast at tick time were computed
    // from IQ captured ~D earlier; stamping them with the LO from
    // LookupAt(now − D) makes frames self-describing — the client renders
    // data where it actually belongs, killing the mislabeled-frame
    // snap-back at the root (no wire change: same CenterHz field).
    // D = ½·FFT-fill + display-EMA lag + per-protocol transport. Override
    // the transport+EMA constant with ZEUS_CENTER_STAMP_LAG_MS for bench
    // tuning at 48/96/192/384 kHz on P1 (HL2) and P2 (G2). When the LO is
    // stable longer than D the stamp equals live RadioLoHz — WWV cal
    // (#325) and every stable-LO consumer is byte-identical (see
    // LoHistoryRingTests regression).
    private readonly LoHistoryRing _loHistory = new();
    private const double CenterStampEmaLagMs = 20.0;    // fast-attack tau during gestures (Phase 0)
    private const double CenterStampTransportP1Ms = 40.0;
    private const double CenterStampTransportP2Ms = 15.0;
    private static readonly double? CenterStampLagOverrideMs = ReadCenterStampLagOverrideMs();

    private static double? ReadCenterStampLagOverrideMs()
    {
        var raw = Environment.GetEnvironmentVariable("ZEUS_CENTER_STAMP_LAG_MS");
        return double.TryParse(raw, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var ms) && ms >= 0
            ? ms
            : null;
    }
    // RX S-meter broadcast throttle. Pipeline ticks at 30 Hz; broadcasting
    // every 6 ticks = 5 Hz gives a smoother meter than Thetis's 4 Hz baseline
    // without spamming the WS (30 Hz dBm readouts add nothing a UI can use).
    private int _rxMeterTickMod;
    private const int RxMeterTickModulus = 6;

    // RX audio fade envelope across MOX edges. WDSP's RXA SetChannelState
    // (dmp=1 on TX-engage) damps the outgoing side internally, but the resume
    // edge (dmp=0 at MOX-off) and the buffer-drain endpoint in the browser
    // audio-client both produce audible clicks under some setups (audio
    // interfaces, USB-DAC headphones). Smoothing here is cheap insurance:
    // key-down gets a short demod-stage fade-out, and key-up waits for the
    // post-TX mute drain before applying a raised-cosine fade-in to the final
    // post-processed RX block that is actually broadcast.
    private const int RxFadeSamples = 240;          // 5 ms @ 48 kHz
    internal const int RxPostTxFadeInSamples = 3840; // 80 ms @ 48 kHz
    // Raised-cosine fade-in applied to the first audio blocks of a freshly
    // opened secondary receiver (RX2..RXn). A newly opened WDSP RX channel
    // starts with an unconverged AGC and can emit a large first block; summed
    // unramped into the additive RX mix bus (MixRxAudioN) that lands as an
    // audible click/distortion the instant RX2 is toggled on. Mirrors RX1's
    // post-TX raised-cosine resume so every receiver enters the mix smoothly.
    internal const int SecondaryRxOpenFadeInSamples = RxPostTxFadeInSamples;
    private const int RxPostTxAudioCadenceHz = 30;
    internal const int DefaultRxPostTxMuteBlocks = 6; // ~200 ms at the 30 Hz audio cadence
    private volatile bool _rxFadeOutPending;        // first RX block after MOX↑
    private volatile bool _rxAudioSuppressedForTx;  // publish silence/sidetone instead of RX while TX is keyed
    private volatile bool _fullDuplexRxActiveForCurrentTx;
    private volatile bool _localRxAudioContinuousForCurrentTx;
    // Set on the MOX edge; consumed by the first suppressed tick, which latches
    // the operator's DUP choice without taking the radio-state lock.
    private volatile bool _fullDuplexLatchPendingForCurrentTx;
    // Latch key-down policy so UI/PS changes cannot alter the matching keyed
    // interval and key-up cleanup. P2 keeps RX display analyzers intact; P1 PS
    // retains its established TX-domain reset until P1 has independent receive
    // and transmit tuning frames.
    private volatile bool _stopRxForPureSignalForCurrentTx;
    private bool _resetDisplayPixelsForCurrentTx;
    private bool _p2DisplayDuplexForCurrentTx;
    private int _rxPostTxMuteBlocksRemaining;       // RXA transition-drain blocks after MOX↓
    private int _rxPostTxFadeInSamplesRemaining;    // final-output soft resume after post-TX drain
    private int _rxPostTxDisplayFramesRemaining;    // RXA analyzer transition frames after MOX↓

    // ---- iter5 single-DSP-thread scaffolding -----------------------------
    // The pipeline now owns its hot path via IRxPacketSink: when a radio
    // connects we AttachRxSink to the protocol client and every IQ/PS-feedback
    // packet flows synchronously into OnIqFrame/OnPsFeedbackFrame on the RX
    // OS thread. WDSP calls happen inline on that thread. The display
    // Tick is piggybacked: OnIqFrame checks Stopwatch.GetTimestamp() and
    // fires Tick inline when the next cadence deadline has arrived.
    //
    // While a sink is attached the ExecuteAsync PeriodicTimer skips Tick
    // (the "watcher" pauses). With no sink attached (synthetic mode, pre-
    // connect, or post-disconnect) the PeriodicTimer drives Tick at 30 Hz
    // so the display chain stays live even when no IQ is flowing.
    //
    // Cross-thread mutations that should run on the DSP thread post Action
    // commands here; the DSP thread drains the queue at the top of every
    // IqFrame (and every Tick when no sink is attached). After pass 2:
    // SetMox / SetTxTune route through this queue so WDSP TXA state edges
    // happen on the same thread that feeds RX IQ. OnRadioStateChanged still
    // calls engine.* directly (rare operator-edge path — the engine's own
    // disposed-check guards cover engine-swap-mid-call); engine swaps
    // serialise through _engineLock (writer side only).
    private volatile bool _rxSinkAttached;
    // issue #1167: during the RX-sink attach/detach window the timer thread and
    // the RX inline-tick thread can both be live, so Tick can run on two threads
    // at once. FloatSpscRing's producer side is strict single-thread; this gate
    // makes Tick mutually exclusive so the producer stays single-threaded.
    private readonly SingleEntryGate _tickGate = new();
    private long _tickReentrySkips;
    /// <summary>Count of Tick calls skipped because another thread held the
    /// gate — non-zero only during the sink attach/detach window (issue #1167).
    /// Telemetry/test seam.</summary>
    internal long TickReentrySkips => Interlocked.Read(ref _tickReentrySkips);
    // Reference to the protocol client this pipeline is currently sinking RX
    // packets from. Cached so we can explicitly DetachRxSink on disconnect —
    // RadioService nulls its ActiveClient before raising Disconnected, so the
    // event handler can't pull the client off that surface.
    private IProtocol1Client? _attachedSinkP1;
    private Zeus.Protocol2.Protocol2Client? _attachedSinkP2;
    private long _lastTickStopwatchTicks;
    private long _lastInlineTickStopwatchTicks;
    private long _inlineTickDeadlineStopwatchTicks;
    private long _inlineDisplayDeadlineStopwatchTicks;
    private static readonly long TickPeriodStopwatchTicks =
        (long)(Stopwatch.Frequency / 30.0);
    private static readonly long TickMaxSlipStopwatchTicks =
        TickPeriodStopwatchTicks * 2;

    // #1148 inline-tick cadence telemetry. The active RX packet thread drives
    // the full audio/DSP Tick inline (one audio block per tick, normally 30 Hz /
    // 33.33 ms). If the radio-speaker UDP burst perturbs RX-IQ delivery, ticks
    // slip toward longer intervals and the host soundcard ring underruns (the
    // #1148 symptom). Emit mean / p99 / max interval + a count of intervals
    // beyond the historical 50 ms underrun threshold at ~1 Hz. Single-threaded
    // — only the active RX thread calls MaybeTickInline — so plain fields + a
    // fixed ring suffice.
    private long _tickDiagLastEmitTicks;
    private long _tickDiagCount;
    private long _tickDiagSumTicks;
    private long _tickDiagMaxTicks;
    private long _tickDiagSlowCount;
    private readonly long[] _tickDiagRing = new long[256]; // power of two for the index wrap
    private readonly long[] _tickDiagSortScratch = new long[256];
    private int _tickDiagRingPos;
    private int _tickDiagRingFill;
    private static readonly long TickSlowThresholdTicks =
        (long)(TickPeriodStopwatchTicks * 1.5);

    private readonly ConcurrentQueue<Action> _dspCommands = new();

    // DSP-thread-owned scratch buffers. Allocated once at construction so
    // both the PeriodicTimer-driven Tick (synthetic mode) and the inline
    // RX-thread Tick (sink mode) share the same memory. Sink-mode and
    // timer-mode are mutually exclusive (see _rxSinkAttached gate in
    // ExecuteAsync), so no synchronisation is needed.
    private readonly float[] _panBuf;
    private readonly float[] _wfBuf;
    private readonly float[] _panDecimatedBuf;
    private readonly float[] _wfDecimatedBuf;
    // Last fresh TX-analyzer pan/wf frame captured this transmission. Reused
    // on stale TX ticks while keyed so the display doesn't fall through to
    // the RX analyzer (RX noise floor plotted against the TX display window
    // maps to fully-below-floor pixels — a black flash per stale tick, seen
    // by the operator as a panadapter/waterfall strobe during SSB TX;
    // issue #162). Cleared on every MOX edge so a new transmission starts
    // from scratch and no stale TX pixels leak into RX. Valid flags gate
    // reuse for the first-tick / no-TX-analyzer (Synthetic) cases, and the
    // capture timestamps bound reuse during analyzer stalls; those cases
    // still fall through to the established RX / PS-feedback sources.
    private readonly object _displayFrameRateLock = new();
    private long _displayFrameBudgetLastTicks;
    private double _displayFrameBudget = 1.0;
    // P2 raw-wideband and P3 sidecar ownership are mutually exclusive in steady
    // state; sharing this deadline preserves one cadence across their handoff.
    private long _unpacedDisplayDeadlineStopwatchTicks;
    private int _waterfallFrameCounter;
    // Per-receiver display/audio probe: 1 Hz tally of how often each receiver's
    // pan/wf pixout reports fresh data. If a secondary's wf advances but pan stays
    // ~0, the freeze is backend (pan pixout never goes fresh); if both advance like
    // RX1, the freeze is frontend rendering. RX1 counters are _rx1*; each secondary
    // tracks its own PanCnt/WfCnt (see SecondaryRx).
    private long _dispFlagLogMs;
    // 1 Hz throttle for the TX pixel dB-range diagnostic (see Tick).
    private long _txPixelDbgMs;
    private int _dispTicks, _rx1PanCnt, _rx1WfCnt;
    private readonly float[] _audioBuf = new float[AudioDrainCapacity];

    private readonly float[] _lastTxPanBuf;
    private readonly float[] _lastTxWfBuf;
    private volatile bool _lastTxPanValid;
    private volatile bool _lastTxWfValid;
    private long _lastTxPanCaptureMs;
    private long _lastTxWfCaptureMs;
    internal const long TxDisplayHoldMaxAgeMs = 400;

    // Cached panadapter snapshot for the frequency-calibration service
    // (issue #325). Tick fills this every cycle that produced a valid
    // pan frame; the cal service reads it without racing for the WDSP
    // "fresh frame" flag. Single-writer (Tick) + occasional reader
    // (cal) — protected by _calPanLock.
    private readonly float[] _calPanSnapshot;
    private float _calPanHzPerPixel;
    private long _calPanCenterHz;
    private long _calPanSnapshotMs;
    private long _calPanSnapshotVersion;
    private readonly object _calPanLock = new();

    // Scratch buffer for the auto-AGC noise-floor estimate (issue #806). Filled
    // and gated in-place by the floor tracker on the single meter thread;
    // never read concurrently, so it needs no lock.
    private readonly float[] _autoAgcFloorBuf;
    // Dedicated WDSP pixout 2. Unlike the visual pan/waterfall outputs this is
    // average power in linear space, normalized to a one-hertz bandwidth, and
    // is drained at the meter cadence even when no spectrum panel is mounted.
    private readonly float[] _snrPowerBuf;
    private readonly InPassbandSnrEstimator _snrEstimator = new();
    // RX0 passband evidence sampled at the analyzer/meter cadence. The audio
    // hot path only reads these scalar fields; no allocation, lock or I/O.
    private int _rxLevelerRfSignalResolved;
    private int _rxLevelerAdcOverloadRisk;
    private int _rxLevelerRfAcquireHits;
    private int _rxLevelerRfReleaseMisses;
    private long _rxLevelerRfEvidenceMs = long.MinValue;
    // Thetis-faithful band noise-floor tracker (display.cs processNoiseFloor
    // port): gated quiet-bin mean + 2-tap power smoothing + 2 s attack lerp +
    // fast-attack. Fed at the 5 Hz meter cadence; consumed by RadioService's
    // auto-AGC-T servo. Replaces the old 20th-percentile estimator, which read
    // ~2-5 dB low on dB-averaged pixels and is not what Thetis does.
    private readonly AutoAgcNoiseFloorTracker _autoAgcTracker = new(feedFps: 5.0);
    // Fast-attack trigger memory (Thetis sets FastAttackNoiseFloor on band
    // change / >0.5 MHz VFO jump / preamp / attenuator step / auto re-engage).
    // Detected here from the state snapshots the meter tick already reads. The
    // MinValue sentinels suppress a false trigger on the first tick after
    // construction or after auto is re-enabled.
    private long _autoAgcLastVfoHz = long.MinValue;
    private bool _autoAgcLastPreampOn;
    private int _autoAgcLastAttenDb = int.MinValue;
    private bool _autoAgcWasEnabled;
    private long _autoAgcLastSpectrumFeedMs = long.MinValue;
    private int _autoAgcFeedSource; // 0 none / 1 spectrum bins / 2 S-meter scalar
    // Serializes tracker access between the P1/P2 meter tick and the P3
    // sidecar meter publisher. The two are mutually exclusive on
    // IsProtocol3Active in steady state, but a switchover tick can overlap by
    // a frame, and the tracker is deliberately not thread-safe.
    private readonly object _autoAgcTrackerLock = new();
    // A VFO move this large means a band change (Thetis fast-attacks the noise
    // floor on a > 0.5 MHz frequency delta, display.cs:911).
    private const double AutoAgcFastAttackVfoDeltaHz = 500_000.0;
    // Sustained spectrum outage before the tracker falls back to the S-meter:
    // 1.5 s > normal frame gaps, short enough to keep tracking on engines that
    // never produce a spectrum.
    private const long AutoAgcSpectrumStaleMs = 1500;
    private readonly float[] _diagWfSnapshot;
    private long _diagWfSnapshotMs;
    private long _diagDisplayFrameMs;
    private uint _diagDisplaySeq;
    private long _diagDisplayFrameCount;
    private bool _diagLastPanValid;
    private bool _diagLastWfValid;
    private string _diagLastPanSource = "none";
    private string _diagLastWfSource = "none";
    private bool _diagLastKeyed;
    private bool _diagLastPsMonitorRequested;
    private bool _diagLastPsFeedbackCorrecting;
    private const long DisplayFreshMs = 2_000;
    private const long DisplayAgingMs = 5_000;
    private readonly object _rxMeterDiagLock = new();
    private bool _diagRxMetersValid;
    private long _diagRxMetersMs;
    private int _diagRxMetersChannelId;
    private double _diagRxDbm = double.NaN;
    private RxMetersV2Frame _diagRxMeters;
    private const long RxMetersFreshMs = 2_500;
    private const long RxMetersAgingMs = 10_000;
    private readonly object _audioDiagLock = new();
    private bool _diagAudioValid;
    private long _diagAudioFrameMs;
    private uint _diagAudioSeq;
    private long _diagAudioFrameCount;
    private string _diagAudioSource = "none";
    private int _diagAudioSampleRateHz;
    private int _diagAudioSampleCount;
    private double _diagAudioRms = double.NaN;
    private double _diagAudioPeak = double.NaN;
    private bool _diagAudioLevelerValid;
    private double _diagAudioLevelerInputRmsDbfs = double.NaN;
    private double _diagAudioLevelerOutputRmsDbfs = double.NaN;
    private double _diagAudioLevelerInputPeakDbfs = double.NaN;
    private double _diagAudioLevelerOutputPeakDbfs = double.NaN;
    private double _diagAudioLevelerDesiredGainDb = double.NaN;
    private double _diagAudioLevelerAppliedGainDb = double.NaN;
    private double _diagAudioLevelerGainDeltaDb = double.NaN;
    private double _diagAudioLevelerPeakHeadroomDb = double.NaN;
    private double _diagAudioLevelerPreLimitPeakDbfs = double.NaN;
    private double _diagAudioLevelerOutputLimitReductionDb = double.NaN;
    private int _diagAudioLevelerOutputLimitSampleCount;
    private int _diagAudioLevelerPauseHoldBlocks;
    private bool _diagAudioLevelerBoostSlewLimited;
    private bool _diagAudioLevelerPeakLimited;
    private bool _diagAudioLevelerOutputLimited;
    private bool _diagAudioTxMonitorRequested;
    private bool _diagAudioSquelchEnabled;
    private bool _diagAudioSquelchOpen;
    private bool _diagAudioSquelchTailActive;
    private double _diagAudioSquelchGain = double.NaN;
    private string _diagAudioSquelchMode = "off";
    private string _diagAudioSquelchGateSource = "disabled";
    private bool _diagAudioSquelchOpenKnown = true;
    private long _diagAudioMonitorBacklogSamples;
    private int _diagAudioSinkCount;
    private const long AudioFreshMs = 2_000;
    private const long AudioAgingMs = 5_000;
    private const double AudioClippingRiskLinear = 0.98;
    private const double AudioSilentRmsDbfs = -90.0;

    // CW sidetone source mixed into the RX audio bus while a CW keying
    // path (CwEngine macros / cw_msg / raw-key, or ExternalPttService
    // hardware key in CW mode) holds the keyed state. Optional in DI so
    // tests that build the pipeline without the CW services don't have
    // to register a stub. See CwSidetoneSource for the keying contract.
    private readonly CwSidetoneSource? _sidetone;
    // Product-neutral audio modem coordinator. The null port is the no-plugin path.
    // When FreeDV is the active RX0 mode, the post-demod insert below replaces
    // the received modem audio with decoded speech.
    private readonly IAudioModemPort _audioModem;
    private readonly IProductTxAudioPort _productAudio;
    private readonly ProductPluginAudioPort? _productPluginAudio;

    public DspPipelineService(
        RadioService radio,
        StreamingHub hub,
        IEnumerable<IRxAudioSink> audioSinks,
        ILoggerFactory loggerFactory,
        CwSidetoneSource? sidetone = null,
        IAudioModemPort? audioModem = null,
        Func<TxAudioIngest?>? txIngestFactory = null,
        Nr3ModelStore? nr3ModelStore = null,
        IExternalRxAudioSource? externalRxAudioSource = null,
        RxAudioMuteState? rxAudioMute = null,
        TxIqRing? txIqRing = null,
        IExternalRadioSidecar? externalRadioSidecar = null,
        IConfiguration? configuration = null,
        IProductTxAudioPort? productAudio = null,
        ProductPluginAudioPort? productPluginAudio = null,
        SMeterCalibrationStore? sMeterCalibrationStore = null,
        TxMonitorSettingsStore? txMonitorSettingsStore = null)
    {
        _radio = radio;
        _txMonitorSettingsStore = txMonitorSettingsStore;
        _txMonitorVolumeDb = txMonitorSettingsStore?.VolumeDb
            ?? TxMonitorSettingsStore.DefaultVolumeDb;
        _sMeterCalibrationStore = sMeterCalibrationStore;
        if (_sMeterCalibrationStore is not null)
        {
            _sMeterCalibrationStore.Changed += OnSMeterCalibrationChanged;
            RefreshOperatorSMeterOffset(force: true);
        }
        _hub = hub;
        _txIqRing = txIqRing;
        _audioModem = audioModem ?? new NullAudioModemPort();
        _productAudio = productAudio ?? new NullProductTxAudioPort();
        _productPluginAudio = productPluginAudio;
        _productPluginAudio?.ConfigureLocalMonitorSink(EnqueueMonitorAudio);
        _externalRxAudioSource = externalRxAudioSource ?? new NullExternalRxAudioSource();
        _rxAudioMute = rxAudioMute;
        _hasExternalRadioSidecar = externalRadioSidecar is not null and not NullExternalRadioSidecar;
        _externalRadioSidecar = externalRadioSidecar ?? new NullExternalRadioSidecar();
        _externalRadioSidecar.ConfigureTxIqSafetyGate(_txEgressGate.IsCurrent);
        // Materialise once at construction so the per-tick fan-out is an
        // array-index loop (no enumerator allocation, no LINQ on the hot path).
        _audioSinks = audioSinks.ToArray();
        _sidetone = sidetone;
        _loggerFactory = loggerFactory;
        _txIngestFactory = txIngestFactory;
        _log = loggerFactory.CreateLogger<DspPipelineService>();
        _externalDspProvider = new ExternalDspEngineProviderLoader(configuration, _log);
        _txTurnaround = new TxTurnaroundTelemetry(_log);
        var displayHardware = DisplayHardwareProfile.Current();
        var displayPerformance = DisplayPerformanceOptions.Resolve(
            configuration,
            hardware: displayHardware);
        _rxAnalyzerFftSize = displayPerformance.RxAnalyzerFftSize;
        // WDSP's AGC threshold→max-gain conversion needs the FFT size of the
        // analyzer the noise floor is measured from (wcpAGC.c:482) — give the
        // auto-AGC servo the REAL analyzer size, not the WDSP channel block
        // size (the old 1024 hardcode seated auto AGC-T ~12 dB too hot).
        _radio.SetAutoAgcAnalyzerFftSize(_rxAnalyzerFftSize);
        _panadapterWidth = displayPerformance.PanadapterWidth;
        _panBuf = new float[_panadapterWidth];
        _wfBuf = new float[_panadapterWidth];
        _panDecimatedBuf = new float[_panadapterWidth];
        _wfDecimatedBuf = new float[_panadapterWidth];
        _lastTxPanBuf = new float[_panadapterWidth];
        _lastTxWfBuf = new float[_panadapterWidth];
        _calPanSnapshot = new float[_panadapterWidth];
        _autoAgcFloorBuf = new float[_panadapterWidth];
        _snrPowerBuf = new float[RxSnrSpectrumInfo.MaxPixelCount];
        _diagWfSnapshot = new float[_panadapterWidth];
        var loggedDisplayPerformanceProfile = !DisplayPerformanceOptions.IsStock(displayPerformance);
        if (displayHardware.IsLinuxArm64)
        {
            var profile = string.Equals(displayPerformance.Profile, "auto->low-power", StringComparison.Ordinal)
                ? "auto->low-power (Pi-class detected)"
                : displayPerformance.Profile;
            _log.LogInformation(
                "dsp.pipeline performance profile={Profile} deviceTreeModel={DeviceTreeModel} maxFps={MaxFps:F1} rxFft={RxFft} width={Width}",
                profile,
                displayHardware.DeviceTreeModel ?? "unreadable",
                displayPerformance.MaxFrameRateHz,
                displayPerformance.RxAnalyzerFftSize,
                displayPerformance.PanadapterWidth);
        }
        else if (loggedDisplayPerformanceProfile)
        {
            var profile = string.Equals(displayPerformance.Profile, "auto->low-power", StringComparison.Ordinal)
                ? "auto->low-power (Pi-class detected)"
                : displayPerformance.Profile;
            _log.LogInformation(
                "dsp.pipeline performance profile={Profile} maxFps={MaxFps:F1} rxFft={RxFft} width={Width}",
                profile,
                displayPerformance.MaxFrameRateHz,
                displayPerformance.RxAnalyzerFftSize,
                displayPerformance.PanadapterWidth);
        }
        _displayMaxFrameRateHz = displayPerformance.MaxFrameRateHz;
        _displayDecimation = DisplayPerformanceOptions.DefaultDisplayDecimation;
        _waterfallUpdatePeriod = DisplayPerformanceOptions.DefaultWaterfallUpdatePeriod;
        if (!loggedDisplayPerformanceProfile &&
            _displayMaxFrameRateHz < DisplayPerformanceOptions.DefaultFrameRateHz)
        {
            _log.LogInformation(
                "display.performance maxFrameRateHz={MaxFrameRateHz:F1} decimation={Decimation} waterfallUpdatePeriod={WaterfallUpdatePeriod}",
                _displayMaxFrameRateHz,
                _displayDecimation,
                _waterfallUpdatePeriod);
        }
        Volatile.Write(ref _widebandDisplayEnabled, 0);
        // Allocate secondary-receiver slots 1..N-1 up front (slot 0 stays null —
        // RX1 is _channelId). Buffers live for the service lifetime, mirroring the
        // old _rx2* fields; only activated slots open a WDSP channel.
        _secondaryRx = new SecondaryRx[MaxReceivers];
        for (int i = 1; i < MaxReceivers; i++)
            _secondaryRx[i] = new SecondaryRx(_panadapterWidth);

        _nr3ModelStore = nr3ModelStore;
        if (_nr3ModelStore is not null)
        {
            // NR3 (RNNoise) model is process-global in libwdsp. Re-push the
            // operator's installed model whenever a fresh engine spins up
            // (reconnect / WDSP↔synthetic swap) and whenever the operator
            // installs/removes a model. Both funnel through LoadNr3ModelInto so
            // the engine-lock discipline lives in one place.
            EngineChanged += LoadNr3ModelInto;
            _nr3ModelStore.Changed += _ => ReloadNr3ModelToCurrentEngine();
        }
    }

    private void OnSMeterCalibrationChanged() =>
        RefreshOperatorSMeterOffset(force: true);

    private double OperatorSMeterOffsetDb() =>
        RefreshOperatorSMeterOffset(force: false);

    private double RefreshOperatorSMeterOffset(bool force)
    {
        if (_sMeterCalibrationStore is null) return 0.0;

        HpsdrBoardKind board = _radio.EffectiveBoardKind;
        OrionMkIIVariant variant = _radio.EffectiveOrionMkIIVariant;
        lock (_sMeterCalibrationSync)
        {
            if (!force && board == _cachedSMeterBoard && variant == _cachedSMeterVariant)
                return _operatorSMeterOffsetDb;

            double offset = _sMeterCalibrationStore.Get(board, variant);
            _cachedSMeterBoard = board;
            _cachedSMeterVariant = variant;
            _operatorSMeterOffsetDb = offset;
            return _operatorSMeterOffsetDb;
        }
    }

    public override void Dispose()
    {
        if (_sMeterCalibrationStore is not null)
            _sMeterCalibrationStore.Changed -= OnSMeterCalibrationChanged;
        base.Dispose();
    }

    // Operator-installed RNNoise (NR3) model store. Optional so test
    // constructions keep working; when null, NR3 model loading is skipped
    // entirely (NR3 stays inert).
    private readonly Nr3ModelStore? _nr3ModelStore;

    // Push the active NR3 model path into the given engine under the engine
    // lock. A null/empty path clears the model (NR3 inert) — we always call so
    // a reused process never keeps a stale model after a remove.
    private void LoadNr3ModelInto(IDspEngine engine)
    {
        var path = _nr3ModelStore?.GetActiveModelPath();
        Zeus.Dsp.Nr3ModelLoadResult result;
        lock (_engineLock) { result = engine.LoadNr3Model(path); }
        // Record the outcome so RadioService.InstallNr3Model — which triggers this
        // synchronously via the store's Changed event — can reject a model the
        // native loader couldn't parse.
        _nr3ModelStore?.ReportLoadStatus(result);
        if (result == Zeus.Dsp.Nr3ModelLoadResult.LoadFailed)
            _log.LogWarning(
                "dsp.nr3.loadFailed path=\"{Path}\" — not a compatible RNNoise weights file", path);
    }

    private void ReloadNr3ModelToCurrentEngine()
    {
        var engine = Volatile.Read(ref _engine);
        if (engine is not null) LoadNr3ModelInto(engine);
    }

    // Lazily resolve TxAudioIngest (DI cycle avoidance — see field comment) and
    // build the radio-mic re-blocker on first use. Returns null in tests that
    // didn't supply a factory; the radio-mic path then simply no-ops.
    private TxAudioIngest? ResolveTxIngest()
    {
        if (_txIngest is not null) return _txIngest;
        _txIngest = _txIngestFactory?.Invoke();
        if (_txIngest is not null && _radioMicReceiver is null)
        {
            var ingest = _txIngest;
            _radioMicReceiver = new RadioMicReceiver(
                block => ingest.OnMicPcmBytesFromRadioMic(block),
                _loggerFactory.CreateLogger<RadioMicReceiver>());
            _p1RadioMicReceiver = new P1RadioMicReceiver(
                block => ingest.OnMicPcmBytesFromRadioMic(block),
                _loggerFactory.CreateLogger<P1RadioMicReceiver>());
        }
        return _txIngest;
    }

    /// <summary>
    /// FreeDV end-of-over TX tail: complete the final modem frame and clock it
    /// out to the radio before PTT drops, so the receiver gets whole OFDM frames
    /// (no end-of-over garble). Called by <see cref="TxService"/> on un-key,
    /// before the wire MOX bit drops. No-op unless FreeDV is engaged. Blocks the
    /// caller for the bounded tail duration.
    /// </summary>
    public void DrainFreeDvTxTail() => ResolveTxIngest()?.DrainFreeDvTxTail();

    public bool IsFreeDvTailDraining => ResolveTxIngest()?.IsFreeDvTailDraining ?? false;

    /// <summary>True while FreeDV is the active TX modem. Used by
    /// <see cref="TxService"/> to skip the plain voice-mode TX tail delay
    /// (issue #1294) — FreeDV runs its own bounded end-of-over drain instead.</summary>
    public bool IsFreeDvActive => _audioModem.Active;

    /// <summary>
    /// Voice-mode end-of-over tail. Holds the wire key for the configured
    /// operator delay, flushes the final partial mic block through TXA, and
    /// lets the P1/P2 transport drain before MOX drops.
    /// </summary>
    public virtual bool DrainVoiceTxTail(int tailDelayMs) => ResolveTxIngest()?.DrainVoiceTxTail(tailDelayMs) ?? false;

    /// <summary>
    /// Old-school roger beep tail. Called by TxService on an accepted local
    /// MOX release, before the wire MOX bit drops.
    /// </summary>
    public virtual bool DrainRogerBeepTail() => ResolveTxIngest()?.DrainRogerBeepTail() ?? false;

    /// <summary>
    /// Clocks any stale WDSP TXA output through silence and discards it before
    /// the radio wire MOX bit is asserted on a new key-down.
    /// </summary>
    public virtual bool PrimeTxDspForKeyDown() => ResolveTxIngest()?.PrimeTxDspForKeyDown() ?? false;

    /// <summary>
    /// Leaves a zero word at the output of a legacy Protocol-2 radio's TX FIFO
    /// before wire MOX rises. Other transports have no retained radio-side FIFO
    /// word to sanitize and remain unchanged.
    /// </summary>
    public virtual bool SanitizeProtocol2TxBeforeKeyDown() =>
        _p2Client?.PreloadZeroTxIqBeforeKeyDown() ?? true;

    public virtual bool DrainTxIqTransportTail(TimeSpan timeout)
    {
        var p2 = _p2Client;
        if (p2 is not null)
        {
            p2.FlushPendingTxIqTailPacket();
            return p2.WaitForTxIqQueueIdle(timeout);
        }

        if (_radio.ActiveClient is not null && _txIqRing is not null)
            return _txIqRing.WaitForEmpty(timeout);

        return true;
    }

    // dB added to the TX panadapter/waterfall pixels (Thetis TXDisplayCalOffset).
    // Read on the hot Tick path; written from the connect path + endpoint.
    private double _txDisplayCalOffsetDb;

    // Default TX display analyzer params — mirror WdspDspEngine's constants and
    // the frontend's TX_DISPLAY_* defaults. Used when the persisted value is null.
    private const int DefaultTxDisplayFftSize = 16384;
    private const int DefaultTxDisplayWindow = 2;
    private const double DefaultTxDisplayAvgTauMs = 175.0;
    private const double TxDisplayCalOffsetAbsDb = 60.0;
    private int _txDisplayFftSize = DefaultTxDisplayFftSize;
    private int _txDisplayWindow = DefaultTxDisplayWindow;
    private double _txDisplayAvgTauSec = DefaultTxDisplayAvgTauMs / 1000.0;

    /// <summary>Seed a freshly constructed engine with the persisted TX display
    /// config BEFORE its TX channel opens, so the analyzer comes up with the
    /// operator's FFT/window/smoothing rather than engine defaults. Also seeds
    /// the cal-offset field read by <see cref="Tick"/>. Display-only — never
    /// touches the transmitted signal.</summary>
    // FORK-LOCAL: RX display smoothing, held here because engines are
    // recreated on every connect. Seeded alongside the TX display config,
    // which every site that builds an engine already calls.
    private double _rxPanAvgTauSec = 0.100;
    private double _rxWfAvgTauSec = 0.0;

    public (double PanTauMs, double WfTauMs) RxDisplayAveraging =>
        (Volatile.Read(ref _rxPanAvgTauSec) * 1000.0, Volatile.Read(ref _rxWfAvgTauSec) * 1000.0);

    public void ApplyRxDisplayAveraging(double panTauMs, double wfTauMs)
    {
        Volatile.Write(ref _rxPanAvgTauSec, panTauMs / 1000.0);
        Volatile.Write(ref _rxWfAvgTauSec, wfTauMs / 1000.0);
        var engine = CurrentEngine;
        if (engine is null) return;
        lock (_engineLock) engine.ConfigureRxDisplayAveraging(panTauMs / 1000.0, wfTauMs / 1000.0);
    }

    private void SeedTxDisplayConfig(IDspEngine engine)
    {
        engine.ConfigureRxDisplayAveraging(
            Volatile.Read(ref _rxPanAvgTauSec), Volatile.Read(ref _rxWfAvgTauSec));
        int fft = Volatile.Read(ref _txDisplayFftSize);
        int window = Volatile.Read(ref _txDisplayWindow);
        double tauSec = Volatile.Read(ref _txDisplayAvgTauSec);
        engine.ConfigureTxDisplayAnalyzer(
            fft,
            window,
            tauSec);
    }

    /// <summary>Live update from the /api/display-settings endpoint — pushes the
    /// new cal offset + analyzer config to the running engine (if any). Safe to
    /// call with no radio connected; the values are re-seeded on next connect.
    /// Display-only.</summary>
    public void ApplyTxDisplaySettings(DisplaySettingsDto dto)
    {
        Volatile.Write(ref _txDisplayCalOffsetDb, ResolveCalOffset(dto));
        int fft = dto.TxDisplayFftSize ?? DefaultTxDisplayFftSize;
        int win = dto.TxDisplayWindow ?? DefaultTxDisplayWindow;
        double tauSec = (dto.TxDisplayAvgTauMs ?? DefaultTxDisplayAvgTauMs) / 1000.0;
        Volatile.Write(ref _txDisplayFftSize, fft);
        Volatile.Write(ref _txDisplayWindow, win);
        Volatile.Write(ref _txDisplayAvgTauSec, tauSec);
        var engine = CurrentEngine;
        if (engine is null) return;
        lock (_engineLock)
        {
            engine.ConfigureTxDisplayAnalyzer(fft, win, tauSec);
        }
    }

    public void ApplyDisplaySettings(DisplaySettingsDto dto)
    {
        ApplyTxDisplaySettings(dto);
        SetWidebandDisplayEnabled(dto.WidebandDisplayEnabled);
        SetWidebandSignalMarkersEnabled(dto.WidebandSignalMarkersEnabled);
        SetDisplayPerformance(
            dto.DisplayMaxFrameRateHz,
            dto.DisplayDecimation,
            dto.WaterfallUpdatePeriod);
    }

    private void SetWidebandSignalMarkersEnabled(bool enabled)
    {
        Volatile.Write(ref _widebandSignalMarkersEnabled, enabled ? 1 : 0);
        if (enabled) return;
        // Toggled off: drop the published snapshot immediately so polling
        // clients see an empty, current answer instead of stale markers.
        lock (_widebandSignalSnapshotLock)
        {
            _widebandSignalSnapshotCount = 0;
            _widebandSignalSnapshotMs = 0;
        }
    }

    /// <summary>
    /// Latest detected wideband signals plus the viewport they were detected
    /// under. Empty when markers are disabled, wideband is off, or no signal
    /// has been confirmed yet.
    /// </summary>
    public WidebandSignalsSnapshotDto GetWidebandSignalsSnapshot()
    {
        lock (_widebandSignalSnapshotLock)
        {
            var signals = new WidebandSignalMarkerDto[_widebandSignalSnapshotCount];
            for (int i = 0; i < _widebandSignalSnapshotCount; i++)
            {
                var m = _widebandSignalSnapshot[i];
                signals[i] = new WidebandSignalMarkerDto(
                    m.CenterHz, m.LowHz, m.HighHz, m.PeakDb, m.NoiseFloorDb, m.SnrDb, m.Confidence);
            }
            return new WidebandSignalsSnapshotDto(
                _widebandSignalSnapshotMs,
                _widebandSignalSnapshotCenterHz,
                _widebandSignalSnapshotHzPerPixel,
                signals);
        }
    }

    private void SetDisplayPerformance(
        double frameRateHz,
        int displayDecimation,
        int waterfallUpdatePeriod)
    {
        var nextFrameRate = DisplayPerformanceOptions.NormalizeFrameRate(frameRateHz);
        var nextDecimation = DisplayPerformanceOptions.NormalizeDisplayDecimation(displayDecimation);
        var nextWaterfallUpdatePeriod =
            DisplayPerformanceOptions.NormalizeWaterfallUpdatePeriod(waterfallUpdatePeriod);
        var changed = false;
        lock (_displayFrameRateLock)
        {
            if (Math.Abs(_displayMaxFrameRateHz - nextFrameRate) < 0.0001 &&
                _displayDecimation == nextDecimation &&
                _waterfallUpdatePeriod == nextWaterfallUpdatePeriod)
            {
                return;
            }
            _displayMaxFrameRateHz = nextFrameRate;
            _displayDecimation = nextDecimation;
            _waterfallUpdatePeriod = nextWaterfallUpdatePeriod;
            _displayFrameBudgetLastTicks = 0;
            _displayFrameBudget = 1.0;
            _unpacedDisplayDeadlineStopwatchTicks = 0;
            _waterfallFrameCounter = 0;
            Volatile.Write(ref _inlineDisplayDeadlineStopwatchTicks, 0);
            changed = true;
        }

        if (changed)
        {
            _log.LogInformation(
                "display.performance maxFrameRateHz={MaxFrameRateHz:F1} decimation={Decimation} waterfallUpdatePeriod={WaterfallUpdatePeriod}",
                nextFrameRate,
                nextDecimation,
                nextWaterfallUpdatePeriod);
        }
    }

    private void SetWidebandDisplayEnabled(bool enabled)
    {
        Volatile.Write(ref _widebandDisplayEnabled, enabled ? 1 : 0);
        Interlocked.Increment(ref _widebandSourceGeneration);
        RefreshWidebandDisplayState();
        PostDspCommand(() =>
        {
            var engine = Volatile.Read(ref _engine);
            if (engine is not null)
                ReconcileWidebandDetailSource(engine, _radio.Snapshot());
        });
    }

    private bool RefreshWidebandDisplayState(StateDto? state = null)
    {
        var client = _p2Client;
        bool enabled = Volatile.Read(ref _widebandDisplayEnabled) != 0;
        bool displayRequested = _hub.DisplayStreamRequested;
        var currentState = state ?? _radio.Snapshot();
        bool p2DetailReady = client is not null && enabled && displayRequested &&
            P2WidebandZoomPolicy.Resolve(
                Volatile.Read(ref _sampleRateHz),
                currentState.ZoomLevel).UseDdcDetail &&
            Volatile.Read(ref _widebandDetailReady) != 0 &&
            Volatile.Read(ref _widebandDetailChannelId) >= 0;
        // Keep raw snapshots flowing until the private detail analyzer has
        // received IQ. This avoids a blank handoff; once ready, only one source
        // can publish RxId0 frames.
        bool p2Desired = client is not null && enabled && displayRequested && !p2DetailReady;
        bool p3Desired = client is null && _hasExternalRadioSidecar &&
            _radio.IsProtocol3Active && enabled && displayRequested;
        bool anyDesired = p2Desired || p3Desired;

        bool p2Current = Volatile.Read(ref _p2WidebandTransportEnabled) != 0;
        if (p2Desired != p2Current)
        {
            Interlocked.Increment(ref _widebandSourceGeneration);
            Volatile.Write(ref _p2WidebandTransportEnabled, p2Desired ? 1 : 0);
            try { client?.SetWidebandDisplayEnabled(p2Desired); }
            catch (ObjectDisposedException) { }
        }

        bool current = Volatile.Read(ref _widebandTransportEnabled) != 0;
        if (anyDesired != current)
            Volatile.Write(ref _widebandTransportEnabled, anyDesired ? 1 : 0);
        if (!anyDesired)
        {
            lock (_widebandFrameLock) { _widebandFramePending = false; }
        }
        return anyDesired;
    }

    private static double ResolveCalOffset(DisplaySettingsDto? dto)
    {
        double v = dto?.TxDisplayCalOffsetDb ?? 0.0;
        if (double.IsNaN(v) || double.IsInfinity(v)) return 0.0;
        return Math.Clamp(v, -TxDisplayCalOffsetAbsDb, TxDisplayCalOffsetAbsDb);
    }

    // Add a dB offset to every pixel of a display buffer (TX cal offset).
    private static void AddDbOffset(float[] buf, double db)
    {
        if (db == 0.0) return;
        float d = (float)db;
        for (int i = 0; i < buf.Length; i++) buf[i] += d;
    }

    private uint NextDisplaySeq() => unchecked((uint)Interlocked.Increment(ref _seq));

    // Capture-time stamp for DisplayFrame/AudioFrame.TsUnixMs. Both fields are
    // double on the wire, but ToUnixTimeMilliseconds() truncates to a whole
    // millisecond, discarding the sub-ms precision UtcNow actually carries. The
    // WebGPU spectrum surfaces derive per-row depth and the row-cadence estimate
    // from these stamps, so the ±0.5 ms rounding shows up as a ~2% row-spacing
    // shimmer and biases the measured period a few percent high. Keep the full
    // fractional resolution; every consumer already treats it as a real number,
    // and the diagnostic (long) casts still truncate to ms as before.
    private static double UnixTimeMillisecondsHighRes() =>
        (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).TotalMilliseconds;

    private readonly record struct DisplayFramePlan(int Decimation, bool IncludeWaterfall);

    private bool TryBeginDisplayFrame(
        long nowTicks,
        bool producerIsRatePaced,
        out DisplayFramePlan plan)
    {
        lock (_displayFrameRateLock)
        {
            if (!ShouldEmitDisplayFrameForPublisher(
                    nowTicks,
                    _displayMaxFrameRateHz,
                    Stopwatch.Frequency,
                    producerIsRatePaced,
                    ref _displayFrameBudgetLastTicks,
                    ref _displayFrameBudget,
                    ref _unpacedDisplayDeadlineStopwatchTicks))
            {
                plan = default;
                return false;
            }

            var waterfallPeriod = Math.Max(1, _waterfallUpdatePeriod);
            var includeWaterfall = _waterfallFrameCounter == 0;
            _waterfallFrameCounter = (_waterfallFrameCounter + 1) % waterfallPeriod;
            plan = new DisplayFramePlan(_displayDecimation, includeWaterfall);
            return true;
        }
    }

    internal static bool ShouldEmitDisplayFrameForPublisher(
        long nowTicks,
        double frameRateHz,
        long stopwatchFrequency,
        bool producerIsRatePaced,
        ref long budgetLastTicks,
        ref double budget,
        ref long unpacedDeadlineTicks)
    {
        if (frameRateHz < DisplayPerformanceOptions.DefaultFrameRateHz)
        {
            return ShouldEmitBudgetedDisplayFrame(
                nowTicks,
                frameRateHz,
                stopwatchFrequency,
                ref budgetLastTicks,
                ref budget);
        }

        // The inline/full-tick publishers are already paced at 30 Hz or at the
        // configured faster cadence. Keep their >= 30 Hz emission pattern unchanged.
        if (producerIsRatePaced) return true;

        var periodTicks = Math.Max(
            1,
            (long)Math.Ceiling(stopwatchFrequency / frameRateHz));
        if (unpacedDeadlineTicks == 0)
        {
            unpacedDeadlineTicks = nowTicks + periodTicks;
            return true;
        }
        if (nowTicks < unpacedDeadlineTicks) return false;

        // Translate the absolute next deadline to AdvanceTickDeadline's last-
        // cadence representation, then back. Its max-slip clamp prevents a
        // long idle producer from burst-emitting to catch up.
        var previousCadenceTicks = unpacedDeadlineTicks - periodTicks;
        unpacedDeadlineTicks = AdvanceTickDeadline(
            nowTicks,
            previousCadenceTicks,
            periodTicks,
            periodTicks * 2) + periodTicks;
        return true;
    }

    // Must be called from the display-update serialisation context because the budget is passed by ref.
    internal static bool ShouldEmitBudgetedDisplayFrame(
        long nowTicks,
        double frameRateHz,
        long stopwatchFrequency,
        ref long lastTicks,
        ref double budget)
    {
        if (lastTicks == 0)
        {
            lastTicks = nowTicks;
            budget = 0.0;
            return false;
        }

        var elapsedTicks = Math.Max(0, nowTicks - lastTicks);
        lastTicks = nowTicks;
        budget = Math.Min(
            1.0,
            budget + (elapsedTicks / (double)stopwatchFrequency * frameRateHz));
        if (budget + 1.0e-9 < 1.0) return false;

        budget -= 1.0;
        return true;
    }

    private long CurrentInlineDisplayPeriodTicks()
    {
        lock (_displayFrameRateLock)
        {
            return InlineDisplayPeriodTicks(
                _displayMaxFrameRateHz,
                TickPeriodStopwatchTicks,
                Stopwatch.Frequency);
        }
    }

    internal static long InlineDisplayPeriodTicks(
        double displayMaxFrameRateHz,
        long defaultPeriodTicks,
        long stopwatchFrequency)
    {
        if (double.IsFinite(displayMaxFrameRateHz) &&
            displayMaxFrameRateHz > DisplayPerformanceOptions.DefaultFrameRateHz &&
            stopwatchFrequency > 0)
        {
            return Math.Max(1, (long)Math.Round(stopwatchFrequency / displayMaxFrameRateHz));
        }

        return defaultPeriodTicks;
    }

    internal static int DecimatedDisplayWidth(int sourceWidth, int displayDecimation)
    {
        var decimation = DisplayPerformanceOptions.NormalizeDisplayDecimation(displayDecimation);
        return Math.Max(1, sourceWidth / decimation);
    }

    internal static int DownsampleDisplayBins(
        ReadOnlySpan<float> source,
        Span<float> destination,
        int displayDecimation)
    {
        var decimation = DisplayPerformanceOptions.NormalizeDisplayDecimation(displayDecimation);
        var width = DecimatedDisplayWidth(source.Length, decimation);
        if (destination.Length < width)
            throw new ArgumentException("Destination is smaller than the decimated display width.", nameof(destination));

        if (decimation == 1)
        {
            source.CopyTo(destination);
            return width;
        }

        for (int i = 0; i < width; i++)
        {
            var start = i * decimation;
            var end = Math.Min(source.Length, start + decimation);
            var max = float.NegativeInfinity;
            for (int j = start; j < end; j++)
            {
                var v = source[j];
                if (v > max) max = v;
            }
            destination[i] = max;
        }

        return width;
    }

    private static ReadOnlyMemory<float> FrameBins(
        float[] source,
        float[] decimated,
        int displayDecimation,
        out ushort width)
    {
        var decimation = DisplayPerformanceOptions.NormalizeDisplayDecimation(displayDecimation);
        if (decimation == 1)
        {
            width = checked((ushort)source.Length);
            return source;
        }

        var decimatedWidth = DownsampleDisplayBins(source, decimated, decimation);
        width = checked((ushort)decimatedWidth);
        return decimated.AsMemory(0, decimatedWidth);
    }

    private static ReadOnlyMemory<float> InvalidFrameBins(
        float[] scratch,
        int sourceWidth,
        int displayDecimation,
        out ushort width)
    {
        var decimatedWidth = DecimatedDisplayWidth(sourceWidth, displayDecimation);
        width = checked((ushort)decimatedWidth);
        return scratch.AsMemory(0, decimatedWidth);
    }

    private static ReadOnlyMemory<float> InvalidFrameBins(
        float[] scratch,
        int displayDecimation,
        out ushort width) =>
        InvalidFrameBins(scratch, DisplayPerformanceOptions.DefaultPanadapterWidth, displayDecimation, out width);

    private static void CopyDiagnosticDisplayBins(ReadOnlySpan<float> source, Span<float> destination)
    {
        if (source.Length == 0 || destination.Length == 0) return;
        if (source.Length == destination.Length)
        {
            source.CopyTo(destination);
            return;
        }

        double scale = source.Length / (double)destination.Length;
        for (int i = 0; i < destination.Length; i++)
        {
            int start = Math.Clamp((int)Math.Floor(i * scale), 0, source.Length - 1);
            int end = Math.Clamp((int)Math.Ceiling((i + 1) * scale), start + 1, source.Length);
            var max = float.NegativeInfinity;
            for (int j = start; j < end; j++)
            {
                var v = source[j];
                if (v > max) max = v;
            }
            destination[i] = max;
        }
    }

    private static float DiagnosticHzPerPixel(float sourceHzPerPixel, int sourceWidth, int targetWidth) =>
        sourceHzPerPixel * Math.Max(1.0f, sourceWidth / (float)Math.Max(1, targetWidth));

    private static int DdcZoomLevel(int zoomLevel) =>
        Math.Clamp(zoomLevel, SyntheticDspEngine.MinZoomLevel, SyntheticDspEngine.MaxZoomLevel);

    internal static float VisibleDdcHzPerPixel(int sampleRateHz, int globalZoomLevel, int pixelWidth) =>
        (float)((double)sampleRateHz /
            DdcZoomLevel(globalZoomLevel) /
            Math.Max(1, pixelWidth));

    public StateDto ApplyWidebandZoomRequest(int requestedLevel, long? centerHz)
    {
        lock (_widebandZoomRequestLock)
        {
            int normalizedLevel = NormalizeWidebandZoomRequest(requestedLevel);
            var before = _radio.Snapshot();
            long nextCenter = 0;
            bool centerChanged = false;

            // A rejected deep step (normally: no spare DDC) must not pan the raw
            // overview using geometry the server cannot honor.
            if (centerHz is long requestedCenter && normalizedLevel == requestedLevel)
            {
                nextCenter = Math.Clamp(
                    requestedCenter,
                    0L,
                    (long)WidebandSpectrumAnalyzer.DisplaySpanHz);
                centerChanged = Interlocked.Read(ref _widebandTargetCenterHz) != nextCenter;
            }

            bool levelChanged = normalizedLevel != before.ZoomLevel;

            // Center-only pan: glide path. A pan retunes the hidden DDC's NCO
            // (radio-side, sample-continuous) — the detail channel, its
            // analyzer, the ready/publish barriers and RadioService zoom state
            // all stay put, so display frames keep flowing mid-drag instead of
            // re-aperturing on every pointer step. Frame centre coherence comes
            // from _widebandCenterHistory at publish time.
            if (centerChanged && !levelChanged)
            {
                Interlocked.Exchange(ref _widebandTargetCenterHz, nextCenter);
                _widebandCenterHistory.Append(Stopwatch.GetTimestamp(), nextCenter);
                PostDspCommand(() =>
                {
                    var engine = Volatile.Read(ref _engine);
                    if (engine is not null)
                        ReconcileWidebandDetailSource(engine, before);
                });
                return before;
            }

            bool sourceChanged = centerChanged || levelChanged;
            if (sourceChanged)
            {
                // Pre-barrier: neither raw nor detail may publish a frame captured
                // between the target update and the RadioService zoom mutation.
                Volatile.Write(ref _widebandDetailReady, 0);
                Interlocked.Increment(ref _widebandSourceGeneration);
            }
            if (centerChanged)
            {
                Interlocked.Exchange(ref _widebandTargetCenterHz, nextCenter);
                _widebandCenterHistory.Append(Stopwatch.GetTimestamp(), nextCenter);
            }

            StateDto result = _radio.SetZoom(normalizedLevel);

            // Post-barrier: an overview analyzer that raced the state mutation is
            // invalidated before it can broadcast a mixed center/zoom frame.
            Interlocked.Increment(ref _widebandSourceGeneration);
            PostDspCommand(() =>
            {
                var engine = Volatile.Read(ref _engine);
                if (engine is not null)
                    ReconcileWidebandDetailSource(engine, result);
            });
            return result;
        }
    }

    public int NormalizeWidebandZoomRequest(int requestedLevel)
    {
        var state = _radio.Snapshot();
        int rate = state.SampleRate > 0 ? state.SampleRate : Volatile.Read(ref _sampleRateHz);
        int level = Math.Clamp(
            requestedLevel,
            RadioService.MinDisplayZoomLevel,
            P2WidebandZoomPolicy.MaxGlobalZoomForRate(rate));
        if (_p2Client is null || Volatile.Read(ref _widebandDisplayEnabled) == 0)
            return Math.Min(level, RadioService.LegacyMaxDisplayZoomLevel);
        var plan = P2WidebandZoomPolicy.Resolve(rate, level);
        if (!plan.UseDdcDetail)
            return Math.Min(level, RadioService.LegacyMaxDisplayZoomLevel);
        return ResolveWidebandDetailSource(state, plan).IsAvailable
            ? level
            : Math.Min(
                state.ZoomLevel,
                P2WidebandZoomPolicy.MaxOverviewZoomForRate(rate));
    }

    private P2WidebandDetailSource ResolveWidebandDetailSource(
        StateDto state,
        P2WidebandZoomPlan zoomPlan)
    {
        int rate = Volatile.Read(ref _sampleRateHz);
        if (rate <= 0) rate = state.SampleRate;
        int baseDdc = Zeus.Protocol2.Protocol2Client.RxBaseDdc(_radio.EffectiveBoardKind);
        bool diversitySource = state.Diversity is { Enabled: true, SourceRx: 1 };
        int extraCount = 0;
        if (state.Rx2Enabled && state.Receivers is { } receivers)
            for (int ri = 2; ri < receivers.Count && ri < MaxReceivers && receivers[ri].Enabled; ri++)
                extraCount++;

        return P2WidebandZoomPolicy.ResolveDetailSource(
            baseDdc,
            state.Rx2Enabled,
            diversitySource,
            extraCount,
            rate,
            WidebandViewportTargetCenterHz(state),
            zoomPlan.RequestedSpanHz);
    }

    private bool ReconcileWidebandDetailSource(IDspEngine engine, StateDto state)
    {
        var p2 = _p2Client;
        int rate = Volatile.Read(ref _sampleRateHz);
        if (rate <= 0) rate = state.SampleRate;
        var zoomPlan = P2WidebandZoomPolicy.Resolve(rate, state.ZoomLevel);
        bool requested = p2 is not null &&
            Volatile.Read(ref _widebandDisplayEnabled) != 0 &&
            _hub.DisplayStreamRequested &&
            zoomPlan.UseDdcDetail;
        var source = requested
            ? ResolveWidebandDetailSource(state, zoomPlan)
            : P2WidebandDetailSource.None;

        if (!source.IsAvailable)
        {
            p2?.SetDisplayDdc(-1, 0, 0);
            CloseWidebandDetailChannel(engine);
            return false;
        }

        int oldSource = Volatile.Read(ref _widebandDetailSourceReceiverIndex);
        int oldDdc = Volatile.Read(ref _widebandDetailDdcIndex);
        long oldCenter = Interlocked.Read(ref _widebandDetailSourceCenterHz);
        // Identity (which DDC feeds us) and centre (where that DDC sits) are
        // deliberately NOT the same change. An identity or zoom change rebuilds
        // the channel; a centre-only pan keeps the channel and analyzer alive —
        // the NCO retune below is sample-continuous, so the display glides.
        bool identityChanged = oldSource != source.ReceiverIndex ||
            oldDdc != source.DdcIndex;
        bool centerOnlyChanged = !identityChanged && oldCenter != source.SourceCenterHz;
        bool zoomChanged = Volatile.Read(ref _widebandDetailZoomLevel) != zoomPlan.DdcZoomLevel;

        int detailChannel = Volatile.Read(ref _widebandDetailChannelId);
        if (detailChannel >= 0 && (identityChanged || zoomChanged))
        {
            Volatile.Write(ref _widebandDetailChannelId, -1);
            try { engine.CloseRxDisplayChannel(detailChannel); } catch { /* best-effort reset */ }
            detailChannel = -1;
        }
        if (detailChannel < 0)
        {
            detailChannel = engine.OpenRxDisplayChannel(rate, _panadapterWidth);
            Volatile.Write(ref _widebandDetailChannelId, detailChannel);
            identityChanged = true;
        }
        if (identityChanged || zoomChanged)
        {
            Volatile.Write(ref _widebandDetailSourceReceiverIndex, source.ReceiverIndex);
            Volatile.Write(ref _widebandDetailDdcIndex, source.DdcIndex);
            Interlocked.Exchange(ref _widebandDetailSourceCenterHz, source.SourceCenterHz);
            Volatile.Write(ref _widebandDetailReady, 0);
            Interlocked.Exchange(ref _widebandDetailLastIqMs, Environment.TickCount64);
            Interlocked.Increment(ref _widebandSourceGeneration);
        }
        else if (centerOnlyChanged)
        {
            // Glide: latch the new centre only. The channel, its analyzer (and
            // its full FFT aperture) survive the pan; publish-side centre
            // coherence comes from _widebandCenterHistory.
            Interlocked.Exchange(ref _widebandDetailSourceCenterHz, source.SourceCenterHz);
        }

        long target = WidebandViewportTargetCenterHz(state);
        if (source.Kind == P2WidebandDetailSourceKind.HiddenDdc)
            p2!.SetDisplayDdc(source.DdcIndex, target, ReceiverAdcSource(state, 0));
        else
            p2!.SetDisplayDdc(-1, 0, 0);

        if (zoomChanged || identityChanged)
        {
            // Pixel-dense detail: size the analyzer FFT so true bins never
            // exceed one display pixel at this DDC zoom (65,536 points at
            // zoom 32). Applied before the zoom reconfig so the sticky
            // per-channel override is already in place when SetRxDisplayZoom
            // rebuilds the analyzer.
            int detailFftSize =
                P2WidebandZoomPolicy.DetailAnalyzerFftSize(_panadapterWidth, zoomPlan.DdcZoomLevel);
            engine.SetRxDisplayFftSize(detailChannel, detailFftSize);
            Volatile.Write(ref _widebandDetailAnalyzerFftSize, detailFftSize);
            engine.SetRxDisplayZoom(detailChannel, zoomPlan.DdcZoomLevel);
            Volatile.Write(ref _widebandDetailZoomLevel, zoomPlan.DdcZoomLevel);
        }
        engine.SetVfoHz(detailChannel, target);
        return Volatile.Read(ref _widebandDetailReady) != 0;
    }

    private void CloseWidebandDetailChannel(IDspEngine engine)
    {
        int channel = Interlocked.Exchange(ref _widebandDetailChannelId, -1);
        Volatile.Write(ref _widebandDetailDdcIndex, -1);
        Volatile.Write(ref _widebandDetailSourceReceiverIndex, int.MinValue);
        Volatile.Write(ref _widebandDetailReady, 0);
        Interlocked.Exchange(ref _widebandDetailLastIqMs, long.MinValue);
        if (channel >= 0)
        {
            try { engine.CloseRxDisplayChannel(channel); }
            catch (Exception ex) { _log.LogDebug(ex, "dsp.pipeline wideband detail close failed channel={Channel}", channel); }
            Interlocked.Increment(ref _widebandSourceGeneration);
        }
    }

    private void ApplyVisibleDdcZoom(IDspEngine engine, StateDto state)
    {
        // RX1 and each secondary now gate independently — they no longer share
        // one zoom value, so an unchanged RX1 zoom can no longer short-circuit
        // the whole function the way it used to (a secondary-only zoom change
        // would otherwise be silently dropped).
        int zoom = DdcZoomLevel(state.ZoomLevel);
        if (zoom != _appliedZoomLevel)
        {
            engine.SetZoom(Volatile.Read(ref _channelId), zoom);
            _appliedZoomLevel = zoom;
        }
        for (int receiverIndex = 1; receiverIndex < MaxReceivers; receiverIndex++)
        {
            var rx = _secondaryRx[receiverIndex];
            int channelId = Volatile.Read(ref rx.ChannelId);
            if (channelId < 0) continue;
            int rxZoom = DdcZoomLevel(RadioService.ReceiverZoomLevel(state, receiverIndex));
            if (rx.AppliedZoom == rxZoom) continue;
            engine.SetZoom(channelId, rxZoom);
            rx.AppliedZoom = rxZoom;
        }
    }

    internal static bool IsWidebandDetailIqStale(long nowMs, long lastIqMs) =>
        lastIqMs != long.MinValue && nowMs - lastIqMs > WidebandDetailIqStaleMs;

    internal static bool CanPromoteWidebandDetail(
        bool analyzerFresh,
        long nowMs,
        long lastIqMs,
        long frameGeneration,
        long currentGeneration,
        int frameChannel,
        int currentChannel) =>
        analyzerFresh &&
        !IsWidebandDetailIqStale(nowMs, lastIqMs) &&
        frameGeneration == currentGeneration &&
        frameChannel == currentChannel;

    internal static bool CanPublishWidebandDetail(
        bool detailReady,
        long frameGeneration,
        long currentGeneration,
        int frameChannel,
        int currentChannel) =>
        detailReady &&
        frameGeneration == currentGeneration &&
        frameChannel == currentChannel;

    private long WidebandViewportTargetCenterHz(StateDto state)
    {
        long configured = Interlocked.Read(ref _widebandTargetCenterHz);
        long rfFallbackHz = state.RadioLoHz > 0
            ? state.RadioLoHz
            : CwOffset.EffectiveLoHz(state);
        return ResolveWidebandViewportCenterHz(
            configured, rfFallbackHz, _radio.ToHardwareFrequencyHz);
    }

    // The raw wideband display spectrum is the hardware 0–60 MHz IF span, and
    // this centre is fed straight to the detail DDC's NCO (SetDisplayDdc /
    // SetVfoHz) — a hardware frequency. A configured pan target is already in
    // that hardware IF domain (ApplyWidebandZoomRequest clamps it to 0–60 MHz)
    // and is only re-clamped, but the tuned-LO fallback (RadioLoHz / CW LO) is
    // external RF. With a transverter active that RF LO sits far outside
    // 0–60 MHz (e.g. 144 MHz on 2 m), so translate it to the hardware IF where
    // the transverter injects the signal (e.g. 28 MHz) before clamping —
    // otherwise it pins to the 60 MHz display ceiling and both the panadapter
    // scale and the detail DDC land on an empty band edge instead of the
    // transverter passband (issue #1486). Pure/injected converter so the
    // domain translation can be unit-tested without a live pipeline.
    internal static long ResolveWidebandViewportCenterHz(
        long configuredCenterHz, long rfFallbackHz, Func<long, long> toHardwareHz)
    {
        long hardwareCenterHz = configuredCenterHz != long.MinValue
            ? configuredCenterHz
            : toHardwareHz(rfFallbackHz);
        return Math.Clamp(hardwareCenterHz, 0L, (long)WidebandSpectrumAnalyzer.DisplaySpanHz);
    }

    private async Task RunWidebandDisplayAnalyzerAsync(CancellationToken ct)
    {
        while (true)
        {
            await _widebandFrameSignal.WaitAsync(ct).ConfigureAwait(false);

            int sampleRateHz;
            int sampleCount;
            lock (_widebandFrameLock)
            {
                if (!_widebandFramePending) continue;
                sampleCount = _widebandPendingSampleCount;
                Array.Copy(_widebandPendingSamples, _widebandAnalysisSamples, sampleCount);
                sampleRateHz = _widebandPendingSampleRateHz;
                _widebandFramePending = false;
            }

            if (Volatile.Read(ref _p2WidebandTransportEnabled) == 0 || !_hub.DisplayStreamRequested)
                continue;
            // Packet arrival is this publisher's only pacer; the packet rate is
            // unrelated to the operator's configured display cap.
            if (!TryBeginDisplayFrame(
                    Stopwatch.GetTimestamp(),
                    producerIsRatePaced: false,
                    out var displayPlan))
                continue;
            long sourceGeneration = Interlocked.Read(ref _widebandSourceGeneration);

            var state = _radio.Snapshot();
            double frameIntervalMs =
                _p2Client?.CurrentWidebandGeometry.UpdateRateMs
                ?? Zeus.Protocol2.Protocol2Client.ClassicWidebandUpdateRateMs;
            WidebandSpectrumViewport viewport;
            int markerCount = 0;
            try
            {
                viewport = _widebandAnalyzer.Analyze(
                    _widebandAnalysisSamples.AsSpan(0, sampleCount),
                    sampleRateHz,
                    _widebandPanBuf,
                    _widebandWfBuf,
                    state.ZoomLevel,
                    WidebandViewportTargetCenterHz(state),
                    frameIntervalMs,
                    Volatile.Read(ref _widebandSignalMarkersEnabled) != 0 ? _widebandSignalDetector : null,
                    _widebandMarkersBuf,
                    out markerCount);
            }
            catch (Exception ex)
            {
                // Fault containment: one malformed frame must not kill the
                // wideband display worker. Skip the frame, count it, and log
                // at most once per 30 s so a persistent fault stays visible
                // without flooding the log.
                Interlocked.Increment(ref _widebandAnalyzerErrorCount);
                long nowLogMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                long lastLogMs = Interlocked.Read(ref _widebandAnalyzerErrorLogMs);
                if (nowLogMs - lastLogMs > 30_000 &&
                    Interlocked.CompareExchange(ref _widebandAnalyzerErrorLogMs, nowLogMs, lastLogMs) == lastLogMs)
                {
                    _log.LogWarning(ex,
                        "wideband analyzer frame failed; frame skipped (total {ErrorCount})",
                        Interlocked.Read(ref _widebandAnalyzerErrorCount));
                }
                continue;
            }
            SanitizeDisplayBuffer(_widebandPanBuf);
            if (displayPlan.IncludeWaterfall)
                SanitizeDisplayBuffer(_widebandWfBuf);

            var panBins = FrameBins(
                _widebandPanBuf,
                _widebandPanDecimatedBuf,
                displayPlan.Decimation,
                out var frameWidth);
            var wfBins = displayPlan.IncludeWaterfall
                ? FrameBins(
                    _widebandWfBuf,
                    _widebandWfDecimatedBuf,
                    displayPlan.Decimation,
                    out _)
                : InvalidFrameBins(
                    _widebandWfDecimatedBuf,
                    _widebandWfBuf.Length,
                    displayPlan.Decimation,
                    out _);
            var bodyFlags = DisplayBodyFlags.PanValid;
            if (displayPlan.IncludeWaterfall)
                bodyFlags |= DisplayBodyFlags.WfValid;

            double nowMs = UnixTimeMillisecondsHighRes();
            var frame = new DisplayFrame(
                Seq: NextDisplaySeq(),
                TsUnixMs: nowMs,
                RxId: 0,
                BodyFlags: bodyFlags,
                Width: frameWidth,
                CenterHz: viewport.CenterHz,
                HzPerPixel: viewport.HzPerPixel * displayPlan.Decimation,
                PanDb: panBins,
                WfDb: wfBins);

            if (sourceGeneration != Interlocked.Read(ref _widebandSourceGeneration) ||
                Volatile.Read(ref _p2WidebandTransportEnabled) == 0)
            {
                continue;
            }

            lock (_calPanLock)
            {
                CopyDiagnosticDisplayBins(_widebandPanBuf, _calPanSnapshot);
                if (displayPlan.IncludeWaterfall)
                    CopyDiagnosticDisplayBins(_widebandWfBuf, _diagWfSnapshot);
                _calPanHzPerPixel = DiagnosticHzPerPixel(
                    viewport.HzPerPixel,
                    _widebandPanBuf.Length,
                    _panadapterWidth);
                _calPanCenterHz = viewport.CenterHz;
                _calPanSnapshotMs = (long)nowMs;
                _calPanSnapshotVersion++;
                if (displayPlan.IncludeWaterfall)
                    _diagWfSnapshotMs = (long)nowMs;
                _diagDisplayFrameMs = (long)nowMs;
                _diagDisplaySeq = frame.Seq;
                _diagDisplayFrameCount++;
                _diagLastPanValid = true;
                _diagLastWfValid = displayPlan.IncludeWaterfall;
                _diagLastPanSource = "wideband";
                _diagLastWfSource = displayPlan.IncludeWaterfall ? "wideband" : "waterfall-decimated";
                _diagLastKeyed = _keyed;
                _diagLastPsMonitorRequested = false;
                _diagLastPsFeedbackCorrecting = false;
            }

            // Recheck at the final ownership boundary too: a hidden analyzer can
            // become ready while diagnostic bookkeeping above is running.
            if (sourceGeneration == Interlocked.Read(ref _widebandSourceGeneration) &&
                Volatile.Read(ref _p2WidebandTransportEnabled) != 0)
            {
                // Publish the markers snapshot only for frames that pass the
                // ownership checks and actually reach operators, so markers
                // always agree with the rendered trace they overlay.
                if (Volatile.Read(ref _widebandSignalMarkersEnabled) != 0)
                {
                    lock (_widebandSignalSnapshotLock)
                    {
                        Array.Copy(_widebandMarkersBuf, _widebandSignalSnapshot, markerCount);
                        _widebandSignalSnapshotCount = markerCount;
                        _widebandSignalSnapshotMs = (long)nowMs;
                        _widebandSignalSnapshotCenterHz = viewport.CenterHz;
                        _widebandSignalSnapshotHzPerPixel = viewport.HzPerPixel;
                    }
                }
                _hub.Broadcast(frame);
            }
        }
    }

    private async Task RunP3WidebandDisplayPollerAsync(CancellationToken ct)
    {
        if (!_hasExternalRadioSidecar) return;

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
        {
            if (!ShouldPollP3WidebandDisplay()) continue;
            // The fixed 100 ms poller is not paced to the operator's display cap.
            if (!TryBeginDisplayFrame(
                    Stopwatch.GetTimestamp(),
                    producerIsRatePaced: false,
                    out var displayPlan))
            {
                continue;
            }

            ExternalDisplayFrame? sidecarFrame;
            try
            {
                var state = _radio.Snapshot();
                var viewport = WidebandSpectrumAnalyzer.ResolveViewport(
                    state.ZoomLevel,
                    WidebandViewportTargetCenterHz(state));
                sidecarFrame = await _externalRadioSidecar.FetchDisplayFrameAsync(
                        WidebandSpectrumAnalyzer.DisplayWidth,
                        viewport.ZoomLevel,
                        viewport.CenterHz,
                        ct)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                LogP3WidebandDisplayError(ex);
                continue;
            }

            if (sidecarFrame is null)
            {
                LogP3WidebandDisplayMissing();
                continue;
            }
            if (!ShouldPollP3WidebandDisplay()) continue;

            PublishP3WidebandDisplayFrame(sidecarFrame, displayPlan);
        }
    }

    private bool ShouldPollP3WidebandDisplay() =>
        _hasExternalRadioSidecar &&
        _radio.IsProtocol3Active &&
        Volatile.Read(ref _widebandDisplayEnabled) != 0 &&
        _hub.DisplayStreamRequested;

    private void PublishP3WidebandDisplayFrame(
        ExternalDisplayFrame source,
        DisplayFramePlan displayPlan)
    {
        if (source.PanDb.Length <= 0 || source.WfDb.Length <= 0 || source.WfDb.Length != source.PanDb.Length) return;
        if (source.PanDb.Length > _widebandPanDecimatedBuf.Length) return;

        SanitizeDisplayBuffer(source.PanDb);
        if (displayPlan.IncludeWaterfall)
            SanitizeDisplayBuffer(source.WfDb);

        var panBins = FrameBins(
            source.PanDb,
            _widebandPanDecimatedBuf,
            displayPlan.Decimation,
            out var frameWidth);
        var wfBins = displayPlan.IncludeWaterfall
            ? FrameBins(
                source.WfDb,
                _widebandWfDecimatedBuf,
                displayPlan.Decimation,
                out _)
            : InvalidFrameBins(
                _widebandWfDecimatedBuf,
                source.WfDb.Length,
                displayPlan.Decimation,
                out _);
        var bodyFlags = source.BodyFlags | DisplayBodyFlags.PanValid;
        if (!displayPlan.IncludeWaterfall)
            bodyFlags &= ~DisplayBodyFlags.WfValid;

        double nowMs = UnixTimeMillisecondsHighRes();
        var frame = new DisplayFrame(
            Seq: NextDisplaySeq(),
            TsUnixMs: nowMs,
            RxId: source.RxId,
            BodyFlags: bodyFlags,
            Width: frameWidth,
            CenterHz: source.CenterHz,
            HzPerPixel: source.HzPerPixel * displayPlan.Decimation,
            PanDb: panBins,
            WfDb: wfBins);

        lock (_calPanLock)
        {
            CopyDiagnosticDisplayBins(source.PanDb, _calPanSnapshot);
            if (displayPlan.IncludeWaterfall)
                CopyDiagnosticDisplayBins(source.WfDb, _diagWfSnapshot);
            _calPanHzPerPixel = DiagnosticHzPerPixel(source.HzPerPixel, source.PanDb.Length, _panadapterWidth);
            _calPanCenterHz = source.CenterHz;
            _calPanSnapshotMs = (long)nowMs;
            _calPanSnapshotVersion++;
            if (displayPlan.IncludeWaterfall)
                _diagWfSnapshotMs = (long)nowMs;
            _diagDisplayFrameMs = (long)nowMs;
            _diagDisplaySeq = frame.Seq;
            _diagDisplayFrameCount++;
            _diagLastPanValid = true;
            _diagLastWfValid = displayPlan.IncludeWaterfall;
            _diagLastPanSource = "p3-wideband";
            _diagLastWfSource = displayPlan.IncludeWaterfall ? "p3-wideband" : "waterfall-decimated";
            _diagLastKeyed = _keyed;
            _diagLastPsMonitorRequested = false;
            _diagLastPsFeedbackCorrecting = false;
        }

        _hub.Broadcast(frame);
    }

    private void LogP3WidebandDisplayMissing()
    {
        long now = Environment.TickCount64;
        long last = Interlocked.Read(ref _p3WidebandDisplayMissingLogMs);
        if (now - last < 5_000) return;
        if (Interlocked.CompareExchange(ref _p3WidebandDisplayMissingLogMs, now, last) != last) return;
        _log.LogInformation(
            "p3.wideband-display.waiting reason=sidecar-wideband-frame-unavailable");
    }

    private void LogP3WidebandDisplayError(Exception ex)
    {
        long now = Environment.TickCount64;
        long last = Interlocked.Read(ref _p3WidebandDisplayErrorLogMs);
        if (now - last < 5_000) return;
        if (Interlocked.CompareExchange(ref _p3WidebandDisplayErrorLogMs, now, last) != last) return;
        _log.LogWarning(ex, "p3.wideband-display.poll.error");
    }

    private void PublishAudio(in AudioFrame frame)
    {
        for (int i = 0; i < _audioSinks.Length; i++)
            _audioSinks[i].Publish(in frame);
    }

    private void PublishCwSidetoneAudio(in AudioFrame frame)
    {
        for (int i = 0; i < _audioSinks.Length; i++)
            _audioSinks[i].PublishCwSidetone(in frame);
    }

    // Fan out a mute-EXEMPT frame (local monitor audio the operator explicitly
    // asked to hear, such as Recorder playback or TX Monitor preview).
    // Only NativeAudioSink honours it; every other sink inherits the interface's
    // default no-op, so exempt playback reaches the desktop PC output but never
    // the WebSocket fan-out or the onboard radio speakers.
    private void PublishExemptAudio(in AudioFrame frame)
    {
        for (int i = 0; i < _audioSinks.Length; i++)
            _audioSinks[i].PublishExempt(in frame);
    }

    private void PublishTxMonitorAudio(in AudioFrame frame)
    {
        for (int i = 0; i < _audioSinks.Length; i++)
            _audioSinks[i].PublishTxMonitor(in frame);
    }

    private void PublishTxMonitorExemptAudio(in AudioFrame frame)
    {
        for (int i = 0; i < _audioSinks.Length; i++)
            _audioSinks[i].PublishTxMonitorExempt(in frame);
    }

    private void PublishTxSuppressedAudio(float[] audioBuf, int sampleCount, double nowMs, SquelchConfig squelch)
        => PublishTxSuppressedAudio(
            audioBuf, sampleCount, nowMs, squelch, ReadOnlySpan<float>.Empty, 0);

    private void PublishTxSuppressedAudio(
        float[] audioBuf,
        int sampleCount,
        double nowMs,
        SquelchConfig squelch,
        ReadOnlySpan<float> externalRx,
        int externalRxCount)
    {
        int count = sampleCount > 0
            ? Math.Min(sampleCount, audioBuf.Length)
            : Math.Min(audioBuf.Length, MonitorInjectSilentBlockSamples);
        externalRxCount = Math.Min(
            audioBuf.Length,
            Math.Clamp(externalRxCount, 0, externalRx.Length));
        count = Math.Max(count, externalRxCount);
        if (count <= 0) return;

        var span = audioBuf.AsSpan(0, count);
        span.Clear();
        bool sidetoneWrote = _sidetone?.RenderInto(span) ?? false;
        if (sidetoneWrote)
        {
            var sidetoneFrame = new AudioFrame(
                Seq: _audioSeq + 1,
                TsUnixMs: nowMs,
                RxId: 0,
                Channels: 1,
                SampleRateHz: (uint)AudioOutputRateHz,
                SampleCount: (ushort)count,
                Samples: new ReadOnlyMemory<float>(audioBuf, 0, count));
            PublishCwSidetoneAudio(in sidetoneFrame);
        }
        if (externalRxCount > 0)
            for (int i = 0; i < externalRxCount; i++) span[i] += externalRx[i];
        if (sidetoneWrote || externalRxCount > 0)
            LimitRxAudioBuffer(span);

        double finalAudioRms = Rms(span);
        double finalAudioPeak = PeakAbs(span);
        var frame = new AudioFrame(
            Seq: ++_audioSeq,
            TsUnixMs: nowMs,
            RxId: 0,
            Channels: 1,
            SampleRateHz: (uint)AudioOutputRateHz,
            SampleCount: (ushort)count,
            Samples: new ReadOnlyMemory<float>(audioBuf, 0, count));

        CaptureAudioDiagnostics(
            externalRxCount > 0 ? "external-rx" : sidetoneWrote ? "cw-sidetone" : "tx-rx-muted",
            in frame,
            finalAudioRms,
            finalAudioPeak,
            txMonitorRequested: false,
            squelch);
        PublishAudio(in frame);
        if (sidetoneWrote || externalRxCount > 0)
            RxAudioAvailable?.Invoke(0, AudioOutputRateHz, new ReadOnlyMemory<float>(audioBuf, 0, count));
    }

    internal static bool ShouldPublishNormalRxAudio(
        bool txMonitorOn,
        bool txAudioSuppressed,
        bool txMonitorMeterOnly = false) =>
        (!txMonitorOn || txMonitorMeterOnly) && !txAudioSuppressed;

    private bool ShouldSuppressRxAudioForCurrentTick(out bool activelyKeyed)
    {
        // This volatile acquire pairs with SetMox(true)'s final suppression
        // write, publishing the per-transmission DUP and PureSignal decision.
        activelyKeyed = _rxAudioSuppressedForTx;
        int postTxMuteBlocksRemaining = Volatile.Read(ref _rxPostTxMuteBlocksRemaining);
        if (!activelyKeyed && postTxMuteBlocksRemaining <= 0)
        {
            _fullDuplexRxActiveForCurrentTx = false;
            if (Volatile.Read(ref _rxPostTxFadeInSamplesRemaining) <= 0)
                _localRxAudioContinuousForCurrentTx = false;
        }
        return activelyKeyed || postTxMuteBlocksRemaining > 0;
    }

    internal bool ShouldSuppressRxDisplayForCurrentTick()
    {
        if (_keyed) return false;

        while (true)
        {
            int remaining = Volatile.Read(ref _rxPostTxDisplayFramesRemaining);
            if (remaining <= 0) return false;

            int next = remaining - 1;
            if (Interlocked.CompareExchange(ref _rxPostTxDisplayFramesRemaining, next, remaining) == remaining)
                return true;
        }
    }

    internal void MarkTxSuppressedAudioBlockPublished()
    {
        // While actively keyed, the suppression latch stays high and the
        // post-TX drain counter must not tick down. The counter is only for
        // the short MOX-off settle window before real RX resumes.
        if (_rxAudioSuppressedForTx) return;

        while (true)
        {
            int remaining = Volatile.Read(ref _rxPostTxMuteBlocksRemaining);
            if (remaining <= 0) return;

            int next = remaining - 1;
            if (Interlocked.CompareExchange(ref _rxPostTxMuteBlocksRemaining, next, remaining) != remaining)
                continue;

            if (next == 0)
            {
                Volatile.Write(ref _rxPostTxFadeInSamplesRemaining, RxPostTxFadeInSamples);
                _fullDuplexRxActiveForCurrentTx = false;
            }
            return;
        }
    }

    internal static int ApplyRxPostTxFadeIn(
        Span<float> samples,
        int remainingSamples,
        int totalSamples)
    {
        if (samples.Length == 0 || remainingSamples <= 0 || totalSamples <= 0)
            return Math.Max(0, remainingSamples);

        int toFade = Math.Min(samples.Length, remainingSamples);
        int startRemaining = remainingSamples;
        for (int i = 0; i < toFade; i++)
        {
            int samplesLeftAfterThis = startRemaining - i - 1;
            int completed = Math.Clamp(totalSamples - samplesLeftAfterThis, 0, totalSamples);
            double phase = completed / (double)totalSamples;
            float gain = (float)(0.5 - 0.5 * Math.Cos(Math.PI * phase));
            samples[i] *= gain;
        }

        return remainingSamples - toFade;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await Task.Yield();

        OpenSynthetic();
        _radio.Connected += OnRadioConnected;
        _radio.Disconnected += OnRadioDisconnected;
        _radio.StateChanged += OnRadioStateChanged;
        _radio.PaSnapshotChanged += OnPaSnapshotChanged;
        // Audio front-end (external-audio-jacks re-port) — global per-radio.
        // RadioService can't reach the P2 client directly (ActiveClient is
        // P1-only), so forward TxSpecific bytes 50/51 here, and route the
        // radio-mic STREAM (1026) gate.
        _radio.AudioFrontEndChanged += OnAudioFrontEndChanged;
        _radio.MoxChanged += OnRadioMoxChanged;
        _radio.TunActiveChanged += OnRadioTunActiveChanged;
        _radio.PreampChanged += OnRadioPreampChanged;
        _radio.SampleRateChanged += OnRadioSampleRateChanged;
        _radio.NotchesChanged += OnRadioNotchesChanged;
        // Frequency-correction factor (issue #325) — RadioService can't
        // push to the P2 client directly (ActiveClient is P1-only), so we
        // listen for changes here and forward them to the live P2 client.
        _radio.FrequencyCorrectionFactorChanged += OnFrequencyCorrectionFactorChanged;
        using var timer = new PeriodicTimer(TickPeriod);
        using var widebandAnalyzerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var widebandAnalyzerTask = RunWidebandDisplayAnalyzerAsync(widebandAnalyzerCts.Token);
        var p3WidebandDisplayTask = RunP3WidebandDisplayPollerAsync(widebandAnalyzerCts.Token);

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                long nowTicks = Stopwatch.GetTimestamp();
                // iter5: when a radio is connected, the sink (called on the
                // RX OS thread) normally drives Tick inline via Stopwatch
                // elapsed checks — see OnIqFrame. If the sink is attached but
                // RX packets stall, the timer becomes the fallback driver so
                // monitor/preview audio, commands, and diagnostics still drain.
                if (_rxSinkAttached &&
                    !ShouldTimerTickWhenSinkAttached(
                        nowTicks,
                        Volatile.Read(ref _lastInlineTickStopwatchTicks),
                        Volatile.Read(ref _lastTickStopwatchTicks),
                        TickPeriodStopwatchTicks))
                    continue;
                // Drain any cross-thread commands posted while no sink was
                // attached, or while an attached sink has gone stale.
                DrainDspCommands();
                long tickDeadline = Volatile.Read(ref _inlineTickDeadlineStopwatchTicks);
                long displayDeadline = Volatile.Read(ref _inlineDisplayDeadlineStopwatchTicks);
                if (Tick(_panBuf, _wfBuf, _audioBuf))
                {
                    Volatile.Write(ref _lastTickStopwatchTicks, nowTicks);
                    TryUpdateTickDeadline(
                        ref _inlineTickDeadlineStopwatchTicks,
                        tickDeadline,
                        nowTicks);
                    TryUpdateTickDeadline(
                        ref _inlineDisplayDeadlineStopwatchTicks,
                        displayDeadline,
                        nowTicks);
                }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            _radio.Connected -= OnRadioConnected;
            _radio.Disconnected -= OnRadioDisconnected;
            _radio.StateChanged -= OnRadioStateChanged;
            _radio.PaSnapshotChanged -= OnPaSnapshotChanged;
            _radio.AudioFrontEndChanged -= OnAudioFrontEndChanged;
            _radio.MoxChanged -= OnRadioMoxChanged;
            _radio.TunActiveChanged -= OnRadioTunActiveChanged;
            _radio.PreampChanged -= OnRadioPreampChanged;
            _radio.SampleRateChanged -= OnRadioSampleRateChanged;
            _radio.NotchesChanged -= OnRadioNotchesChanged;
            _radio.FrequencyCorrectionFactorChanged -= OnFrequencyCorrectionFactorChanged;
            // iter5: no more pump tasks to stop — the sink path runs on the
            // protocol client's RX thread, which the protocol client tears
            // down via its own StopAsync. Detach defensively in case a
            // disconnect didn't fire (e.g., abrupt host shutdown).
            DetachRxSinkP1();
            DetachRxSinkP2();
            CloseCurrentEngine();
            widebandAnalyzerCts.Cancel();
            try { await widebandAnalyzerTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            try { await p3WidebandDisplayTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
    }

    internal virtual void SetMox(bool on)
    {
        // Direct call, not queued: HL2 stops RX while MOX is asserted, so a
        // PostDspCommand queued from the HTTP thread would not drain until
        // MOX releases — TXA stays in RX state and TX produces buzz. WDSP
        // tolerates concurrent state edges from the HTTP thread vs the RX
        // sink thread via its own internal locking, and SetMox/SetTxTune
        // are rare operator-edge events (not the per-frame hot path).
        if (on)
        {
            Volatile.Write(ref _rxPostTxMuteBlocksRemaining, 0);
            Volatile.Write(ref _rxPostTxFadeInSamplesRemaining, 0);
            Volatile.Write(ref _rxPostTxDisplayFramesRemaining, 0);
            _p2DisplayDuplexForCurrentTx = _radio.IsProtocol2Active;
            _stopRxForPureSignalForCurrentTx = _appliedPsEnabled;
            // The operator's DUP choice is latched by the first suppressed tick
            // rather than read here. SetMox runs on the caller's thread (see the
            // note above) and RadioService.Snapshot takes the radio-state lock,
            // so reading it here would put a cross-service lock acquisition on
            // the MOX edge. The tick already holds a projected StateDto and can
            // latch without any lock at all.
            _fullDuplexRxActiveForCurrentTx = false;
            _localRxAudioContinuousForCurrentTx = false;
            _fullDuplexLatchPendingForCurrentTx = true;
            _resetDisplayPixelsForCurrentTx = _appliedPsEnabled && !_p2DisplayDuplexForCurrentTx;
            _rxFadeOutPending = true;
            // Publish every per-transmission decision above before the RX sink
            // thread observes keyed audio suppression.
            _rxAudioSuppressedForTx = true;
            lock (_engineLock)
            {
                _engine?.SetMox(true, _stopRxForPureSignalForCurrentTx);
                if (_resetDisplayPixelsForCurrentTx)
                    _engine?.ResetDisplayPixelBuffers();
            }
        }
        else
        {
            int postTxMuteBlocks = PostTxMuteBlocksForDelayMs(_radio.TxPostTxRxMuteDelayMs);
            try
            {
                lock (_engineLock)
                {
                    _engine?.SetMox(false, _stopRxForPureSignalForCurrentTx);
                    if (_resetDisplayPixelsForCurrentTx)
                        _engine?.ResetDisplayPixelBuffers();
                }
            }
            finally
            {
                // Clear the RX-audio suppression latch and prime the post-TX
                // drain counters even if the engine MOX-off throws. The caller
                // (TxService.ConvergeToSafeIdle) invokes this through a Safe(…)
                // wrapper that swallows exceptions, so a latch left high here
                // would keep RX audio muted until a reconnect rebuilt the
                // engine — the "no receive audio after TX" symptom (issue #993).
                Volatile.Write(ref _rxPostTxMuteBlocksRemaining, postTxMuteBlocks);
                Volatile.Write(ref _rxPostTxDisplayFramesRemaining, postTxMuteBlocks);
                _rxAudioSuppressedForTx = false;
                _stopRxForPureSignalForCurrentTx = false;
                _resetDisplayPixelsForCurrentTx = false;
                _p2DisplayDuplexForCurrentTx = false;
            }
        }
    }

    internal static int PostTxMuteBlocksForDelayMs(int delayMs)
    {
        if (delayMs <= 0) return 0;
        return Math.Max(1, (delayMs * RxPostTxAudioCadenceHz + 999) / 1000);
    }

    internal virtual void SetTxTune(bool on)
    {
        lock (_engineLock) { _engine?.SetTxTune(on); }
    }

    public virtual void SetPsMox(bool on)
    {
        lock (_engineLock) { _engine?.SetPsMox(on); }
    }

    internal bool RxAudioSuppressedForTx => _rxAudioSuppressedForTx;
    internal int RxPostTxMuteBlocksRemaining => Volatile.Read(ref _rxPostTxMuteBlocksRemaining);
    internal int RxPostTxFadeInSamplesRemaining => Volatile.Read(ref _rxPostTxFadeInSamplesRemaining);
    internal int RxPostTxDisplayFramesRemaining => Volatile.Read(ref _rxPostTxDisplayFramesRemaining);

    /// <summary>Current engine snapshot (may be <see cref="OfflinePreviewDspEngine"/>
    /// or <see cref="SyntheticDspEngine"/> while disconnected). TxAudioIngest calls ProcessTxBlock on this; the
    /// engine handles a disposed-during-call race internally by returning 0.
    /// Virtual so tests can subclass this service and substitute a stub engine
    /// without running the full Synthetic/WDSP lifecycle.
    ///
    /// iter5 pass-2: read lock-free via Volatile.Read. The previous
    /// _engineLock-guarded getter provided pointer-atomic reads only —
    /// Volatile.Read provides the same guarantee on .NET reference types
    /// without acquiring the lock. Engine swap writers continue to take
    /// _engineLock to serialise themselves against each other.</summary>
    public virtual IDspEngine? CurrentEngine => Volatile.Read(ref _engine);

    /// <summary>
    /// Version of the libwdsp actually loaded, for the diagnostics snapshot.
    /// Cached behind a Lazy in <see cref="WdspDspEngine"/> — the loaded library
    /// cannot change for the lifetime of the process — so this costs a field
    /// read per snapshot.
    ///
    /// Virtual purely so tests can drive a stale engine through the real
    /// endpoint: the bench library reports exactly the required version, so
    /// without a seam the "loud mismatch" path has no observable behaviour.
    /// Mirrors <see cref="CurrentEngine"/>.
    /// </summary>
    internal virtual WdspEngineVersion ResolveEngineVersion() =>
        WdspDspEngine.ResolveEngineVersion();

    public virtual void ResetTxPhaseRotatorAuto()
    {
        lock (_engineLock)
        {
            CurrentEngine?.ResetTxPhaseRotatorAuto(_channelId);
        }
    }

    public virtual TxPhaseRotatorAsymmetry? GetTxPhaseRotatorAsymmetry()
    {
        lock (_engineLock)
        {
            return CurrentEngine?.GetTxPhaseRotatorAsymmetry(_channelId);
        }
    }

    public DspNrRuntimeSnapshot SnapshotNrRuntime()
    {
        var engine = Volatile.Read(ref _engine);
        var state = _radio.Snapshot();
        return BuildNrRuntime(engine, state);
    }

    public object SnapshotDiagnostics(WdspWisdomInitializer wisdom)
    {
        var engine = Volatile.Read(ref _engine);
        var state = _radio.Snapshot();
        int channelId = Volatile.Read(ref _channelId);
        var nrRuntime = BuildNrRuntime(engine, state);
        bool wdspActive = nrRuntime.WdspActive;
        bool synthetic = engine is SyntheticDspEngine;
        bool offlinePreview = engine is OfflinePreviewDspEngine;
        var squelchConfig = state.Squelch ?? new SquelchConfig();
        var rxDsp = BuildRxDspChainDiagnostics(
            state,
            _radio.Notches,
            nrRuntime,
            _appliedNr,
            _appliedAgc,
            _appliedSquelch);
        var rxMeters = SnapshotRxMetersDiagnostics();
        var adcProtection = _radio.GetAdcProtectionStatus();
        var squelch = SnapshotAdaptiveSquelchDiagnostics(squelchConfig);
        var audio = SnapshotAudioDiagnostics();
        var display = SnapshotDisplayDiagnostics(engine);
        var secondReceiverHealth = SnapshotSecondReceiverHealth(state);
        var engineVersion = ResolveEngineVersion();
        return new
        {
            schemaVersion = 1,
            engine = engine?.GetType().Name ?? "None",
            engineKind = wdspActive ? "WDSP" : offlinePreview ? "OfflinePreview" : synthetic ? "Synthetic" : engine is null ? "None" : "Other",
            protocol3TxEngine = CurrentProtocol3TxEngine,
            externalPureSignal = engine is ExternalProtocol3TxDspEngine { PureSignalEnabled: true } externalPs
                ? externalPs.GetPureSignalStatus()
                : (ExternalPureSignalStatus?)null,
            wdspActive = nrRuntime.WdspActive,
            synthetic,
            offlinePreview,
            wdspNativeLoadable = nrRuntime.WdspNativeLoadable,
            wdspEmnrPost2Available = nrRuntime.WdspEmnrPost2Available,
            wdspNr4SbnrAvailable = nrRuntime.WdspNr4SbnrAvailable,
            nr4Readiness = nrRuntime.Nr4Readiness,
            requestedNrMode = nrRuntime.RequestedNrMode,
            effectiveNrMode = nrRuntime.EffectiveNrMode,
            rxDsp,
            rxMeters,
            rxDynamicRange = BuildRxDynamicRangeDiagnostics(state, rxMeters, adcProtection),
            squelch,
            filterGeometry = BuildFilterGeometryDiagnostics(
                state,
                engine,
                wisdom,
                BoardCapabilitiesTable.For(_radio.EffectiveBoardKind, _radio.EffectiveOrionMkIIVariant),
                _radio.ConnectedBoardKind,
                _radio.EffectiveBoardKind,
                _radio.EffectiveOrionMkIIVariant,
                state.Status == ConnectionStatus.Connected && _radio.ActiveClient is null),
            channelId = Volatile.Read(ref _channelId),
            sampleRateHz = Volatile.Read(ref _sampleRateHz),
            displayWidth = _panadapterWidth,
            tickRateHz = Math.Round(1.0 / TickPeriod.TotalSeconds, 1),
            audioOutputRateHz = AudioOutputRateHz,
            txBlockSamples = engine?.TxBlockSamples ?? 0,
            txOutputSamples = engine?.TxOutputSamples ?? 0,
            txMonitorRequested = engine?.IsTxMonitorOn ?? false,
            rxSinkAttached = _rxSinkAttached,
            audioSinkCount = _audioSinks.Length,
            monitorBacklogSamples = MonitorBacklog,
            audio,
            listenability = BuildRxListenabilityDiagnostics(rxMeters, audio, squelchConfig),
            display,
            secondReceiverHealth,
            wdspWisdomPhase = wisdom.Phase.ToString(),
            wdspWisdomStatus = wisdom.Status,
            // Identity of the libwdsp actually loaded, alongside the FFTW wisdom
            // status it shares a home with. Additive only. wdspVersionMismatch is
            // the machine-readable "the loaded engine is older than Zeus targets"
            // flag the UI surfaces; a missing symbol or an unloadable library
            // leaves it false and shows up in wdspVersionStatus instead.
            wdspVersion = engineVersion.Display,
            wdspVersionRaw = engineVersion.Raw,
            wdspVersionStatus = engineVersion.StatusToken,
            wdspVersionMismatch = engineVersion.IsMismatch,
            wdspVersionRequired = WdspEngineVersion.RequiredDisplay,
            readiness = wdspActive
                ? "wdsp-active"
                : offlinePreview
                    ? "offline-preview-active"
                : synthetic
                    ? "synthetic-idle-or-fallback"
                    : "no-engine",
        };
    }

    public DspLiveRuntimeEvidenceDto SnapshotLiveRuntimeEvidence()
    {
        var rxMeters = SnapshotRxMetersDiagnostics();
        var audio = SnapshotAudioDiagnostics();
        string status = LiveRuntimeEvidenceStatus(rxMeters, audio);

        return new DspLiveRuntimeEvidenceDto(
            SchemaVersion: 4,
            GeneratedUtc: DateTimeOffset.UtcNow,
            Status: status,
            RxMetersFresh: rxMeters.Fresh,
            RxMetersStale: rxMeters.Stale,
            RxMetersAgeMs: rxMeters.AgeMs,
            RxDbm: rxMeters.RxDbm,
            AdcHeadroomDb: rxMeters.AdcHeadroomDb,
            AgcGainDb: rxMeters.AgcGainDb,
            AudioFresh: audio.Fresh,
            AudioStale: audio.Stale,
            AudioAgeMs: audio.AgeMs,
            AudioStatus: audio.Status,
            AudioSource: audio.Source,
            AudioFramesBroadcast: audio.FramesBroadcast,
            AudioLastSeq: audio.LastSeq,
            AudioSampleRateHz: audio.SampleRateHz,
            AudioSampleCount: audio.SampleCount,
            AudioRmsDbfs: audio.RmsDbfs,
            AudioPeakDbfs: audio.PeakDbfs,
            TxMonitorRequested: audio.TxMonitorRequested,
            SquelchEnabled: audio.SquelchEnabled,
            SquelchOpen: audio.SquelchOpen,
            SquelchTailActive: audio.SquelchTailActive,
            SquelchGateGain: audio.SquelchGateGain,
            RxAudioLevelerInputRmsDbfs: audio.RxAudioLevelerInputRmsDbfs,
            RxAudioLevelerOutputRmsDbfs: audio.RxAudioLevelerOutputRmsDbfs,
            RxAudioLevelerInputPeakDbfs: audio.RxAudioLevelerInputPeakDbfs,
            RxAudioLevelerOutputPeakDbfs: audio.RxAudioLevelerOutputPeakDbfs,
            RxAudioLevelerDesiredGainDb: audio.RxAudioLevelerDesiredGainDb,
            RxAudioLevelerAppliedGainDb: audio.RxAudioLevelerAppliedGainDb,
            RxAudioLevelerGainDeltaDb: audio.RxAudioLevelerGainDeltaDb,
            RxAudioLevelerPeakHeadroomDb: audio.RxAudioLevelerPeakHeadroomDb,
            RxAudioLevelerPreLimitPeakDbfs: audio.RxAudioLevelerPreLimitPeakDbfs,
            RxAudioLevelerOutputLimitReductionDb: audio.RxAudioLevelerOutputLimitReductionDb,
            RxAudioLevelerOutputLimitSampleCount: audio.RxAudioLevelerOutputLimitSampleCount,
            RxAudioLevelerPauseHoldBlocks: audio.RxAudioLevelerPauseHoldBlocks,
            RxAudioLevelerBoostSlewLimited: audio.RxAudioLevelerBoostSlewLimited,
            RxAudioLevelerPeakLimited: audio.RxAudioLevelerPeakLimited,
            RxAudioLevelerOutputLimited: audio.RxAudioLevelerOutputLimited,
            MonitorBacklogSamples: audio.MonitorBacklogSamples,
            AudioSinkCount: audio.AudioSinkCount,
            DiagnosticRecommendation: LiveRuntimeEvidenceRecommendation(status, rxMeters, audio));
    }

    private static string LiveRuntimeEvidenceStatus(RxMetersDiagnosticsDto rxMeters, AudioPathDiagnosticsDto audio)
    {
        if (!audio.Fresh)
            return $"audio-{audio.Status}";
        if (audio.Status is not "fresh")
            return $"audio-{audio.Status}";
        if (!rxMeters.Fresh)
            return rxMeters.Stale ? "rx-meters-stale" : "rx-meters-missing";
        if (rxMeters.AdcHeadroomDb is < 6.0)
            return "adc-headroom-low";
        return "fresh";
    }

    private static string LiveRuntimeEvidenceRecommendation(
        string status,
        RxMetersDiagnosticsDto rxMeters,
        AudioPathDiagnosticsDto audio) =>
        status switch
        {
            "fresh" => "Final RX audio and RXA meters are fresh; use AGC gain/headroom, audio RMS/peak, and squelch state with fixture metrics before changing DSP behavior.",
            "adc-headroom-low" => "ADC headroom is low; add attenuation or reduce front-end gain before judging NR/AGC improvements.",
            "rx-meters-stale" or "rx-meters-missing" => rxMeters.DiagnosticRecommendation,
            _ => audio.DiagnosticRecommendation,
        };

    private static object BuildFilterGeometryDiagnostics(
        StateDto state,
        IDspEngine? engine,
        WdspWisdomInitializer wisdom,
        BoardCapabilities caps,
        HpsdrBoardKind connectedBoard = HpsdrBoardKind.Unknown,
        HpsdrBoardKind effectiveBoard = HpsdrBoardKind.Unknown,
        OrionMkIIVariant variant = OrionMkIIVariant.G2,
        bool protocol2Active = false)
    {
        bool wdsp = engine is WdspDspEngine or OfflinePreviewDspEngine;
        int txBlock = engine?.TxBlockSamples ?? 0;
        int txOut = engine?.TxOutputSamples ?? 0;
        int txDspRateHz = txBlock == 256 && txOut == 1024 ? 96_000 : 48_000;
        string status = engine is OfflinePreviewDspEngine
            ? "offline-preview-tx-profile"
            : wdsp
                ? "runtime-rate-phase-taps-writable-fixed-buffer-profile"
                : engine is SyntheticDspEngine ? "synthetic-profile" : "engine-unavailable";
        int[] sampleRates = [48_000, 96_000, 192_000, 384_000, 768_000, 1_536_000];
        int[] iqBufferSizes = [64, 128, 256, 512, 1024];
        int[] filterTapSizes = [64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144];
        string[] filterTypes = ["Linear Phase", "Minimum Phase"];
        object[] filterWindows =
        [
            new { id = 0, label = "BH-4", notes = "Thetis default in DSP Options; sharper transition." },
            new { id = 1, label = "BH-7", notes = "Deeper cutoff; this is the current Zeus WDSP call." },
        ];
        var receiverBandwidth = BuildReceiverBandwidthDiagnostics(
            state,
            caps,
            connectedBoard,
            effectiveBoard,
            variant,
            protocol2Active);

        return new
        {
            schemaVersion = 1,
            status,
            operatorConfigurable = true,
            hardwareLimits = new
            {
                rxAdcCount = caps.RxAdcCount,
                maxRxSampleRateHz = caps.MaxRxSampleRateHz,
                activeSampleRateHz = state.SampleRate,
                sampleRates = sampleRates.Select(rate => new
                {
                    sampleRateHz = rate,
                    label = $"{rate / 1000} kHz",
                    boardSupported = rate <= caps.MaxRxSampleRateHz,
                    protocol2Required = rate > 384_000,
                    active = rate == state.SampleRate,
                    status = rate <= caps.MaxRxSampleRateHz
                        ? rate > 384_000 ? "hardware-supported-p2-only" : "hardware-supported"
                        : "above-board-capability",
                }).ToArray(),
            },
            runtimeSampleRateControl = BuildRuntimeSampleRateControlDiagnostics(
                state,
                caps,
                protocol2Active),
            optionCatalog = new
            {
                iqBufferSizes,
                filterTapSizes,
                filterTypes,
                filterWindows,
                slowModeChangeWarning = "Tap or phase changes rebuild FIR partitions while the affected channel is quiescent. Extreme cold minimum-phase changes can take longer than cached repeats.",
                source = "Thetis DSP Options mode defaults + Zeus WDSPwisdom 64..262144 startup planning ladder",
            },
            activeRx = new
            {
                mode = state.Mode.ToString(),
                filterLowHz = state.FilterLowHz,
                filterHighHz = state.FilterHighHz,
                filterPresetName = state.FilterPresetName,
                inputBufferSize = 512,
                dspBufferSize = 512,
                dspRateHz = 48_000,
                schedulingQuantumMs = Math.Round(512.0 / 48_000.0 * 1000.0, 3),
                filterWindowId = 1,
                filterWindow = "BH-7",
                filterType = state.RxFilterPhase == FilterPhaseMode.Minimum ? "Minimum Phase" : "Linear Phase",
                filterPhase = state.RxFilterPhase.ToString(),
                filterTaps = state.RxFilterWindow switch
                {
                    BandpassWindow.Soft => 1024,
                    BandpassWindow.Normal => 2048,
                    BandpassWindow.Sharp => 4096,
                    BandpassWindow.Taps8192 => 8192,
                    BandpassWindow.Taps16384 => 16384,
                    BandpassWindow.Taps32768 => 32768,
                    BandpassWindow.Taps65536 => 65536,
                    BandpassWindow.Taps131072 => 131072,
                    BandpassWindow.Taps262144 => 262144,
                    _ => 2048,
                },
                linearPhaseFirDelayMs = state.RxFilterPhase == FilterPhaseMode.Linear
                    ? Math.Round((ResolveFilterTapCount(state.RxFilterWindow) - 1.0) / (2.0 * 48_000.0) * 1000.0, 3)
                    : (double?)null,
                latencyAccounting = state.RxFilterPhase == FilterPhaseMode.Linear
                    ? "Exact single-stage FIR group delay; active serial filters can add multiple stages. Scheduling quantum is reported separately and sink/network latency is excluded."
                    : "Minimum-phase delay is impulse- and passband-dependent; scheduling quantum is exact and total sink/network latency is excluded.",
                status = wdsp ? "operator-selectable" : "not-wdsp",
            },
            activeTx = new
            {
                mode = state.Mode.ToString(),
                filterLowHz = state.TxFilterLowHz,
                filterHighHz = state.TxFilterHighHz,
                inputBufferSize = txBlock,
                dspBufferSize = txBlock > 0 ? 512 : 0,
                dspRateHz = txBlock > 0 ? txDspRateHz : 0,
                schedulingQuantumMs = txBlock > 0
                    ? Math.Round(512.0 / txDspRateHz * 1000.0, 3)
                    : 0.0,
                outputBufferSize = txOut,
                filterWindowId = 1,
                filterWindow = "BH-7",
                filterType = state.TxFilterPhase == FilterPhaseMode.Minimum ? "Minimum Phase" : "Linear Phase",
                filterPhase = state.TxFilterPhase.ToString(),
                filterTaps = state.TxFilterWindow switch
                {
                    BandpassWindow.Soft => 1024,
                    BandpassWindow.Normal => 2048,
                    BandpassWindow.Sharp => 4096,
                    BandpassWindow.Taps8192 => 8192,
                    BandpassWindow.Taps16384 => 16384,
                    BandpassWindow.Taps32768 => 32768,
                    BandpassWindow.Taps65536 => 65536,
                    BandpassWindow.Taps131072 => 131072,
                    BandpassWindow.Taps262144 => 262144,
                    _ => 2048,
                },
                linearPhaseFirDelayMs = state.TxFilterPhase == FilterPhaseMode.Linear && txBlock > 0
                    ? Math.Round((ResolveFilterTapCount(state.TxFilterWindow) - 1.0) / (2.0 * txDspRateHz) * 1000.0, 3)
                    : (double?)null,
                latencyAccounting = state.TxFilterPhase == FilterPhaseMode.Linear
                    ? "Exact single-stage FIR group delay; active serial filters can add multiple stages. Scheduling quantum is reported separately and radio/network latency is excluded."
                    : "Minimum-phase delay is impulse- and passband-dependent. The selected taps apply to one fixed master FIR and the final overshoot stage uses a short cleanup FIR; scheduling quantum is exact and total radio/network latency is excluded.",
                cfirCompensation = txOut > txBlock && txBlock > 0,
                status = wdsp ? "operator-selectable" : "not-wdsp",
            },
            receiverBandwidth,
            thetisMatrix = new[]
            {
                ThetisFilterRow("SSB/AM", "RX", 1024, 16384, "Low Latency", "BH-4", "reference"),
                ThetisFilterRow("SSB/AM", "TX", 1024, 16384, "Linear Phase", "BH-4", "reference"),
                ThetisFilterRow("FM", "RX", 256, 4096, "Low Latency", "BH-4", "reference"),
                ThetisFilterRow("FM", "TX", 128, 1024, "Low Latency", "BH-4", "reference"),
                ThetisFilterRow("CW", "RX", 64, 4096, "Low Latency", "BH-4", "reference"),
                ThetisFilterRow("CW", "TX", null, null, "Mode generated", "BH-4", "reference-no-separate-tx-row"),
                ThetisFilterRow("Digital", "RX", 64, 4096, "Low Latency", "BH-4", "reference"),
                ThetisFilterRow("Digital", "TX", 64, 4096, "Low Latency", "BH-4", "reference"),
            },
            impulseCache = new
            {
                fftwWisdomPhase = wisdom.Phase.ToString(),
                fftwWisdomStatus = wisdom.Status,
                fftwWisdomCache = true,
                filterImpulseCache = true,
                cacheByteBudgetPerBucket = 64 * 1024 * 1024,
                cacheScope = "process-wide byte-bounded LRU",
                saveRestoreImpulseCacheFile = false,
                status = "fftw-wisdom-plus-bounded-impulse-cache",
                notes = "WDSP self-initializes a thread-safe, byte-bounded impulse cache. Cache-file save/restore is not an operator setting.",
            },
            highResolutionFilterDisplay = new
            {
                enabled = false,
                status = "not-exposed-as-filter-display-setting",
                notes = "Zeus exposes live filter edges, presets, panadapter scale, and mini-pan visuals, but not Thetis's separate high-resolution filter-characteristics display toggle yet.",
            },
            diagnosticRecommendation = "The live DDC rate, exact RX/TX tap counts, and independent FIR phase are operator-writable. RXA/TXA use fixed low-latency 512-sample DSP partitions; linear FIR delay and the scheduling quantum are reported separately.",
            source = "Thetis DSP Options filter matrix + Zeus WdspDspEngine OpenChannel/SetRXABandpassWindow/SetTXABandpassWindow profile",
        };
    }

    private static int ResolveFilterTapCount(BandpassWindow window) => window switch
    {
        BandpassWindow.Soft => 1024,
        BandpassWindow.Normal => 2048,
        BandpassWindow.Sharp => 4096,
        BandpassWindow.Taps8192 => 8192,
        BandpassWindow.Taps16384 => 16384,
        BandpassWindow.Taps32768 => 32768,
        BandpassWindow.Taps65536 => 65536,
        BandpassWindow.Taps131072 => 131072,
        BandpassWindow.Taps262144 => 262144,
        _ => 2048,
    };

    private static object BuildRuntimeSampleRateControlDiagnostics(
        StateDto state,
        BoardCapabilities caps,
        bool protocol2Active)
    {
        bool connected = state.Status == ConnectionStatus.Connected;
        int activeRate = Math.Max(0, state.SampleRate);
        int boardMax = Math.Max(48_000, caps.MaxRxSampleRateHz);
        int protocolMax = protocol2Active ? boardMax : Math.Min(boardMax, 384_000);
        bool p2WidebandCapable = boardMax > 384_000;
        bool widebandWritable = connected && protocol2Active && p2WidebandCapable;
        string status;
        string recommendation;

        if (!connected)
        {
            status = "waiting-for-connection";
            recommendation = "Connect a radio before changing the runtime DDC sample rate.";
        }
        else if (p2WidebandCapable && !protocol2Active)
        {
            status = "wideband-requires-p2";
            recommendation = "This radio can use 768/1536 kHz DDC rates, but the current connection is not Protocol 2; reconnect over P2 before widening the span.";
        }
        else if (p2WidebandCapable && activeRate < boardMax)
        {
            status = "wideband-control-ready";
            recommendation = "Wider 768/1536 kHz spans are available now; increase the DDC sample rate when weak-signal search bandwidth matters and host/network headroom is clean.";
        }
        else if (p2WidebandCapable)
        {
            status = "max-wideband-active";
            recommendation = "The active DDC sample rate is already at the verified board maximum; improve copy with filters, dynamic range, and display intelligence rather than widening the span.";
        }
        else
        {
            status = "board-capability-limited";
            recommendation = "The verified board ceiling is 384 kHz or lower; keep the DDC rate within the P1/P2 baseline ladder and optimize with front-end staging and DSP.";
        }

        return new
        {
            status,
            writable = connected,
            requiresReconnect = false,
            activeSampleRateHz = activeRate,
            maxBoardSampleRateHz = boardMax,
            maxWritableSampleRateHz = protocolMax,
            protocol2Active,
            widebandWritable,
            settingsSurface = "Settings > DSP > Bandwidth",
            apiRoute = "/api/sampleRate",
            diagnosticRecommendation = recommendation,
        };
    }

    private static object BuildReceiverBandwidthDiagnostics(
        StateDto state,
        BoardCapabilities caps,
        HpsdrBoardKind connectedBoard,
        HpsdrBoardKind effectiveBoard,
        OrionMkIIVariant variant,
        bool protocol2Active)
    {
        bool connected = state.Status == ConnectionStatus.Connected;
        bool g2Class = effectiveBoard == HpsdrBoardKind.OrionMkII
            && variant is OrionMkIIVariant.G2 or OrionMkIIVariant.G2_1K
            && caps.MaxRxSampleRateHz >= 1_536_000;
        int activeRate = Math.Max(0, state.SampleRate);
        int maxRate = Math.Max(48_000, caps.MaxRxSampleRateHz);
        double utilization = maxRate > 0
            ? Math.Round(Math.Clamp(activeRate / (double)maxRate, 0.0, 1.0) * 100.0, 1)
            : 0.0;
        bool p2WidebandCapable = maxRate > 384_000;
        bool widebandActive = connected && activeRate > 384_000;
        int activeSoftwareReceivers = connected ? 1 : 0;
        int manualReceiverCapacity = g2Class ? 10 : Math.Max(1, caps.RxAdcCount);
        int unexposedReceivers = Math.Max(0, manualReceiverCapacity - activeSoftwareReceivers);
        HpsdrBoardKind wireBoard = connectedBoard != HpsdrBoardKind.Unknown
            ? connectedBoard
            : effectiveBoard;
        int? activeUserDdcIndex = connected && protocol2Active
            ? Zeus.Protocol2.Protocol2Client.RxBaseDdc(wireBoard)
            : null;
        object[] activeSlots = activeUserDdcIndex.HasValue
            ? [DdcSlot(activeUserDdcIndex.Value, "RX1", "active", "Primary operator receive DDC feeding WDSP RXA and the panadapter/waterfall.")]
            : [];
        object[] reservedSlots = activeUserDdcIndex == 2
            ? [
                DdcSlot(0, "PureSignal RX feedback", "reserved", "Saturn/G2 P2 convention reserves DDC0 for post-PA feedback when PureSignal is armed."),
                DdcSlot(1, "PureSignal TX reference", "reserved", "Saturn/G2 P2 convention reserves DDC1 for TX-DAC reference feedback when PureSignal is armed."),
            ]
            : [];

        string status;
        string tone;
        string recommendation;
        if (!connected)
        {
            status = "waiting-for-connection";
            tone = "verify";
            recommendation = "Connect the radio before judging receiver bandwidth utilization or DDC-slot assignment.";
        }
        else if (p2WidebandCapable && !protocol2Active)
        {
            status = "wideband-requires-p2";
            tone = "verify";
            recommendation = "This board can use P2 wideband rates above 384 kHz, but the current runtime is not on a P2 wideband path.";
        }
        else if (p2WidebandCapable && activeRate < maxRate)
        {
            status = "wideband-underused";
            tone = "ready";
            recommendation = "Receiver hardware has unused DDC bandwidth; use the existing Settings > DSP > Bandwidth control to test wider 768 kHz or 1536 kHz spans when host/network load allows.";
        }
        else if (p2WidebandCapable)
        {
            status = "max-wideband-active";
            tone = "ready";
            recommendation = "Receiver DDC bandwidth is at the board maximum; refine copy with filters, dynamic-range staging, and display intelligence rather than widening the span.";
        }
        else
        {
            status = "board-capability-limited";
            tone = "standby";
            recommendation = "This board's verified DDC bandwidth ceiling is 384 kHz or lower; dynamic-range gains should come from front-end staging, filters, and DSP rather than P2 wideband rates.";
        }

        return new
        {
            schemaVersion = 1,
            status,
            tone,
            connected,
            protocol2Active,
            p2WidebandCapable,
            widebandActive,
            activeSampleRateHz = activeRate,
            maxSampleRateHz = maxRate,
            activeNyquistHz = activeRate / 2,
            maxNyquistHz = maxRate / 2,
            utilizationPct = utilization,
            unusedSampleRateHz = Math.Max(0, maxRate - activeRate),
            unusedNyquistHz = Math.Max(0, (maxRate - activeRate) / 2),
            activeSoftwareReceivers,
            manualReceiverCapacity,
            unexposedReceiverCount = unexposedReceivers,
            activeUserDdcIndex,
            activeSlots,
            reservedSlots,
            source = "ANAN G2 manual receiver architecture + Protocol2Client DDC map + BoardCapabilities",
            diagnosticRecommendation = recommendation,
        };
    }

    private static object DdcSlot(int slot, string purpose, string status, string notes) => new
    {
        slot,
        purpose,
        status,
        notes,
    };

    private static object ThetisFilterRow(
        string modeFamily,
        string direction,
        int? iqBufferSize,
        int? filterTaps,
        string filterType,
        string filterWindow,
        string status) => new
        {
            modeFamily,
            direction,
            iqBufferSize,
            filterTaps,
            filterType,
            filterWindow,
            status,
        };

    private object SnapshotDisplayDiagnostics(IDspEngine? engine)
    {
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        int clients = _hub.ClientCount;

        lock (_calPanLock)
        {
            long? frameAgeMs = _diagDisplayFrameMs > 0 ? Math.Max(0, nowMs - _diagDisplayFrameMs) : null;
            long? panAgeMs = _calPanSnapshotMs > 0 ? Math.Max(0, nowMs - _calPanSnapshotMs) : null;
            long? wfAgeMs = _diagWfSnapshotMs > 0 ? Math.Max(0, nowMs - _diagWfSnapshotMs) : null;
            string status = DisplayHealthStatus(engine, clients, frameAgeMs, _diagLastPanValid, _diagLastWfValid);

            return new
            {
                schemaVersion = 1,
                status,
                clientCount = clients,
                framesBroadcast = _diagDisplayFrameCount,
                lastSeq = _diagDisplaySeq,
                lastFrameAgeMs = frameAgeMs,
                lastFrameUnixMs = _diagDisplayFrameMs > 0 ? _diagDisplayFrameMs : (long?)null,
                panValid = _diagLastPanValid,
                waterfallValid = _diagLastWfValid,
                panSource = _diagLastPanSource,
                waterfallSource = _diagLastWfSource,
                keyed = _diagLastKeyed,
                psMonitorRequested = _diagLastPsMonitorRequested,
                psFeedbackCorrecting = _diagLastPsFeedbackCorrecting,
                width = _panadapterWidth,
                centerHz = _calPanCenterHz == 0 ? (long?)null : _calPanCenterHz,
                hzPerPixel = _calPanHzPerPixel > 0 ? Math.Round(_calPanHzPerPixel, 3) : (double?)null,
                pan = BuildDisplayBufferDiagnostics(_diagLastPanValid, _calPanSnapshot, panAgeMs),
                waterfall = BuildDisplayBufferDiagnostics(_diagLastWfValid, _diagWfSnapshot, wfAgeMs),
                diagnosticRecommendation = DisplayDiagnosticRecommendation(status, clients, _diagLastPanValid, _diagLastWfValid, _diagLastPanSource, _diagLastWfSource),
            };
        }
    }

    private object SnapshotSecondReceiverHealth(StateDto state)
    {
        var connectedBoard = _radio.ConnectedBoardKind;
        var effectiveBoard = _radio.EffectiveBoardKind;
        var wireBoard = connectedBoard != HpsdrBoardKind.Unknown ? connectedBoard : effectiveBoard;
        int rx1Ddc = Zeus.Protocol2.Protocol2Client.RxBaseDdc(wireBoard);
        int rx2Ddc = Zeus.Protocol2.Protocol2Client.Rx2Ddc(wireBoard);
        long[] rates = _attachedSinkP2?.SnapshotRxPortPacketRates() ?? [];

        long PacketRate(int ddc) =>
            ddc >= 0 && ddc < rates.Length ? Math.Max(0, rates[ddc]) : 0;

        long rx1PacketRate = PacketRate(rx1Ddc);
        long rx2PacketRate = PacketRate(rx2Ddc);
        bool protocol2Attached = _attachedSinkP2 is not null;
        string status = !state.Rx2Enabled
            ? "rx2-disabled"
            : !protocol2Attached
                ? "rx2-waiting-for-protocol2"
                : rx2PacketRate > 0
                    ? "rx2-streaming"
                    : "rx2-streaming-missing";

        return new
        {
            schemaVersion = 1,
            status,
            rx2Enabled = state.Rx2Enabled,
            protocol2Attached,
            rx1Ddc,
            rx2Ddc,
            rx1UdpPort = 1035 + rx1Ddc,
            rx2UdpPort = 1035 + rx2Ddc,
            displayFramesPerWindow = new
            {
                rx1Panadapter = Math.Max(0, Volatile.Read(ref _rx1PanCnt)),
                rx1Waterfall = Math.Max(0, Volatile.Read(ref _rx1WfCnt)),
                rx2Panadapter = Math.Max(0, Volatile.Read(ref _secondaryRx[1].PanCnt)),
                rx2Waterfall = Math.Max(0, Volatile.Read(ref _secondaryRx[1].WfCnt)),
            },
            iqSignal = new
            {
                rx1 = new
                {
                    rms = (double?)null,
                    peak = (double?)null,
                },
                rx2 = new
                {
                    rms = (double?)null,
                    peak = (double?)null,
                },
            },
            ddcPacketRatePerSec = new Dictionary<string, long>
            {
                [$"ddc{rx1Ddc}_port{1035 + rx1Ddc}"] = rx1PacketRate,
                [$"ddc{rx2Ddc}_port{1035 + rx2Ddc}"] = rx2PacketRate,
            },
            diagnosticRecommendation = status switch
            {
                "rx2-disabled" => "RX2 is disabled; enable RX2 before evaluating second-receiver DDC, display, or audio health.",
                "rx2-streaming" => "RX2 DDC packets are arriving; compare RX2 display and audio counters with RX1 before diagnosing frontend rendering.",
                "rx2-streaming-missing" => "RX2 is enabled but no RX2 DDC packets were observed in the latest Protocol 2 packet-rate window.",
                _ => "Protocol 2 is not attached, so RX2 DDC packet health cannot be evaluated yet.",
            },
        };
    }

    private RxMetersDiagnosticsDto SnapshotRxMetersDiagnostics()
    {
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        bool valid;
        long sampleMs;
        int channelId;
        double rxDbm;
        RxMetersV2Frame meters;
        lock (_rxMeterDiagLock)
        {
            valid = _diagRxMetersValid;
            sampleMs = _diagRxMetersMs;
            channelId = _diagRxMetersChannelId;
            rxDbm = _diagRxDbm;
            meters = _diagRxMeters;
        }

        long? ageMs = valid && sampleMs > 0 ? Math.Max(0, nowMs - sampleMs) : null;
        return BuildRxMetersDiagnostics(valid, ageMs, channelId, rxDbm, meters);
    }

    private AudioPathDiagnosticsDto SnapshotAudioDiagnostics()
    {
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        bool rxLevelerEnabled = _radio.Snapshot().RxLevelerEnabled;
        bool valid;
        long frameMs;
        uint lastSeq;
        long framesBroadcast;
        string source;
        int sampleRateHz;
        int sampleCount;
        double rms;
        double peak;
        bool txMonitorRequested;
        bool squelchEnabled;
        bool squelchOpen;
        bool squelchTailActive;
        double squelchGain;
        bool levelerValid;
        double levelerInputRmsDbfs;
        double levelerOutputRmsDbfs;
        double levelerInputPeakDbfs;
        double levelerOutputPeakDbfs;
        double levelerDesiredGainDb;
        double levelerAppliedGainDb;
        double levelerGainDeltaDb;
        double levelerPeakHeadroomDb;
        double levelerPreLimitPeakDbfs;
        double levelerOutputLimitReductionDb;
        int levelerOutputLimitSampleCount;
        int levelerPauseHoldBlocks;
        bool levelerBoostSlewLimited;
        bool levelerPeakLimited;
        bool levelerOutputLimited;
        string squelchMode;
        string squelchGateSource;
        bool squelchOpenKnown;
        long monitorBacklogSamples;
        int audioSinkCount;

        lock (_audioDiagLock)
        {
            valid = _diagAudioValid;
            frameMs = _diagAudioFrameMs;
            lastSeq = _diagAudioSeq;
            framesBroadcast = _diagAudioFrameCount;
            source = _diagAudioSource;
            sampleRateHz = _diagAudioSampleRateHz;
            sampleCount = _diagAudioSampleCount;
            rms = _diagAudioRms;
            peak = _diagAudioPeak;
            txMonitorRequested = _diagAudioTxMonitorRequested;
            squelchEnabled = _diagAudioSquelchEnabled;
            squelchOpen = _diagAudioSquelchOpen;
            squelchTailActive = _diagAudioSquelchTailActive;
            squelchGain = _diagAudioSquelchGain;
            levelerValid = _diagAudioLevelerValid;
            levelerInputRmsDbfs = _diagAudioLevelerInputRmsDbfs;
            levelerOutputRmsDbfs = _diagAudioLevelerOutputRmsDbfs;
            levelerInputPeakDbfs = _diagAudioLevelerInputPeakDbfs;
            levelerOutputPeakDbfs = _diagAudioLevelerOutputPeakDbfs;
            levelerDesiredGainDb = _diagAudioLevelerDesiredGainDb;
            levelerAppliedGainDb = _diagAudioLevelerAppliedGainDb;
            levelerGainDeltaDb = _diagAudioLevelerGainDeltaDb;
            levelerPeakHeadroomDb = _diagAudioLevelerPeakHeadroomDb;
            levelerPreLimitPeakDbfs = _diagAudioLevelerPreLimitPeakDbfs;
            levelerOutputLimitReductionDb = _diagAudioLevelerOutputLimitReductionDb;
            levelerOutputLimitSampleCount = _diagAudioLevelerOutputLimitSampleCount;
            levelerPauseHoldBlocks = _diagAudioLevelerPauseHoldBlocks;
            levelerBoostSlewLimited = _diagAudioLevelerBoostSlewLimited;
            levelerPeakLimited = _diagAudioLevelerPeakLimited;
            levelerOutputLimited = _diagAudioLevelerOutputLimited;
            squelchMode = _diagAudioSquelchMode;
            squelchGateSource = _diagAudioSquelchGateSource;
            squelchOpenKnown = _diagAudioSquelchOpenKnown;
            monitorBacklogSamples = _diagAudioMonitorBacklogSamples;
            audioSinkCount = _diagAudioSinkCount;
        }

        long? ageMs = valid && frameMs > 0 ? Math.Max(0, nowMs - frameMs) : null;
        return BuildAudioPathDiagnostics(
            valid,
            ageMs,
            lastSeq,
            framesBroadcast,
            source,
            sampleRateHz,
            sampleCount,
            rms,
            peak,
            txMonitorRequested,
            squelchEnabled,
            squelchOpen,
            squelchTailActive,
            squelchGain,
            monitorBacklogSamples,
            audioSinkCount,
            levelerValid,
            levelerInputRmsDbfs,
            levelerOutputRmsDbfs,
            levelerInputPeakDbfs,
            levelerOutputPeakDbfs,
            levelerDesiredGainDb,
            levelerAppliedGainDb,
            levelerGainDeltaDb,
            levelerPeakHeadroomDb,
            levelerPreLimitPeakDbfs,
            levelerOutputLimitReductionDb,
            levelerOutputLimitSampleCount,
            levelerPauseHoldBlocks,
            levelerBoostSlewLimited,
            levelerPeakLimited,
            levelerOutputLimited,
            squelchMode,
            squelchGateSource,
            squelchOpenKnown,
            rxLevelerEnabled);
    }

    internal static AudioPathDiagnosticsDto BuildAudioPathDiagnostics(
        bool valid,
        long? ageMs,
        uint lastSeq,
        long framesBroadcast,
        string? source,
        int sampleRateHz,
        int sampleCount,
        double rms,
        double peak,
        bool txMonitorRequested,
        bool squelchEnabled,
        bool squelchOpen,
        bool squelchTailActive,
        double squelchGain,
        long monitorBacklogSamples,
        int audioSinkCount,
        bool levelerValid = false,
        double levelerInputRmsDbfs = double.NaN,
        double levelerOutputRmsDbfs = double.NaN,
        double levelerInputPeakDbfs = double.NaN,
        double levelerOutputPeakDbfs = double.NaN,
        double levelerDesiredGainDb = double.NaN,
        double levelerAppliedGainDb = double.NaN,
        double levelerGainDeltaDb = double.NaN,
        double levelerPeakHeadroomDb = double.NaN,
        double levelerPreLimitPeakDbfs = double.NaN,
        double levelerOutputLimitReductionDb = double.NaN,
        int levelerOutputLimitSampleCount = 0,
        int levelerPauseHoldBlocks = 0,
        bool levelerBoostSlewLimited = false,
        bool levelerPeakLimited = false,
        bool levelerOutputLimited = false,
        string? squelchMode = null,
        string? squelchGateSource = null,
        bool? squelchOpenKnown = null,
        bool rxLevelerEnabled = false)
    {
        source = string.IsNullOrWhiteSpace(source) ? "none" : source;
        squelchMode = NormalizeSquelchMode(squelchMode, squelchEnabled);
        squelchGateSource = NormalizeSquelchGateSource(squelchGateSource, squelchMode);
        bool openKnown = squelchOpenKnown ?? (!squelchEnabled || string.Equals(squelchMode, "adaptive", StringComparison.OrdinalIgnoreCase));
        bool fresh = valid && ageMs is <= AudioFreshMs;
        bool stale = !valid || ageMs is null || ageMs > AudioAgingMs;
        bool clippingRisk = valid && double.IsFinite(peak) && peak >= AudioClippingRiskLinear;
        bool mutedBySquelch = valid && string.Equals(source, "rx", StringComparison.OrdinalIgnoreCase)
            && squelchEnabled && !squelchOpen;
        double? rmsDbfs = AudioLinearToDbfs(rms);
        double? peakDbfs = AudioLinearToDbfs(peak);
        bool silent = valid && (rmsDbfs is null || rmsDbfs <= AudioSilentRmsDbfs);
        bool monitorBacklog = valid && monitorBacklogSamples > Math.Max(sampleRateHz / 10, sampleCount * 3L);

        string status;
        string recommendation;
        if (!valid)
        {
            status = "missing";
            recommendation = "No RX audio frame has been published yet; connect the radio or attach an audio client before judging receive audio fidelity.";
        }
        else if (stale)
        {
            status = "stale";
            recommendation = "RX audio frames are stale; verify the DSP tick path, audio sinks, and websocket/native audio consumers before tuning weak-signal audio.";
        }
        else if (clippingRisk)
        {
            status = "clipping-risk";
            recommendation = "RX audio is approaching full scale; reduce RX leveler boost, front-end gain, or plugin output before evaluating fidelity.";
        }
        else if (string.Equals(source, "tx-monitor", StringComparison.OrdinalIgnoreCase))
        {
            status = "tx-monitor";
            recommendation = "TX monitor audio is replacing RX audio, so listen-time diagnostics are currently showing the processed transmit monitor path.";
        }
        else if (mutedBySquelch)
        {
            status = "muted-by-squelch";
            recommendation = "RX audio is fresh but gated by adaptive squelch; lower the threshold or disable squelch before using silence as weak-signal evidence.";
        }
        else if (monitorBacklog)
        {
            status = "monitor-backlog";
            recommendation = "Local playback monitor audio is queued faster than the RX bus is draining; reduce injected playback level/rate before judging band audio.";
        }
        else if (silent)
        {
            status = "silent";
            recommendation = squelchEnabled && string.Equals(squelchMode, "fixed", StringComparison.OrdinalIgnoreCase)
                ? "RX audio frames are fresh but near silence while fixed SQL is active; WDSP fixed squelch may be closed, so verify fixed threshold/sensitivity before treating silence as no-signal evidence."
                : "RX audio frames are fresh but near silence; cross-check S-meter, panadapter peaks, squelch, mode/filter, and audio sink volume.";
        }
        else
        {
            status = "fresh";
            recommendation = "RX audio frames are fresh; use RMS/peak dBFS with RXA meters, squelch state, and display SNR to tune weak-signal fidelity.";
        }

        return new AudioPathDiagnosticsDto(
            SchemaVersion: 1,
            Status: status,
            Source: source,
            Fresh: fresh,
            Stale: stale,
            AgeMs: ageMs,
            FramesBroadcast: framesBroadcast,
            LastSeq: lastSeq,
            SampleRateHz: sampleRateHz,
            SampleCount: sampleCount,
            RmsLinear: valid && double.IsFinite(rms) ? Math.Round(rms, 6) : null,
            PeakLinear: valid && double.IsFinite(peak) ? Math.Round(peak, 6) : null,
            RmsDbfs: rmsDbfs,
            PeakDbfs: peakDbfs,
            TxMonitorRequested: txMonitorRequested,
            SquelchEnabled: squelchEnabled,
            SquelchOpen: squelchOpen,
            SquelchTailActive: squelchTailActive,
            SquelchGateGain: double.IsFinite(squelchGain) ? Math.Round(Math.Clamp(squelchGain, 0.0, 1.0), 3) : null,
            RxAudioLevelerEnabled: rxLevelerEnabled,
            RxAudioLevelerInputRmsDbfs: RoundLevelerDb(levelerValid, levelerInputRmsDbfs),
            RxAudioLevelerOutputRmsDbfs: RoundLevelerDb(levelerValid, levelerOutputRmsDbfs),
            RxAudioLevelerInputPeakDbfs: RoundLevelerDb(levelerValid, levelerInputPeakDbfs),
            RxAudioLevelerOutputPeakDbfs: RoundLevelerDb(levelerValid, levelerOutputPeakDbfs),
            RxAudioLevelerDesiredGainDb: RoundLevelerDb(levelerValid, levelerDesiredGainDb),
            RxAudioLevelerAppliedGainDb: RoundLevelerDb(levelerValid, levelerAppliedGainDb),
            RxAudioLevelerGainDeltaDb: RoundLevelerDb(levelerValid, levelerGainDeltaDb),
            RxAudioLevelerPeakHeadroomDb: RoundLevelerDb(levelerValid, levelerPeakHeadroomDb),
            RxAudioLevelerPreLimitPeakDbfs: RoundLevelerDb(levelerValid, levelerPreLimitPeakDbfs),
            RxAudioLevelerOutputLimitReductionDb: RoundLevelerDb(levelerValid, levelerOutputLimitReductionDb),
            RxAudioLevelerOutputLimitSampleCount: levelerValid ? Math.Max(0, levelerOutputLimitSampleCount) : null,
            RxAudioLevelerPauseHoldBlocks: levelerValid ? Math.Max(0, levelerPauseHoldBlocks) : null,
            RxAudioLevelerBoostSlewLimited: levelerValid ? levelerBoostSlewLimited : null,
            RxAudioLevelerPeakLimited: levelerValid ? levelerPeakLimited : null,
            RxAudioLevelerOutputLimited: levelerValid ? levelerOutputLimited : null,
            SquelchMode: squelchMode,
            SquelchGateSource: squelchGateSource,
            SquelchOpenKnown: openKnown,
            MonitorBacklogSamples: monitorBacklogSamples,
            AudioSinkCount: audioSinkCount,
            DiagnosticRecommendation: recommendation);
    }

    private static double? RoundLevelerDb(bool valid, double value) =>
        valid && double.IsFinite(value) ? Math.Round(value, 1) : null;

    private static double AudioLinearToDbfsRaw(double value) =>
        double.IsFinite(value) && value > 0.0
            ? 20.0 * Math.Log10(Math.Max(value, 1e-12))
            : double.NaN;

    private static string NormalizeSquelchMode(string? mode, bool enabled)
    {
        if (!enabled) return "off";
        if (string.Equals(mode, "fixed", StringComparison.OrdinalIgnoreCase)) return "fixed";
        if (string.Equals(mode, "adaptive", StringComparison.OrdinalIgnoreCase)) return "adaptive";
        return "adaptive";
    }

    private static string NormalizeSquelchGateSource(string? source, string mode)
    {
        if (string.Equals(mode, "off", StringComparison.OrdinalIgnoreCase)) return "disabled";
        if (string.Equals(source, "wdsp-fixed", StringComparison.OrdinalIgnoreCase)) return "wdsp-fixed";
        if (string.Equals(source, "backend-adaptive", StringComparison.OrdinalIgnoreCase)) return "backend-adaptive";
        return string.Equals(mode, "fixed", StringComparison.OrdinalIgnoreCase)
            ? "wdsp-fixed"
            : "backend-adaptive";
    }

    private static double? AudioLinearToDbfs(double value) =>
        double.IsFinite(value) && value > 0.0
            ? Math.Round(AudioLinearToDbfsRaw(value), 1)
            : null;

    private void CaptureAudioDiagnostics(
        string source,
        in AudioFrame frame,
        double rms,
        double peak,
        bool txMonitorRequested,
        SquelchConfig squelch)
    {
        string squelchMode = !squelch.Enabled ? "off" : squelch.Adaptive ? "adaptive" : "fixed";
        string squelchGateSource = squelchMode switch
        {
            "adaptive" => "backend-adaptive",
            "fixed" => "wdsp-fixed",
            _ => "disabled",
        };
        bool squelchOpen = !squelch.Enabled || !squelch.Adaptive || _adaptiveSquelch.Open;
        bool squelchTailActive = IsAdaptiveSquelchTailActive(squelch, _adaptiveSquelch);
        double squelchGain = squelch.Enabled && squelch.Adaptive ? _adaptiveSquelch.Gain : 1.0;
        long monitorBacklogSamples = MonitorBacklog;
        var leveler = _rxAudioLeveler;
        bool levelerValid = string.Equals(source, "rx", StringComparison.OrdinalIgnoreCase)
            && leveler.DiagnosticsValid;

        lock (_audioDiagLock)
        {
            _diagAudioValid = true;
            _diagAudioFrameMs = (long)frame.TsUnixMs;
            _diagAudioSeq = frame.Seq;
            _diagAudioFrameCount++;
            _diagAudioSource = source;
            _diagAudioSampleRateHz = checked((int)frame.SampleRateHz);
            _diagAudioSampleCount = frame.SampleCount;
            _diagAudioRms = rms;
            _diagAudioPeak = peak;
            _diagAudioTxMonitorRequested = txMonitorRequested;
            _diagAudioSquelchEnabled = squelch.Enabled;
            _diagAudioSquelchOpen = squelchOpen;
            _diagAudioSquelchTailActive = squelchTailActive;
            _diagAudioSquelchGain = squelchGain;
            _diagAudioLevelerValid = levelerValid;
            _diagAudioLevelerInputRmsDbfs = levelerValid ? leveler.InputRmsDbfs : double.NaN;
            _diagAudioLevelerOutputRmsDbfs = levelerValid ? leveler.OutputRmsDbfs : double.NaN;
            _diagAudioLevelerInputPeakDbfs = levelerValid ? leveler.InputPeakDbfs : double.NaN;
            _diagAudioLevelerOutputPeakDbfs = levelerValid ? leveler.OutputPeakDbfs : double.NaN;
            _diagAudioLevelerDesiredGainDb = levelerValid ? leveler.DesiredGainDb : double.NaN;
            _diagAudioLevelerAppliedGainDb = levelerValid ? leveler.AppliedGainDb : double.NaN;
            _diagAudioLevelerGainDeltaDb = levelerValid ? leveler.GainDeltaDb : double.NaN;
            _diagAudioLevelerPeakHeadroomDb = levelerValid ? leveler.PeakHeadroomDb : double.NaN;
            _diagAudioLevelerPreLimitPeakDbfs = levelerValid ? leveler.PreLimitPeakDbfs : double.NaN;
            _diagAudioLevelerOutputLimitReductionDb = levelerValid ? leveler.OutputLimitReductionDb : double.NaN;
            _diagAudioLevelerOutputLimitSampleCount = levelerValid ? leveler.OutputLimitSampleCount : 0;
            _diagAudioLevelerPauseHoldBlocks = levelerValid ? leveler.PauseHoldBlocks : 0;
            _diagAudioLevelerBoostSlewLimited = levelerValid && leveler.BoostSlewLimited;
            _diagAudioLevelerPeakLimited = levelerValid && leveler.PeakLimited;
            _diagAudioLevelerOutputLimited = levelerValid && leveler.OutputLimited;
            _diagAudioSquelchMode = squelchMode;
            _diagAudioSquelchGateSource = squelchGateSource;
            _diagAudioSquelchOpenKnown = !squelch.Enabled || squelch.Adaptive;
            _diagAudioMonitorBacklogSamples = monitorBacklogSamples;
            _diagAudioSinkCount = _audioSinks.Length;
        }
    }

    private static bool IsAdaptiveSquelchTailActive(SquelchConfig cfg, AdaptiveSquelchState state)
    {
        if (!cfg.Enabled || !cfg.Adaptive || !state.Open || state.CloseHoldBlocks <= 0) return false;
        if (!double.IsFinite(state.NoiseFloorDbm) || !double.IsFinite(state.LastSignalDbm)) return false;
        double marginDb = AdaptiveSquelchMarginDb();
        double closeThreshold = state.NoiseFloorDbm + marginDb - AdaptiveSquelchCloseHysteresisDb(marginDb);
        return state.LastSignalDbm < closeThreshold;
    }

    internal static RxMetersDiagnosticsDto BuildRxMetersDiagnostics(
        bool valid,
        long? ageMs,
        int channelId,
        double rxDbm,
        RxMetersV2Frame meters)
    {
        double? signalPk = valid ? RxStageLevelDb(meters.SignalPk) : null;
        double? signalAv = valid ? RxStageLevelDb(meters.SignalAv) : null;
        double? adcPk = valid ? RxStageLevelDb(meters.AdcPk) : null;
        double? adcAv = valid ? RxStageLevelDb(meters.AdcAv) : null;
        double? agcGain = valid && double.IsFinite(meters.AgcGain) ? Math.Round(meters.AgcGain, 1) : null;
        double? agcEnvPk = valid ? RxStageLevelDb(meters.AgcEnvPk) : null;
        double? agcEnvAv = valid ? RxStageLevelDb(meters.AgcEnvAv) : null;
        double? rxDbmOut = valid && double.IsFinite(rxDbm) ? Math.Round(rxDbm, 1) : null;
        double? adcHeadroomDb = adcPk is { } pk ? Math.Round(Math.Max(0.0, -pk), 1) : null;
        bool fresh = valid && ageMs is <= RxMetersFreshMs;
        bool stale = !valid || ageMs is null || ageMs > RxMetersAgingMs;
        bool signalUsable = signalPk.HasValue || signalAv.HasValue || rxDbmOut.HasValue;
        bool adcUsable = adcPk.HasValue || adcAv.HasValue;
        bool agcEnvUsable = agcEnvPk.HasValue || agcEnvAv.HasValue;

        string status;
        string recommendation;
        if (!valid)
        {
            status = "missing";
            recommendation = "No RXA stage-meter frame has been captured yet; connect the radio and confirm IQ/audio ticks before judging RX fidelity.";
        }
        else if (stale)
        {
            status = "stale";
            recommendation = "RXA stage meters are stale; verify the DSP tick path and active websocket/radio connection before tuning weak-signal or AGC settings.";
        }
        else if (adcPk is > -3.0)
        {
            status = "adc-hot";
            recommendation = "RX ADC peak is within 3 dB of full scale; add attenuation or reduce preamp/front-end gain before increasing NR or AGC boost.";
        }
        else if (agcGain is < -20.0 && adcHeadroomDb is <= 15.0)
        {
            status = "agc-cutting";
            recommendation = "RX AGC is cutting heavily, which indicates a hot signal or overload-prone front end; restore ADC/AGC headroom before judging recovered audio.";
        }
        else if (agcGain is < -20.0)
        {
            status = "agc-normalizing";
            recommendation = "RX AGC is normalizing a strong signal while ADC headroom is clean; keep AGC-T and RF gain stable, and judge recovered audio with RX audio RMS/peak and scene SNR.";
        }
        else if (agcGain is > 35.0 && (signalPk is null || signalPk < -90.0))
        {
            status = "weak-signal-boost";
            recommendation = "RX AGC is strongly boosting a weak signal; use Smart NR and narrow filtering carefully while watching ADC headroom and coherent SNR.";
        }
        else if (!signalUsable && !adcUsable && !agcEnvUsable)
        {
            status = "sentinel";
            recommendation = "RXA meter fields are still at sentinel/bypassed values; wait for WDSP RXA meters to tick or use the fallback S-meter only as proof of audio activity.";
        }
        else
        {
            status = "fresh";
            recommendation = "RXA stage meters are fresh; use S-meter, ADC dBFS, AGC gain, and AGC envelope together when tuning weak-signal fidelity.";
        }

        return new RxMetersDiagnosticsDto(
            SchemaVersion: 1,
            Status: status,
            Source: "wdsp-rxa-meter-ring",
            Fresh: fresh,
            Stale: stale,
            AgeMs: ageMs,
            ChannelId: channelId,
            RxDbm: rxDbmOut,
            SignalPkDbm: signalPk,
            SignalAvDbm: signalAv,
            AdcPkDbfs: adcPk,
            AdcAvDbfs: adcAv,
            AdcHeadroomDb: adcHeadroomDb,
            AgcGainDb: agcGain,
            AgcEnvPkDbm: agcEnvPk,
            AgcEnvAvDbm: agcEnvAv,
            SignalUsable: signalUsable,
            AdcUsable: adcUsable,
            AgcEnvelopeUsable: agcEnvUsable,
            DiagnosticRecommendation: recommendation);
    }

    internal static RxDynamicRangeDiagnosticsDto BuildRxDynamicRangeDiagnostics(
        StateDto state,
        RxMetersDiagnosticsDto rxMeters,
        AdcProtectionStatusDto adc)
    {
        const double targetMinDb = 6.0;
        const double targetMaxDb = 30.0;
        const double weakSignalHeadroomDb = 32.0;
        const double weakSignalFloorDbm = -92.0;

        double? headroom = rxMeters.AdcHeadroomDb;
        double? adcPk = rxMeters.AdcPkDbfs;
        double? agcGain = rxMeters.AgcGainDb;
        double? signalPk = rxMeters.SignalPkDbm;
        bool fresh = rxMeters.Fresh && !rxMeters.Stale;
        bool missingMeters = string.Equals(rxMeters.Status, "missing", StringComparison.OrdinalIgnoreCase)
            || string.Equals(rxMeters.Status, "unavailable", StringComparison.OrdinalIgnoreCase);
        bool overloadRisk = state.AdcOverloadWarning || adc.Warning || headroom is <= targetMinDb || adcPk is > -targetMinDb;
        bool frontEndHot = fresh && agcGain is < -20.0 && headroom is <= 15.0;
        bool weakSignalOpportunity = fresh
            && !overloadRisk
            && headroom is >= weakSignalHeadroomDb
            && (agcGain is >= 30.0 || signalPk is null || signalPk <= weakSignalFloorDbm);
        bool frontEndUnderused = weakSignalOpportunity
            && (!state.PreampOn || adc.EffectiveDb > 0);
        bool headroomOptimal = fresh
            && !overloadRisk
            && headroom is >= targetMinDb and <= targetMaxDb
            && agcGain is > -20.0 and < 35.0;

        var reasons = new List<string>();
        var actions = new List<RxDynamicRangeActionDto>();

        if (!fresh)
        {
            reasons.Add(missingMeters ? "rx-meters-missing" : "rx-meters-stale");
            actions.Add(new RxDynamicRangeActionDto(
                "verify-rx-meter-feed",
                "Verify RXA meters",
                "required",
                "Wait for fresh RXA stage-meter frames before using dynamic-range guidance."));
        }
        else
        {
            if (overloadRisk)
            {
                reasons.Add(state.AdcOverloadWarning || adc.Warning ? "adc-overload-warning" : "adc-headroom-low");
                actions.Add(new RxDynamicRangeActionDto(
                    "add-attenuation",
                    "Add 3-6 dB attenuation",
                    state.AutoAttEnabled ? "auto-or-manual" : "manual",
                    state.AutoAttEnabled
                        ? "Auto-ATT is enabled; confirm it is raising offset quickly enough, or add manual attenuation if the ADC remains hot."
                        : "Increase S-ATT or reduce external/front-end gain before applying more AGC or NR."));
                if (state.PreampOn)
                {
                    actions.Add(new RxDynamicRangeActionDto(
                        "disable-preamp",
                        "Disable preamp",
                        "candidate",
                        "The preamp is on while ADC headroom is limited; turn it off before adding large attenuation."));
                }
            }

            if (frontEndHot)
            {
                reasons.Add("agc-cutting-with-limited-headroom");
                actions.Add(new RxDynamicRangeActionDto(
                    "restore-agc-headroom",
                    "Restore AGC headroom",
                    "candidate",
                    "AGC is cutting hard while ADC headroom is limited; lower RF gain first, then judge recovered audio."));
            }

            if (weakSignalOpportunity)
            {
                reasons.Add(frontEndUnderused ? "front-end-underused" : "weak-signal-headroom-available");
                if (adc.EffectiveDb > 0)
                {
                    actions.Add(new RxDynamicRangeActionDto(
                        "reduce-attenuation",
                        "Reduce attenuation 3-6 dB",
                        "candidate",
                        "ADC headroom is large and the signal is weak; reduce S-ATT in small steps while watching overload bits."));
                }
                if (!state.PreampOn)
                {
                    actions.Add(new RxDynamicRangeActionDto(
                        "enable-preamp",
                        "Try preamp",
                        "candidate",
                        "ADC headroom is large enough to test the hardware preamp for weak-signal lift; keep it off if band noise jumps without copy improvement."));
                }
                actions.Add(new RxDynamicRangeActionDto(
                    "hold-narrow-nr",
                    "Use narrow filter / Smart NR",
                    "active",
                    "The front end has headroom; use coherent display evidence, narrower filters, and low-artifact NR before chasing more gain."));
            }

            if (headroomOptimal)
            {
                reasons.Add("adc-headroom-in-target-window");
                actions.Add(new RxDynamicRangeActionDto(
                    "hold-current-rf-chain",
                    "Hold RF chain",
                    "active",
                    "ADC headroom is in the target window; tune copy with filters, AGC mode/top, and Smart NR rather than changing preamp or attenuation."));
            }

            if (actions.Count == 0)
            {
                reasons.Add("observe");
                actions.Add(new RxDynamicRangeActionDto(
                    "observe",
                    "Observe",
                    "standby",
                    "RX dynamic-range telemetry is fresh but does not call for an RF-chain change."));
            }
        }

        string status;
        string tone;
        string recommendation;
        if (!fresh)
        {
            status = missingMeters ? "missing" : "stale";
            tone = "verify";
            recommendation = "RX dynamic-range advisor is waiting for fresh RXA stage meters before recommending RF-chain changes.";
        }
        else if (overloadRisk)
        {
            status = "adc-headroom-limited";
            tone = "danger";
            recommendation = "ADC headroom is limited; protect the converter first with attenuation/preamp changes before increasing AGC, NR, or audio gain.";
        }
        else if (frontEndHot)
        {
            status = "front-end-hot";
            tone = "warning";
            recommendation = "AGC is cutting hard with limited converter headroom; reduce RF gain until both ADC and AGC have room.";
        }
        else if (frontEndUnderused)
        {
            status = "weak-signal-rf-chain-underused";
            tone = "ready";
            recommendation = "The weak-signal path has spare ADC headroom; try less attenuation or preamp in small steps while watching overload telemetry.";
        }
        else if (weakSignalOpportunity)
        {
            status = "weak-signal-headroom-ready";
            tone = "ready";
            recommendation = "Weak-signal evidence has adequate ADC headroom; hold RF gain and refine with filter width, AGC, and Smart NR.";
        }
        else if (headroomOptimal)
        {
            status = "dynamic-range-ready";
            tone = "ready";
            recommendation = "RX front-end dynamic range is in the target window; preserve this RF-chain state while tuning DSP.";
        }
        else
        {
            status = "watching";
            tone = "standby";
            recommendation = "RX dynamic-range telemetry is fresh; keep watching ADC headroom, AGC gain, and signal level as band conditions change.";
        }

        return new RxDynamicRangeDiagnosticsDto(
            SchemaVersion: 1,
            Status: status,
            Tone: tone,
            Fresh: fresh,
            Stale: rxMeters.Stale,
            AgeMs: rxMeters.AgeMs,
            Source: "rx-meters+radio-state+adc-protection",
            SampleRateHz: state.SampleRate,
            AttenDb: adc.AttenDb,
            AttOffsetDb: adc.OffsetDb,
            EffectiveAttenDb: adc.EffectiveDb,
            PreampOn: state.PreampOn,
            AutoAttEnabled: state.AutoAttEnabled,
            AdcProtectionEnabled: adc.Config.Enabled,
            AdcOverloadWarning: state.AdcOverloadWarning || adc.Warning,
            AdcOverloadLevel: adc.OverloadLevel,
            TargetHeadroomMinDb: targetMinDb,
            TargetHeadroomMaxDb: targetMaxDb,
            RxDbm: rxMeters.RxDbm,
            SignalPkDbm: signalPk,
            AdcPkDbfs: adcPk,
            AdcHeadroomDb: headroom,
            AgcGainDb: agcGain,
            HeadroomOptimal: headroomOptimal,
            OverloadRisk: overloadRisk,
            WeakSignalOpportunity: weakSignalOpportunity,
            FrontEndUnderused: frontEndUnderused,
            Reasons: reasons.ToArray(),
            Actions: actions.ToArray(),
            DiagnosticRecommendation: recommendation);
    }

    internal static RxListenabilityDiagnosticsDto BuildRxListenabilityDiagnostics(
        RxMetersDiagnosticsDto rxMeters,
        AudioPathDiagnosticsDto audio,
        SquelchConfig squelch)
    {
        bool rxFresh = rxMeters.Fresh && !rxMeters.Stale;
        bool audioFresh = audio.Fresh && !audio.Stale;
        bool signalPresent = rxFresh
            && (Above(rxMeters.SignalPkDbm, -120.0)
                || Above(rxMeters.SignalAvDbm, -125.0)
                || Above(rxMeters.RxDbm, -125.0));
        bool audioRecovered = audioFresh
            && string.Equals(audio.Source, "rx", StringComparison.OrdinalIgnoreCase)
            && (Above(audio.RmsDbfs, -60.0) || Above(audio.PeakDbfs, -45.0));

        string status;
        string tone;
        string blocker;
        string recommendation;

        if (!rxFresh)
        {
            status = "waiting-for-rx-meters";
            tone = "verify";
            blocker = "rx-meters";
            recommendation = "RX listenability cannot be scored until WDSP RXA meters are fresh; verify radio connection, DSP tick, and RX meter feed.";
        }
        else if (!audioFresh)
        {
            status = "waiting-for-audio";
            tone = "verify";
            blocker = "audio";
            recommendation = "RX signal evidence is available, but final audio frames are missing or stale; verify audio sinks and websocket/native audio delivery before tuning NR or AGC.";
        }
        else if (string.Equals(audio.Source, "tx-monitor", StringComparison.OrdinalIgnoreCase))
        {
            status = "tx-monitor-active";
            tone = "standby";
            blocker = "tx-monitor";
            recommendation = "TX monitor audio is replacing listen audio; disable TX monitor before using RX listenability to tune weak-signal copy.";
        }
        else if (string.Equals(audio.Status, "clipping-risk", StringComparison.OrdinalIgnoreCase))
        {
            status = "audio-clipping-risk";
            tone = "protect";
            blocker = "audio-headroom";
            recommendation = "Recovered RX audio is near full scale; reduce RX leveler/plugin output before optimizing NR, AGC, or squelch.";
        }
        else if (string.Equals(rxMeters.Status, "adc-hot", StringComparison.OrdinalIgnoreCase))
        {
            status = "adc-headroom-limited";
            tone = "protect";
            blocker = "adc-headroom";
            recommendation = "RX ADC headroom is limiting listenability; add attenuation or reduce preamp/front-end gain before increasing weak-signal processing.";
        }
        else if (string.Equals(audio.Status, "muted-by-squelch", StringComparison.OrdinalIgnoreCase))
        {
            status = "adaptive-squelch-muted";
            tone = "optimize";
            blocker = "adaptive-squelch";
            recommendation = "Backend adaptive squelch is muting fresh RX audio; lower the DYN SQL threshold or disable squelch while evaluating weak-signal copy.";
        }
        else if (signalPresent && !audioRecovered && squelch.Enabled && !squelch.Adaptive)
        {
            status = "fixed-squelch-suspect";
            tone = "optimize";
            blocker = "fixed-squelch";
            recommendation = "Signal evidence is present but recovered RX audio is silent while fixed SQL is active; lower fixed SQL level/sensitivity or disable SQL before judging NR and weak-signal fidelity.";
        }
        else if (signalPresent && !audioRecovered)
        {
            status = "signal-audio-silent";
            tone = "verify";
            blocker = "audio-path";
            recommendation = "RXA meters show signal evidence but final audio is still near silence; verify mode/filter placement, audio gain, plugins, and sink volume before changing RF/DSP settings.";
        }
        else if (signalPresent && audioRecovered)
        {
            status = "audio-recovered";
            tone = "ready";
            blocker = "none";
            recommendation = "RX signal evidence and recovered audio agree; use coherent SNR, AGC gain, and audio RMS/peak trends to fine-tune NR and filters.";
        }
        else if (audioRecovered)
        {
            status = "audio-without-meter-evidence";
            tone = "verify";
            blocker = "rx-meter-correlation";
            recommendation = "Recovered audio is present but RXA signal meters do not show clear signal evidence; cross-check S-meter calibration, filter passband, and meter freshness.";
        }
        else
        {
            status = "no-signal-evidence";
            tone = "standby";
            blocker = "none";
            recommendation = "No clear RX signal or recovered audio is present; keep weak-signal automation conservative until panadapter or RXA evidence rises above the floor.";
        }

        return new RxListenabilityDiagnosticsDto(
            SchemaVersion: 1,
            Status: status,
            Tone: tone,
            SignalPresent: signalPresent,
            AudioRecovered: audioRecovered,
            Blocker: blocker,
            Recommendation: recommendation);
    }

    private static bool Above(double? value, double threshold) =>
        value is { } v && double.IsFinite(v) && v > threshold;

    private static double? RxStageLevelDb(float value) =>
        float.IsFinite(value) && value > -199.5f
            ? Math.Round(value, 1)
            : null;

    private static string DisplayHealthStatus(
        IDspEngine? engine,
        int clientCount,
        long? frameAgeMs,
        bool panValid,
        bool wfValid)
    {
        if (engine is null) return "no-engine";
        if (engine is SyntheticDspEngine) return "synthetic-idle";
        if (clientCount <= 0) return "idle-no-clients";
        if (frameAgeMs is null) return "missing";
        if (frameAgeMs <= DisplayFreshMs && (panValid || wfValid)) return "fresh";
        if (frameAgeMs <= DisplayAgingMs) return "aging";
        return "stale";
    }

    private static string DisplayDiagnosticRecommendation(
        string status,
        int clientCount,
        bool panValid,
        bool wfValid,
        string panSource,
        string wfSource) =>
        status switch
        {
            "fresh" => $"Display analyzer frames are fresh; panadapter={panSource} valid={panValid}, waterfall={wfSource} valid={wfValid}.",
            "aging" => "Display analyzer frames are aging; watch for UI disconnects, analyzer starvation, or a paused frontend display path.",
            "stale" => "Display analyzer frames are stale; verify a Zeus client is connected and that the DSP pipeline is receiving IQ frames.",
            "idle-no-clients" => "No realtime clients are attached to the streaming hub, so the server is skipping panadapter/waterfall frame generation to save DSP work.",
            "synthetic-idle" => "The DSP engine is synthetic; connect a radio before judging panadapter or waterfall fidelity.",
            "no-engine" => "No DSP engine is active; connect or restart the DSP pipeline before judging display telemetry.",
            _ when clientCount > 0 && !panValid && !wfValid => "A realtime client is attached but no valid panadapter or waterfall frame has been captured yet; wait for the next analyzer tick or inspect WDSP readiness.",
            _ => "Display analyzer telemetry is not ready yet.",
        };

    internal static object BuildDisplayBufferDiagnostics(bool valid, ReadOnlySpan<float> samples, long? ageMs)
    {
        int validBins = 0;
        double sum = 0.0;
        float min = float.PositiveInfinity;
        float max = float.NegativeInfinity;

        if (valid)
        {
            foreach (float sample in samples)
            {
                if (!float.IsFinite(sample)) continue;
                validBins++;
                sum += sample;
                if (sample < min) min = sample;
                if (sample > max) max = sample;
            }
        }

        bool hasStats = valid && validBins > 0;
        return new
        {
            valid,
            ageMs,
            validBins,
            minDb = hasStats ? Math.Round(min, 1) : (double?)null,
            maxDb = hasStats ? Math.Round(max, 1) : (double?)null,
            meanDb = hasStats ? Math.Round(sum / validBins, 1) : (double?)null,
            dynamicRangeDb = hasStats ? Math.Round(max - min, 1) : (double?)null,
        };
    }

    private static double? FiniteOrNull(double value) =>
        double.IsFinite(value) ? Math.Round(value, 2) : null;

    private object SnapshotAdaptiveSquelchDiagnostics(SquelchConfig cfg)
    {
        var s = _adaptiveSquelch;
        double marginDb = AdaptiveSquelchMarginDb();
        double hysteresisDb = AdaptiveSquelchCloseHysteresisDb(marginDb);
        double? floorDbm = FiniteOrNull(s.NoiseFloorDbm);
        double? signalDbm = FiniteOrNull(s.LastSignalDbm);
        double? openThresholdDbm = floorDbm + marginDb;
        double? closeThresholdDbm = openThresholdDbm - hysteresisDb;
        double? deltaDb = signalDbm - floorDbm;
        bool ready = s.WindowFill >= AdaptiveSquelchMinSamples && floorDbm.HasValue;
        bool adaptiveGateActive = cfg.Enabled && cfg.Adaptive;
        bool effectiveOpen = !cfg.Enabled || !cfg.Adaptive || s.Open;
        double effectiveGain = adaptiveGateActive ? s.Gain : 1.0;
        string gateSource = !cfg.Enabled
            ? "disabled"
            : cfg.Adaptive
                ? "backend-adaptive"
                : "wdsp-fixed";
        bool tailActive = ready
            && s.Open
            && signalDbm.HasValue
            && closeThresholdDbm.HasValue
            && signalDbm.Value < closeThresholdDbm.Value
            && s.CloseHoldBlocks > 0;
        string status = !cfg.Enabled
            ? "disabled"
            : !cfg.Adaptive
                ? "fixed-mode"
                : !ready
                    ? "learning-floor"
                    : tailActive
                        ? "tail-hold"
                        : s.Open
                            ? "open"
                            : "closed";

        double blockMs = TickPeriod.TotalMilliseconds;
        double holdMs = Math.Round(s.CloseHoldBlocks * blockMs, 0);
        double configuredHoldMs = Math.Round(AdaptiveSquelchCloseHoldBlocks * blockMs, 0);
        double configuredReleaseMs = Math.Round(Math.Ceiling(1.0 / AdaptiveSquelchReleasePerBlock) * blockMs, 0);

        return new
        {
            schemaVersion = 1,
            enabled = cfg.Enabled,
            adaptive = cfg.Adaptive,
            status,
            ready,
            open = effectiveOpen,
            openKnown = !cfg.Enabled || cfg.Adaptive,
            gateSource,
            adaptiveGateOpen = s.Open,
            adaptiveGateGain = Math.Round(Math.Clamp(double.IsFinite(s.Gain) ? s.Gain : 0.0, 0.0, 1.0), 3),
            gateGain = Math.Round(Math.Clamp(double.IsFinite(effectiveGain) ? effectiveGain : 0.0, 0.0, 1.0), 3),
            signalDbm,
            noiseFloorDbm = floorDbm,
            signalOverFloorDb = deltaDb,
            openThresholdDbm = FiniteOrNull(openThresholdDbm ?? double.NaN),
            closeThresholdDbm = FiniteOrNull(closeThresholdDbm ?? double.NaN),
            marginDb,
            hysteresisDb,
            tailActive,
            closeHoldBlocks = s.CloseHoldBlocks,
            closeHoldMs = holdMs,
            configuredHoldMs,
            configuredReleaseMs,
            windowFill = s.WindowFill,
            windowSamples = AdaptiveSquelchWindowSamples,
            attackPerBlock = AdaptiveSquelchAttackPerBlock,
            releasePerBlock = AdaptiveSquelchReleasePerBlock,
            source = "rx-audio-rms",
            diagnosticRecommendation = status switch
            {
                "learning-floor" => "DYN SQL is learning the current audio noise floor.",
                "tail-hold" => "DYN SQL is holding the gate open briefly to preserve word endings.",
                "open" => "DYN SQL is open on a signal above the learned noise floor.",
                "closed" => "DYN SQL is closed; signal is below the learned open threshold.",
                "fixed-mode" => "Fixed SQL is active in WDSP; backend DYN diagnostics are learning but not gating, so fixed gate closure must be inferred from final RX audio and WDSP state.",
                _ => "SQL is disabled; DYN diagnostics are learning but not gating.",
            },
        };
    }

    private static DspNrRuntimeSnapshot BuildNrRuntime(
        IDspEngine? engine,
        StateDto state)
    {
        bool wdspActive = engine is WdspDspEngine;
        bool wdspNativeLoadable = WdspDspEngine.NativeLibraryLoadable;
        bool wdspEmnrPost2Available = WdspDspEngine.EmnrPost2Available;
        bool wdspNr3RnnrAvailable = WdspDspEngine.Nr3RnnrAvailable;
        bool wdspNr4SbnrAvailable = WdspDspEngine.Nr4SbnrAvailable;
        bool nr3ModelActive = !string.IsNullOrWhiteSpace(state.Nr3ModelName);
        var nr = NormalizeNrConfig(state.Nr ?? new NrConfig());
        string requestedNrMode = nr.NrMode.ToString();
        string effectiveNrMode = wdspActive
            ? nr.NrMode switch
            {
                NrMode.Rnnr when !wdspNr3RnnrAvailable || !nr3ModelActive => NrMode.Off.ToString(),
                NrMode.Sbnr when !wdspNr4SbnrAvailable => NrMode.Off.ToString(),
                NrMode.Nnr when !WdspDspEngine.NnrAvailable => NrMode.Off.ToString(),
                _ => requestedNrMode,
            }
            : NrMode.Off.ToString();
        return new(
            WdspActive: wdspActive,
            WdspNativeLoadable: wdspNativeLoadable,
            WdspEmnrPost2Available: wdspEmnrPost2Available,
            WdspNr4SbnrAvailable: wdspNr4SbnrAvailable,
            Nr4Readiness: wdspNr4SbnrAvailable
                ? "available"
                : wdspNativeLoadable
                    ? "missing-sbnr-exports"
                    : "wdsp-native-unloadable",
            RequestedNrMode: requestedNrMode,
            EffectiveNrMode: effectiveNrMode);
    }

    internal static NrConfig NormalizeNrConfig(NrConfig cfg) =>
        IsSupportedNrMode(cfg.NrMode) ? cfg : cfg with { NrMode = NrMode.Off };

    internal static bool IsSupportedNrMode(NrMode mode) =>
        mode is NrMode.Off or NrMode.Anr or NrMode.Emnr or NrMode.Rnnr or NrMode.Sbnr or NrMode.Nnr;

    internal static double EffectiveAgcGainDb(StateDto state) => Math.Clamp(
        RadioService.AgcBaseline(state) + state.AgcOffsetDb,
        RadioService.MinAgcFixedGainDb,
        RadioService.MaxAgcTopDb);

    internal static double EffectiveAfGainDb(double masterAfGainDb, double ownAfGainDb) =>
        Math.Clamp(
            masterAfGainDb + ownAfGainDb,
            RadioService.MinRxAfGainDb,
            RadioService.MaxRxAfGainDb);

    internal static AgcConfig EffectiveAgcConfig(AgcConfig configured, double effectiveGainDb) =>
        configured.Mode == AgcMode.Fixed
            ? configured with
            {
                FixedGainDb = Math.Clamp(
                    effectiveGainDb,
                    RadioService.MinAgcFixedGainDb,
                    RadioService.MaxAgcTopDb),
            }
            : configured;

    private static void SetP2Attenuator(Zeus.Protocol2.Protocol2Client client, int adc, int db)
    {
        if (adc == 1)
            client.SetRx1Attenuator(db);
        else
            client.SetAttenuator(db);
    }

    internal static DspRxChainDiagnosticsDto BuildRxDspChainDiagnostics(
        StateDto state,
        IReadOnlyList<NotchDto>? notches,
        DspNrRuntimeSnapshot nrRuntime,
        NrConfig? appliedNr = null,
        AgcConfig? appliedAgc = null,
        SquelchConfig? appliedSquelch = null)
    {
        var nr = NormalizeNrConfig(state.Nr ?? new NrConfig());
        var agc = EffectiveAgcConfig(
            state.Agc ?? new AgcConfig(AgcMode.Med),
            EffectiveAgcGainDb(state));
        var squelch = state.Squelch ?? new SquelchConfig();
        int notchCount = notches?.Count ?? 0;
        int activeNotchCount = notches?.Count(static n => n.Active) ?? 0;
        bool effectiveNbpRun = nr.NbpNotchesEnabled || activeNotchCount > 0;
        bool requestedNr = nr.NrMode != NrMode.Off;
        bool effectiveNr = !string.Equals(nrRuntime.EffectiveNrMode, NrMode.Off.ToString(), StringComparison.OrdinalIgnoreCase);
        bool nrCapabilityLimited = requestedNr && !effectiveNr;
        bool weakSignalAssist = effectiveNr || nr.AnfEnabled || nr.SnbEnabled;
        bool impulseControl = nr.NbMode != NbMode.Off;
        bool notchControl = effectiveNbpRun || activeNotchCount > 0;
        bool appliedNrMatches = appliedNr is null || nr.Equals(appliedNr);
        bool appliedAgcMatches = appliedAgc is null || agc.Equals(appliedAgc);
        bool appliedSquelchMatches = appliedSquelch is null || squelch.Equals(appliedSquelch);
        double effectiveAgcTopDb = Math.Round(EffectiveAgcGainDb(state), 1);

        var activeFeatures = new List<string>();
        if (effectiveNr) activeFeatures.Add($"nr-{nrRuntime.EffectiveNrMode.ToLowerInvariant()}");
        if (nr.AnfEnabled) activeFeatures.Add("anf");
        if (nr.SnbEnabled) activeFeatures.Add("snb");
        if (effectiveNbpRun) activeFeatures.Add("nbp-notches");
        if (activeNotchCount > 0) activeFeatures.Add("manual-notches");
        if (impulseControl) activeFeatures.Add(nr.NbMode.ToString().ToLowerInvariant());
        if (squelch.Enabled) activeFeatures.Add(squelch.Adaptive ? "adaptive-squelch" : "fixed-squelch");
        if (state.AutoAgcEnabled) activeFeatures.Add("auto-agc");
        if (state.AutoAttEnabled) activeFeatures.Add("auto-att");

        var reasons = new List<string>();
        reasons.Add(nrRuntime.WdspActive ? "wdsp-active" : "wdsp-inactive");
        reasons.Add(effectiveNr ? "nr-effective" : requestedNr ? "nr-requested-not-effective" : "nr-off");
        if (nrCapabilityLimited) reasons.Add("nr-capability-limited");
        if (nr.AnfEnabled) reasons.Add("anf-enabled");
        if (nr.SnbEnabled) reasons.Add("snb-enabled");
        if (effectiveNbpRun) reasons.Add("nbp-notches-running");
        if (activeNotchCount > 0) reasons.Add("manual-notches-active");
        if (impulseControl) reasons.Add("noise-blanker-enabled");
        if (squelch.Enabled) reasons.Add(squelch.Adaptive ? "adaptive-squelch-enabled" : "fixed-squelch-enabled");
        if (state.AutoAgcEnabled) reasons.Add("auto-agc-enabled");
        if (state.AgcOffsetDb != 0.0) reasons.Add("agc-offset-active");
        if (!appliedNrMatches) reasons.Add("nr-apply-pending");
        if (!appliedAgcMatches) reasons.Add("agc-apply-pending");
        if (!appliedSquelchMatches) reasons.Add("squelch-apply-pending");

        string status;
        string recommendation;
        if (!nrRuntime.WdspActive)
        {
            status = "dsp-engine-unavailable";
            recommendation = "WDSP RX processing is not active; connect or restart the DSP engine before judging NR, notch, blanker, or AGC fidelity.";
        }
        else if (nrCapabilityLimited)
        {
            status = "nr-capability-limited";
            recommendation = "The requested NR mode is not effective on the active WDSP build; use NR2/EMNR or update the bundled WDSP NR4 exports before relying on newer weak-signal cleanup modes.";
        }
        else if (!appliedNrMatches || !appliedAgcMatches || !appliedSquelchMatches)
        {
            status = "apply-pending";
            recommendation = "The requested RX DSP state has not fully matched the applied engine latch yet; wait for the next state apply before evaluating signal quality.";
        }
        else if (weakSignalAssist && impulseControl && notchControl)
        {
            status = "full-cleanup-chain-active";
            recommendation = "NR/ANF/SNB, impulse blanking, and notch control are active; tune by watching scene SNR, RX headroom, AGC gain, and display ridge stability together.";
        }
        else if (weakSignalAssist)
        {
            status = "weak-signal-assist-active";
            recommendation = "Weak-signal DSP assistance is active; verify that Smart NR scene evidence improves coherent SNR without masking speech or CW edges.";
        }
        else if (impulseControl || notchControl)
        {
            status = "interference-cleanup-active";
            recommendation = "Interference cleanup is active without NR2/NR4; use this for pulse noise or carriers, and enable Smart NR only if the scene evidence shows weak coherent signal structure.";
        }
        else if (squelch.Enabled)
        {
            status = "squelch-gated";
            recommendation = "Squelch is gating RX audio; verify the threshold before using silence as evidence that no weak signal is present.";
        }
        else
        {
            status = "baseline";
            recommendation = "RX DSP cleanup is baseline; for weak signals, use Smart NR suggestions plus targeted ANF/manual notches/NB only when scene and ADC-headroom diagnostics support it.";
        }

        return new DspRxChainDiagnosticsDto(
            SchemaVersion: 1,
            Status: status,
            Mode: state.Mode.ToString(),
            FilterLowHz: state.FilterLowHz,
            FilterHighHz: state.FilterHighHz,
            FilterPresetName: state.FilterPresetName,
            AgcMode: agc.Mode.ToString(),
            AgcTopDb: Math.Round(state.AgcTopDb, 1),
            AutoAgcEnabled: state.AutoAgcEnabled,
            RxLevelerEnabled: state.RxLevelerEnabled,
            AgcOffsetDb: Math.Round(state.AgcOffsetDb, 1),
            EffectiveAgcTopDb: effectiveAgcTopDb,
            SquelchEnabled: squelch.Enabled,
            SquelchAdaptive: squelch.Adaptive,
            SquelchLevel: squelch.Level,
            RequestedNrMode: nrRuntime.RequestedNrMode,
            EffectiveNrMode: nrRuntime.EffectiveNrMode,
            AnfEnabled: nr.AnfEnabled,
            SnbEnabled: nr.SnbEnabled,
            NbpNotchesEnabled: nr.NbpNotchesEnabled,
            EffectiveNbpNotchesRun: effectiveNbpRun,
            NbMode: nr.NbMode.ToString(),
            NbThreshold: Math.Round(nr.NbThreshold, 1),
            ManualNotchCount: notchCount,
            ActiveManualNotchCount: activeNotchCount,
            WdspActive: nrRuntime.WdspActive,
            WdspNativeLoadable: nrRuntime.WdspNativeLoadable,
            WdspEmnrPost2Available: nrRuntime.WdspEmnrPost2Available,
            WdspNr4SbnrAvailable: nrRuntime.WdspNr4SbnrAvailable,
            Nr4Readiness: nrRuntime.Nr4Readiness,
            AppliedNrMatchesRequested: appliedNrMatches,
            AppliedAgcMatchesRequested: appliedAgcMatches,
            AppliedSquelchMatchesRequested: appliedSquelchMatches,
            ActiveFeatures: activeFeatures.ToArray(),
            QualityReasons: reasons.ToArray(),
            DiagnosticRecommendation: recommendation);
    }

    /// <summary>Raised after the engine instance is swapped (Synthetic ↔ WDSP).
    /// Subscribers receive the new <see cref="IDspEngine"/> (never null).</summary>
    public event Action<IDspEngine>? EngineChanged;

    private void RaiseEngineChanged(IDspEngine engine)
    {
        try { EngineChanged?.Invoke(engine); }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "dsp.pipeline EngineChanged subscriber threw");
        }
    }

    /// <summary>Snapshot of the active Protocol2 client, or null on P1 / no
    /// connection. Exposed for the PS auto-attenuate service which needs to
    /// call <c>SetTxAttenuationDb</c> on the same client this pipeline is
    /// driving. Non-virtual — auto-attenuate is hard-gated on a P2 connection
    /// and tests don't exercise it.</summary>
    public Zeus.Protocol2.Protocol2Client? CurrentP2Client => _p2Client;

    /// <summary>Test seam: inject a caller-owned, socketless Protocol-2 client
    /// so service-level tests can exercise the normal P2 command fan-out and
    /// capture its wire packets without opening a network socket. Production
    /// composition never calls this method.</summary>
    internal void SetP2ClientForTest(Zeus.Protocol2.Protocol2Client? client) =>
        _p2Client = client;

    /// <summary>
    /// Manually set the PS TX feedback attenuation (operator alternative to
    /// AutoAttenuate). Pushes the value to the connected radio — HL2 via the
    /// AD9866 TX-PGA step, HermesC10 on P1 via the gateware atten_on_Tx
    /// register, every other board via the P2 step attenuator — then
    /// persists it per board and surfaces it in state via RadioService. This
    /// is what lets an operator on a fixed external-tap chain dial the
    /// feedback into calcc's range once and run with AutoAttenuate off.
    /// Clamped to the connected board's range.
    /// </summary>
    public void SetPsFeedbackAttenuationDb(int db)
    {
        if (_radio.ConnectedBoardKind == HpsdrBoardKind.HermesLite2)
        {
            int clamped = Math.Clamp(db, -28, 31);
            _radio.ActiveClient?.SetHl2TxStepAttenuationDb(clamped);
            _radio.SetPsTxAttenuationDb(clamped);
        }
        else if ((_radio.ConnectedBoardKind is HpsdrBoardKind.HermesC10 or HpsdrBoardKind.HermesII)
                 && _radio.ActiveClient is { } singleAdcP1)
        {
            // HermesC10 / HermesII on Protocol 1: the Hermes-family gateware
            // muxes atten_on_Tx (0..31 dB) onto the step attenuator while
            // FPGA_PTT. Carried in C3[4:0] of the PS-armed rotation's 0x1c
            // frame (board-branched in ControlFrame).
            int clamped = Math.Clamp(db, 0, 31);
            singleAdcP1.SetPsTxAttenOnTxDb(clamped);
            _radio.SetPsTxAttenuationDb(clamped);
        }
        else
        {
            int clamped = Math.Clamp(db, 0, 31);
            _p2Client?.SetTxAttenuationDb((byte)clamped);
            _radio.SetPsTxAttenuationDb(clamped);
        }
    }

    private void OpenSynthetic()
    {
        var engine = CreateDisconnectedEngine(out int channelId);
        // iter5 pass-2: _engineLock serialises CONCURRENT WRITERS. Volatile.Write
        // is used so a lock-free sink-side Volatile.Read sees the new engine
        // pointer; the lock-release fence also publishes the writes, but
        // explicit Volatile.Write documents intent and survives any future
        // refactor that drops the outer lock.
        lock (_engineLock)
        {
            Volatile.Write(ref _engine, engine);
            Volatile.Write(ref _channelId, channelId);
            ResetSecondaryRxChannels();
            Volatile.Write(ref _sampleRateHz, SyntheticSampleRateHz);
        }
        _log.LogInformation("dsp.pipeline engine={Engine} channel={Id}", engine.GetType().Name, channelId);
        RaiseEngineChanged(engine);
    }

    private IDspEngine CreateDisconnectedEngine(out int channelId)
    {
        OfflinePreviewDspEngine? preview = null;
        try
        {
            preview = new OfflinePreviewDspEngine(_loggerFactory.CreateLogger<WdspDspEngine>(), _rxAnalyzerFftSize);
            channelId = preview.OpenChannel(SyntheticSampleRateHz, _panadapterWidth);
            SeedTxDisplayConfig(preview);
            preview.OpenTxChannel(outputRateHz: OfflinePreviewTxOutputRateHz);
            try
            {
                ApplyStateToNewChannel(preview, channelId);
            }
            catch (EntryPointNotFoundException ex)
            {
                _log.LogWarning(ex, "dsp.pipeline offline-preview wdsp missing entry point - partial config applied");
            }
            return preview;
        }
        catch (Exception ex)
        {
            try { preview?.Dispose(); } catch { }
            _log.LogWarning(ex, "dsp.pipeline offline-preview wdsp open failed, falling back to synthetic engine");
            var synth = new SyntheticDspEngine();
            channelId = synth.OpenChannel(SyntheticSampleRateHz, _panadapterWidth);
            ApplyStateToNewChannel(synth, channelId);
            return synth;
        }
    }

    private void OnRadioConnected(IProtocol1Client client)
    {
        var state = _radio.Snapshot();
        int rate = state.SampleRate;

        var wdsp = new WdspDspEngine(_loggerFactory.CreateLogger<WdspDspEngine>(), _rxAnalyzerFftSize);
        int channelId = wdsp.OpenChannel(rate, _panadapterWidth);
        // Seed the operator's persisted TX display config before TXA opens so
        // the analyzer comes up at their FFT/window/smoothing. Display-only.
        SeedTxDisplayConfig(wdsp);
        // Hermes-family single-ADC P1 boards feed PS back at the WIRE rate:
        // G2E (HermesC10) via its 4-DDC EP6 stream, ANAN-10E (HermesII) via
        // its 2-DDC EP6 stream. All P1 DDCs share the single global rate, not
        // the fixed 192 kHz of the P2 paired-DDC scheme. Tell WDSP the truth
        // BEFORE TXA opens (SetPSFeedbackRate latches at open) or calcc's
        // delay/sample math runs 4x off at 48 kHz and the fit never
        // converges. piHPSDR model (receiver.c:1590-1596). P1 rate changes
        // rebuild the engine through this same path, so the value tracks.
        // HL2 keeps the shipped 192 kHz default untouched.
        ApplyP1PsFeedbackRateOverride(_radio.ConnectedBoardKind, rate, wdsp.SetPsFeedbackRateHz);
        // P1 DAC runs at 48 kHz; keep TXA at the 48/48/48 profile Hermes is
        // calibrated against.
        wdsp.OpenTxChannel(outputRateHz: 48_000);
        ApplyStateToNewChannel(wdsp, channelId);

        IDspEngine? old;
        int oldChannel;
        lock (_engineLock)
        {
            old = _engine;
            oldChannel = _channelId;
            Volatile.Write(ref _engine, wdsp);
            Volatile.Write(ref _channelId, channelId);
            ResetSecondaryRxChannels();
            Volatile.Write(ref _sampleRateHz, rate);
        }

        TeardownEngine(old, oldChannel);
        _log.LogInformation("dsp.pipeline engine=wdsp channel={Id} rate={Rate}", channelId, rate);
        RaiseEngineChanged(wdsp);

        // iter5: attach as the synchronous RX sink. Protocol1Client.RxLoop
        // calls OnIqFrame / OnPsFeedbackFrame directly on its OS thread —
        // no Channel<T> hop, no Task.Run pump, no _engineLock acquisition
        // on the hot path. The Tick is piggybacked on OnIqFrame via a
        // Stopwatch.GetTimestamp() check.
        AttachRxSinkP1(client);
        // Force the next OnRadioStateChanged to re-push every PS field into
        // the freshly-opened WdspDspEngine instance — same rationale as the
        // P2 reconnect path. Without this, a P1 reconnect leaves the engine
        // sitting at field defaults (hwPeak=0.4072) and calcc never sees
        // the operator's HL2 0.233 / hardware-correct numbers.
        _psResyncRequired = true;
        _appliedTxMonitorEnabled = false;
        // Apply the per-board PS HW peak default so the engine sees the
        // right curve scale before the operator arms PS. Mirrors P2's
        // ApplyPsHwPeakForConnection call. ConnectedBoardKind returns the
        // currently-active board (HL2, Hermes, ANAN-class…) — the value
        // is per-board (HL2 → 0.233, others → 0.4072) and only fires a
        // StateChanged when the value actually changes.
        _radio.ApplyPsHwPeakForConnection(isProtocol2: false, _radio.ConnectedBoardKind);
        // Restore the persisted PS feedback attenuation so a hot external-tap
        // chain isn't sitting at 0 dB on a fresh connect — at 0 dB the
        // feedback ADC rails and calcc can never fit. On the P1 side HL2 owns
        // the AD9866 TX-PGA step attenuator and HermesC10 owns the gateware
        // atten_on_Tx register (0x1c C3[4:0], PTT-muxed); other P1 boards
        // have no PS feedback attenuator. No-op when nothing was saved for
        // this board yet — HermesC10 then keeps emitting the silicon reset
        // default 31 via the sentinel path (never a force-seeded value).
        if (_radio.ConnectedBoardKind == HpsdrBoardKind.HermesLite2
            && _radio.GetPersistedPsTxAttnDb() is int hl2Attn)
        {
            _radio.ActiveClient?.SetHl2TxStepAttenuationDb(hl2Attn);
        }
        else if ((_radio.ConnectedBoardKind is HpsdrBoardKind.HermesC10 or HpsdrBoardKind.HermesII)
            && _radio.GetPersistedPsTxAttnDb() is int c10Attn)
        {
            _radio.ActiveClient?.SetPsTxAttenOnTxDb(c10Attn);
        }
        // P1's Connected event is raised after RadioService already broadcast
        // Status=Connected, so the first state callback can hit the synthetic
        // engine we are replacing here. Replay the canonical live state so the
        // disarmed runtime state reaches the freshly-opened WDSP engine even
        // when ApplyPsHwPeakForConnection did not move any StateDto fields.
        // This replay is the sanitize cycle's off half; only after calibration
        // and attenuation restores are complete may persisted intent re-arm.
        OnRadioStateChanged(_radio.Snapshot());
        _radio.ApplyPsArmIntentForConnection(
            isProtocol2: false,
            _radio.ConnectedBoardKind);
    }

    private void OnRadioDisconnected()
    {
        // iter5: detach the synchronous RX sink. Protocol1Client's RxLoop
        // thread is wound down by the protocol client itself (during
        // TearDownClientAsync) — we just clear the sink reference and let
        // the timer-driven Tick take over for synthetic-mode display.
        DetachRxSinkP1();

        var disconnectedEngine = CreateDisconnectedEngine(out int channelId);

        IDspEngine? old;
        int oldChannel;
        lock (_engineLock)
        {
            old = _engine;
            oldChannel = _channelId;
            Volatile.Write(ref _engine, disconnectedEngine);
            Volatile.Write(ref _channelId, channelId);
            ResetSecondaryRxChannels();
            Volatile.Write(ref _sampleRateHz, SyntheticSampleRateHz);
        }

        TeardownEngine(old, oldChannel);
        _appliedTxMonitorEnabled = false;
        _log.LogInformation("dsp.pipeline engine={Engine} channel={Id}", disconnectedEngine.GetType().Name, channelId);
        RaiseEngineChanged(disconnectedEngine);
        OnRadioStateChanged(_radio.Snapshot());
    }

    // FreeDV is a linear digital mode: the OFDM waveform must pass the TX chain
    // undistorted and the decoder must see un-pumped RX audio. When Mode==FreeDv
    // the apply-loop substitutes these spec profiles for the operator's stored
    // values at the ENGINE seam only — the stores/StateDto keep the operator's
    // real SSB settings, so leaving FreeDV restores the SSB chain automatically
    // (the latches re-push the stored values once the effective config flips
    // back). RX AGC goes Fixed (seeded from the operator's AGC-T baseline) so AGC
    // pumping/clipping can't corrupt the modem audio fed to freedv_rx.
    //
    // TX gain staging is the subtle part. The codec2 modem audio is LOW level
    // (~-20 dBFS peak), so with every TX dynamics stage bypassed the SSB
    // modulator is barely driven (measured 0.18 W). The Compressor (CPDR) and CFC
    // are NONLINEAR — they flatten OFDM and splatter, so they stay off. But the
    // Leveler is a *slow* auto-level: on a steady-RMS OFDM signal it converges to
    // a near-constant gain, so it supplies the makeup linearly without destroying
    // the ~10 dB crest factor. We keep it ON with a bounded makeup ceiling
    // (FreeDvLevelerMaxGainDb) chosen to bring the OFDM PEAKS to a safe headroom
    // (~-6 dBFS) rather than slamming the average to 0 (which would clip the peaks
    // and produce the high-power-but-undecodable signal the old SSB chain made).
    // ALC run-state is never touched (the engine keeps ALC on — the SSB modulator
    // emits zero IQ with ALC off) so it still catches stray peaks. Bench-tunable:
    // raise/lower FreeDvLevelerMaxGainDb to trade average power against crest
    // (watch outputCrestFactorDb in /api/diagnostics/v2/tx — keep it ~>=8 dB).
    private static readonly TxLevelingConfig FreeDvTxLevelingProfile =
        new(LevelerEnabled: true, CompressorEnabled: false);
    // Leveler makeup ceiling for FreeDV. ~-20 dBFS modem peak + this should land
    // OFDM peaks near -6 dBFS — clean, decodable, moderate (not full-SSB) power.
    private const double FreeDvLevelerMaxGainDb = 14.0;

    private void OnRadioStateChanged(StateDto s)
    {
        lock (_engineLock)
        {
        // A queued pre-key snapshot must not retune the TX DUC or restore an
        // older sideband while a fixed-channel audio lease owns transmission.
        s = _radio.ReconcileProductPluginTxState(s);
        // FreeDV spec-profile override (see the AGC/TX-leveling pushes below):
        // gates those engine pushes to linear-friendly values while the operator's
        // stored config stays untouched for automatic restore on exit.
        var txReceiver = RadioFrequencyResolver.TxReceiver(s);
        var txEngineMode = RadioService.EffectiveEngineMode(
            txReceiver.Mode, RadioFrequencyResolver.TxFrequencyHz(s));
        bool rxFreeDvMode = s.Mode == RxMode.FreeDv;
        bool txFreeDvMode = txReceiver.Mode == RxMode.FreeDv;
        var effectiveTxAudio = ResolveEffectiveTxAudioProfile(s, txEngineMode);
        bool txDigitalBypass = IsDigitalTxZeusMode(txReceiver.Mode);
        // Forward VFO changes to the P2 client when it's active. RadioService
        // does this for P1 via ActiveClient?.SetVfoAHz() inside SetVfo, but
        // ActiveClient is null for P2 connections, so the radio never learns
        // about tune changes without this forward. Sample rate / mode follow
        // here too when P2-side support is added.
        //
        // Frozen-NCO model: the hardware always sits at RadioLoHz; dial
        // movements stay confined to the WDSP filter-shift path. Push
        // RadioLoHz to the P2 client (the P1 client gets the same push from
        // RadioService.SetRadioLo). See docs/prd/panfall_behavior.md.
        var p2 = _p2Client;
        p2?.SetVfoAHz(_radio.ToHardwareFrequencyHz(s.RadioLoHz));
        p2?.SetReceiverAdcSources(ReceiverAdcSource(s, 0), ReceiverAdcSource(s, 1));
        var diversity = s.Diversity ?? new DiversityConfig();
        bool diversityEnabled = diversity.Enabled && BoardCapabilitiesTable
            .For(_radio.EffectiveBoardKind, _radio.EffectiveOrionMkIIVariant).RxAdcCount > 1;
        byte diversitySourceAdc = BoardCapabilitiesTable
            .For(_radio.EffectiveBoardKind, _radio.EffectiveOrionMkIIVariant)
            .RxAdcCount > 1
                ? (byte)1
                : ReceiverAdcSource(s, 1);
        p2?.SetDiversitySourceEnabled(diversityEnabled, diversitySourceAdc);
        // RX2 (true second receiver): enable/disable its DDC and tune its NCO to
        // VFO B's effective LO so it demodulates its own band, independent of
        // RX1. SetRx2Enabled is idempotent (only re-sends on a real change);
        // SetVfoBHz no-ops on the wire while RX2 is disabled.
        p2?.SetRx2Enabled(s.Rx2Enabled);
        // Tune RX2's DDC to its (CTUN-frozen) centre, not the raw dial, so the
        // panel holds still while VFO B roams under CTUN. The WDSP shift in
        // ApplyStateToRx2Channel moves the dial within that window.
        UpdateRxLo(1, s);
        p2?.SetVfoBHz(_radio.ToHardwareFrequencyHz(
            _secondaryRx[1].LoHz));

        // Protocol 2 has independent RX DDC and TX DUC NCOs. Always program the
        // TX DUC from the selected TX dial while leaving RX0/DDC2 on RadioLoHz.
        // This is Thetis display-DUP routing: keying never drags the receiver's
        // capture centre away and the waterfall keeps one continuous frequency
        // frame through CTUN, XIT, split, and PureSignal overs.
        p2?.SetTxDucFrequency(
            _radio.ToHardwareFrequencyHz(RadioService.TxEffectiveLoHz(s)));

        // Issue #597 Phase 0: arm the RX display fast-attack when the LO
        // moves. First callback after construction only records the LO
        // (sentinel) so connect itself doesn't trigger a pointless arm.
        if (_fastAttackLastLoHz == long.MinValue)
        {
            _fastAttackLastLoHz = s.RadioLoHz;
        }
        else if (s.RadioLoHz != _fastAttackLastLoHz)
        {
            _fastAttackLastLoHz = s.RadioLoHz;
            long nowTicks = Stopwatch.GetTimestamp();
            // Issue #597 Phase 2: the LO history feeding the delay-compensated
            // CenterHz stamp. O(1) append, LO changes only.
            _loHistory.Append(nowTicks, s.RadioLoHz);
            Interlocked.Exchange(ref _fastAttackLoChangedAt, nowTicks);
            if (!_keyed && !_fastAttackActive)
            {
                _engine?.SetRxDisplayFastAttack(_channelId, fast: true);
                _fastAttackActive = true;
            }
        }

        // Realtime VFO push (RX0): the moment the dial or display centre moves —
        // from ANY source, including the front-panel encoder whose tune never
        // touches the web UI — tell the frontend over the socket so it can move
        // the dial readout and glide the pan without waiting for the 1 Hz state
        // poll and the display-frame echo. Edge-gated to real moves; the first
        // callback after construction only records the sentinel. Both dial and
        // centre travel so the frontend can place the pan (RadioLoHz, CW/CTUN-
        // correct) and the readout (VfoHz) each correctly.
        if (_lastPushedVfoHz == long.MinValue)
        {
            _lastPushedVfoHz = s.VfoHz;
            _lastPushedRadioLoHz = s.RadioLoHz;
        }
        else if (s.VfoHz != _lastPushedVfoHz || s.RadioLoHz != _lastPushedRadioLoHz)
        {
            _lastPushedVfoHz = s.VfoHz;
            _lastPushedRadioLoHz = s.RadioLoHz;
            _hub.Broadcast(new VfoStateFrame(Receiver: 0, VfoHz: s.VfoHz, RadioLoHz: s.RadioLoHz));
        }

        // Same push for RX2 / VFO B when it is enabled: its dial is
        // Receivers[1].VfoHz, its display centre the CTUN-frozen DDC centre
        // _secondaryRx[1].LoHz (kept current by UpdateRxLo(1, s) above).
        if (SecondaryReceiverEnabled(1, s))
        {
            long rx2Vfo = SecondaryRxParams(s, 1).vfoHz;
            long rx2Lo = _secondaryRx[1].LoHz;
            if (_lastPushedRx2VfoHz == long.MinValue)
            {
                _lastPushedRx2VfoHz = rx2Vfo;
                _lastPushedRx2LoHz = rx2Lo;
            }
            else if (rx2Vfo != _lastPushedRx2VfoHz || rx2Lo != _lastPushedRx2LoHz)
            {
                _lastPushedRx2VfoHz = rx2Vfo;
                _lastPushedRx2LoHz = rx2Lo;
                _hub.Broadcast(new VfoStateFrame(Receiver: 1, VfoHz: rx2Vfo, RadioLoHz: rx2Lo));
            }
        }
        else if (_lastPushedRx2VfoHz != long.MinValue)
        {
            _lastPushedRx2VfoHz = long.MinValue;
            _lastPushedRx2LoHz = long.MinValue;
        }

        var engine = _engine;
        int channel = _channelId;
        if (engine is null) return;
        int rx2Channel = EnsureSecondaryRxChannel(engine, 1, s);

        // RX3+ (full multi-DDC): count the contiguous enabled receivers beyond
        // RX2 from the canonical Receivers[] array, push their DDC enable + NCO
        // tunes to the radio (P2 only), then ensure each WDSP channel. The
        // RX1/RX2 wire path above is unchanged; extras never touch the PureSignal
        // DDC0/1 pair (SetExtraReceivers uses the N-receiver composer which keeps
        // the PS branch intact).
        int extraCount = 0;
        if (s.Receivers is { } rcvrs)
            for (int ri = 2; ri < rcvrs.Count && ri < MaxReceivers && rcvrs[ri].Enabled; ri++)
                extraCount++;
        if (p2 is not null)
        {
            if (extraCount > 0)
            {
                var adc = new byte[extraCount];
                for (int k = 0; k < extraCount; k++) adc[k] = s.Receivers![2 + k].AdcSource;
                p2.SetExtraReceivers(extraCount, adc);
                for (int ri = 2; ri <= 1 + extraCount; ri++)
                {
                    UpdateRxLo(ri, s);
                    p2.SetExtraReceiverFreqHz(
                        ri,
                        _radio.ToHardwareFrequencyHz(_secondaryRx[ri].LoHz));
                }
            }
            else
            {
                p2.SetExtraReceivers(0);
            }
        }
        for (int ri = 2; ri < MaxReceivers; ri++)
            _ = EnsureSecondaryRxChannel(engine, ri, s);

        if (p2 is not null)
        {
            int normalizedZoom = NormalizeWidebandZoomRequest(s.ZoomLevel);
            if (normalizedZoom != s.ZoomLevel)
            {
                // The receiver/layout edge consumed the last spare DDC. Re-enter
                // once with the honest raw ceiling; the nested snapshot contains
                // every mutation from this state edge, so returning here loses no
                // control update.
                _radio.SetZoom(normalizedZoom);
                return;
            }
        }
        ReconcileWidebandDetailSource(engine, s);

        // FreeDV has no WDSP sideband of its own — resolve the effective demod/mod
        // orientation from the dial (LSB < 10 MHz, USB ≥). For every other mode
        // this equals s.Mode, so the change-detection and pushes are byte-identical
        // to before. Tracked via _appliedEngineMode so a dial crossing 10 MHz while
        // staying in FreeDv re-flips the sideband. See RadioService.EffectiveEngineMode.
        var engineMode = RadioService.EffectiveEngineMode(s.Mode, s.VfoHz);
        if (s.Mode != _appliedMode || engineMode != _appliedEngineMode)
        {
            engine.SetMode(channel, engineMode);
            _appliedMode = s.Mode;
            _appliedEngineMode = engineMode;
        }
        if (txReceiver.Mode != _appliedTxMode || txEngineMode != _appliedTxEngineMode)
        {
            engine.SetTxMode(txEngineMode);
            engine.SetTxDigitalBypass(txDigitalBypass);
            _appliedTxMode = txReceiver.Mode;
            _appliedTxEngineMode = txEngineMode;
        }
        // FreeDV's stored bandpass is USB-positive; re-sign it for the effective
        // sideband so an LSB (sub-10 MHz) FreeDV demod gets a negative-frequency
        // passband instead of a dead positive one. Non-FreeDv modes carry their
        // already-signed width through unchanged.
        var (rxLowHz, rxHighHz) = SignedRxFilterFor(s, engineMode);
        if (rxLowHz != _appliedLowHz || rxHighHz != _appliedHighHz)
        {
            engine.SetFilter(channel, rxLowHz, rxHighHz);
            _appliedLowHz = rxLowHz;
            _appliedHighHz = rxHighHz;
        }
        // Frozen-NCO frequency shift. The dial sits off-centre on the WDSP
        // IF (the radio's NCO is frozen at RadioLoHz); WDSP's `shift` stage
        // moves the IF by shiftHz before demodulation so the unmodified
        // bandpass filter sees the tuned signal at baseband. This is the
        // seam Thetis uses (radio.cs:1419-1420); shifting SetRXABandpassFreqs
        // directly broke SSB demod because the nbp0 stage rejects
        // sign-inverted ranges. See docs/prd/panfall_behavior.md.
        // RIT (Receiver Incremental Tuning) folds straight into the shift: the
        // demod point moves by RitHz while VfoHz (the displayed dial) stays put.
        // RX1 only, matching Thetis (RIT acts on the active receiver). A change
        // to RitHz/RitEnabled changes ctunShiftHz, so it re-applies through the
        // same diff-gate as a retune.
        int ritHz = s.RitEnabled ? (int)s.RitHz : 0;
        int ctunShiftHz = (int)(CwOffset.EffectiveLoHz(s.Mode, s.VfoHz) - s.RadioLoHz) + ritHz;
        if (ctunShiftHz != _appliedCtunOffsetHz)
        {
            engine.SetCtunShift(channel, ctunShiftHz);
            _appliedCtunOffsetHz = ctunShiftHz;
        }
        // Keep WDSP's manual-notch database positioned against the live LO so
        // notches hold their absolute RF frequency across a retune. The engine
        // no-ops when the value is unchanged, so this is cheap to call here.
        engine.SetNotchTuneFrequencyHz(s.RadioLoHz);
        // Re-sign the TX bandpass from the LIVE mode instead of trusting the
        // sign stored in StateDto. WDSP picks the SSB sideband from the sign of
        // the bandpass edges; a state that comes up with Mode=LSB but a positive
        // TX filter (legacy prefs DB, or a writer that set the mode without
        // re-signing the TX width) would otherwise transmit USB. This is
        // idempotent for well-formed state, so it never fights an operator edit.
        var (txLow, txHigh) = SignedTxFilterFor(s, txEngineMode);
        if (txLow != _appliedTxLowHz || txHigh != _appliedTxHighHz)
        {
            engine.SetTxFilter(txLow, txHigh);
            _appliedTxLowHz = txLow;
            _appliedTxHighHz = txHigh;
        }
        if (s.RxFilterWindow != _appliedRxBandpassWindow)
        {
            engine.SetRxBandpassWindow(channel, s.RxFilterWindow);
            if (rx2Channel >= 0) engine.SetRxBandpassWindow(rx2Channel, s.RxFilterWindow);
            _appliedRxBandpassWindow = s.RxFilterWindow;
        }
        if (s.TxFilterWindow != _appliedTxBandpassWindow)
        {
            engine.SetTxBandpassWindow(s.TxFilterWindow);
            _appliedTxBandpassWindow = s.TxFilterWindow;
        }
        if (s.RxFilterPhase != _appliedRxFilterPhase)
        {
            engine.SetRxFilterPhase(channel, s.RxFilterPhase);
            if (rx2Channel >= 0) engine.SetRxFilterPhase(rx2Channel, s.RxFilterPhase);
            _appliedRxFilterPhase = s.RxFilterPhase;
        }
        if (s.TxFilterPhase != _appliedTxFilterPhase)
        {
            engine.SetTxFilterPhase(s.TxFilterPhase);
            _appliedTxFilterPhase = s.TxFilterPhase;
        }
        // AGC-T: rate-cap the effective ceiling pushed to WDSP. wcpAGC's
        // SetRXAAGCTop swaps max_gain instantly and recomputes min_volts /
        // slope_constant without resetting the running envelope (a->volts),
        // so a stair-step jump on a slider drag = click train. Slewing
        // _appliedAgcCeilingDb toward target by AgcTopSlewMaxDbPerTick gives
        // a smooth ceiling; the secondary RX block fans the same slewed dB
        // to every active secondary so RX2..N see one consistent ceiling
        // this tick (and don't double-step against the main-block push).
        double effectiveAgcTarget = EffectiveAgcGainDb(s);
        if (effectiveAgcTarget != _appliedAgcCeilingDb)
        {
            _appliedAgcCeilingDb = StepTowardCappedDb(
                _appliedAgcCeilingDb, effectiveAgcTarget, AgcTopSlewMaxDbPerTick);
            engine.SetAgcTop(channel, _appliedAgcCeilingDb);
            if (rx2Channel >= 0)
            {
                engine.SetAgcTop(rx2Channel, _appliedAgcCeilingDb);
                _secondaryRx[1].AppliedAgcTopDb = _appliedAgcCeilingDb;
            }
            for (int ri = 2; ri < MaxReceivers; ri++)
            {
                int sec = Volatile.Read(ref _secondaryRx[ri].ChannelId);
                if (sec >= 0)
                {
                    engine.SetAgcTop(sec, _appliedAgcCeilingDb);
                    _secondaryRx[ri].AppliedAgcTopDb = _appliedAgcCeilingDb;
                }
            }
        }
        // (Removed: the manual AGC "knee" push. WDSP's threshold and AGC-T are
        // the SAME register (max_gain) — driving both independently clobbered
        // each other and made AGC-T hair-trigger. AGC-T is now the single
        // manual control via SetRXAAGCTop above; Auto-AGC tracks the noise floor
        // on top of it. See the AGC knee removal commit.)
        // RX1 AF gain: same rate-cap rationale — WDSP's SetRXAPanelGain1 is a
        // one-block constant multiply, so a slider drag stair-steps into a
        // click train. Secondary RX AF gain slews per-receiver inside
        // ApplyStateToSecondaryRxChannel (each receiver has its own slider).
        //
        // FreeDV: WDSP's panel gain runs on the received OFDM modem audio, which
        // ProcessRx then discards when it replaces the block in place with
        // decoded speech (the vocoder output level is fixed by the codec, not the
        // input amplitude) — so the WDSP panel gain can't set the listening
        // volume. Drive the WDSP panel to unity in FreeDV so the decoder always
        // sees a consistent-level feed (a low AF setting can't starve sync), and
        // re-apply the operator's AF on the decoded speech in the audio tick
        // (ApplyFreeDvAfGain / _freeDvAfGainLinear). On exit the latch re-slews
        // the WDSP panel back to s.RxAfGainDb automatically.
        double afTargetDb = rxFreeDvMode
            ? 0.0
            : EffectiveAfGainDb(s.RxAfGainDb, s.Rx1AfGainDb);
        if (afTargetDb != _appliedRxAfGainDb)
        {
            _appliedRxAfGainDb = StepTowardCappedDb(
                _appliedRxAfGainDb, afTargetDb, AfGainSlewMaxDbPerTick);
            engine.SetRxAfGainDb(channel, _appliedRxAfGainDb);
        }
        // TX mic gain: dB → linear (10^(db/20)) at the engine seam. Conversion
        // matches the historical /api/mic-gain inline (Math.Pow(10.0, db/20.0));
        // moved here so the operator-friendly dB is what gets stored and broadcast.
        int effectiveMicGainDb = effectiveTxAudio.MicGainDb;
        double micLinear = Math.Pow(10.0, effectiveMicGainDb / 20.0);
        if (micLinear != _appliedTxMicGainLinear)
        {
            engine.SetTxPanelGain(micLinear);
            _appliedTxMicGainLinear = micLinear;
        }
        // FreeDV: raise the Leveler makeup ceiling so the low-level OFDM is driven
        // to a usable (but headroom-safe) level. Operator's value restored on exit.
        double levelerMax = txFreeDvMode
            ? FreeDvLevelerMaxGainDb
            : effectiveTxAudio.LevelerMaxGainDb;
        if (levelerMax != _appliedTxLevelerMaxGainDb)
        {
            engine.SetTxLevelerMaxGain(levelerMax);
            _appliedTxLevelerMaxGainDb = levelerMax;
        }
        var nr = NormalizeNrConfig(s.Nr ?? new NrConfig());
        if (!nr.Equals(_appliedNr))
        {
            engine.SetNoiseReduction(channel, nr);
            if (rx2Channel >= 0) engine.SetNoiseReduction(rx2Channel, nr);
            _appliedNr = nr;
        }
        // Diversity combiner — managed complex combine in the P2 ingest (see
        // ApplyDiversityConfig / OnIqFrame). Applied once (not per-channel) when
        // the config changes. Default-off makes the ingest byte-identical.
        if (!diversity.Equals(_appliedDiversity))
        {
            ApplyDiversityConfig(diversity);
            _appliedDiversity = diversity;
        }
        var agc = rxFreeDvMode
            ? new AgcConfig(AgcMode.Fixed, FixedGainDb: RadioService.AgcBaseline(s))
            : EffectiveAgcConfig(
                s.Agc ?? new AgcConfig(AgcMode.Med),
                _appliedAgcCeilingDb);
        if (!agc.Equals(_appliedAgc))
        {
            engine.SetAgc(channel, agc);
            if (rx2Channel >= 0) engine.SetAgc(rx2Channel, agc);
            _appliedAgc = agc;
        }
        var squelch = s.Squelch ?? new SquelchConfig();
        if (!squelch.Equals(_appliedSquelch))
        {
            engine.SetSquelch(channel, squelch);
            if (rx2Channel >= 0) engine.SetSquelch(rx2Channel, squelch);
            _appliedSquelch = squelch;
        }
        var txLeveling = txFreeDvMode
            ? FreeDvTxLevelingProfile
            : effectiveTxAudio.TxLeveling;
        if (!txLeveling.Equals(_appliedTxLeveling))
        {
            engine.SetTxLeveling(channel, txLeveling);
            _appliedTxLeveling = txLeveling;
        }
        var txPhaseRotator = effectiveTxAudio.TxPhaseRotator;
        if (!txPhaseRotator.Equals(_appliedTxPhaseRotator))
        {
            engine.SetTxPhaseRotator(channel, txPhaseRotator);
            _appliedTxPhaseRotator = txPhaseRotator;
        }
        double amCarrierLevel = effectiveTxAudio.CarrierCoefficient;
        if (amCarrierLevel != _appliedTxAmCarrierLevel)
        {
            engine.SetTxAmCarrierLevel(amCarrierLevel);
            _appliedTxAmCarrierLevel = amCarrierLevel;
        }
        ApplyVisibleDdcZoom(engine, s);

        // ---- TwoTone (protocol-agnostic; PostGen mode=1 inside TXA) ----
        // TwoTone is safe on P1 even though PS itself is P2-only in v1
        // because it touches only the TXA stage, not the wire format.
        if (s.TwoToneEnabled != _appliedTwoToneEnabled
            || s.TwoToneFreq1 != _appliedTwoToneFreq1
            || s.TwoToneFreq2 != _appliedTwoToneFreq2
            || s.TwoToneMag != _appliedTwoToneMag)
        {
            engine.SetTwoTone(s.TwoToneEnabled, s.TwoToneFreq1, s.TwoToneFreq2, s.TwoToneMag);
            _appliedTwoToneEnabled = s.TwoToneEnabled;
            _appliedTwoToneFreq1 = s.TwoToneFreq1;
            _appliedTwoToneFreq2 = s.TwoToneFreq2;
            _appliedTwoToneMag = s.TwoToneMag;
        }

        // ---- PureSignal ----
        // Apply HW-peak first because SetPsAdvanced may also touch it; then
        // advanced timing/preset; then control mode; then master arm last so
        // the engine is fully configured before the cal state machine starts.
        // _psResyncRequired (set by DisconnectP2Async) forces every push on
        // the first state-change after a P2 reconnect so the new engine
        // instance picks up the canonical state instead of running on its
        // field defaults.
        bool resync = _psResyncRequired;
        // All three blocks below issue WDSP calls that perturb calcc state —
        // SetPSHWPeak rewrites hw_scale and forces an internal re-bin;
        // SetPsAdvanced/SetPsControl issue SetPSControl(reset=1, ...) which
        // flips the calcc state machine back through LRESET, truncating any
        // in-flight polynomial fit. Doing any of that mid-MOX is the
        // sporadic-splatter trigger: any unrelated Mutate() during a live
        // key-down (e.g. RX ADC overload nudging _attOffsetDb at 10 Hz, S-meter
        // retracking, panadapter zoom, operator UI nudge) would otherwise
        // reset PS and bloom IMD3 sidebands for 50-500 ms until calcc
        // walked back to LSTAYON. Thetis avoids this by construction —
        // PSForm only issues SetPSControl from explicit state-machine
        // transitions, never from a generic dispatcher.
        //
        // While _keyed is true (MOX or TUN), defer the apply; OnRadioMoxChanged
        // re-invokes OnRadioStateChanged on the falling edge to pick up
        // anything that was deferred during the key-down. SetPsEnabled
        // (arm/disarm) is intentionally NOT guarded — the operator must
        // be able to disable PS mid-TX to stop a splatter event.
        var psApplyDeferred = _keyed;
        bool psArmRising = s.PsEnabled && !_appliedPsEnabled;
        if (!psApplyDeferred && (resync || psArmRising || s.PsHwPeak != _appliedPsHwPeak))
        {
            engine.SetPsHwPeak(s.PsHwPeak);
            _appliedPsHwPeak = s.PsHwPeak;
        }
        if (!psApplyDeferred && (resync
            || psArmRising
            || s.PsMoxDelaySec != _appliedPsMoxDelaySec
            || s.PsLoopDelaySec != _appliedPsLoopDelaySec
            || s.PsAmpDelayNs != _appliedPsAmpDelayNs))
        {
            engine.SetPsAdvanced(
                s.PsMoxDelaySec,
                s.PsLoopDelaySec,
                s.PsAmpDelayNs,
                s.PsHwPeak);
            _appliedPsMoxDelaySec = s.PsMoxDelaySec;
            _appliedPsLoopDelaySec = s.PsLoopDelaySec;
            _appliedPsAmpDelayNs = s.PsAmpDelayNs;
        }
        if (!psApplyDeferred && (resync || psArmRising || s.PsAuto != _appliedPsAuto || s.PsSingle != _appliedPsSingle))
        {
            engine.SetPsControl(s.PsAuto, s.PsSingle);
            _appliedPsAuto = s.PsAuto;
            _appliedPsSingle = s.PsSingle;
        }
        if (engine is ExternalProtocol3TxDspEngine { PureSignalEnabled: true } externalPs)
        {
            int feedbackAttenuationDb = Math.Clamp(s.PsTxFeedbackAttenuationDb, 0, 31);
            int requestedCorrectionMode = s.PsSingle ? 2 : s.PsAuto ? 1 : 0;
            // Match the existing native control deferral above: source and
            // attenuation may remain live controls, but switching the
            // correction/calibration mode mid-over would split radio-route
            // state from the corrector until key-up.
            int correctionMode = _keyed && _appliedExternalPsCorrectionMode >= 0
                ? _appliedExternalPsCorrectionMode
                : requestedCorrectionMode;
            if (resync ||
                s.PsFeedbackSource != _appliedPsFeedbackSource ||
                feedbackAttenuationDb != _appliedExternalPsFeedbackAttenuationDb ||
                correctionMode != _appliedExternalPsCorrectionMode)
            {
                externalPs.SetPureSignalRouteSettings(
                    new ExternalPureSignalRouteSettings(
                        s.PsFeedbackSource == PsFeedbackSource.External,
                        feedbackAttenuationDb,
                        correctionMode));
                _appliedExternalPsFeedbackAttenuationDb = feedbackAttenuationDb;
                _appliedExternalPsCorrectionMode = correctionMode;
            }
        }
        var p1Active = _radio.ActiveClient;
        bool p1ArmMismatch = p1Active is not null
            && p1Active.PsEnabled != s.PsEnabled
            && IsPsArmWorkComplete();
        bool externalArmMismatch = engine is ExternalProtocol3TxDspEngine
            {
                PureSignalEnabled: true
            } externalArmEngine
            && externalArmEngine.PureSignalArmed != s.PsEnabled
            && IsPsArmWorkComplete();
        if (resync || s.PsEnabled != _appliedPsEnabled || p1ArmMismatch || externalArmMismatch)
        {
            if (s.PsEnabled && _keyed)
            {
                // Defer the ARM while transmitting. Arming mid-MOX fires
                // calcc's SetPSControl(reset) into a live fit, which races the
                // feedback stream and wedges calcc in LCALC on a stale curve
                // (the mid-TX arm/disarm wedge — frozen info5, cor=1 but never
                // updating → splatter). Re-arming mid-over isn't a real need;
                // leaving _appliedPsEnabled stale here makes the
                // OnRadioMoxChanged falling-edge re-apply arm it cleanly on
                // key-up. Disarm (abort) below stays immediate.
            }
            else
            {
                if (p1ArmMismatch)
                {
                    _log.LogWarning(
                        "ps.arm reconcile requested={Requested} actual={Actual} board={Board} clientLive={ClientLive} source=state-push — re-driving transition",
                        s.PsEnabled,
                        p1Active!.PsEnabled,
                        p1Active.BoardKind,
                        ReferenceEquals(_radio.ActiveClient, p1Active));
                }
                // PS engine arm requires a feedback path that delivers paired
                // samples. On P2 ANAN-class that's SetPsFeedbackEnabled. On
                // P1, HermesLite2 and HermesC10 (ANAN-G2E) deliver the 4-DDC
                // paired layout PS needs; on any other P1 board WDSP would
                // arm with no possible feedback source, sit in COLLECT, and
                // freeze RX audio + waterfall (GH #426) — skip the engine arm
                // there. The wire calls are board-gated no-ops on those
                // boards (WriteAttenuatorPayload + SnapshotState).
                bool psEngineSupported =
                    engine is ExternalProtocol3TxDspEngine { PureSignalEnabled: true } ||
                    P1PsEngineArmSupported(
                        p1Connected: p1Active is not null,
                        _radio.ConnectedBoardKind);
                // #1302 F1/F6: the arm/disarm sequence is async now — on a
                // HermesC10 the wire flip rides a stop/drain/restart
                // transition (SetPsEnabledAsync) that must never run inline
                // under _engineLock on the state-change thread, and the
                // 100 ms pihpsdr settle (transmitter.c:2467-2473: wire first,
                // settle, then engine arm) becomes a proper await instead of
                // Task.Delay(100).Wait(). Single-flight FIFO worker: requests
                // serialize, and SetPsEnabledAsync's idempotence collapses
                // redundant state pushes around the connect-time disarmed
                // replay and subsequent sanitized arm.
                SchedulePsArmTransition(s.PsEnabled, p1Active, engine, psEngineSupported);
            }
            // Mark applied only when we actually armed or disarmed. A deferred
            // (keyed) arm leaves _appliedPsEnabled stale on purpose so the
            // MOX-off re-apply re-enters this block and arms.
            // This still latches at scheduling time so repeated state pushes
            // do not enqueue duplicate work. Once the worker chain completes,
            // the client-flag comparison above detects a missed/failed wire
            // transition and re-enters on the next existing state push.
            if (!(s.PsEnabled && _keyed))
                _appliedPsEnabled = s.PsEnabled;
        }
        if (resync || s.PsFeedbackSource != _appliedPsFeedbackSource)
        {
            // Wire-only change — flips ALEX_RX_ANTENNA_BYPASS in alex0 on
            // the next CmdHighPriority emission. WDSP is unaffected.
            _p2Client?.SetPsFeedbackSource(s.PsFeedbackSource == PsFeedbackSource.External);
            _appliedPsFeedbackSource = s.PsFeedbackSource;
        }

        // ---- CFC (Continuous Frequency Compressor) ---------------------
        // issue #123. Same resync rule as PS: a P2 disconnect tears down the
        // engine, so the next state-change push has to re-assert the operator
        // CFC config even when the StateDto value hasn't changed. Equality
        // check uses CfcConfig record value semantics (the Bands array length
        // is fixed at 10, contents compared element-wise via the auto-record
        // Equals — but `record` only does reference equality on arrays, so
        // value-compare manually). null on the wire (legacy state frame)
        // falls back to CfcConfig.Default → engine sees a clean OFF profile.
        // Digital TX mode gates the CFC master run flag at the engine seam; the
        // operator's profile still lands so leaving the mode restores instantly.
        var cfc = effectiveTxAudio.Cfc;
        if (resync || !CfcConfigsEqual(cfc, _appliedCfc))
        {
            engine.SetCfcConfig(cfc);
            _appliedCfc = cfc;
        }

        // ---- TX/RX equalizer + TX noise gate (fork-local) --------------
        // Same resync rule as CFC and PS: a P2 disconnect tears the engine
        // down, so the next push has to re-assert the operator's profile
        // even when the StateDto value has not changed. Comparison goes
        // through ValueEquals because these records hold an int[], and
        // record equality on an array is reference equality — the exact
        // trap the CFC block above documents.
        var txEq = s.TxEq ?? GraphicEqConfig.Default;
        if (resync || !txEq.ValueEquals(_appliedTxEq))
        {
            engine.SetTxEq(txEq);
            _appliedTxEq = txEq;
        }

        var rxEq = s.RxEq ?? GraphicEqConfig.Default;
        if (resync || !rxEq.ValueEquals(_appliedRxEq))
        {
            // Both receivers, the way SetSquelch above does it — an RX EQ
            // that only applied to RX1 would be a surprise the moment the
            // operator listened to RX2.
            engine.SetRxEq(channel, rxEq);
            if (rx2Channel >= 0) engine.SetRxEq(rx2Channel, rxEq);
            _appliedRxEq = rxEq;
        }

        // Parametric profiles drive the same stages as the ten-band ones,
        // so they are applied AFTER: with both set, the parametric curve is
        // the one that survives, which is what an operator who has imported
        // a Thetis profile expects. Null means never set — nothing pushed.
        if (s.TxEqParametric is { } txEqP && (resync || !txEqP.ValueEquals(_appliedTxEqP)))
        {
            engine.SetTxEqParametric(txEqP);
            _appliedTxEqP = txEqP;
        }
        if (s.RxEqParametric is { } rxEqP && (resync || !rxEqP.ValueEquals(_appliedRxEqP)))
        {
            engine.SetRxEqParametric(channel, rxEqP);
            if (rx2Channel >= 0) engine.SetRxEqParametric(rx2Channel, rxEqP);
            _appliedRxEqP = rxEqP;
        }
        if (s.CfcParametric is { } cfcP && (resync || !cfcP.ValueEquals(_appliedCfcP)))
        {
            engine.SetCfcParametric(cfcP);
            _appliedCfcP = cfcP;
        }

        var txDexp = s.TxDexp ?? TxDexpConfig.Default;
        if (resync || txDexp != _appliedTxDexp)
        {
            engine.SetTxDexp(txDexp);
            _appliedTxDexp = txDexp;
        }

        var txGate = s.TxGate ?? TxGateConfig.Default;
        if (resync || txGate != _appliedTxGate)
        {
            engine.SetTxGate(txGate);
            _appliedTxGate = txGate;
        }

        // ---- RX step attenuator (operator + auto-ATT offset) -----------
        // Issue #126. Mirror RadioService's effective-atten composition
        // (operator baseline AttenDb + auto-ATT overload offset AttOffsetDb,
        // clamped 0..31) onto a live Protocol2Client. RadioService already
        // pushes the same value to the P1 client directly via
        // ActiveClient?.SetAttenuator on every operator change AND every
        // auto-ATT tick — but on a P2 connection ActiveClient is null, so
        // without this forward the S-ATT slider and the auto-ATT overload
        // ramp both fail silently on Angelia / ANAN-100D. RadioService
        // raises StateChanged whenever AttOffsetDb moves, so the auto-ATT
        // control loop reaches the wire through this block too. Route the
        // single primary-RX control to the ADC selected by that receiver;
        // ADC1 uses the independent byte-1442 attenuator.
        int effectiveAttDb = Math.Clamp(s.AttenDb + s.AttOffsetDb, 0, 31);
        int attenuatorAdc = RadioService.ReceiverAdcSource(s, 0) == 1 ? 1 : 0;
        if (resync
            || effectiveAttDb != _appliedEffectiveAttDb
            || attenuatorAdc != _appliedAttenuatorAdc)
        {
            if (_p2Client is { } attenuatorClient)
            {
                if (_appliedAttenuatorAdc != attenuatorAdc)
                {
                    if (_appliedAttenuatorAdc == 0)
                        attenuatorClient.SetAttenuator(0);
                    else if (_appliedAttenuatorAdc == 1)
                        _radio.ApplyG2AdcOptionsToP2Client(attenuatorClient, _radio.ConnectedBoardKind);
                }
                SetP2Attenuator(attenuatorClient, attenuatorAdc, effectiveAttDb);
            }
            _appliedEffectiveAttDb = effectiveAttDb;
            _appliedAttenuatorAdc = attenuatorAdc;
        }

        // PS-Monitor (issue #121) — pure UI source routing. No engine call,
        // no wire write; Tick reads _psMonitorEnabled and prefers the
        // PS-feedback analyzer when on + PS armed + correcting. Latched
        // here so the volatile read in Tick stays cheap.
        if (_psMonitorEnabled != s.PsMonitorEnabled)
        {
            _log.LogInformation("psMonitor.latch enabled={Enabled}", s.PsMonitorEnabled);
            _psMonitorEnabled = s.PsMonitorEnabled;
        }

        // TX Monitor (issue #106 follow-up) — engages the engine's parallel
        // demod path on the post-CFIR TX IQ. Edge-triggered call to the
        // engine so a re-tick with the same flag is a no-op. The engine
        // tolerates being called before TXA is open (lazy-open inside) so
        // ordering vs SetTxMode/SetTxFilter above doesn't matter.
        if (_appliedTxMonitorEnabled != s.TxMonitorEnabled)
        {
            _log.LogInformation("txMonitor.latch enabled={Enabled}", s.TxMonitorEnabled);
            engine.SetTxMonitorEnabled(s.TxMonitorEnabled);
            _appliedTxMonitorEnabled = s.TxMonitorEnabled;
            // Meter-only is a per-monitor-session flag — drop it whenever the
            // monitor turns off so a later audible Preview is never silenced by
            // a stale Auto Tune sample that ended abnormally.
            if (!s.TxMonitorEnabled)
                _txMonitorMeterOnly = false;
        }

        // Resync done — clear the flag so subsequent state changes use
        // normal change-detect (no spurious wire writes on each tick).
        _psResyncRequired = false;
        }
    }

    // CfcConfig auto-generated record Equals does reference equality on the
    // Bands array, which would always trigger a re-push on every tick where
    // the panel rebuilt the array. Explicit element-wise compare so a no-op
    // POST round-trip stays cheap.
    private static bool CfcConfigsEqual(CfcConfig a, CfcConfig b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a is null || b is null) return false;
        if (a.Enabled != b.Enabled) return false;
        if (a.PostEqEnabled != b.PostEqEnabled) return false;
        if (a.PreCompDb != b.PreCompDb) return false;
        if (a.PrePeqDb != b.PrePeqDb) return false;
        if (a.Bands is null || b.Bands is null) return ReferenceEquals(a.Bands, b.Bands);
        if (a.Bands.Length != b.Bands.Length) return false;
        for (int i = 0; i < a.Bands.Length; i++)
        {
            if (a.Bands[i].FreqHz != b.Bands[i].FreqHz) return false;
            if (a.Bands[i].CompLevelDb != b.Bands[i].CompLevelDb) return false;
            if (a.Bands[i].PostGainDb != b.Bands[i].PostGainDb) return false;
        }
        return true;
    }

    // "16/256" → (16, 256). Falls back to (16, 256) on any parse failure
    // because that's the only ints/spi pair WDSP allows save/restore on
    // (Thetis PSForm.cs:865) — a safe default.
    /// <summary>
    /// Whether the WDSP PS engine may be armed for the live connection — the
    /// GH #426 guard. On Protocol 2 (no P1 client) the ANAN-class feedback
    /// DDC path always exists. On Protocol 1 only HermesLite2 and HermesC10
    /// (ANAN-G2E, classic Hermes v3.3 — relay-routed feedback tap on DDC2 +
    /// TX DAC reference on DDC3) deliver the 4-DDC paired layout PS needs;
    /// on any other P1 board WDSP would arm with no possible feedback
    /// source, park in COLLECT, and the blocking 100 ms settle would freeze
    /// RX audio + waterfall (GH #426). Pure so the carve-out is pinned by
    /// tests board-by-board.
    /// </summary>
    internal static bool P1PsEngineArmSupported(bool p1Connected, HpsdrBoardKind board) =>
        !p1Connected
        || board == HpsdrBoardKind.HermesLite2
        || board == HpsdrBoardKind.HermesC10
        || board == HpsdrBoardKind.HermesII;

    internal static void ApplyP1PsFeedbackRateOverride(
        HpsdrBoardKind board,
        int wireRateHz,
        Action<int> setPsFeedbackRateHz)
    {
        if (board is HpsdrBoardKind.HermesC10 or HpsdrBoardKind.HermesII)
            setPsFeedbackRateHz(wireRateHz);
    }

    // ---- PS arm/disarm worker (#1302 F1/F6) --------------------------------
    // Single-flight FIFO chain: each request runs strictly after the previous
    // one completes, off the state-change thread and outside _engineLock.
    // Ordering per request preserves the shipped sequences:
    //   arm    = wire (P2 bit / P1 SetPsEnabledAsync) → 100 ms settle →
    //            engine.SetPsEnabled(true)          (pihpsdr order)
    //   disarm = engine.SetPsEnabled(false) → wire → drain leftover frames
    // On HermesC10 the P1 wire call is the stop/drain/restart transition —
    // the receiver count is never flipped on a live stream.
    private readonly object _psArmWorkSync = new();
    private Task<bool> _psArmWork = Task.FromResult(true);

    /// <summary>Tail of the PS arm/disarm worker chain — awaitable by tests
    /// to observe completion of all scheduled transitions.</summary>
    internal Task PsArmWorkForTests => WaitForPsArmTransitionsAsync();

    internal Task WaitForPsArmTransitionsAsync()
    {
        lock (_psArmWorkSync) return _psArmWork;
    }

    internal async Task<bool> WaitForPsDisarmAsync()
    {
        Task<bool> work;
        lock (_psArmWorkSync) work = _psArmWork;
        if (!await work.ConfigureAwait(false)) return false;
        if (_radio.Snapshot().PsEnabled || _radio.ActiveClient?.PsEnabled == true)
            return false;
        lock (_engineLock)
            return _engine is not ExternalProtocol3TxDspEngine
                {
                    PureSignalEnabled: true,
                    PureSignalArmed: true,
                };
    }

    private bool IsPsArmWorkComplete()
    {
        lock (_psArmWorkSync) return _psArmWork.IsCompleted;
    }

    internal void ReconcileExternalPureSignalStatusForTick(IDspEngine engine, StateDto state)
    {
        if (engine is not ExternalProtocol3TxDspEngine { PureSignalEnabled: true } externalPs ||
            !state.PsEnabled)
        {
            Interlocked.Exchange(ref _externalPsFailCloseLatched, 0);
            return;
        }

        if (!IsPsArmWorkComplete() || externalPs.GetPureSignalStatus().Armed)
            return;
        if (Interlocked.Exchange(ref _externalPsFailCloseLatched, 1) != 0)
            return;

        _log.LogWarning(
            "ps.external.fail-close provider reported disarmed while runtime state was armed; clearing runtime arm state");
        _ = Task.Run(() =>
        {
            try { _radio.FailClosedPureSignalRuntime(); }
            catch (Exception ex)
            {
                Interlocked.Exchange(ref _externalPsFailCloseLatched, 0);
                _log.LogWarning(ex, "ps.external.fail-close runtime disarm failed");
            }
        });
    }

    private void SchedulePsArmTransition(
        bool enable, IProtocol1Client? p1, IDspEngine engine, bool engineArmSupported)
    {
        lock (_psArmWorkSync)
        {
            var prev = _psArmWork;
            _psArmWork = Task.Run(async () =>
            {
                try { await prev.ConfigureAwait(false); }
                catch { /* previous request already logged its failure */ }
                try
                {
                    // Resolve the active client when the FIFO request actually
                    // runs. Capturing null during a connect/state-change race
                    // must not silently turn a requested P1 arm into an
                    // engine-only arm for the rest of the session.
                    var executionP1 = _radio.ActiveClient;
                    if (!ReferenceEquals(executionP1, p1)
                        && (executionP1 is null || executionP1.PsEnabled != enable))
                    {
                        _log.LogWarning(
                            "ps.arm reconcile requested={Requested} actual={Actual} board={Board} clientLive={ClientLive} source=worker-client-change — using current client",
                            enable,
                            executionP1?.PsEnabled,
                            executionP1?.BoardKind ?? _radio.ConnectedBoardKind,
                            executionP1 is not null);
                    }
                    bool executionEngineArmSupported = ReferenceEquals(executionP1, p1)
                        ? engineArmSupported
                        : P1PsEngineArmSupported(
                            p1Connected: executionP1 is not null,
                            executionP1?.BoardKind ?? _radio.ConnectedBoardKind);
                    return await RunPsArmTransitionAsync(
                            enable,
                            executionP1,
                            engine,
                            executionEngineArmSupported)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "ps.arm transition target={Enable} failed", enable);
                    return false;
                }
            });
        }
    }

    private async Task<bool> RunPsArmTransitionAsync(
        bool enable, IProtocol1Client? p1, IDspEngine engine, bool engineArmSupported)
    {
        // A capability-gated external P3 engine owns its matching p3app route
        // transaction. Zeus still owns operator intent and serialization;
        // startup remains disarmed and P1/P2 retain their wire-first ordering.
        if (engine is ExternalProtocol3TxDspEngine { PureSignalEnabled: true })
        {
            bool applied = false;
            lock (_engineLock)
            {
                if (ReferenceEquals(engine, _engine))
                {
                    engine.SetPsEnabled(enable);
                    applied = true;
                }
            }
            if (!enable) DrainPsFeedback();
            return applied && externalPsStateMatches(engine, enable);
        }

        if (enable)
        {
            _p2Client?.SetPsFeedbackEnabled(true);
            bool wireConverged = await SetAndReconcileP1PsEnabledAsync(p1, true)
                .ConfigureAwait(false);
            if (!wireConverged)
            {
                _log.LogError(
                    "ps.arm reconcile requested=true actual={Actual} board={Board} clientLive={ClientLive} source=engine-arm — wire did not converge; engine arm skipped",
                    p1?.PsEnabled,
                    p1?.BoardKind ?? _radio.ConnectedBoardKind,
                    p1 is not null && ReferenceEquals(_radio.ActiveClient, p1));
                return false;
            }
            if (engineArmSupported)
            {
                // pihpsdr settle window: without it the first 5-20 pscc calls
                // receive partial/glitched samples, scheck flags binfo[6] and
                // calcc thrashes through LRESET instead of converging.
                await Task.Delay(100).ConfigureAwait(false);
                bool applied;
                lock (_engineLock)
                {
                    // The engine may have been replaced (reconnect) since this
                    // request was scheduled; the new engine's resync re-arms
                    // through its own state replay, so skip a stale arm.
                    applied = ReferenceEquals(engine, _engine);
                    if (applied)
                        engine.SetPsEnabled(true);
                }
                if (!applied) return false;
            }
            return true;
        }
        else
        {
            bool engineDisarmed;
            lock (_engineLock)
            {
                engineDisarmed = ReferenceEquals(engine, _engine);
                if (engineDisarmed)
                    engine.SetPsEnabled(false);
            }
            _p2Client?.SetPsFeedbackEnabled(false);
            bool wireDisarmed = await SetAndReconcileP1PsEnabledAsync(p1, false)
                .ConfigureAwait(false);
            _radio.ApplyBoardKindToActiveClientIfSafe();
            DrainPsFeedback();
            return engineDisarmed && wireDisarmed;
        }

        static bool externalPsStateMatches(IDspEngine engine, bool enabled) =>
            engine is ExternalProtocol3TxDspEngine externalPs &&
            externalPs.PureSignalArmed == enabled;
    }

    private async Task<bool> SetAndReconcileP1PsEnabledAsync(
        IProtocol1Client? p1,
        bool requested)
    {
        if (p1 is null)
        {
            // Null is expected for Protocol 2. A transition with neither P1
            // nor P2 live used to be completely silent; the next state push
            // will retry once a P1 client is present.
            if (_p2Client is null)
            {
                _log.LogWarning(
                    "ps.arm reconcile requested={Requested} board={Board} clientLive={ClientLive} source=no-client — deferred until a client is available",
                    requested,
                    _radio.ConnectedBoardKind,
                    false);
            }
            return _p2Client is not null;
        }

        await p1.SetPsEnabledAsync(requested).ConfigureAwait(false);
        bool actual = p1.PsEnabled;
        if (actual == requested) return true;

        bool clientLive = ReferenceEquals(_radio.ActiveClient, p1);
        _log.LogWarning(
            "ps.arm reconcile requested={Requested} actual={Actual} board={Board} clientLive={ClientLive} source=post-transition — re-driving transition",
            requested,
            actual,
            p1.BoardKind,
            clientLive);

        await p1.SetPsEnabledAsync(requested).ConfigureAwait(false);
        actual = p1.PsEnabled;
        if (actual != requested)
        {
            _log.LogError(
                "ps.arm reconcile requested={Requested} actual={Actual} board={Board} clientLive={ClientLive} source=post-redrive — client did not converge",
                requested,
                actual,
                p1.BoardKind,
                ReferenceEquals(_radio.ActiveClient, p1));
        }
        return actual == requested;
    }

    private void ApplyStateToNewChannel(IDspEngine engine, int channelId)
    {
        _rxAudioLeveler = default;
        Volatile.Write(ref _rxLevelerRfSignalResolved, 0);
        Volatile.Write(ref _rxLevelerAdcOverloadRisk, 0);
        _rxLevelerRfAcquireHits = 0;
        _rxLevelerRfReleaseMisses = 0;
        Interlocked.Exchange(ref _rxLevelerRfEvidenceMs, long.MinValue);
        _adaptiveSquelch = new AdaptiveSquelchState();
        var s = _radio.Snapshot();
        var nr = NormalizeNrConfig(s.Nr ?? new NrConfig());
        double effectiveAgc = EffectiveAgcGainDb(s);
        var agc = EffectiveAgcConfig(
            s.Agc ?? new AgcConfig(AgcMode.Med),
            effectiveAgc);
        var squelch = s.Squelch ?? new SquelchConfig();
        // FreeDV resolves to the band-convention sideband (LSB < 10 MHz, USB ≥);
        // every other mode passes through as itself. See RadioService.EffectiveEngineMode.
        var openEngineMode = RadioService.EffectiveEngineMode(s.Mode, s.VfoHz);
        var openTxReceiver = RadioFrequencyResolver.TxReceiver(s);
        var openTxEngineMode = RadioService.EffectiveEngineMode(
            openTxReceiver.Mode, RadioFrequencyResolver.TxFrequencyHz(s));
        bool openTxFreeDvMode = openTxReceiver.Mode == RxMode.FreeDv;
        var effectiveTxAudio = ResolveEffectiveTxAudioProfile(s, openTxEngineMode);
        var txLeveling = openTxFreeDvMode
            ? FreeDvTxLevelingProfile
            : effectiveTxAudio.TxLeveling;
        var txPhaseRotator = effectiveTxAudio.TxPhaseRotator;
        var cfc = effectiveTxAudio.Cfc;
        engine.SetMode(channelId, openEngineMode);
        // Sync TXA modulator with RX mode at engine-open time so the first
        // key-down lands with the correct sideband (no-op on Synthetic / pre-
        // OpenTxChannel).
        engine.SetTxMode(openTxEngineMode);
        engine.SetTxDigitalBypass(IsDigitalTxZeusMode(openTxReceiver.Mode));
        var (openRxLow, openRxHigh) = SignedRxFilterFor(s, openEngineMode);
        engine.SetFilter(channelId, openRxLow, openRxHigh);
        // Sign the TX bandpass from the live mode (see SignedTxFilterFor) so a
        // fresh engine doesn't key up with a USB-positive default while in LSB.
        var (txOpenLow, txOpenHigh) = SignedTxFilterFor(s, openTxEngineMode);
        engine.SetTxFilter(txOpenLow, txOpenHigh);
        // Issue #871 — push the operator's chosen FIR window onto the fresh
        // engine so a reconnect rebuilds the RX/TX bandpass at the saved
        // shoulder shape rather than the WDSP open-time Sharp default.
        engine.SetRxBandpassWindow(channelId, s.RxFilterWindow);
        engine.SetTxBandpassWindow(s.TxFilterWindow);
        engine.SetRxFilterPhase(channelId, s.RxFilterPhase);
        engine.SetTxFilterPhase(s.TxFilterPhase);
        engine.SetVfoHz(channelId, s.VfoHz);
        // Replay the WDSP shift on fresh-channel open so a connect landing
        // with VfoHz != RadioLoHz (persisted across restart) is demodulating
        // the same dial the operator saw last session.
        // See docs/prd/panfall_behavior.md. RIT folds in here too so a
        // fresh-channel open replays the active RX offset.
        int ritHz = s.RitEnabled ? (int)s.RitHz : 0;
        int ctunShiftHz = (int)(CwOffset.EffectiveLoHz(s.Mode, s.VfoHz) - s.RadioLoHz) + ritHz;
        engine.SetCtunShift(channelId, ctunShiftHz);
        engine.SetAgcTop(channelId, effectiveAgc);
        engine.SetRxAfGainDb(channelId, EffectiveAfGainDb(s.RxAfGainDb, s.Rx1AfGainDb));
        // Re-push TX mic gain + Leveler on every fresh engine so the channel
        // doesn't sit at the WDSP open-time defaults when the operator's last
        // values differ. The engine's TXA reopen path resets PanelGain1=1.0 and
        // LevelerTop=8.0 internally; without this re-push, a relaunch would
        // ignore the just-hydrated StateDto values.
        int micGainDb = effectiveTxAudio.MicGainDb;
        double micLinearInit = Math.Pow(10.0, micGainDb / 20.0);
        engine.SetTxPanelGain(micLinearInit);
        double levelerMax = openTxFreeDvMode
            ? FreeDvLevelerMaxGainDb
            : effectiveTxAudio.LevelerMaxGainDb;
        engine.SetTxLevelerMaxGain(levelerMax);
        double carrierLevel = effectiveTxAudio.CarrierCoefficient;
        engine.SetTxAmCarrierLevel(carrierLevel);
        engine.SetNoiseReduction(channelId, nr);
        // Force-apply AGC mode/custom params on a fresh engine so the operator's
        // persisted choice survives a reconnect. The engine's channel-open path
        // installs the Med default (ApplyAgcDefaults); this overrides it with the
        // hydrated config. The max-gain (top) is pushed separately above.
        engine.SetAgc(channelId, agc);
        // Force-apply squelch on a fresh engine so the operator's persisted
        // choice survives a reconnect. Mode is already set above so the engine
        // routes run/threshold to the correct stage (SSQL/AMSQ/FMSQ).
        engine.SetSquelch(channelId, squelch);
        // Force-apply TX leveling on a fresh engine so the operator's persisted
        // ALC/Leveler/Compressor config survives a reconnect. The TXA-open path
        // installs the TxLevelingConfig defaults; this overrides with the
        // hydrated config (and re-arms the engine's _txLevelerEnabled so the
        // TUN/two-tone Leveler restore honours the operator's on/off). The
        // Leveler max-gain is pushed separately above (SetTxLevelerMaxGain).
        engine.SetTxLeveling(channelId, txLeveling);
        // Force-apply TX phase rotator on a fresh engine so a saved profile or
        // Auto Tune-locked setting survives reconnect. Reverse is part of the
        // same config but remains operator-controlled.
        engine.SetTxPhaseRotator(channelId, txPhaseRotator);
        engine.SetCfcConfig(cfc);
        // Manual notches: feed the LO first (notch positioning reference), then
        // re-apply the operator's notch set onto the fresh engine. A reconnect
        // builds a brand-new engine whose notch DB is empty; RadioService holds
        // the authoritative list so EMF notches survive the reconnect.
        engine.SetNotchTuneFrequencyHz(s.RadioLoHz);
        engine.SetNotches(_radio.Notches);
        int ddcZoomLevel = DdcZoomLevel(s.ZoomLevel);
        engine.SetZoom(channelId, ddcZoomLevel);
        _appliedMode = s.Mode;
        _appliedEngineMode = openEngineMode;
        _appliedTxMode = openTxReceiver.Mode;
        _appliedTxEngineMode = openTxEngineMode;
        // Cache the SIGNED values we actually pushed (FreeDv re-signs by sideband)
        // so the first OnRadioStateChanged tick doesn't see a phantom width change.
        _appliedLowHz = openRxLow;
        _appliedHighHz = openRxHigh;
        _appliedCtunOffsetHz = ctunShiftHz;
        _appliedTxLowHz = txOpenLow;
        _appliedTxHighHz = txOpenHigh;
        _appliedAgcCeilingDb = effectiveAgc;
        _appliedRxAfGainDb = EffectiveAfGainDb(s.RxAfGainDb, s.Rx1AfGainDb);
        // Reset every secondary's per-RX AF-gain slew state — the engine has
        // just opened a fresh RX1 channel (full engine swap or reconnect), so
        // any RX2..N channels will be reopened too and need to snap to their
        // operator value rather than drag from a stale slewed dB.
        for (int i = 1; i < MaxReceivers; i++)
            _secondaryRx[i].AppliedAfGainDb = double.NaN;
        _appliedTxMicGainLinear = micLinearInit;
        _appliedTxLevelerMaxGainDb = levelerMax;
        _appliedTxAmCarrierLevel = carrierLevel;
        _appliedNr = nr;
        _appliedAgc = agc;
        _appliedSquelch = squelch;
        _appliedTxLeveling = txLeveling;
        _appliedTxPhaseRotator = txPhaseRotator;
        _appliedCfc = cfc;
        _appliedRxBandpassWindow = s.RxFilterWindow;
        _appliedTxBandpassWindow = s.TxFilterWindow;
        _appliedRxFilterPhase = s.RxFilterPhase;
        _appliedTxFilterPhase = s.TxFilterPhase;
        _appliedZoomLevel = ddcZoomLevel;
    }

    // Whether secondary receiver <paramref name="rxIndex"/> (1..MaxReceivers-1) is
    // enabled for the current state. Today only slot 1 (the historical RX2) has
    // wire/UI state, so it's the only one that can be active; B3 replaces this with
    // s.Receivers[rxIndex].Enabled once StateDto carries the receiver array.
    // RX2 (index 1) keeps reading the flat Rx2Enabled flag so its behaviour is
    // byte-exact; RX3+ (index >= 2) read the canonical StateDto.Receivers[]
    // (B3 projection / per-receiver store) so full multi-DDC lights up without
    // touching the proven RX1/RX2 path.
    private static bool SecondaryReceiverEnabled(int rxIndex, StateDto s) =>
        rxIndex == 1
            ? s.Rx2Enabled
            : rxIndex >= 2 && s.Receivers is { } rs && rxIndex < rs.Count && rs[rxIndex].Enabled;

    // Per-RX audio mute (Thetis chkMUT / chkRX2Mute). Reads the projected
    // Receivers[] array, which RadioService keeps current on every Mutate.
    private static bool IsReceiverMuted(StateDto s, int rxIndex) =>
        s.Receivers is { } rs && rxIndex >= 0 && rxIndex < rs.Count && rs[rxIndex].Muted;

    private static byte ReceiverAdcSource(StateDto s, int rxIndex) =>
        s.Receivers is { } rs && rxIndex >= 0 && rxIndex < rs.Count ? rs[rxIndex].AdcSource : (byte)0;

    // Per-secondary-receiver tuning params, read from the canonical Receivers[]
    // entry (RX2 = index 1, RX3+ = index N). RadioService keeps the array
    // current on every Mutate; RX2's flat VFO-B fields were retired in the A/B
    // wire collapse, so every secondary receiver now flows through one path.
    private static (RxMode mode, long vfoHz, int filterLow, int filterHigh, double afGainDb)
        SecondaryRxParams(StateDto s, int rxIndex)
    {
        var r = s.Receivers is { } rs && rxIndex >= 0 && rxIndex < rs.Count ? rs[rxIndex] : null;
        return r is null
            ? (RxMode.USB, 0L, 100, 2850, 0.0)
            : (r.Mode, r.VfoHz, r.FilterLowHz, r.FilterHighHz, r.AfGainDb);
    }

    // Convenience helper: open/sync/close the WDSP channel for one secondary
    // receiver, mirroring RX1's lifecycle. Returns the channel id (or -1 when the
    // receiver is disabled). Generalised from the old EnsureRx2Channel.
    private int EnsureSecondaryRxChannel(IDspEngine engine, int rxIndex, StateDto s)
    {
        var rx = _secondaryRx[rxIndex];
        int chan = Volatile.Read(ref rx.ChannelId);
        if (!SecondaryReceiverEnabled(rxIndex, s))
        {
            CloseSecondaryRxChannel(engine, rxIndex, chan);
            return -1;
        }

        if (chan >= 0)
        {
            ApplyStateToSecondaryRxChannel(engine, rxIndex, chan, s);
            return chan;
        }

        int rateHz = Volatile.Read(ref _sampleRateHz);
        if (rateHz <= 0) rateHz = s.SampleRate > 0 ? s.SampleRate : SyntheticSampleRateHz;
        int opened = engine.OpenChannel(rateHz, _panadapterWidth);
        try
        {
            ApplyStateToSecondaryRxChannel(engine, rxIndex, opened, s);
            // Fresh channel: ramp its first audio blocks into the additive mix
            // bus so toggling the receiver on doesn't dump an unconverged,
            // full-scale block and click/distort. Mirrors RX1's post-TX resume.
            // Arm the fade BEFORE publishing ChannelId: the audio tick gates on
            // ChannelId (Volatile.Read, acquire) then reads FadeInSamplesRemaining.
            // Writing the counter first, then ChannelId (Volatile.Write, release),
            // guarantees any tick that observes the new channel also observes the
            // armed fade — otherwise a tick interleaving between the two writes
            // would mix the first unramped block with the counter still 0.
            Volatile.Write(ref rx.FadeInSamplesRemaining, SecondaryRxOpenFadeInSamples);
            Volatile.Write(ref rx.ChannelId, opened);
            _log.LogInformation(
                "dsp.pipeline rx{Rx} opened channel={Channel} rate={Rate} vfoHz={VfoHz}",
                rxIndex + 1,
                opened,
                rateHz,
                s.Rx2().VfoHz);
            return opened;
        }
        catch
        {
            // ApplyState advances per-control latches only after each successful
            // setter. If a later setter fails, the native channel is discarded;
            // none of those remembered values describe the next fresh channel.
            // Reset all of them so a retry performs the complete initialization.
            rx.ResetAppliedState();
            try { engine.CloseChannel(opened); } catch { /* best-effort */ }
            throw;
        }
    }

    private void CloseSecondaryRxChannel(IDspEngine engine, int rxIndex, int chan)
    {
        if (chan < 0) return;
        Volatile.Write(ref _secondaryRx[rxIndex].ChannelId, -1);
        Volatile.Write(ref _secondaryRx[rxIndex].FadeInSamplesRemaining, 0);
        // NaN = "no value applied yet" — a future reopen snaps to the
        // operator's target instead of slewing from this stale value.
        _secondaryRx[rxIndex].ResetAppliedState();
        try { engine.CloseChannel(chan); }
        catch (Exception ex)
        {
            _log.LogDebug(ex, "dsp.pipeline rx{Rx} close failed channel={Channel}", rxIndex + 1, chan);
        }
        _log.LogInformation("dsp.pipeline rx{Rx} closed channel={Channel}", rxIndex + 1, chan);
    }

    // Reset all secondary-receiver channel ids to -1 WITHOUT closing (used by the
    // engine-swap sites, where the whole engine — and thus its channels — is being
    // torn down/replaced, so there's nothing to close on the old engine).
    private void ResetSecondaryRxChannels()
    {
        for (int i = 1; i < MaxReceivers; i++)
        {
            Volatile.Write(ref _secondaryRx[i].ChannelId, -1);
            Volatile.Write(ref _secondaryRx[i].FadeInSamplesRemaining, 0);
            // See SecondaryRx.AppliedAfGainDb — engine swap discards any
            // prior slewed state, so the new channel snaps to its target.
            _secondaryRx[i].ResetAppliedState();
        }
        Volatile.Write(ref _widebandDetailChannelId, -1);
        Volatile.Write(ref _widebandDetailDdcIndex, -1);
        Volatile.Write(ref _widebandDetailSourceReceiverIndex, int.MinValue);
        Volatile.Write(ref _widebandDetailReady, 0);
        Interlocked.Exchange(ref _widebandDetailLastIqMs, long.MinValue);
        Interlocked.Increment(ref _widebandSourceGeneration);
    }

    /// <summary>
    /// Recompute RX2's DDC centre (<see cref="_rx2LoHz"/>) for the current state,
    /// mirroring RX1's RadioLoHz/CTUN model. CTUN off → follow VFO B (recentre);
    /// CTUN on → freeze, so the dial roams within the window — unless the dial
    /// would leave the captured DDC bandwidth, in which case recentre (the
    /// stand-in for RX1's band-button SetRadioLo, which RX2 has no UI for).
    /// Idempotent and deterministic, so it can run from several tick paths.
    /// </summary>
    private void UpdateRxLo(int rxIndex, StateDto s)
    {
        var rx = _secondaryRx[rxIndex];
        if (!SecondaryReceiverEnabled(rxIndex, s))
        {
            rx.LoInit = false; // re-enabling recentres from scratch
            return;
        }
        var (mode, vfoHz, _, _, _) = SecondaryRxParams(s, rxIndex);
        long effB = CwOffset.EffectiveLoHz(mode, vfoHz);
        long span = Volatile.Read(ref _sampleRateHz);
        if (span <= 0) span = s.SampleRate > 0 ? s.SampleRate : SyntheticSampleRateHz;
        long edge = (long)(span * 0.45); // recentre before the dial hits the DDC edge
        if (!rx.LoInit || !s.CtunEnabled || Math.Abs(effB - rx.LoHz) > edge)
        {
            rx.LoHz = effB;
        }
        rx.LoInit = true;
    }

    /// <summary>
    /// Pan a secondary receiver's DDC centre to <paramref name="hz"/> — the
    /// client-side keep-in-view autopan's analogue of RX1's
    /// <see cref="RadioService.SetRadioLo"/>. Under CTUN the centre is frozen and
    /// the dial roams within the captured window, so the dial/filter can leave the
    /// (much narrower) visible span long before <see cref="UpdateRxLo"/>'s
    /// DDC-edge recentre fires; the client — which knows the visible span — calls
    /// this to recentre before that happens.
    ///
    /// <para>Only meaningful for a true independent DDC (Protocol 2). P1 / synthetic
    /// secondaries sub-receive RX1's shared NCO (their WDSP shift is RadioLoHz-
    /// relative, see <see cref="ComputeSecondaryCtunShiftHz"/>), so their centre
    /// can't move independently — no-op there. Also a no-op with CTUN off (the
    /// centre follows the dial) or when the receiver is disabled.</para>
    ///
    /// <para>Re-applies the current state under <c>_engineLock</c> on the caller
    /// thread, exactly like normal tuning via <see cref="RadioService.StateChanged"/>,
    /// so the WDSP shift recompute and the P2 DDC retune land in lockstep with the
    /// new centre. The new centre survives the <see cref="UpdateRxLo"/> calls inside
    /// because the client keeps the dial within the captured window (visible span ⊆
    /// DDC window), so the edge-recentre guard never trips.</para>
    /// </summary>
    public void RequestSecondaryLo(int rxIndex, long hz)
    {
        if (rxIndex < 1 || rxIndex >= MaxReceivers) return;
        if (_p2Client is null) return; // P1 secondaries share RadioLoHz — not pannable
        long clamped = _radio.ClampTuningFrequency(hz);
        lock (_engineLock)
        {
            var s = _radio.Snapshot();
            if (!s.CtunEnabled || !SecondaryReceiverEnabled(rxIndex, s)) return;
            var rx = _secondaryRx[rxIndex];
            if (rx.LoInit && rx.LoHz == clamped) return; // already centred there
            rx.LoHz = clamped;
            rx.LoInit = true;
            OnRadioStateChanged(s);
        }
    }

    internal static int ComputeRx2CtunShiftHz(
        StateDto s,
        long rx2LoHz,
        bool protocol2)
    {
        var rx2 = s.Rx2();
        long effectiveVfoBHz = CwOffset.EffectiveLoHz(rx2.Mode, rx2.VfoHz);
        return protocol2
            ? (int)(effectiveVfoBHz - rx2LoHz)
            : (int)(effectiveVfoBHz - s.RadioLoHz);
    }

    // N-receiver generalization of ComputeRx2CtunShiftHz: the WDSP shift for any
    // secondary, from its own mode/VFO. For RX2 (Receivers[1].Mode/VfoHz) this
    // returns the same value as ComputeRx2CtunShiftHz, keeping RX2 byte-exact.
    private static int ComputeSecondaryCtunShiftHz(
        StateDto s, RxMode mode, long vfoHz, long rxLoHz, bool protocol2)
    {
        long eff = CwOffset.EffectiveLoHz(mode, vfoHz);
        return protocol2 ? (int)(eff - rxLoHz) : (int)(eff - s.RadioLoHz);
    }

    private void ApplyStateToSecondaryRxChannel(IDspEngine engine, int rxIndex, int channelId, StateDto s)
    {
        var rx = _secondaryRx[rxIndex];
        var nr = NormalizeNrConfig(s.Nr ?? new NrConfig());
        var agc = EffectiveAgcConfig(
            s.Agc ?? new AgcConfig(AgcMode.Med),
            _appliedAgcCeilingDb);
        var squelch = s.Squelch ?? new SquelchConfig();
        var (mode, vfoHz, filterLow, filterHigh, afGainDb) = SecondaryRxParams(s, rxIndex);
        // FreeDV on a secondary RX follows the same band-convention sideband as the
        // primary (LSB < 10 MHz, USB ≥). The helper also repairs legacy
        // symmetric DIGU/DIGL state before it reaches the secondary engine.
        var (secEngineMode, signedFilterLow, signedFilterHigh) = SecondaryEngineFilterFor(
            mode, vfoHz, filterLow, filterHigh);
        if (rx.AppliedMode != secEngineMode)
        {
            engine.SetMode(channelId, secEngineMode);
            rx.AppliedMode = secEngineMode;
        }
        if (rx.AppliedFilterLowHz != signedFilterLow ||
            rx.AppliedFilterHighHz != signedFilterHigh)
        {
            engine.SetFilter(channelId, signedFilterLow, signedFilterHigh);
            rx.AppliedFilterLowHz = signedFilterLow;
            rx.AppliedFilterHighHz = signedFilterHigh;
        }
        if (rx.AppliedVfoHz != vfoHz)
        {
            engine.SetVfoHz(channelId, vfoHz);
            rx.AppliedVfoHz = vfoHz;
        }
        UpdateRxLo(rxIndex, s);
        // P2 true-DDC: the secondary's hardware DDC sits at rx.LoHz, so the WDSP
        // shift roams the dial within that window — EffectiveLoHz(vfo) − rx.LoHz.
        // Under CTUN off, rx.LoHz == EffectiveLoHz(vfo) so the shift is 0 and the
        // panel recentres on the dial. P1 / synthetic secondaries are sub-receivers
        // of RX1's window, so they still shift against RadioLoHz.
        int shiftHz = ComputeSecondaryCtunShiftHz(
            s,
            mode,
            vfoHz,
            rx.LoHz,
            protocol2: _p2Client is not null);
        if (rx.AppliedCtunShiftHz != shiftHz)
        {
            engine.SetCtunShift(channelId, shiftHz);
            rx.AppliedCtunShiftHz = shiftHz;
        }
        // AGC-T fanout uses the per-tick slewed ceiling computed in the
        // main OnRadioStateChanged block, not the raw target — so RX2..N
        // see the same rate-capped dB as RX1 (no extra fan-out wiring
        // needed at the slew-advance site). The applied-value latch still
        // guarantees a freshly-opened channel receives the current value.
        if (rx.AppliedAgcTopDb != _appliedAgcCeilingDb)
        {
            engine.SetAgcTop(channelId, _appliedAgcCeilingDb);
            rx.AppliedAgcTopDb = _appliedAgcCeilingDb;
        }
        // Per-secondary AF-gain slew: each receiver has its own slider
        // (Receivers[i].AfGainDb) so the rate-cap state is per-SecondaryRx.
        // NaN sentinel = "no value applied yet" — snaps on the first push
        // after a fresh channel-open so we don't drag from a stale 0 dB.
        double afTarget = EffectiveAfGainDb(s.RxAfGainDb, afGainDb);
        double afNext = double.IsNaN(rx.AppliedAfGainDb)
            ? afTarget
            : StepTowardCappedDb(rx.AppliedAfGainDb, afTarget, AfGainSlewMaxDbPerTick);
        if (rx.AppliedAfGainDb != afNext)
        {
            engine.SetRxAfGainDb(channelId, afNext);
            rx.AppliedAfGainDb = afNext;
        }
        if (rx.AppliedNr != nr)
        {
            engine.SetNoiseReduction(channelId, nr);
            rx.AppliedNr = nr;
        }
        if (rx.AppliedAgc != agc)
        {
            engine.SetAgc(channelId, agc);
            rx.AppliedAgc = agc;
        }
        if (rx.AppliedSquelch != squelch)
        {
            engine.SetSquelch(channelId, squelch);
            rx.AppliedSquelch = squelch;
        }
        if (rx.AppliedBandpassWindow != s.RxFilterWindow)
        {
            engine.SetRxBandpassWindow(channelId, s.RxFilterWindow);
            rx.AppliedBandpassWindow = s.RxFilterWindow;
        }
        if (rx.AppliedFilterPhase != s.RxFilterPhase)
        {
            engine.SetRxFilterPhase(channelId, s.RxFilterPhase);
            rx.AppliedFilterPhase = s.RxFilterPhase;
        }
        int zoom = DdcZoomLevel(RadioService.ReceiverZoomLevel(s, rxIndex));
        if (rx.AppliedZoom != zoom)
        {
            engine.SetZoom(channelId, zoom);
            rx.AppliedZoom = zoom;
        }
    }

    // iter5 (task #4): the four channel pumps that used to live here
    //   - StartIqPump            (P1 IQ → engine.FeedIq)
    //   - StartIqPumpP2          (P2 IQ → engine.FeedIq)
    //   - StartPsFeedbackPumpP1  (P1 PS paired blocks → engine.FeedPsFeedbackBlock)
    //   - StartPsFeedbackPumpP2  (P2 PS paired blocks → engine.FeedPsFeedbackBlock)
    // ...have been replaced by the synchronous IRxPacketSink path. Each
    // pump did one `await Channel.WaitToReadAsync` + drain + `lock(_engineLock)`
    // per packet — burning ~52% of busy CPU on swtch_pri /
    // ThreadNative_SpinWait by perf3 iter4 sampling. Their work now happens
    // INLINE on Protocol1Client / Protocol2Client's RxLoop thread via
    // OnIqFrame / OnPsFeedbackFrame above. The ArrayPool return for P1 IQ
    // happens in the OnIqFrame finally block (same contract).

    // Best-effort drain of any in-flight paired frames after PS disarm.
    // Called synchronously from OnRadioStateChanged so the channel is empty
    // by the next re-arm. Iter5: with the sink path live, the protocol
    // clients invoke OnPsFeedbackFrame INSTEAD of writing the channel, so
    // the channels here are normally empty already — this function is a
    // near-no-op (one TryRead returning false) but stays as defensive
    // belt-and-suspenders for the rare case where a sink swap is in
    // flight or a non-sink consumer (test, probe) is in use.
    // Drains either active client (P1 or P2 — only one is non-null at a time).
    private void DrainPsFeedback()
    {
        var p2 = _p2Client;
        if (p2 is not null)
        {
            var reader = p2.PsFeedbackFrames;
            while (reader.TryRead(out _)) { }
            return;
        }
        var p1 = _radio.ActiveClient;
        if (p1 is not null)
        {
            var reader = p1.PsFeedbackFrames;
            while (reader.TryRead(out _)) { }
        }
    }

    /// <summary>
    /// Connect to a Protocol 2 radio and start streaming RX IQ into the DSP
    /// engine. Parallel path to RadioService.ConnectAsync (which is Protocol 1
    /// only); both swap the engine to WDSP and attach this pipeline as the
    /// synchronous RX sink on the client (iter5 — no more Task.Run pumps).
    /// Only one client at a time.
    /// </summary>
    public async Task<int> ConnectP2Async(
        IPEndPoint radioEndpoint,
        int sampleRateKhz,
        byte numAdc,
        CancellationToken ct,
        HpsdrBoardKind boardKind = HpsdrBoardKind.Unknown,
        string? firmware = null,
        bool sampleRateExplicit = true)
    {
        _radio.NotifyOperatorConnectionAction();
        await _radio.RadioLifecycleGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await ConnectP2WithLifecycleGateHeldAsync(
                radioEndpoint,
                sampleRateKhz,
                numAdc,
                ct,
                boardKind,
                firmware,
                sampleRateExplicit).ConfigureAwait(false);
        }
        finally
        {
            _radio.RadioLifecycleGate.Release();
        }
    }

    /// <summary>
    /// Connects while the caller owns <see cref="RadioService.RadioLifecycleGate"/>.
    /// Used by the P2 reclaim endpoint so stop, stable-Idle verification, and
    /// start form one server-side lifecycle transaction.
    /// </summary>
    internal async Task<int> ConnectP2WithLifecycleGateHeldAsync(
        IPEndPoint radioEndpoint,
        int sampleRateKhz,
        byte numAdc,
        CancellationToken ct,
        HpsdrBoardKind boardKind = HpsdrBoardKind.Unknown,
        string? firmware = null,
        bool sampleRateExplicit = true)
    {
        try
        {
            await _radio.DisconnectSupersededP1AutomaticRetryAsync().ConfigureAwait(false);
            return await ConnectP2CoreAsync(
                radioEndpoint,
                sampleRateKhz,
                numAdc,
                ct,
                boardKind,
                firmware,
                sampleRateExplicit).ConfigureAwait(false);
        }
        catch
        {
            var failedClient = _p2ConnectingClient;
            if (failedClient is not null)
            {
                if (ReferenceEquals(_p2Client, failedClient))
                    await DisconnectP2CoreAsync(CancellationToken.None).ConfigureAwait(false);
                else
                    await DisposeUnpublishedP2ClientAsync(failedClient).ConfigureAwait(false);
            }
            throw;
        }
        finally
        {
            _p2ConnectingClient = null;
        }
    }

    private async Task<int> ConnectP2CoreAsync(
        IPEndPoint radioEndpoint,
        int sampleRateKhz,
        byte numAdc,
        CancellationToken ct,
        HpsdrBoardKind boardKind,
        string? firmware,
        bool sampleRateExplicit)
    {
        if (_p2Client is not null)
        {
            var snap = _radio.Snapshot();
            if (string.Equals(snap.ConnectedProtocol, "P2", StringComparison.OrdinalIgnoreCase)
                && RadioService.TryParseEndpoint(snap.Endpoint ?? string.Empty, out var currentEndpoint)
                && currentEndpoint.Address.Equals(radioEndpoint.Address))
            {
                return Math.Max(1, snap.SampleRate / 1000);
            }
            throw new InvalidOperationException("Already connected (P2).");
        }
        if (_radio.ActiveClient is not null)
            throw new InvalidOperationException("Already connected (P1). Disconnect first.");

        boardKind = RadioService.ResolveProtocol2BoardKind(boardKind);
        var client = new Zeus.Protocol2.Protocol2Client(
            _loggerFactory.CreateLogger<Zeus.Protocol2.Protocol2Client>());
        _p2ConnectingClient = client;
        client.SetNumAdc(numAdc);
        // Tell the P2 client which board it's talking to so RX-decode quirks
        // (Hermes-on-P2 48 kHz IQ gain correction; future per-board branches)
        // are gated correctly.
        client.SetBoardKind(boardKind);
        // 0x0A wire-byte alias variant (issue #218). For non-OrionMkII
        // boards the value is ignored; for OrionMkII it picks the right
        // calibration/PA constants AND unlocks the Anvelina-PRO3 DX OC
        // byte-1397 write (issue #407) when the operator has selected
        // AnvelinaPro3 in the radio chooser.
        client.SetOrionMkIIVariant(_radio.EffectiveOrionMkIIVariant);
        await client.ConnectAsync(radioEndpoint, ct).ConfigureAwait(false);
        // Seed the operator's RX front-end (preamp + step attenuator) BEFORE
        // StartAsync so the very first CmdHighPriority emitted inside the
        // start sequence carries the correct values. The setters below
        // pre-StartAsync only stash into private fields (the early-return on
        // _rxTask==null path), so no wire packets fly here — they ride the
        // CmdHighPriority(run=1) inside StartAsync below. Without this seed
        // a P2 reconnect would leave the radio at preamp=off / atten=0
        // until the operator nudged either control. Issue #126.
        bool initialPreamp = _radio.PreampOn;
        int initialAttDb = _radio.EffectiveAttenDb;
        int initialAttenuatorAdc = RadioService.ReceiverAdcSource(
            _radio.Snapshot(),
            0) == 1 ? 1 : 0;
        client.SetPreamp(initialPreamp);
        // Frequency-correction factor (issue #325) — rehydrate before the
        // first CmdHighPriority(run=1) so the operator's calibration applies
        // to the very first NCO phase-word. 1.0 = factory default, no-op.
        client.SetFrequencyCorrectionFactor(_radio.GetFrequencyCorrectionFactor());
        // ANAN-G2/Saturn ADC dither/random options live in CmdRx bytes 5/6.
        // Seed before StartAsync so the first receive-specific command
        // matches the persisted setting; RadioService also replays after
        // MarkProtocol2Connected and on live setting changes.
        _radio.ApplyG2AdcOptionsToP2Client(client, boardKind);
        SetP2Attenuator(client, initialAttenuatorAdc, initialAttDb);
        client.AttachWidebandFrameHandler(OnP2WidebandFrame);
        bool initialWidebandTransport =
            Volatile.Read(ref _widebandDisplayEnabled) != 0 && _hub.DisplayStreamRequested;
        Volatile.Write(ref _widebandTransportEnabled, initialWidebandTransport ? 1 : 0);
        Volatile.Write(ref _p2WidebandTransportEnabled, initialWidebandTransport ? 1 : 0);
        client.SetWidebandDisplayEnabled(initialWidebandTransport);

        int rateHz = _radio.ResolveConnectSampleRateHz(
            boardKind,
            sampleRateKhz * 1000,
            protocol2: true,
            requestedExplicitly: sampleRateExplicit);
        sampleRateKhz = rateHz / 1000;
        await client.StartAsync(sampleRateKhz, ct).ConfigureAwait(false);

        IDspEngine newEngine;
        int newChannelId;
        try
        {
            var wdsp = new WdspDspEngine(_loggerFactory.CreateLogger<WdspDspEngine>(), _rxAnalyzerFftSize);
            newChannelId = wdsp.OpenChannel(rateHz, _panadapterWidth);
            // Seed the operator's persisted TX display config before TXA opens
            // so the analyzer comes up at their FFT/window/smoothing. Display-only.
            SeedTxDisplayConfig(wdsp);
            // G2 MkII DUC on P2 expects 192 kHz TX IQ. WDSP upsamples internally
            // (48k mic → 96k DSP → 192k out) and CFIR compensates the sinc
            // droop. Feeding 48 kHz IQ to a 192 kHz DUC as we did before
            // produced 8-10 kHz close-in spurs around the carrier.
            wdsp.OpenTxChannel(outputRateHz: 192_000);
            // Best-effort apply. Some local WDSP builds are missing newer
            // entry points (e.g. SetRXAEMNRpost2Run); the channel itself is
            // open and capable of spectrum work even if a noise-reduction
            // toggle can't be set. Narrow catch so a genuinely broken engine
            // still surfaces via the outer handler.
            try { ApplyStateToNewChannel(wdsp, newChannelId); }
            catch (EntryPointNotFoundException ex)
            {
                _log.LogWarning(ex, "dsp.pipeline p2 wdsp missing entry point — partial config applied");
            }
            newEngine = wdsp;
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "dsp.pipeline p2 wdsp open failed, falling back to synthetic engine");
            var synth = new SyntheticDspEngine();
            newChannelId = synth.OpenChannel(rateHz, _panadapterWidth);
            try { ApplyStateToNewChannel(synth, newChannelId); }
            catch (EntryPointNotFoundException) { }
            newEngine = synth;
        }

        IDspEngine? old;
        int oldChannel;
        lock (_engineLock)
        {
            old = _engine;
            oldChannel = _channelId;
            Volatile.Write(ref _engine, newEngine);
            Volatile.Write(ref _channelId, newChannelId);
            ResetSecondaryRxChannels();
            Volatile.Write(ref _sampleRateHz, rateHz);
        }
        TeardownEngine(old, oldChannel);
        _log.LogInformation("dsp.pipeline p2 engine={Engine} rate={Rate}", newEngine.GetType().Name, rateHz);
        RaiseEngineChanged(newEngine);

        long generation = Interlocked.Increment(ref _p2ConnectionGeneration);
        Action disconnectedHandler = () => OnP2ClientDisconnected(client, generation);
        client.Disconnected += disconnectedHandler;
        _p2DisconnectedHandler = disconnectedHandler;
        _p2Client = client;
        client.ConfigureTxIqSafetyGate(_txEgressGate.IsCurrent);
        RefreshWidebandDisplayState();
        // Sync the change-detect cache with the values we just seeded so the
        // first OnRadioStateChanged after connect doesn't redundantly re-push
        // (which would emit a duplicate CmdHighPriority). Re-read in case the
        // operator changed either control during the connect window — the
        // PreampChanged / StateChanged handlers would have early-returned on
        // _p2Client==null. Comparing here recovers any drift before the cache
        // settles.
        _appliedPreampOn = initialPreamp;
        _appliedEffectiveAttDb = initialAttDb;
        _appliedAttenuatorAdc = initialAttenuatorAdc;
        bool nowPreamp = _radio.PreampOn;
        int nowAttDb = _radio.EffectiveAttenDb;
        int nowAttenuatorAdc = RadioService.ReceiverAdcSource(
            _radio.Snapshot(),
            0) == 1 ? 1 : 0;
        if (nowPreamp != initialPreamp)
        {
            client.SetPreamp(nowPreamp);
            _appliedPreampOn = nowPreamp;
        }
        if (nowAttDb != initialAttDb || nowAttenuatorAdc != initialAttenuatorAdc)
        {
            if (nowAttenuatorAdc != initialAttenuatorAdc)
            {
                if (initialAttenuatorAdc == 0)
                    client.SetAttenuator(0);
                else
                    _radio.ApplyG2AdcOptionsToP2Client(client, boardKind);
            }
            SetP2Attenuator(client, nowAttenuatorAdc, nowAttDb);
            _appliedEffectiveAttDb = nowAttDb;
            _appliedAttenuatorAdc = nowAttenuatorAdc;
        }
        // iter5: attach as the synchronous RX sink. See AttachRxSinkP1 in
        // OnRadioConnected for full rationale — same lock-free hot path.
        AttachRxSinkP2(client);
        // Force the next OnRadioStateChanged to re-push every PS field into
        // the freshly-opened WdspDspEngine instance, regardless of whether
        // the canonical state in StateDto has changed since the prior
        // session. The new engine starts with field defaults (hwPeak=0.4072,
        // timing defaults, etc.) and the change-detect cache `_appliedPs*`
        // doesn't know that — without this flag the engine never gets the
        // operator's settings back, calcc runs on wrong hw_scale, and PS
        // doesn't converge after a reconnect. See
        // `project_ps_reconnect_state_loss.md`.
        _psResyncRequired = true;
        // TX-monitor: same re-push problem as PS — the new engine starts at
        // monitor=off, so if the operator had it on the latch's change-detect
        // would skip the push. Reset the latch so the next UpdateState fires.
        _appliedTxMonitorEnabled = false;
        // Pass the live client so RadioService can fire P2Connected with a
        // reference to the freshly-opened Protocol2Client. TxMetersService
        // subscribes through that event to hook hi-priority status (#174).
        _radio.MarkProtocol2Connected(radioEndpoint.ToString(), rateHz, client, boardKind, firmware);
        // P2 G2/MkII default HW peak = 0.6121; ANAN-7000/8000 = 0.2899. The
        // RadioService switch covers both so we don't bake a value in here.
        // ConnectedBoardKind now returns the discovered board kind when the
        // caller plumbed it through (issue #171); falls back to OrionMkII when
        // the byte wasn't supplied.
        _radio.ApplyPsHwPeakForConnection(isProtocol2: true, _radio.ConnectedBoardKind);
        // Restore the persisted PS feedback attenuation (0..31 dB) before the
        // operator arms PS, so a hot external-tap chain (e.g. RF2K-S −55 dB
        // coupler) doesn't boot at 0 dB and rail the feedback ADC — the
        // saturation that left calcc unable to fit on a fresh connect. No-op
        // when nothing was saved for this board yet.
        if (_radio.GetPersistedPsTxAttnDb() is int txAttn)
        {
            CurrentP2Client?.SetTxAttenuationDb((byte)Math.Clamp(txAttn, 0, 31));
        }
        // Push current PA snapshot into the brand-new client so byte 345 /
        // byte 1401 / CmdGeneral[58] reflect PaSettingsStore from frame 1.
        _radio.ReplayPaSnapshot();
        // Push the persisted audio front-end (TxSpecific bytes 50/51) into the
        // fresh P2 client so mic/line-in/boost/bias/gain are correct from the
        // first CmdTx, not deferred until a store edit. No-op on boards without
        // an audio front-end (gated + OFF defaults).
        _radio.ReplayAudioFrontEnd();
        // Calibration and feedback attenuation are now restored, and the
        // disarmed resync above has initialized the fresh engine. Apply any
        // persisted operator intent through the normal disarm/rearm pipeline.
        _radio.ApplyPsArmIntentForConnection(
            isProtocol2: true,
            _radio.ConnectedBoardKind);
        return sampleRateKhz;
    }

    private async Task DisposeUnpublishedP2ClientAsync(Zeus.Protocol2.Protocol2Client client)
    {
        try { client.SetDisplayDdc(-1, 0, 0); } catch { }
        try { client.SetWidebandDisplayEnabled(false); } catch { }
        try { client.DetachWidebandFrameHandler(); } catch { }
        try { await client.StopAsync(CancellationToken.None).ConfigureAwait(false); } catch { }
        try { await client.DisposeAsync().ConfigureAwait(false); } catch { }
        Volatile.Write(ref _widebandTransportEnabled, 0);
        Volatile.Write(ref _p2WidebandTransportEnabled, 0);
        Volatile.Write(ref _widebandDetailReady, 0);
        Interlocked.Exchange(ref _widebandDetailLastIqMs, long.MinValue);
        Interlocked.Increment(ref _widebandSourceGeneration);
    }

    private void OnP2ClientDisconnected(Zeus.Protocol2.Protocol2Client client, long generation)
    {
        // The protocol event is raised on its RX thread.  Never join that
        // thread inline, and never let a delayed event from an older session
        // tear down a replacement connection.
        _ = Task.Run(async () =>
        {
            try { await DisconnectP2IfCurrentAsync(client, generation).ConfigureAwait(false); }
            catch (Exception ex) { _log.LogWarning(ex, "dsp.pipeline p2 automatic disconnect failed"); }
        });
    }

    private async Task DisconnectP2IfCurrentAsync(
        Zeus.Protocol2.Protocol2Client client,
        long generation)
    {
        await _radio.RadioLifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            // Recheck only after owning the lifecycle gate. A manual disconnect
            // and reconnect may have completed while this callback was queued.
            if (Volatile.Read(ref _p2ConnectionGeneration) != generation
                || !ReferenceEquals(_p2Client, client))
                return;
            await DisconnectP2CoreAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _radio.RadioLifecycleGate.Release();
        }
    }

    private void OnPaSnapshotChanged(PaRuntimeSnapshot snap)
    {
        var p2 = _p2Client;
        if (p2 is null) return;
        // The operator can select Anvelina after connecting. Keep the live
        // variant in step with the profile that supplied this snapshot.
        var variant = _radio.EffectiveOrionMkIIVariant;
        p2.SetOrionMkIIVariant(variant);
        p2.SetDriveByte(snap.DriveByte);
        p2.SetOcMasks(snap.OcTxMask, snap.OcRxMask, snap.OcTuneMask);
        // Anvelina-PRO3 DX OC masks (#407). Always forwarded; Protocol2Client
        // gates whether they hit byte 1397 on the wire by checking the
        // connected board+variant. Non-Anvelina P2 boards see byte 1397
        // stay at zero per EU2AV's reserved-bit rule.
        p2.SetOcDxMasks(snap.OcDxTxMask, snap.OcDxRxMask);
        p2.SetPaEnabled(snap.PaEnabled);
        p2.SetXvtrEnabled(snap.XvtrEnabled);
        // Limit the hybrid PA/filter correction to a selected Anvelina on its
        // discovered transport. Other profiles retain their existing encoding;
        // an incomplete connection must not replace discovery with Unknown.
        var connectedBoard = _radio.ConnectedBoardKind;
        HpsdrBoardKind? filterBoard =
            _radio.DiscoveredBoardKind == HpsdrBoardKind.OrionMkII
            && variant == OrionMkIIVariant.AnvelinaPro3
            && connectedBoard != HpsdrBoardKind.Unknown
                ? connectedBoard
                : null;
        p2.SetRfFilters(snap.RfFilters, filterBoard);
        // External antenna (antenna slice — #804). HpsdrAntenna.Ant1=0 → wire 1
        // → ALEX_TX_ANTENNA_1, so the +1 maps the 0-based enum to the 1-based
        // wire selector. SetAntennas gates the TX-antenna emission on
        // HasTxAntennaRelays, defers a mid-key relay change to the unkey edge,
        // and routes the operator RX-aux strictly BEFORE the PS coupler OR
        // (the PS-K36 firewall lives in Protocol2Client.SendCmdHighPriority).
        p2.SetAntennas(
            (int)snap.TxAntenna + 1,
            (int)snap.RxAntenna + 1,
            snap.HasTxAntennaRelays,
            snap.RxAuxInput,
            snap.MkiiBpfRxSelect,
            BoardCapabilitiesTable.HasXvtrTxRelay(
                _radio.EffectiveBoardKind,
                _radio.EffectiveOrionMkIIVariant));
    }

    private void OnAudioFrontEndChanged(AudioFrontEndPush a)
    {
        // Route the radio-mic STREAM (external-audio-jacks re-port, §3). This
        // runs on EVERY resolved-source push (store edit AND connect, via
        // ReplayAudioFrontEnd) so the pipeline's active source tracks the wire
        // bytes. Done BEFORE the byte forward and independent of _p2Client so a
        // switch back to Host always quiesces the gate even mid-disconnect.
        ApplyRadioMicRouting(a.Source);

        var p2 = _p2Client;
        if (p2 is null) return;
        // TxSpecific byte 50 mic_control flags + byte 51 line_in_gain, already
        // RESOLVED (board-clamped + source-encoded, Host → 0) by
        // RadioService.PushAudioFrontEnd. The forwarder pushes the literal bytes
        // with no source interpretation of its own.
        p2.SetAudioFrontEndBytes(a.MicControlByte, a.LineInGain);
    }

    // Arm/disarm the radio-mic stream for a resolved TxAudioSource
    // (external-audio-jacks re-port, §3, atomic single-select). Idempotent and
    // cheap on Host (the common case): it sets the in-lock _activeSource on
    // TxAudioIngest (which quiesces its WDSP accumulator), resets the 1026
    // re-blocker so no pre-switch audio stitches onto the post-switch source,
    // and attaches / detaches the UDP-1026 decode on the live P2 client so there
    // is zero added RX cost while Host is selected. P1 codec radios (ANAN-10E /
    // Hermes / ANAN-100D/200D) carry the codec samples inline in EP6, so the P1
    // path also attaches a per-packet mic extractor on the active P1 client
    // (issue #992); the audio capability overlay also admits an explicitly
    // installed HL2+ codec while ordinary HL2 stays host-only.
    private void ApplyRadioMicRouting(TxAudioSource source)
    {
        var ingest = ResolveTxIngest();
        if (ingest is null) return;

        bool radioActive = source != TxAudioSource.Host;

        // Arm the in-lock single-select gate FIRST. Under TxAudioIngest._sync
        // this flips _activeSource and clears its half-written WDSP block, so the
        // instant the gate is armed the host mic is dropped (radio armed) or the
        // radio mic is dropped (Host armed) — no overlap window.
        ingest.SetActiveSource(source);

        // Always reset the re-blockers on any switch so a <960-sample remainder
        // of the old source can't stitch onto the new one.
        _radioMicReceiver?.Reset();
        _p1RadioMicReceiver?.Reset();

        var p2 = _p2Client;
        var p1 = _attachedSinkP1;
        if (radioActive)
        {
            // Subscribe the 1026 decode only while a radio jack is armed. No-op
            // if already attached or if P1 (no 1026 stream / null p2Client).
            if (!_radioMicAttached && p2 is not null && _radioMicReceiver is not null)
            {
                var rb = _radioMicReceiver;
                p2.AttachRadioMicHandler(rb.Accept);
                _radioMicAttached = true;
            }
            // P1 codec mic extraction, including the optional HL2+ AK4951.
            // Ordinary HL2 still has no codec and does not attach a handler.
            if (!_p1RadioMicAttached && p1 is not null && _p1RadioMicReceiver is not null
                && _radio.AudioCapabilities.HasOnboardCodec)
            {
                var rb = _p1RadioMicReceiver;
                p1.AttachRadioMicHandler(rb.Accept);
                _p1RadioMicAttached = true;
            }
        }
        else
        {
            if (_radioMicAttached)
            {
                p2?.DetachRadioMicHandler();
                _radioMicAttached = false;
            }
            if (_p1RadioMicAttached)
            {
                p1?.DetachRadioMicHandler();
                _p1RadioMicAttached = false;
            }
        }
    }

    private void OnRadioMoxChanged(bool on)
    {
        // Clear before publishing the new keyed state so the first tick of a
        // new transmission cannot observe a hold from the prior over.
        _lastTxPanValid = false;
        _lastTxWfValid = false;
        _keyed = on;
        // Normal MOX-off drops the radio wire before SetMox(false) tears down
        // WDSP. Arm display suppression here too so the RX fallback cannot leak
        // a transition FFT in that small sequencing window.
        if (on)
            Volatile.Write(ref _rxPostTxDisplayFramesRemaining, 0);
        else
            Volatile.Write(
                ref _rxPostTxDisplayFramesRemaining,
                PostTxMuteBlocksForDelayMs(_radio.TxPostTxRxMuteDelayMs));
        // Measurement-only: stamp the MOX edge for TX-turnaround latency. No
        // effect on the TX IQ path — pure observation.
        _txTurnaround?.OnMoxEdge(on);
        _p2Client?.SetMox(on);
        // Reset the FreeDV RECEIVER on both MOX edges so it resumes empty and
        // unsynced. WDSP RX is drained every tick regardless of MOX, so without
        // this the modem keeps decoding the operator's own transmission during
        // the over and the resuming RX dumps that self-decoded backlog at
        // un-key — an end-of-over garble in Zeus's own audio, on both RADE and
        // codec2. Key-down drops any pre-TX residual; key-up clears anything
        // decoded from TX bleed. No-op when FreeDV isn't engaged.
        _audioModem.FlushRx();
        // Falling edge: pick up any PS knob changes that OnRadioStateChanged
        // deferred while we were keyed (HwPeak / Advanced / Control).
        // Without this re-trigger a deferred change would sit unapplied until
        // the next unrelated StateChanged event, which could be several seconds
        // away. The state-change handler is idempotent against equality checks,
        // so re-invoking it when nothing was deferred is harmless.
        if (!on)
        {
            try { OnRadioStateChanged(_radio.Snapshot()); }
            catch (Exception ex) { _log.LogWarning(ex, "dsp.pipeline mox-off restate failed"); }
        }
    }

    private void OnRadioTunActiveChanged(bool on)
    {
        _p2Client?.SetTune(on);
    }

    // Mirror operator preamp toggles into a live Protocol2Client. P1 is
    // pushed by RadioService.SetPreamp directly via ActiveClient. PreampOn
    // isn't on the StateDto wire format, so this event-driven path is the
    // only way the bit reaches CmdHighPriority byte 1403 on P2 (issue #126).
    private void OnRadioPreampChanged(bool on)
    {
        var p2 = _p2Client;
        if (p2 is null) return;
        if (on == _appliedPreampOn) return;
        p2.SetPreamp(on);
        _appliedPreampOn = on;
    }

    // Operator changed the DDC sample rate (display bandwidth) while connected.
    // P1's rate is already on the wire via RadioService → ActiveClient; here we
    // handle the P2 side, which RadioService can't reach (ActiveClient is null
    // on P2). The whole re-rate is posted to the DSP thread (PostDspCommand →
    // DrainDspCommands, run between RX frames on the same thread that calls
    // FeedIq) so it never races the hot path. See RerateRxChannelForP2 for why
    // it must be in-place on the existing engine.
    private void OnRadioSampleRateChanged(int rateHz)
    {
        if (_p2Client is null) return;
        PostDspCommand(() => RerateRxChannelForP2(rateHz));
    }

    // Re-rate the RX channel to a new DDC input rate, IN PLACE on the existing
    // engine instance. Two hazards this avoids, both of which produced the
    // 0xc0000005 native crash on the first naive attempt:
    //
    //   1. Channel aliasing. WdspDspEngine.OpenChannel allocates the first free
    //      id from its *per-instance* _channels dict, but WDSP's channel table
    //      is global/native. A second engine instance would therefore re-open
    //      global channel 0 — the slot the old engine still owns — and tearing
    //      the old engine down would free the channel the new one is using.
    //      Closing then re-opening on the SAME instance reuses id 0 cleanly
    //      (the rebuild WdspDspEngine.OpenChannel was written to support — it
    //      re-applies the notch DB after a "sample-rate or mode change").
    //   2. Hot-path teardown. CloseChannel stops the channel worker; doing it
    //      on the DSP thread (this runs inside DrainDspCommands) means no FeedIq
    //      is in flight on the channel being torn down.
    //
    // FeedIq no-ops on a missing channel id, so the brief CloseChannel→OpenChannel
    // window is safe even though it isn't atomic with _channelId.
    private void RerateRxChannelForP2(int rateHz)
    {
        var p2 = _p2Client;
        if (p2 is null) return; // disconnected between post and drain
        var engine = Volatile.Read(ref _engine);
        if (engine is null) return;

        int oldChannel = Volatile.Read(ref _channelId);
        try
        {
            p2.SetDisplayDdc(-1, 0, 0);
            CloseWidebandDetailChannel(engine);
            // Re-rate keeps the SAME engine, so close every open secondary channel
            // here (they reopen at the new rate via EnsureSecondaryRxChannel below).
            for (int i = 1; i < MaxReceivers; i++)
            {
                int sc = Volatile.Read(ref _secondaryRx[i].ChannelId);
                if (sc < 0) continue;
                Volatile.Write(ref _secondaryRx[i].ChannelId, -1);
                try { engine.CloseChannel(sc); } catch { /* best-effort */ }
            }
            engine.CloseChannel(oldChannel);
            int newChannel = engine.OpenChannel(rateHz, _panadapterWidth);
            try { ApplyStateToNewChannel(engine, newChannel); }
            catch (EntryPointNotFoundException ex)
            {
                _log.LogWarning(ex, "dsp.pipeline p2 re-rate missing entry point — partial config applied");
            }
            Volatile.Write(ref _channelId, newChannel);
            Volatile.Write(ref _sampleRateHz, rateHz);
            var state = _radio.Snapshot();
            for (int i = 1; i < MaxReceivers; i++)
                _ = EnsureSecondaryRxChannel(engine, i, state);
            // RX channel is ready at the new rate — now tell the radio to re-rate
            // its DDC (re-emits the RX-spec). Ordering this last means new-rate
            // IQ only starts arriving once the channel can decode it.
            p2.SetSampleRateKhz(rateHz / 1000);
            ReconcileWidebandDetailSource(engine, state);
            _log.LogInformation("dsp.pipeline p2 re-rate channel={Ch} rate={Rate}", newChannel, rateHz);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "dsp.pipeline p2 re-rate failed rate={Rate}", rateHz);
        }
    }

    // Operator added/removed/changed a manual notch. Forward the full list to
    // the live engine; the engine rewrites WDSP's notch database. A no-op when
    // no engine is up — ApplyStateToNewChannel re-applies on the next connect.
    private void OnRadioNotchesChanged(IReadOnlyList<NotchDto> notches)
    {
        Volatile.Read(ref _engine)?.SetNotches(notches);
    }

    private void OnFrequencyCorrectionFactorChanged(double factor)
    {
        // RadioService handles the P1 client + the re-tune; we only have
        // to forward to the live P2 client here. No-op when no P2 is up.
        _p2Client?.SetFrequencyCorrectionFactor(factor);
    }

    /// <summary>
    /// Forward a WDSP TXA block of interleaved float IQ to the live protocol
    /// client. P2 sends directly to the radio DUC; P3 sends the same IQ
    /// payload into the hosted sidecar's credit-paced TX egress.
    /// </summary>
    public void ForwardTxIqToP2(ReadOnlySpan<float> iqInterleaved)
    {
        long revision = _txEgressGate.CommittedRevision;
        if (!_txEgressGate.IsCurrent(revision)) return;
        _p2Client?.SendTxIq(iqInterleaved, revision);
        if (_radio.IsProtocol3Active)
            _externalRadioSidecar.ForwardTxIq(iqInterleaved, revision);
        // Measurement-only: record the egress instant for TX-turnaround stats
        // after the IQ has been forwarded, so observation never delays egress.
        _txTurnaround?.OnTxIqEgress();
    }

    internal void CommitTxEgress(long revision) => _txEgressGate.Commit(revision);

    internal void RevokeTxEgress()
    {
        _txEgressGate.Revoke();
        _txIqRing?.Clear();
        _p2Client?.ResetTxIqForSafety();
        _externalRadioSidecar.RevokeTxIq();
    }

    /// <summary>
    /// Measurement-only TX-turnaround latency snapshot (MOX-assert →
    /// first-TX-IQ-at-egress and PTT-release → egress-drain), as last value
    /// plus p50/p95 in milliseconds. Surfaced on the sidecar frame forwarder's
    /// diagnostics/Status object. Never null; empty until the first over.
    /// </summary>
    public object TxTurnaroundStatus =>
        _txTurnaround?.Snapshot() ?? new
        {
            overs = 0L,
            moxToFirstIq = new { lastMs = (double?)null, p50Ms = (double?)null, p95Ms = (double?)null, samples = 0 },
            releaseToDrain = new { lastMs = (double?)null, p50Ms = (double?)null, p95Ms = (double?)null, samples = 0 },
        };

    /// <summary>
    /// Protocol 3 RX/display/audio is supplied by the hosted sidecar. Zeus still
    /// owns the local TX-audio-to-IQ engine and opens it explicitly on connect so
    /// MOX/TUN can produce 192 kHz IQ for the existing sidecar ingress. WDSP is
    /// the unchanged default. An external engine is loaded only after explicit
    /// configuration and is never silently replaced with WDSP if loading fails.
    /// </summary>
    public Protocol3TxEngineConnection ConnectP3TxEngine(int sampleRateHz)
    {
        int rateHz = sampleRateHz > 0 ? sampleRateHz : 192_000;
        var mode = _externalDspProvider.Mode;

        var current = Volatile.Read(ref _engine);
        var existingConnection = Volatile.Read(ref _protocol3TxEngineConnection);
        if (current is not null &&
            existingConnection is not null &&
            existingConnection.EngineRateHz == rateHz &&
            existingConnection.External == (mode == Protocol3TxEngineMode.External))
        {
            return existingConnection;
        }

        IDspEngine next;
        string engineName;
        bool external;
        if (mode == Protocol3TxEngineMode.External)
        {
            bool pureSignal = _externalDspProvider.Capabilities.HasFlag(
                ExternalDspEngineCapabilities.PureSignal);
            var externalEngine = _externalDspProvider.CreateEngine(
                new ExternalDspEngineRequest(
                    ExternalDspEngineRole.Transmit,
                    rateHz,
                    _panadapterWidth,
                    TxOutputRateHz: 192_000,
                    PureSignalEnabled: pureSignal),
                out engineName);
            next = new ExternalProtocol3TxDspEngine(externalEngine, pureSignal);
            external = true;
        }
        else
        {
            next = new WdspDspEngine(_loggerFactory.CreateLogger<WdspDspEngine>(), _rxAnalyzerFftSize);
            engineName = "WDSP";
            external = false;
        }

        int channelId = -1;
        try
        {
            channelId = next.OpenChannel(rateHz, _panadapterWidth);
            SeedTxDisplayConfig(next);
            // Saturn/G2 DUC-compatible host-IQ mode is 48 kHz mic to 192 kHz
            // IQ, matching the proven P2 G2 path and the sidecar ingest rate.
            next.OpenTxChannel(outputRateHz: 192_000);
            try { ApplyStateToNewChannel(next, channelId); }
            catch (EntryPointNotFoundException ex) when (!external)
            {
                _log.LogWarning(ex, "dsp.pipeline p3 wdsp missing entry point - partial config applied");
            }

        }
        catch
        {
            TeardownEngine(next, channelId);
            throw;
        }

        IDspEngine? old;
        int oldChannel;
        lock (_engineLock)
        {
            old = _engine;
            oldChannel = _channelId;
            Volatile.Write(ref _engine, next);
            Volatile.Write(ref _channelId, channelId);
            ResetSecondaryRxChannels();
            Volatile.Write(ref _sampleRateHz, rateHz);
        }

        TeardownEngine(old, oldChannel);
        var connection = new Protocol3TxEngineConnection(
            engineName,
            rateHz,
            TxIqRateHz: 192_000,
            external,
            PureSignal: next is ExternalProtocol3TxDspEngine { PureSignalEnabled: true });
        Volatile.Write(ref _protocol3TxEngineConnection, connection);
        _psResyncRequired = true;
        _appliedTxMonitorEnabled = false;
        _log.LogInformation(
            "dsp.pipeline p3 tx engine={Engine} external={External} channel={Id} rxRate={Rate} txIqRate=192000",
            engineName,
            external,
            channelId,
            rateHz);
        RaiseEngineChanged(next);
        OnRadioStateChanged(_radio.Snapshot());
        return connection;
    }

    public Protocol3TxEngineConnection? CurrentProtocol3TxEngine =>
        Volatile.Read(ref _protocol3TxEngineConnection);

    public ExternalDspEngineCapabilities Protocol3ExternalCapabilities =>
        _externalDspProvider.Capabilities;

    public void DisconnectP3TxEngine()
    {
        if (!_radio.IsProtocol3Active && Volatile.Read(ref _engine) is SyntheticDspEngine or OfflinePreviewDspEngine)
            return;

        var disconnectedEngine = CreateDisconnectedEngine(out int channelId);

        IDspEngine? old;
        int oldChannel;
        lock (_engineLock)
        {
            old = _engine;
            oldChannel = _channelId;
            Volatile.Write(ref _engine, disconnectedEngine);
            Volatile.Write(ref _channelId, channelId);
            ResetSecondaryRxChannels();
            Volatile.Write(ref _sampleRateHz, SyntheticSampleRateHz);
        }

        TeardownEngine(old, oldChannel);
        Volatile.Write(ref _protocol3TxEngineConnection, null);
        _psResyncRequired = true;
        _appliedTxMonitorEnabled = false;
        RaiseEngineChanged(disconnectedEngine);
        OnRadioStateChanged(_radio.Snapshot());
        _log.LogInformation("dsp.pipeline p3 tx disconnected, engine={Engine}", disconnectedEngine.GetType().Name);
    }

    public async Task DisconnectP2Async(CancellationToken ct)
    {
        _radio.NotifyOperatorConnectionAction();
        await _radio.RadioLifecycleGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await DisconnectP2CoreAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _radio.RadioLifecycleGate.Release();
        }
    }

    private async Task DisconnectP2CoreAsync(CancellationToken ct)
    {
        var client = _p2Client;
        _p2Client = null;
        Interlocked.Increment(ref _p2ConnectionGeneration);
        var disconnectedHandler = _p2DisconnectedHandler;
        _p2DisconnectedHandler = null;
        if (client is null)
        {
            // Idempotent reconciliation: an earlier partial failure must not
            // leave the shared API state claiming P2 is connected merely
            // because the pipeline no longer has a client to stop.
            if (_radio.IsProtocol2Active)
                _radio.MarkProtocol2Disconnected();
            return;
        }

        if (disconnectedHandler is not null)
            client.Disconnected -= disconnectedHandler;

        // iter5: detach the sink BEFORE the Protocol2Client teardown so any
        // in-flight RxLoop callback completes against the still-valid engine
        // and no further callbacks land. client.StopAsync joins the RX task,
        // so by the time it returns the RX thread is gone.
        DetachRxSinkP2();
        // The 1026 radio-mic handler is bound to this client instance; clear our
        // attach latch so a reconnect (which re-fires ReplayAudioFrontEnd) re-
        // attaches against the fresh client. The handler reference dies with the
        // disposed client. Quiesce the re-blocker so no stale remainder survives.
        _radioMicAttached = false;
        _radioMicReceiver?.Reset();
        try { await client.StopAsync(ct).ConfigureAwait(false); } catch { }
        await client.DisposeAsync().ConfigureAwait(false);

        var disconnectedEngine = CreateDisconnectedEngine(out int channelId);

        IDspEngine? old;
        int oldChannel;
        lock (_engineLock)
        {
            old = _engine;
            oldChannel = _channelId;
            Volatile.Write(ref _engine, disconnectedEngine);
            Volatile.Write(ref _channelId, channelId);
            ResetSecondaryRxChannels();
            Volatile.Write(ref _sampleRateHz, SyntheticSampleRateHz);
        }
        TeardownEngine(old, oldChannel);
        RaiseEngineChanged(disconnectedEngine);
        // Mark PS state for forced re-push on the next ConnectP2Async. The
        // change-detect cache (`_appliedPs*`) is preserved across disconnect
        // — by design, so a reconnect with unchanged operator state doesn't
        // generate spurious wire writes — but a fresh WdspDspEngine starts
        // with field defaults (hwPeak=0.4072, timing defaults, etc.) that don't
        // match the canonical state. Without this flag, OnRadioStateChanged
        // skips every PS push because s.PsX == _appliedPsX, and the new
        // engine never gets the operator's settings. See
        // `project_ps_reconnect_state_loss.md` for the rack reproduction.
        _psResyncRequired = true;
        _appliedTxMonitorEnabled = false;
        OnRadioStateChanged(_radio.Snapshot());
        _radio.MarkProtocol2Disconnected();
        _log.LogInformation("dsp.pipeline p2 disconnected, engine={Engine}", disconnectedEngine.GetType().Name);
    }

    public Zeus.Protocol2.Protocol2Client? ActiveP2Client => _p2Client;

    /// <summary>
    /// Live per-DDC / per-receiver RX ingest health for the diagnostics surface
    /// (overflow/underrun verification at high sample rate + multi-DDC). Reads
    /// only cached, lock-free snapshots — no realtime/WDSP work on this path:
    ///   * per-WDSP-channel ingest health (queue depth/cap, frames-in, queue-full,
    ///     dropped-oldest, worker avg/max ms vs the per-frame budget, audio
    ///     overrun) from <see cref="WdspDspEngine.SnapshotRxChannels"/>; and
    ///   * per-DDC UDP packet rate (last ~1 s window) from the active P2 client,
    ///     indexed by DDC (0..MaxRxDdc-1) — the ground truth for which DDCs the
    ///     radio is actually streaming.
    /// </summary>
    public object SnapshotRxIngestHealth()
    {
        var channels = (CurrentEngine as WdspDspEngine)?.SnapshotRxChannels()
                       ?? (IReadOnlyList<WdspDspEngine.RxChannelHealth>)System.Array.Empty<WdspDspEngine.RxChannelHealth>();
        long[] portRates = ActiveP2Client?.SnapshotRxPortPacketRates() ?? System.Array.Empty<long>();

        var channelDtos = new object[channels.Count];
        for (int i = 0; i < channels.Count; i++)
        {
            var h = channels[i];
            // Per-frame WDSP budget: a 512-sample frame must be processed in
            // (1000·512/rate) ms on average or the queue backs up. workerMaxMs
            // approaching this is the realtime "CPU-bound" signal.
            double frameBudgetMs = h.SampleRateHz > 0 ? 1000.0 * 512.0 / h.SampleRateHz : 0.0;
            double headroomPct = frameBudgetMs > 0
                ? System.Math.Round(100.0 * (1.0 - h.WorkerMaxMs / frameBudgetMs), 1)
                : 0.0;
            channelDtos[i] = new
            {
                channelId = h.ChannelId,
                sampleRateHz = h.SampleRateHz,
                queueDepth = h.QueueDepth,
                queueCapacity = h.QueueCapacity,
                framesInPerWindow = h.FramesInPerWindow,
                queueFullPerWindow = h.QueueFullPerWindow,
                droppedPerWindow = h.DroppedPerWindow,
                workerFramesPerWindow = h.WorkerFramesPerWindow,
                workerAvgMs = System.Math.Round(h.WorkerAvgMs, 3),
                workerMaxMs = System.Math.Round(h.WorkerMaxMs, 3),
                frameBudgetMs = System.Math.Round(frameBudgetMs, 3),
                workerHeadroomPct = headroomPct,
                audioRingDepth = h.AudioRingDepth,
                audioOverrunPerWindow = h.AudioOverrunPerWindow,
                ageMs = h.AgeMs,
            };
        }

        // Pipeline's raw per-receiver WDSP channel ids (not gated on health
        // emission like `channels` above). -1 = receiver not open. Lets the
        // diagnostics distinguish "DDC streaming but no channel" (a wiring bug)
        // from "channel open but starved".
        var secondaryIds = new object[MaxReceivers - 1];
        for (int i = 1; i < MaxReceivers; i++)
            secondaryIds[i - 1] = new
            {
                receiverIndex = i,
                channelId = Volatile.Read(ref _secondaryRx[i].ChannelId),
                routedFrames = _secondaryRx[i].RoutedFrames,
                fedFrames = _secondaryRx[i].FedFrames,
            };

        return new
        {
            schemaVersion = 1,
            maxRxDdc = Zeus.Protocol2.Protocol2Client.MaxRxDdc,
            activeChannels = channels.Count,
            rxPortPacketRates = portRates,
            primaryChannelId = Volatile.Read(ref _channelId),
            secondaryRxChannelIds = secondaryIds,
            channels = channelDtos,
        };
    }

    /// <summary>
    /// Panadapter pixel column width — exposed so the frequency-calibration
    /// service (issue #325) can size its capture buffer correctly without
    /// hard-coding the constant.
    /// </summary>
    public static int PanadapterWidth => DisplayPerformanceOptions.DefaultPanadapterWidth;

    /// <summary>Resolved panadapter pixel column width for this pipeline.</summary>
    public int ConfiguredPanadapterWidth => _panadapterWidth;

    /// <summary>
    /// Reads the latest cached panadapter snapshot (dB values, display
    /// order — low frequency left). Caches are filled by <see cref="Tick"/>
    /// at 30 Hz; the frequency-calibration service (issue #325) reads from
    /// here to avoid racing for WDSP's once-per-frame "fresh data" flag,
    /// which Tick is also consuming and would always win.
    /// </summary>
    /// <param name="dest">Buffer of length <see cref="ConfiguredPanadapterWidth"/>.</param>
    /// <param name="hzPerPixel">Hz spacing between adjacent pixels (out).</param>
    /// <param name="centerHz">Frequency of the centre pixel — the radio's LO
    /// (out). In CW modes this is dial ± cw_pitch; outside CW it equals dial.</param>
    /// <param name="maxAgeMs">Reject the cached snapshot if it is older than
    /// this many milliseconds. Default 200 ms — six analyzer frames at 30 Hz,
    /// generous tolerance for a one-off cal measurement without risking
    /// pre-tune stale data.</param>
    public bool TryCapturePanadapterSnapshot(
        Span<float> dest,
        out float hzPerPixel,
        out long centerHz,
        long maxAgeMs = 200)
    {
        return TryCapturePanadapterSnapshot(
            dest, out hzPerPixel, out centerHz, out _, maxAgeMs);
    }

    /// <summary>
    /// Reads the latest cached panadapter snapshot and returns its monotonically
    /// increasing publication version. Consumers that need independent FFTs
    /// can reject a version they have already processed.
    /// </summary>
    public bool TryCapturePanadapterSnapshot(
        Span<float> dest,
        out float hzPerPixel,
        out long centerHz,
        out long snapshotVersion,
        long maxAgeMs = 200)
    {
        hzPerPixel = 0;
        centerHz = 0;
        snapshotVersion = 0;
        if (dest.Length != _panadapterWidth) return false;

        lock (_calPanLock)
        {
            if (_calPanSnapshotMs == 0) return false;
            long ageMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - _calPanSnapshotMs;
            if (ageMs > maxAgeMs) return false;

            _calPanSnapshot.AsSpan().CopyTo(dest);
            hzPerPixel = _calPanHzPerPixel;
            centerHz = _calPanCenterHz;
            snapshotVersion = _calPanSnapshotVersion;
        }
        return true;
    }

    /// <summary>
    /// Drives the Thetis-faithful noise-floor tracker (AutoAgcNoiseFloorTracker,
    /// a port of Thetis display.cs processNoiseFloor) once per meter tick and
    /// returns the SETTLED band floor in raw display dB (caller adds the board
    /// RX cal offset), or NaN when the tracker has not settled — during
    /// fast-attack, before the first sample, or while keyed — so RadioService
    /// holds its current offset exactly as Thetis's auto-AGC timer skips ticks
    /// whose IsNoiseFloorGood is false (console.cs:46066).
    ///
    /// Feed arbitration (meter tick = 5 Hz): a fresh panadapter snapshot
    /// (&le;300 ms) feeds the gated-bin estimator; a brief gap holds (no
    /// sample); only a SUSTAINED outage (&gt; <see cref="AutoAgcSpectrumStaleMs"/>)
    /// feeds the S-meter proxy via <paramref name="sMeterDbm"/> so engines
    /// without a spectrum still track. Fast-attack triggers are detected from
    /// the state snapshot the tick already reads: band-scale VFO jump, preamp
    /// flip, attenuator step, and the auto-enabled rising edge (Thetis
    /// display.cs:831-923). TX pauses the estimator (Thetis: no noise-floor
    /// processing while MOX).
    /// </summary>
    private double UpdateAutoAgcNoiseFloorDbm(StateDto state, double sMeterDbm, bool keyed, long nowMs, out bool spectrumSourced)
    {
        lock (_autoAgcTrackerLock)
        {
            return UpdateAutoAgcNoiseFloorDbmLocked(state, sMeterDbm, keyed, nowMs, out spectrumSourced);
        }
    }

    private double UpdateAutoAgcNoiseFloorDbmLocked(StateDto state, double sMeterDbm, bool keyed, long nowMs, out bool spectrumSourced)
    {
        spectrumSourced = false;
        if (!state.AutoAgcEnabled)
        {
            if (_autoAgcWasEnabled)
            {
                _autoAgcTracker.Reset();
                _autoAgcWasEnabled = false;
                _autoAgcLastVfoHz = long.MinValue;
                _autoAgcLastAttenDb = int.MinValue;
                _autoAgcLastSpectrumFeedMs = long.MinValue;
                _autoAgcFeedSource = 0;
            }
            return double.NaN;
        }

        // No real radio engine (synthetic placeholder before first connect, or
        // the offline-preview engine after a disconnect): there is no band to
        // measure and the S-meter is a silence proxy, so engaging auto would
        // crawl the gain to a rail off garbage. Hold instead. P3 is exempt —
        // its meter rides the sidecar and its engine never has a spectrum by
        // design, so the S-meter fallback below is its normal path.
        if (!_radio.IsProtocol3Active && Volatile.Read(ref _engine) is not WdspDspEngine)
            return double.NaN;

        // Fast-attack triggers (Thetis FastAttackNoiseFloorRX1 setters).
        if (!_autoAgcWasEnabled)
        {
            _autoAgcWasEnabled = true;
            _autoAgcTracker.FastAttack(nowMs);
        }
        else
        {
            if (_autoAgcLastVfoHz != long.MinValue &&
                Math.Abs(state.VfoHz - _autoAgcLastVfoHz) > AutoAgcFastAttackVfoDeltaHz)
                _autoAgcTracker.FastAttack(nowMs);
            if (state.PreampOn != _autoAgcLastPreampOn)
                _autoAgcTracker.FastAttack(nowMs);
            // Manual attenuator step only — EffectiveAttenDb would fast-attack
            // on every auto-ATT ramp tick; the lerp follows that smoothly.
            if (_autoAgcLastAttenDb != int.MinValue && state.AttenDb != _autoAgcLastAttenDb)
                _autoAgcTracker.FastAttack(nowMs);
        }
        _autoAgcLastVfoHz = state.VfoHz;
        _autoAgcLastPreampOn = state.PreampOn;
        _autoAgcLastAttenDb = state.AttenDb;

        // Thetis processes no noise floor while transmitting.
        if (keyed) return double.NaN;

        int feedSource;
        if (TryCapturePanadapterSnapshot(_autoAgcFloorBuf, out _, out _, maxAgeMs: 300))
        {
            _autoAgcLastSpectrumFeedMs = nowMs;
            _autoAgcTracker.AddBins(_autoAgcFloorBuf, nowMs);
            feedSource = 1;
        }
        else if (_autoAgcLastSpectrumFeedMs != long.MinValue &&
                 nowMs - _autoAgcLastSpectrumFeedMs <= AutoAgcSpectrumStaleMs)
        {
            // Brief gap (stale frame): hold — no sample this tick.
            feedSource = 0;
        }
        else if (double.IsFinite(sMeterDbm) && sMeterDbm > -250.0)
        {
            // Sustained outage (engine genuinely produces no spectrum): S-meter
            // fallback, smoothed by the tracker so it cannot step the gain.
            _autoAgcTracker.AddScalar(sMeterDbm, nowMs);
            feedSource = 2;
        }
        else
        {
            feedSource = 0;
        }

        // The two sources sit on different dBm frames (raw display bins vs
        // calibrated S-meter): never blend them in one lerp — snap on switch
        // (the old window code re-seeded on source change for the same reason).
        if (feedSource != 0)
        {
            if (_autoAgcFeedSource != 0 && _autoAgcFeedSource != feedSource)
                _autoAgcTracker.FastAttack(nowMs);
            _autoAgcFeedSource = feedSource;
        }

        spectrumSourced = _autoAgcFeedSource == 1;
        return _autoAgcTracker.IsGood ? _autoAgcTracker.FloorDbm : double.NaN;
    }

    internal static void FeedProtocol1Iq(
        IDspEngine engine,
        int channel,
        int rx2Channel,
        ReadOnlySpan<double> interleavedIqSamples)
    {
        engine.FeedIq(channel, interleavedIqSamples);
        if (rx2Channel >= 0 && rx2Channel != channel)
            engine.FeedIq(rx2Channel, interleavedIqSamples);
    }

    // Apply a diversity config change (DSP thread). Precomputes the complex
    // weight from gain magnitude + phase so the hot path only does a multiply-add.
    private void ApplyDiversityConfig(DiversityConfig cfg)
    {
        double theta = cfg.PhaseDeg * Math.PI / 180.0;
        _divWeightI = cfg.Gain * Math.Cos(theta);
        _divWeightQ = cfg.Gain * Math.Sin(theta);
        _divReferenceRx = cfg.ReferenceRx;
        _divOutput = cfg.Output;
        _divEnabled = cfg.Enabled;
        _log.LogInformation(
            "dsp.diversity enabled={En} gain={G:F3} phaseDeg={P:F1} sourceRx={Src} referenceRx={Ref} output={Output} weight=({I:F3},{Q:F3})",
            cfg.Enabled, cfg.Gain, cfg.PhaseDeg, cfg.SourceRx, cfg.ReferenceRx, cfg.Output, _divWeightI, _divWeightQ);
    }

    // The source shares RX0's producer-owned lifetime and sample identity.
    // Unpaired or malformed frames pass through raw, never against old IQ.
    private void FeedRx0WithOptionalDiversity(IDspEngine engine, int channel,
        ReadOnlySpan<double> rx0, ReadOnlySpan<double> source)
    {
        if (!_divEnabled || source.IsEmpty || source.Length != rx0.Length)
        {
            engine.FeedIq(channel, rx0);
            return;
        }
        if (_divCombineBuf.Length < rx0.Length) _divCombineBuf = new double[rx0.Length];
        var dest = _divCombineBuf.AsSpan(0, rx0.Length);
        DiversityCombine(rx0, source, _divWeightI, _divWeightQ, dest, _divReferenceRx, _divOutput);
        engine.FeedIq(channel, dest);
    }

    // Complex diversity combine: dest = rx0 + (wI + j·wQ)·src, per IQ pair.
    // The selected reference stays unrotated; the other input takes the weight.
    // Isolated listening selects raw reference/source before weighting.
    // Where the source is shorter than RX0, the RX0 tail passes through unchanged
    // (better an un-combined sample than a dropped one). Pure/static for testing.
    internal static void DiversityCombine(
        ReadOnlySpan<double> rx0, ReadOnlySpan<double> src, double wI, double wQ, Span<double> dest,
        int referenceRx = 0, DiversityOutput output = DiversityOutput.Combined)
    {
        int pairs = rx0.Length / 2;
        int srcPairs = src.Length / 2;
        for (int p = 0; p < pairs; p++)
        {
            double i0 = rx0[2 * p], q0 = rx0[2 * p + 1];
            if (p < srcPairs)
            {
                double si = src[2 * p], sq = src[2 * p + 1];
                if (referenceRx == 1) { (i0, si) = (si, i0); (q0, sq) = (sq, q0); }
                if (output != DiversityOutput.Combined)
                {
                    dest[2 * p] = output == DiversityOutput.Reference ? i0 : si;
                    dest[2 * p + 1] = output == DiversityOutput.Reference ? q0 : sq;
                    continue;
                }
                // (wI + j·wQ)·(si + j·sq)
                double ri = wI * si - wQ * sq;
                double rq = wI * sq + wQ * si;
                dest[2 * p] = i0 + ri;
                dest[2 * p + 1] = q0 + rq;
            }
            else
            {
                dest[2 * p] = i0;
                dest[2 * p + 1] = q0;
            }
        }
        // Defensive: copy any odd trailing scalar (IQ frames are even-length).
        if ((rx0.Length & 1) == 1) dest[rx0.Length - 1] = rx0[rx0.Length - 1];
    }

    // ---- IRxPacketSink (Protocol 1) -----------------------------------------
    // Called synchronously on Protocol1Client.RxLoop's OS thread. The body
    // does, in order:
    //   1) drain the cross-thread DSP command queue,
    //   2) read a snapshot of the engine/channel via Volatile.Read (lock-free
    //      — _engineLock is held only by engine-swap writers and never by
    //      readers on the hot path),
    //   3) feed the IQ into WDSP (RX1, and RX2 when P1 is acting as a
    //      sub-receiver inside the same captured window),
    //   4) fire the RxIqAvailable test seam,
    //   5) return the ArrayPool buffer that Protocol1Client.RxLoop rented,
    //   6) check whether the configured inline display interval has elapsed
    //      since the last Tick and, if so, run Tick INLINE on this thread (no
    //      PeriodicTimer involvement).
    //
    // Exceptions cannot propagate — the protocol client catches and logs at
    // p1.rx.sink_threw, then continues. Sink-thrown exceptions still leak the
    // ArrayPool buffer (the client returns it on our behalf when we throw),
    // so we do our own try/finally inside the body to keep ownership tight.
    void Zeus.Protocol1.IRxPacketSink.OnIqFrame(in Zeus.Protocol1.IqFrame frame)
    {
        try
        {
            DrainDspCommands();
            // iter5 pass-2: lock-free hot path. _engine / _channelId are
            // observed via Volatile.Read; the release fence on _engineLock
            // exit (writer side, OnRadioConnected / ConnectP2Async) plus the
            // full fence on AttachRxSink (Interlocked.Exchange) guarantees
            // the sink sees the freshly-installed engine. See _engineLock
            // doc on the field.
            var engine = Volatile.Read(ref _engine);
            int channel = Volatile.Read(ref _channelId);
            if (engine is not null)
            {
                FeedProtocol1Iq(
                    engine,
                    channel,
                    Volatile.Read(ref _secondaryRx[1].ChannelId),
                    frame.InterleavedSamples.Span);
                RxIqAvailable?.Invoke(0, frame.SampleRateHz, frame.InterleavedSamples);
            }
            MaybeTickInline();
        }
        finally
        {
            // Return the rented buffer regardless of whether the engine was
            // null or the call threw. The protocol client transferred
            // ownership to us on a non-throwing return; we keep ownership
            // here (the try/catch in Protocol1Client.RxLoop will also try
            // to return on our throw, but we don't re-throw — sink-side
            // exceptions are swallowed by the try block above via the
            // MaybeTickInline path catching nothing extra, and any
            // exceptions inside engine.FeedIq propagate to the client's
            // catch which then returns the array — a tolerated rare race).
            if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(
                    frame.InterleavedSamples, out var seg) && seg.Array is { } arr)
            {
                System.Buffers.ArrayPool<double>.Shared.Return(arr);
            }
        }
    }

    void Zeus.Protocol1.IRxPacketSink.OnPsFeedbackFrame(in Zeus.Protocol1.PsFeedbackFrame frame)
    {
        DrainDspCommands();
        var engine = Volatile.Read(ref _engine);
        engine?.FeedPsFeedbackBlock(frame.TxI, frame.TxQ, frame.RxI, frame.RxQ);
        // PS-feedback frames drive the display tick too (see the P2 sink for
        // the full rationale): on a single-ADC time-mux board the user-RX IQ
        // stream — the normal tick pacer — stops entirely for the keyed burst,
        // and without this the panadapter/waterfall freeze for the whole
        // transmission. Same RX-loop thread as OnIqFrame; the elapsed-time
        // throttle inside keeps the configured cadence when both streams flow
        // (HL2 4-DDC keeps DDC0 user RX alive during PS, so this is a no-op
        // there in practice).
        MaybeTickInline();
    }

    // ---- IRxPacketSink (Protocol 2) -----------------------------------------
    // Same shape as P1, but the buffer lifetime is owned by the PRODUCER: when a
    // sink is attached, Protocol2Client.HandleDdcPacket rents the IQ buffer and
    // returns it to the pool in its own finally right after this call returns
    // (safe because everything below consumes the span synchronously). So this
    // sink must NOT retain frame.InterleavedSamples past the call and does not
    // return any buffer itself.
    void Zeus.Protocol2.IRxPacketSink.OnIqFrame(in Zeus.Protocol2.IqFrame frame)
    {
        DrainDspCommands();
        var engine = Volatile.Read(ref _engine);
        if (engine is not null)
        {
            if (frame.ReceiverIndex == Zeus.Protocol2.Protocol2Client.DisplayReceiverIndex)
            {
                int displayChannel = Volatile.Read(ref _widebandDetailChannelId);
                if (displayChannel >= 0)
                {
                    Interlocked.Exchange(ref _widebandDetailLastIqMs, Environment.TickCount64);
                    engine.FeedIq(displayChannel, frame.InterleavedSamples.Span);
                }
                return;
            }
            if (frame.ReceiverIndex >= 1)
            {
                // A secondary receiver's own DDC stream (true independent RX). Feed
                // ONLY that secondary's channel — display/TX cadence is paced by the
                // RX1 frames below, so this path takes no tick. Indexed by receiver
                // index so every DDC routes to its own WDSP channel (today only
                // slot 1 / RX2 is ever active).
                int ri = frame.ReceiverIndex;
                if (ri < MaxReceivers)
                {
                    var rx = _secondaryRx[ri];
                    int secChan = Volatile.Read(ref rx.ChannelId);
                    rx.RoutedFrames++;
                    LogRxIqRms(ri, frame.InterleavedSamples.Span, ref rx.IqRmsLogMs);
                    if (secChan >= 0)
                    {
                        engine.FeedIq(secChan, frame.InterleavedSamples.Span);
                        rx.FedFrames++;
                    }
                }
                return;
            }
            int channel = Volatile.Read(ref _channelId);
            LogRxIqRms(0, frame.InterleavedSamples.Span, ref _rx1IqRmsLogMs);
            FeedRx0WithOptionalDiversity(engine, channel, frame.InterleavedSamples.Span,
                frame.DiversitySourceSamples.Span);
            RxIqAvailable?.Invoke(0, frame.SampleRateHz, frame.InterleavedSamples);
        }
        MaybeTickInline();
    }

    void Zeus.Protocol2.IRxPacketSink.OnPsFeedbackFrame(in Zeus.Protocol2.PsFeedbackFrame frame)
    {
        DrainDspCommands();
        var engine = Volatile.Read(ref _engine);
        engine?.FeedPsFeedbackBlock(frame.TxI, frame.TxQ, frame.RxI, frame.RxQ);
        // Display tick (#960 G2E bench): the display cadence is normally paced
        // by OnIqFrame, and ExecuteAsync's PeriodicTimer stands down while an
        // RX sink is attached. On a single-ADC time-mux board (HermesC10/G2E)
        // a keyed PS burst diverts the operator's ONLY DDC to these feedback
        // frames, so no IQ frame — and therefore no display tick — arrives for
        // the entire transmission: the panadapter/waterfall freeze at the last
        // RX frame until unkey. Ticking from the feedback cadence keeps the
        // display alive; while keyed, Tick's source-select already prefers the
        // TX / PS-feedback analyzers, so the operator sees a live TX spectrum
        // during the burst — the same UX as the dual-ADC G2/Orion family.
        // Same RX-loop thread as OnIqFrame (HandlePsPairedPacket runs on the
        // RxLoop thread), and MaybeTickInline's elapsed-time gate throttles to
        // the configured cadence on dual-ADC boards where IQ and feedback
        // frames both flow.
        MaybeTickInline();
    }

    // RX2 bring-up probe: 1 Hz log of incoming IQ RMS/peak per receiver. A live
    // DDC reads grainy noise (rms ~1e-4+); a dead/unconnected ADC reads ~0 even
    // while packets stream at full rate — distinguishing "radio not streaming"
    // from "streaming silence" (wrong ADC source) for RX2.
    private long _rx1IqRmsLogMs;
    // Secondary receivers' IQ-RMS probe timestamps live in SecondaryRx.IqRmsLogMs.
    private void LogRxIqRms(int rx, ReadOnlySpan<double> iq, ref long lastMs)
    {
        long now = Environment.TickCount64;
        if (now - lastMs < 1000) return;
        lastMs = now;
        double sumSq = 0; double peak = 0;
        for (int i = 0; i < iq.Length; i++)
        {
            double v = iq[i];
            sumSq += v * v;
            double a = v < 0 ? -v : v;
            if (a > peak) peak = a;
        }
        double rms = iq.Length > 0 ? Math.Sqrt(sumSq / iq.Length) : 0;
        _log.LogInformation("p2.rx.iqrms rx={Rx} n={N} rms={Rms:E3} peak={Peak:E3}", rx, iq.Length, rms, peak);
    }

    /// <summary>
    /// Drain every queued cross-thread command synchronously on the calling
    /// thread (the DSP thread — either the RxLoop thread when a sink is
    /// attached, or the ExecuteAsync PeriodicTimer thread otherwise).
    /// ConcurrentQueue.TryDequeue is wait-free; an exception in a command
    /// is logged and the remaining commands still drain.
    /// </summary>
    private void DrainDspCommands()
    {
        while (_dspCommands.TryDequeue(out var cmd))
        {
            try { cmd(); }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "dsp.pipeline command threw");
            }
        }
    }

    /// <summary>
    /// Post a command for execution on the DSP thread (the RX OS thread
    /// when a sink is attached, or the ExecuteAsync PeriodicTimer thread
    /// otherwise). Used by <see cref="SetMox"/> and <see cref="SetTxTune"/>
    /// so WDSP TXA-state edges happen on the same thread that feeds RX IQ.
    /// </summary>
    internal void PostDspCommand(Action cmd)
    {
        ArgumentNullException.ThrowIfNull(cmd);
        _dspCommands.Enqueue(cmd);
    }

    private void MaybeTickInline()
    {
        long now = Stopwatch.GetTimestamp();
        long tickDeadline = Volatile.Read(ref _inlineTickDeadlineStopwatchTicks);
        if (ShouldTickInline(now, tickDeadline, TickPeriodStopwatchTicks))
        {
            if (Tick(_panBuf, _wfBuf, _audioBuf))
            {
                long lastActualTick = Volatile.Read(ref _lastTickStopwatchTicks);
                if (lastActualTick != 0) RecordTickInterval(now - lastActualTick, now);
                Volatile.Write(ref _lastTickStopwatchTicks, now);
                Volatile.Write(ref _lastInlineTickStopwatchTicks, now);
                TryUpdateTickDeadline(
                    ref _inlineTickDeadlineStopwatchTicks,
                    tickDeadline,
                    AdvanceTickDeadline(
                        now,
                        tickDeadline,
                        TickPeriodStopwatchTicks,
                        TickMaxSlipStopwatchTicks));

                var baseDisplayPeriod = CurrentInlineDisplayPeriodTicks();
                long baseDisplayDeadline = Volatile.Read(ref _inlineDisplayDeadlineStopwatchTicks);
                TryUpdateTickDeadline(
                    ref _inlineDisplayDeadlineStopwatchTicks,
                    baseDisplayDeadline,
                    ShouldTickInline(now, baseDisplayDeadline, baseDisplayPeriod)
                        ? AdvanceTickDeadline(
                            now,
                            baseDisplayDeadline,
                            baseDisplayPeriod,
                            baseDisplayPeriod * 2)
                        // The full tick just emitted a display frame, so schedule from that emission.
                        : now);
            }
            return;
        }

        var displayPeriod = CurrentInlineDisplayPeriodTicks();
        if (displayPeriod >= TickPeriodStopwatchTicks ||
            !_hub.DisplayStreamRequested ||
            Volatile.Read(ref _widebandTransportEnabled) != 0)
        {
            return;
        }

        var displayDeadline = Volatile.Read(ref _inlineDisplayDeadlineStopwatchTicks);
        if (!ShouldTickInline(now, displayDeadline, displayPeriod)) return;

        if (Tick(_panBuf, _wfBuf, _audioBuf, displayOnly: true))
        {
            TryUpdateTickDeadline(
                ref _inlineDisplayDeadlineStopwatchTicks,
                displayDeadline,
                AdvanceTickDeadline(now, displayDeadline, displayPeriod, displayPeriod * 2));
        }
    }

    /// <summary>
    /// Pure inline-tick gate: tick on the very first call, then whenever the
    /// next cadence deadline arrives. Extracted static + internal so the
    /// cadence contract is unit-testable — it is what keeps multiple producers
    /// on one shared display cadence when they call <see cref="MaybeTickInline"/>
    /// (user-RX IQ frames AND PS-feedback frames both pace it; on a single-ADC
    /// time-mux board the feedback frames are the ONLY pacer during a keyed
    /// burst — see the P2 <c>OnPsFeedbackFrame</c> sink).
    /// </summary>
    internal static bool ShouldTickInline(long nowTicks, long lastTicks, long periodTicks)
        => lastTicks == 0 || (nowTicks - lastTicks) >= periodTicks;

    internal static long AdvanceTickDeadline(
        long nowTicks,
        long deadlineTicks,
        long periodTicks,
        long maxSlipTicks)
    {
        if (deadlineTicks == 0 || nowTicks - deadlineTicks >= maxSlipTicks)
            return nowTicks;

        return deadlineTicks + periodTicks;
    }

    internal static bool TryUpdateTickDeadline(
        ref long deadlineTicks,
        long observedDeadlineTicks,
        long nextDeadlineTicks)
        => Interlocked.CompareExchange(
            ref deadlineTicks,
            nextDeadlineTicks,
            observedDeadlineTicks) == observedDeadlineTicks;

    /// <summary>
    /// Timer fallback cadence while an RX sink is attached. The inline RX packet
    /// thread remains the preferred pacer; the timer takes over after one and a
    /// half periods with no inline tick, then continues at the normal cadence
    /// until inline packets resume. This drains TX monitor/preview audio during
    /// a P2 DDC packet stall without stealing ticks from a healthy RX thread.
    ///
    /// Issue #155: the previous <c>2× period</c> grace left a 66 ms window with
    /// no producer during a Windows scheduler slip of the RX thread. That gap
    /// starves the native output ring during the post-rebuffer refill phase
    /// (see <c>NativeAudioSink.OnPlaybackData</c>) and cascades into repeated
    /// rebuffer events. A <c>1.5× period</c> grace covers the observed slips
    /// sooner while remaining above healthy inline staleness plus packet
    /// jitter, matching the slow-tick diagnostic threshold.
    /// </summary>
    internal static bool ShouldTimerTickWhenSinkAttached(
        long nowTicks,
        long lastInlineTicks,
        long lastAnyTickTicks,
        long periodTicks)
    {
        bool inlineStale = lastInlineTicks == 0 || (nowTicks - lastInlineTicks) >= periodTicks * 3 / 2;
        return inlineStale && ShouldTickInline(nowTicks, lastAnyTickTicks, periodTicks);
    }

    // #1148: accumulate inline-tick interval stats and emit at ~1 Hz. Called
    // only from MaybeTickInline (single active RX thread), so no synchronisation
    // is needed and the buffers are plain instance fields.
    private void RecordTickInterval(long intervalTicks, long now)
    {
        _tickDiagCount++;
        _tickDiagSumTicks += intervalTicks;
        if (intervalTicks > _tickDiagMaxTicks) _tickDiagMaxTicks = intervalTicks;
        if (intervalTicks > TickSlowThresholdTicks) _tickDiagSlowCount++;
        _tickDiagRing[_tickDiagRingPos] = intervalTicks;
        _tickDiagRingPos = (_tickDiagRingPos + 1) & (_tickDiagRing.Length - 1);
        if (_tickDiagRingFill < _tickDiagRing.Length) _tickDiagRingFill++;

        if (_tickDiagLastEmitTicks == 0) { _tickDiagLastEmitTicks = now; return; }
        if (now - _tickDiagLastEmitTicks < Stopwatch.Frequency) return;
        _tickDiagLastEmitTicks = now;

        double msPerTick = 1000.0 / Stopwatch.Frequency;
        double mean = _tickDiagCount > 0 ? _tickDiagSumTicks * msPerTick / _tickDiagCount : 0;
        double max = _tickDiagMaxTicks * msPerTick;
        double p99 = Zeus.Protocol2.DiagStats.Percentile(
            _tickDiagRing, _tickDiagSortScratch, _tickDiagRingFill, 0.99) * msPerTick;

        _log.LogInformation(
            "dsp.tickdiag mean={Mean:F1}ms p99={P99:F1}ms max={Max:F1}ms slow(>50ms)={Slow} n={N}",
            mean, p99, max, _tickDiagSlowCount, _tickDiagCount);

        _tickDiagCount = 0; _tickDiagSumTicks = 0; _tickDiagMaxTicks = 0;
        _tickDiagSlowCount = 0; _tickDiagRingPos = 0; _tickDiagRingFill = 0;
    }

    /// <summary>
    /// Attach this pipeline as the synchronous RX sink for a Protocol-1
    /// client. Must be called AFTER the engine has been swapped to point at
    /// the new client's WDSP instance — once this returns, the RxLoop will
    /// start firing OnIqFrame on the DSP thread and any older engine reference
    /// must already be unused.
    /// </summary>
    private void AttachRxSinkP1(IProtocol1Client client)
    {
        // Reset the tick clock so the first IQ frame on the new connection
        // gets a fresh display tick (avoids a stale ~33 ms gap if the timer
        // was running synthetic ticks just before connect).
        Volatile.Write(ref _lastTickStopwatchTicks, 0);
        Volatile.Write(ref _lastInlineTickStopwatchTicks, 0);
        Volatile.Write(ref _inlineTickDeadlineStopwatchTicks, 0);
        Volatile.Write(ref _inlineDisplayDeadlineStopwatchTicks, 0);
        _attachedSinkP1 = client;
        client.AttachRxSink(this);
        _rxSinkAttached = true;
        _log.LogInformation("dsp.pipeline rx-sink attached protocol=p1");
    }

    private void DetachRxSinkP1()
    {
        var client = _attachedSinkP1;
        _attachedSinkP1 = null;
        _rxSinkAttached = false;
        client?.DetachRxSink();
        // The codec radio-mic handler is bound to this client instance; clear our
        // attach latch and quiesce the re-blocker so a reconnect re-attaches
        // against the fresh client and no stale remainder survives.
        if (_p1RadioMicAttached)
        {
            client?.DetachRadioMicHandler();
            _p1RadioMicAttached = false;
        }
        _p1RadioMicReceiver?.Reset();
        _log.LogInformation("dsp.pipeline rx-sink detached protocol=p1");
    }

    private void AttachRxSinkP2(Zeus.Protocol2.Protocol2Client client)
    {
        Volatile.Write(ref _lastTickStopwatchTicks, 0);
        Volatile.Write(ref _lastInlineTickStopwatchTicks, 0);
        Volatile.Write(ref _inlineTickDeadlineStopwatchTicks, 0);
        Volatile.Write(ref _inlineDisplayDeadlineStopwatchTicks, 0);
        _attachedSinkP2 = client;
        client.AttachRxSink(this);
        _rxSinkAttached = true;
        _log.LogInformation("dsp.pipeline rx-sink attached protocol=p2");
    }

    private void OnP2WidebandFrame(int adcIndex, ReadOnlySpan<short> samples, int sampleRateHz)
    {
        if (adcIndex != 0) return;
        if (Volatile.Read(ref _p2WidebandTransportEnabled) == 0 || !_hub.DisplayStreamRequested) return;

        bool release = false;
        lock (_widebandFrameLock)
        {
            if (!TryCopyWidebandSamples(samples, _widebandPendingSamples, out _widebandPendingSampleCount))
                return;
            _widebandPendingSampleRateHz = sampleRateHz;
            if (!_widebandFramePending)
            {
                _widebandFramePending = true;
                release = true;
            }
        }

        if (release) _widebandFrameSignal.Release();
    }

    internal static bool TryCopyWidebandSamples(
        ReadOnlySpan<short> samples,
        Span<short> destination,
        out int sampleCount)
    {
        if (samples.IsEmpty || samples.Length > destination.Length)
        {
            sampleCount = 0;
            return false;
        }

        samples.CopyTo(destination);
        sampleCount = samples.Length;
        return true;
    }

    private void DetachRxSinkP2()
    {
        var client = _attachedSinkP2;
        _attachedSinkP2 = null;
        _rxSinkAttached = false;
        Volatile.Write(ref _widebandTransportEnabled, 0);
        Volatile.Write(ref _p2WidebandTransportEnabled, 0);
        Volatile.Write(ref _widebandDetailReady, 0);
        Interlocked.Exchange(ref _widebandDetailLastIqMs, long.MinValue);
        Interlocked.Increment(ref _widebandSourceGeneration);
        try { client?.SetDisplayDdc(-1, 0, 0); }
        catch (ObjectDisposedException) { }
        try { client?.SetWidebandDisplayEnabled(false); }
        catch (ObjectDisposedException) { }
        client?.DetachWidebandFrameHandler();
        lock (_widebandFrameLock)
        {
            _widebandFramePending = false;
            _widebandPendingSampleCount = 0;
        }
        client?.DetachRxSink();
        _log.LogInformation("dsp.pipeline rx-sink detached protocol=p2");
    }

    private void CloseCurrentEngine()
    {
        IDspEngine? engine;
        int channel;
        lock (_engineLock)
        {
            engine = _engine;
            channel = _channelId;
            Volatile.Write(ref _engine, null);
            Volatile.Write(ref _channelId, 0);
            ResetSecondaryRxChannels();
        }
        TeardownEngine(engine, channel);
    }

    private static void TeardownEngine(IDspEngine? engine, int channelId)
    {
        if (engine is null) return;
        try { engine.CloseChannel(channelId); } catch { /* best-effort */ }
        try { engine.Dispose(); } catch { /* best-effort; never mask the initiating failure */ }
    }

    // N-receiver additive mix. Averaging silently reduced every receiver by
    // 6.02 dB with two streams (9.54 dB with three), which made strong signals
    // sound weak merely because another RX was enabled. Sum configured streams
    // at unity, as a real mixer does; the existing final-bus soft limiter owns
    // coincident-peak headroom. A stalled, muted, or shorter stream never
    // dilutes the receivers that remain present.
    //
    // <paramref name="rx1Muted"/>: RX1's samples are dropped from the sum (the
    // caller has already zeroed them), but rx1Count still
    // sets the block length so the secondaries stay clocked to RX1. This lets the
    // per-RX mute model express "RX2 only" (mute RX1) without halving RX2.
    internal static int MixRxAudioN(
        Span<float> rx1,
        int rx1Count,
        ReadOnlySpan<RxAudioSlice> slices,
        bool rx1Muted = false)
    {
        rx1Count = Math.Clamp(rx1Count, 0, rx1.Length);

        bool rx1Contributes = !rx1Muted && rx1Count > 0;
        int availableStreams = rx1Contributes ? 1 : 0;
        int count = rx1Count;
        foreach (var s in slices)
        {
            int sc = Math.Clamp(s.Count, 0, s.Buffer.Length);
            if (sc <= 0) continue;
            availableStreams++;
            if (sc > count) count = sc;
        }
        count = Math.Min(count, rx1.Length);
        if (count == 0 || availableStreams == 0) return 0;

        for (int i = 0; i < count; i++)
        {
            float sum = 0f;
            bool contributed = false;
            if (rx1Contributes && i < rx1Count)
            {
                sum = rx1[i];
                contributed = true;
            }
            foreach (var s in slices)
            {
                int sc = Math.Clamp(s.Count, 0, s.Buffer.Length);
                if (sc <= 0 || i >= sc) continue;
                sum += s.Buffer[i];
                contributed = true;
            }
            rx1[i] = contributed ? sum : 0f;
        }
        return count;
    }

    // Local-radio TX suppression must never consume the independent KiwiSDR
    // receive path. Once keyed, discard the already-mixed hardware receivers
    // and retain only the external receiver block. This also seeds the output
    // when a half-duplex board (notably HL2) produces no local RX samples while
    // MOX is asserted.
    internal static int PrepareLocalRxForExternalOutput(
        Span<float> localRx,
        int localRxCount,
        int externalRxCount,
        bool suppressLocalRx)
    {
        localRxCount = Math.Clamp(localRxCount, 0, localRx.Length);
        externalRxCount = Math.Clamp(externalRxCount, 0, localRx.Length);
        if (externalRxCount == 0) return localRxCount;
        if (!suppressLocalRx && localRxCount > 0)
        {
            if (externalRxCount > localRxCount)
                localRx.Slice(localRxCount, externalRxCount - localRxCount).Clear();
            return Math.Max(localRxCount, externalRxCount);
        }

        int count = Math.Max(localRxCount, externalRxCount);
        localRx[..count].Clear();
        return externalRxCount;
    }

    internal static int MixExternalRxIntoTxMonitor(
        Span<float> monitor,
        int monitorCount,
        ReadOnlySpan<float> externalRx,
        int externalRxCount)
    {
        monitorCount = Math.Clamp(monitorCount, 0, monitor.Length);
        externalRxCount = Math.Clamp(externalRxCount, 0, externalRx.Length);
        int count = Math.Min(monitor.Length, Math.Max(monitorCount, externalRxCount));
        int overlap = Math.Min(monitorCount, externalRxCount);
        for (int i = 0; i < overlap; i++) monitor[i] += externalRx[i];
        if (externalRxCount > monitorCount)
            externalRx.Slice(monitorCount, externalRxCount - monitorCount)
                .CopyTo(monitor[monitorCount..]);
        return count;
    }

    internal static void ApplyTxMonitorVolume(Span<float> samples, double volumeDb)
    {
        double clampedDb = Math.Clamp(
            double.IsFinite(volumeDb) ? volumeDb : 0.0,
            MinTxMonitorVolumeDb,
            MaxTxMonitorVolumeDb);
        if (clampedDb >= 0.0) return;
        float gain = (float)Math.Pow(10.0, clampedDb / 20.0);
        for (int i = 0; i < samples.Length; i++) samples[i] *= gain;
    }

    internal static bool ShouldMixExternalRxIntoTxMonitor(
        int externalRxCount,
        bool txMonitorMeterOnly,
        bool rxAudioMuted) =>
        externalRxCount > 0 && !txMonitorMeterOnly && !rxAudioMuted;

    private bool Tick(float[] panBuf, float[] wfBuf, float[] audioBuf, bool displayOnly = false)
    {
        // issue #1167: the timer thread and the RX inline-tick thread can both
        // be live during the sink attach/detach window. FloatSpscRing is strict
        // single-producer; this gate guarantees only one Tick runs at a time so
        // the producer side stays single-threaded. A skipped tick is harmless
        // — the holder is ticking ~now.
        if (!_tickGate.TryEnter()) { Interlocked.Increment(ref _tickReentrySkips); return false; }
        try
        {
        // iter5 pass-2: lock-free hot path. Tick runs inline on the RX OS
        // thread when a sink is attached (paced via Stopwatch elapsed in
        // OnIqFrame), and on the PeriodicTimer thread otherwise. Volatile
        // reads are correctly ordered against the writer-side _engineLock
        // release in OnRadioConnected / ConnectP2Async / etc.
        var engine = Volatile.Read(ref _engine);
        int channel = Volatile.Read(ref _channelId);
        int sampleRate = Volatile.Read(ref _sampleRateHz);
        if (engine is null) return true;

        var state = _radio.Snapshot();
        ReconcileExternalPureSignalStatusForTick(engine, state);
        // Synthetic engine stays open while disconnected so SetMode/SetFilter
        // etc. have somewhere to land, but its sweep+static placeholder used
        // to render a misleading "fake spectrum" before any radio existed.
        // Gate on the engine type rather than the connection status: status
        // flips to Connected before OnRadioConnected swaps the engine, and a
        // status-only check let one or two synthetic frames leak through that
        // race window — visible as a brief flash of the fake waterfall right
        // when the user clicked Connect. The synthetic engine never produces
        // real-radio data, so suppressing it unconditionally is correct.
        if (engine is SyntheticDspEngine) return true;

        // Display subscribers can appear/disappear, and WB can be toggled,
        // without producing a RadioService state mutation. Reconcile the WDSP
        // zoom here on the DSP thread so the frame geometry and analyzer crop
        // always change together at the overview/detail boundary.
        ApplyVisibleDdcZoom(engine, state);
        bool widebandDetailReady = ReconcileWidebandDetailSource(engine, state);
        int widebandDetailChannel = Volatile.Read(ref _widebandDetailChannelId);
        if (widebandDetailReady && IsWidebandDetailIqStale(
            Environment.TickCount64,
            Interlocked.Read(ref _widebandDetailLastIqMs)))
        {
            // A configured DDC can disappear without a state edge (firmware
            // restart, dropped command, or stream fault). Relinquish detail
            // ownership so raw wideband snapshots resume instead of leaving a
            // permanently frozen high-resolution frame on screen.
            Volatile.Write(ref _widebandDetailReady, 0);
            Interlocked.Increment(ref _widebandSourceGeneration);
            widebandDetailReady = false;
        }
        if (!widebandDetailReady && widebandDetailChannel >= 0)
        {
            long generation = Interlocked.Read(ref _widebandSourceGeneration);
            bool fresh = engine.TryGetDisplayPixels(
                widebandDetailChannel,
                DisplayPixout.Panadapter,
                panBuf);
            if (CanPromoteWidebandDetail(
                fresh,
                Environment.TickCount64,
                Interlocked.Read(ref _widebandDetailLastIqMs),
                generation,
                Interlocked.Read(ref _widebandSourceGeneration),
                widebandDetailChannel,
                Volatile.Read(ref _widebandDetailChannelId)))
            {
                // The first analyzer-generated frame is the ownership barrier.
                // Keep raw packets enabled through warmup, then invalidate any
                // raw FFT already in flight before publishing detail.
                Interlocked.Increment(ref _widebandSourceGeneration);
                Volatile.Write(ref _widebandDetailReady, 1);
                widebandDetailReady = true;
            }
        }

        // Issue #597 Phase 0: restore the default display tau once the LO has
        // been quiet for FastAttackRestoreMs. Runs on the RX/pipeline thread;
        // the engine call is idempotent and channel-guarded, so a race with a
        // simultaneous re-arm on the state thread is harmless (the re-arm
        // refreshes _fastAttackLoChangedAt and the restore simply fires later).
        if (_fastAttackActive &&
            Stopwatch.GetTimestamp() - Interlocked.Read(ref _fastAttackLoChangedAt) >= FastAttackRestoreTicks)
        {
            engine.SetRxDisplayFastAttack(channel, fast: false);
            _fastAttackActive = false;
        }

        engine.SetVfoHz(channel, state.VfoHz);
        int rx2Channel = Volatile.Read(ref _secondaryRx[1].ChannelId);
        if (state.Rx2Enabled && rx2Channel >= 0)
            engine.SetVfoHz(rx2Channel, state.Rx2().VfoHz);

        // Skip the entire display pipeline unless at least one client has a
        // mounted spectrum consumer. Saves: 2× engine.TryGet*DisplayPixels
        // P/Invoke per tick (each reads from the WDSP analyzer slot under its
        // lock), Array.Reverse on two 2 048-float buffers, the DisplayFrame
        // record construction, and the 16 KB-ish byte[] payload fanout would
        // allocate. Control-only clients still receive meters/state/audio as
        // appropriate; they just do not pin the high-rate display stream on.
        bool widebandDisplayActive = RefreshWidebandDisplayState(state);
        bool displayStreamRequested = _hub.DisplayStreamRequested;
        DisplayFramePlan displayPlan = default;
        bool hasDisplaySubscribers =
            displayStreamRequested &&
            !widebandDisplayActive &&
            // Full ticks use TickPeriod and faster display-only ticks use the
            // configured inline deadline, so this publisher is already paced.
            TryBeginDisplayFrame(
                Stopwatch.GetTimestamp(),
                producerIsRatePaced: true,
                out displayPlan);
        // Audio path uses nowMs too (it runs even when no clients are connected,
        // for in-process RxAudioAvailable subscribers like TCI). Hoisted above
        // the display gate to keep one timestamp call per tick.
        double nowMs = UnixTimeMillisecondsHighRes();
        long holdNowMs = Environment.TickCount64;
        bool pan = false, wf = false;
        bool psFbPanUsed = false, psFbWfUsed = false;
        bool psFeedbackCorrecting = false;
        string panSource = "none";
        string wfSource = "none";
        double rxAudioRmsForMeter = double.NaN;
        if (hasDisplaySubscribers)
        {
            bool useWidebandDetail = widebandDetailReady && !_keyed;
            int rxDisplayChannel = useWidebandDetail
                ? Volatile.Read(ref _widebandDetailChannelId)
                : channel;
            long widebandDetailGeneration = useWidebandDetail
                ? Interlocked.Read(ref _widebandSourceGeneration)
                : 0;
            bool suppressPostTxRxDisplay = ShouldSuppressRxDisplayForCurrentTick();

            // P2 display-DUP keeps both surfaces in one RX geometry across every
            // keyed interval, including PureSignal. P1 retains its established
            // keyed PS TX/feedback path until it has independent RX/TX tuning.
            //
            // Issue #121 layered on top: if the operator has the "Monitor PA
            // output" toggle on AND PS is armed AND PS has converged
            // (info[14]==1, surfaced via GetPsStageMeters().Correcting), prefer
            // the PS-feedback analyzer (post-PA loopback IQ). Falls back to the
            // TX analyzer if the PS-FB analyzer hasn't produced a fresh FFT yet
            // — same shape as the existing TX → RX fallback. Default-off
            // toggle: when off the codepath is identical to pre-#121, byte for
            // byte, on every board.
            if (_keyed && _appliedPsEnabled && !_p2DisplayDuplexForCurrentTx)
            {
                if (_appliedPsEnabled && _psMonitorEnabled
                    && (psFeedbackCorrecting = engine.GetPsStageMeters().Correcting))
                {
                    pan = engine.TryGetPsFeedbackDisplayPixels(DisplayPixout.Panadapter, panBuf);
                    if (displayPlan.IncludeWaterfall)
                        wf = engine.TryGetPsFeedbackDisplayPixels(DisplayPixout.Waterfall, wfBuf);
                    psFbPanUsed = pan;
                    psFbWfUsed = wf;
                    if (pan) panSource = "ps-feedback";
                    if (wf) wfSource = "ps-feedback";
                }
                if (displayPlan.IncludeWaterfall && !wf)
                {
                    wf = engine.TryGetTxDisplayPixels(DisplayPixout.Waterfall, wfBuf);
                    if (wf)
                    {
                        wfSource = "tx";
                        Array.Copy(wfBuf, _lastTxWfBuf, _panadapterWidth);
                        Interlocked.Exchange(ref _lastTxWfCaptureMs, holdNowMs);
                        _lastTxWfValid = true;
                    }
                }
                if (!pan)
                {
                    pan = engine.TryGetTxDisplayPixels(DisplayPixout.Panadapter, panBuf);
                    if (pan)
                    {
                        panSource = "tx";
                        Array.Copy(panBuf, _lastTxPanBuf, _panadapterWidth);
                        Interlocked.Exchange(ref _lastTxPanCaptureMs, holdNowMs);
                        _lastTxPanValid = true;
                    }
                }
                // Stale-tick TX-hold (issue #162): while keyed, if neither the
                // PS-monitor path nor the TX analyzer produced a fresh frame,
                // reuse the last fresh TX frame captured this transmission
                // instead of falling through to the RX analyzer below. RX
                // pixels (roughly -100..-140 dBFS noise floor) rendered against
                // the TX display window (-80..+20 dBFS) map to fully-below-floor
                // pixels — the panadapter goes black on every stale tick,
                // producing a ~10-15 Hz strobe visible during SSB TX. Skipped
                // when we've never captured a fresh TX frame this transmission
                // (Synthetic engine, or first
                // ~tick after MOX-on before the TX analyzer settles) — the
                // original RX fallback still fires there, matching the
                // pre-#162 behaviour on the first-tick / no-TX-analyzer edges.
                if (!pan && _lastTxPanValid)
                {
                    if (holdNowMs - Interlocked.Read(ref _lastTxPanCaptureMs) <= TxDisplayHoldMaxAgeMs)
                    {
                        Array.Copy(_lastTxPanBuf, panBuf, _panadapterWidth);
                        pan = true;
                        panSource = "tx-hold";
                    }
                    else
                    {
                        _lastTxPanValid = false;
                    }
                }
                if (displayPlan.IncludeWaterfall && !wf && _lastTxWfValid)
                {
                    if (holdNowMs - Interlocked.Read(ref _lastTxWfCaptureMs) <= TxDisplayHoldMaxAgeMs)
                    {
                        Array.Copy(_lastTxWfBuf, wfBuf, _panadapterWidth);
                        wf = true;
                        wfSource = "tx-hold";
                    }
                    else
                    {
                        _lastTxWfValid = false;
                    }
                }
            }
            // P2 display-DUP requests fresh RX analyzer pixels for both surfaces.
            if (_keyed && _psMonitorEnabled)
            {
                _psMonitorTickCount++;
                if (_psMonitorTickCount % 30 == 0)
                {
                    _log.LogInformation(
                        "psMonitor.gate keyed=1 psEn={PsEn} mon=1 corr={Corr} psFbPan={Pan} psFbWf={Wf}",
                        _appliedPsEnabled, psFeedbackCorrecting, psFbPanUsed, psFbWfUsed);
                }
            }
            else
            {
                _psMonitorTickCount = 0;
            }
            if (!suppressPostTxRxDisplay)
            {
                if (!pan)
                {
                    pan = engine.TryGetDisplayPixels(rxDisplayChannel, DisplayPixout.Panadapter, panBuf);
                    if (pan) panSource = useWidebandDetail ? "wideband-detail" : "rx";
                }
                if (displayPlan.IncludeWaterfall && !wf)
                {
                    wf = engine.TryGetDisplayPixels(rxDisplayChannel, DisplayPixout.Waterfall, wfBuf);
                    if (wf) wfSource = useWidebandDetail ? "wideband-detail" : "rx";
                }
            }
            else
            {
                panSource = "post-tx-muted";
                if (displayPlan.IncludeWaterfall)
                    wfSource = "post-tx-muted";
            }

            // Preserve the legacy P1 keyed-PS fallback: if neither the TX nor
            // RX analyzer produced pixels, use the live feedback analyzer. P2
            // deliberately does not substitute this TX-centred geometry into
            // the parked receive display.
            if (_keyed && _appliedPsEnabled && !_p2DisplayDuplexForCurrentTx)
            {
                if (!pan)
                {
                    pan = engine.TryGetPsFeedbackDisplayPixels(DisplayPixout.Panadapter, panBuf);
                    if (pan) { panSource = "ps-feedback"; psFbPanUsed = true; }
                }
                if (displayPlan.IncludeWaterfall && !wf)
                {
                    wf = engine.TryGetPsFeedbackDisplayPixels(DisplayPixout.Waterfall, wfBuf);
                    if (wf) { wfSource = "ps-feedback"; psFbWfUsed = true; }
                }
            }

            // TX display calibration offset (Thetis TXDisplayCalOffset). Pure
            // dB shift of the transmitted-signal trace/waterfall so the operator
            // can sit the in-passband level where they want it — display-only,
            // never the air. Applied only to TX-sourced pixels (tx / PS
            // feedback); RX pixels keep their own calibration.
            double txCal = Volatile.Read(ref _txDisplayCalOffsetDb);
            if (txCal != 0.0)
            {
                if (pan && (panSource == "tx" || panSource == "ps-feedback" || panSource == "tx-hold")) AddDbOffset(panBuf, txCal);
                if (wf && (wfSource == "tx" || wfSource == "ps-feedback" || wfSource == "tx-hold")) AddDbOffset(wfBuf, txCal);
            }

            // Diagnostic (1 Hz): log the actual TX panadapter pixel dB range so
            // we can confirm where the transmitted signal sits relative to the
            // display window (the TX analyzer reads far hotter than RX). Helps
            // verify the frontend TX auto-range is fitting sane values.
            if (pan && panSource == "tx")
            {
                long txDbgNow = Environment.TickCount64;
                if (txDbgNow - _txPixelDbgMs >= 1000)
                {
                    _txPixelDbgMs = txDbgNow;
                    float pmin = float.PositiveInfinity, pmax = float.NegativeInfinity;
                    double psum = 0; int pcnt = 0;
                    for (int i = 0; i < panBuf.Length; i++)
                    {
                        float v = panBuf[i];
                        if (!float.IsFinite(v)) continue;
                        if (v < pmin) pmin = v;
                        if (v > pmax) pmax = v;
                        psum += v; pcnt++;
                    }
                    if (pcnt > 0)
                        _log.LogInformation(
                            "tx.display.pixels min={Min:F1} max={Max:F1} mean={Mean:F1} dB (window default {Lo}..{Hi}; calOffset={Cal:F1})",
                            pmin, pmax, psum / pcnt, -80, 20, txCal);
                }
            }

            // Flip to display order (low freq left, high freq right). WDSP emits
            // pixel 0 = highest positive frequency — see doc 03 §10 and
            // doc 08 §3 "Pixel axis reversal". SyntheticDspEngine already emits
            // in WDSP order so this reversal applies to both engines. Guarded by
            // the freshness flag: TryGetDisplayPixels leaves the buffer untouched
            // when no new FFT is ready, so an unconditional reverse would alternate
            // the orientation on every stale tick and broadcast mirrored garbage
            // (still flagged invalid, but bandwidth wasted and timing-sensitive).
            if (pan) Array.Reverse(panBuf);
            if (wf) Array.Reverse(wfBuf);
            if (pan) SanitizeDisplayBuffer(panBuf);
            if (wf) SanitizeDisplayBuffer(wfBuf);

            var flags = DisplayBodyFlags.None;
            if (pan) flags |= DisplayBodyFlags.PanValid;
            if (wf) flags |= DisplayBodyFlags.WfValid;
            if (!displayPlan.IncludeWaterfall && !wf)
                wfSource = "waterfall-decimated";

            // Zoom narrows the analyzer's display span to sampleRate/level around
            // the VFO, so hzPerPixel shrinks by the same factor. Client re-uses
            // this for axis labels and planWaterfallUpdate horizontal shift — no
            // extra contract field needed, per task #7 scope note.
            int zoomLevel = useWidebandDetail
                ? Volatile.Read(ref _widebandDetailZoomLevel)
                : DdcZoomLevel(state.ZoomLevel);
            float hzPerPixel = useWidebandDetail
                ? (float)((double)sampleRate / zoomLevel / _panadapterWidth)
                : VisibleDdcHzPerPixel(sampleRate, state.ZoomLevel, _panadapterWidth);
            // Panadapter centre: the LO the pixels were actually computed
            // at (issue #597 Phase 2). The analyzer output broadcast this
            // tick reflects IQ captured ~stampLag earlier; LookupAt rewinds
            // the LO history by that much so mid-retune frames carry the
            // frequency their data belongs to instead of the live NCO.
            // Stable LO (≥ stampLag with no tune) ⇒ identical to the old
            // `state.RadioLoHz` stamp, byte for byte.
            double fftFillMs = sampleRate > 0
                ? (useWidebandDetail
                    // The detail channel runs its own adaptive FFT (up to
                    // 65,536 pts), so its aperture — not the engine default —
                    // sets the lookback for delay-compensated centre stamps.
                    ? Volatile.Read(ref _widebandDetailAnalyzerFftSize)
                    : _rxAnalyzerFftSize) / (double)sampleRate * 1000.0
                : 0.0;
            double stampLagMs = 0.5 * fftFillMs
                + (CenterStampLagOverrideMs
                   ?? (CenterStampEmaLagMs
                       + (_p2Client is not null ? CenterStampTransportP2Ms : CenterStampTransportP1Ms)));
            long stampLagTicks = (long)(stampLagMs / 1000.0 * Stopwatch.Frequency);
            long centerHz = useWidebandDetail
                // Mid-pan frames carry the centre their pixels were captured
                // at (glide), not the live target (stick-then-jump). Stable
                // target ≥ stampLag ⇒ identical to the live target, matching
                // the old stamp byte for byte.
                ? _widebandCenterHistory.LookupAt(
                    Stopwatch.GetTimestamp() - stampLagTicks,
                    fallbackLoHz: WidebandViewportTargetCenterHz(state))
                : _loHistory.LookupAt(
                    Stopwatch.GetTimestamp() - stampLagTicks,
                    fallbackLoHz: state.RadioLoHz);

            // Cache for the frequency-calibration service (issue #325). The
            // cal reads from this cache to avoid racing for WDSP's "fresh
            // frame" flag — Tick consumes that flag at 30 Hz, leaving no
            // window for a parallel consumer. Cache only when we actually
            // got pan data this tick.
            if (pan && panSource != "tx-hold")
            {
                lock (_calPanLock)
                {
                    Array.Copy(panBuf, _calPanSnapshot, _panadapterWidth);
                    _calPanHzPerPixel = hzPerPixel;
                    _calPanCenterHz = centerHz;
                    _calPanSnapshotMs = (long)nowMs;
                    _calPanSnapshotVersion++;
                }
            }
            if (wf)
            {
                lock (_calPanLock)
                {
                    Array.Copy(wfBuf, _diagWfSnapshot, _panadapterWidth);
                    _diagWfSnapshotMs = (long)nowMs;
                }
            }
            var panFrameBins = pan
                ? FrameBins(panBuf, _panDecimatedBuf, displayPlan.Decimation, out var frameWidth)
                : InvalidFrameBins(_panDecimatedBuf, _panadapterWidth, displayPlan.Decimation, out frameWidth);
            var wfFrameBins = wf
                ? FrameBins(wfBuf, _wfDecimatedBuf, displayPlan.Decimation, out _)
                : InvalidFrameBins(_wfDecimatedBuf, _panadapterWidth, displayPlan.Decimation, out _);

            var frame = new DisplayFrame(
                Seq: NextDisplaySeq(),
                TsUnixMs: nowMs,
                RxId: 0,
                BodyFlags: flags,
                Width: frameWidth,
                // Panadapter centres on the radio's actual LO, which equals
                // VfoHz outside CW and VfoHz ∓ cw_pitch in CWU/CWL. The CW filter
                // (audio passband centred on cw_pitch) then renders on top of
                // the dial line via PassbandOverlay's `centerHz + filterLow..high`.
                CenterHz: centerHz,
                HzPerPixel: hzPerPixel * displayPlan.Decimation,
                PanDb: panFrameBins,
                WfDb: wfFrameBins);

            lock (_calPanLock)
            {
                _diagDisplayFrameMs = (long)nowMs;
                _diagDisplaySeq = frame.Seq;
                _diagDisplayFrameCount++;
                _diagLastPanValid = pan;
                _diagLastWfValid = wf;
                _diagLastPanSource = panSource;
                _diagLastWfSource = wfSource;
                _diagLastKeyed = _keyed;
                _diagLastPsMonitorRequested = _psMonitorEnabled;
                _diagLastPsFeedbackCorrecting = psFeedbackCorrecting;
            }

            // Only broadcast when the frame actually carries display pixels. A
            // frame with no valid payload renders nothing. Under Protocol-3 the
            // WDSP RX channel is opened (for the local TXA/PureSignal chain) but
            // never fed RX IQ — RX display comes from the sidecar forwarder — so
            // TryGetDisplayPixels returns false every tick and this loop would
            // otherwise broadcast an empty Width-bin frame ~30x/s onto RxId 0.
            // That interleaves a differently-sized empty frame with the sidecar's
            // real frames, flipping slice.width on the client and (historically)
            // storming the waterfall history-texture realloc. Skipping empties
            // also spares P1/P2 a wasted frame on any stale tick with no fresh FFT.
            bool displaySourceCurrent = !useWidebandDetail || CanPublishWidebandDetail(
                Volatile.Read(ref _widebandDetailReady) != 0,
                widebandDetailGeneration,
                Interlocked.Read(ref _widebandSourceGeneration),
                rxDisplayChannel,
                Volatile.Read(ref _widebandDetailChannelId));
            if (flags != DisplayBodyFlags.None && displaySourceCurrent)
                _hub.Broadcast(frame);

            // Secondary receivers (RX2..RXn): each open secondary broadcasts its
            // own DisplayFrame (RxId = receiver index) stamped with its DDC centre
            // (CTUN-frozen under CTUN, so the panel holds still while the dial
            // roams — matching RX1). Today only slot 1 (RX2) is ever active; the
            // loop is N-ready for B3/B4. UpdateRxLo is idempotent.
            bool anySecondary = false;
            if (!suppressPostTxRxDisplay)
            {
                for (int ri = 1; ri < MaxReceivers; ri++)
                {
                    var rx = _secondaryRx[ri];
                    int secChan = Volatile.Read(ref rx.ChannelId);
                    if (!SecondaryReceiverEnabled(ri, state) || secChan < 0) continue;
                    anySecondary = true;

                    bool secPan = engine.TryGetDisplayPixels(secChan, DisplayPixout.Panadapter, rx.PanBuf);
                    bool secWf = displayPlan.IncludeWaterfall &&
                        engine.TryGetDisplayPixels(secChan, DisplayPixout.Waterfall, rx.WfBuf);
                    if (secPan) { Array.Reverse(rx.PanBuf); SanitizeDisplayBuffer(rx.PanBuf); rx.PanCnt++; }
                    if (secWf) { Array.Reverse(rx.WfBuf); SanitizeDisplayBuffer(rx.WfBuf); rx.WfCnt++; }

                    var secFlags = DisplayBodyFlags.None;
                    if (secPan) secFlags |= DisplayBodyFlags.PanValid;
                    if (secWf) secFlags |= DisplayBodyFlags.WfValid;
                    var secPanBins = secPan
                        ? FrameBins(rx.PanBuf, _panDecimatedBuf, displayPlan.Decimation, out var secFrameWidth)
                        : InvalidFrameBins(_panDecimatedBuf, _panadapterWidth, displayPlan.Decimation, out secFrameWidth);
                    var secWfBins = secWf
                        ? FrameBins(rx.WfBuf, _wfDecimatedBuf, displayPlan.Decimation, out _)
                        : InvalidFrameBins(_wfDecimatedBuf, _panadapterWidth, displayPlan.Decimation, out _);

                    UpdateRxLo(ri, state);
                    var secFrame = new DisplayFrame(
                        Seq: NextDisplaySeq(),
                        TsUnixMs: nowMs,
                        RxId: (byte)ri,
                        BodyFlags: secFlags,
                        Width: secFrameWidth,
                        CenterHz: rx.LoHz,
                        HzPerPixel: VisibleDdcHzPerPixel(
                            sampleRate,
                            state.ZoomLevel,
                            _panadapterWidth) * displayPlan.Decimation,
                        PanDb: secPanBins,
                        WfDb: secWfBins);
                    if (secFlags != DisplayBodyFlags.None)
                        _hub.Broadcast(secFrame);
                }
            }

            if (anySecondary)
            {
                _dispTicks++;
                if (pan) _rx1PanCnt++;
                if (wf) _rx1WfCnt++;
                long dispNowMs = Environment.TickCount64;
                if (dispNowMs - _dispFlagLogMs >= 1000)
                {
                    _dispFlagLogMs = dispNowMs;
                    var rx2 = _secondaryRx[1];
                    _log.LogInformation(
                        "p2.display.flags ticks={T} rx1pan={A} rx1wf={B} rx2pan={C} rx2wf={D}",
                        _dispTicks, _rx1PanCnt, _rx1WfCnt, rx2.PanCnt, rx2.WfCnt);
                    _dispTicks = _rx1PanCnt = _rx1WfCnt = 0;
                    for (int i = 1; i < MaxReceivers; i++)
                    {
                        _secondaryRx[i].PanCnt = 0;
                        _secondaryRx[i].WfCnt = 0;
                    }
                }
            }
        }
        else
        {
            // Still reset the PS-monitor tick counter on no-client ticks so a
            // fresh client doesn't pick up a stale gate counter.
            if (!displayStreamRequested) _psMonitorTickCount = 0;
        }

        if (displayOnly)
            return true;

        // Audio broadcast — when TX monitor is on, replace RX audio with the
        // monitor channel's demodulated TX audio so the operator hears the
        // chain output (post-bandpass / post-CFIR, demodulated back to mono)
        // instead of band RX. This unifies "monitor while keyed" (Thetis MON
        // semantics) and "preview without keying" (audio passes through the
        // chain so VST plugins receive samples and their meters animate). RX
        // is drained anyway so the WDSP audio ring doesn't back up — we just
        // don't broadcast it. The VST RX seam still fires on the drained RX
        // so RX-side plugins keep running even while monitor is on.
        bool txMonitorOn = engine.IsTxMonitorOn;
        // Read once, up front: everything below that decides which receiver's
        // audio survives this tick (RX1, each secondary, and the publish
        // branch) needs to agree on the same keyed/post-TX-drain snapshot.
        bool suppressRxAudioForTx = ShouldSuppressRxAudioForCurrentTick(out bool activelyKeyed);
        // The operator's DUP choice and the PureSignal guard are latched on the
        // first suppressed tick of a transmission. A mid-transmission toggle
        // applies on the next transmission, keeping this TX and its drain window
        // on one stable policy. Latching here rather than in SetMox keeps the
        // radio-state lock off the MOX edge: this tick already has a projected
        // StateDto, so no cross-service call is needed.
        if (_fullDuplexLatchPendingForCurrentTx && suppressRxAudioForTx)
        {
            bool latched =
                state.FullDuplexMultiRxEnabled && !_stopRxForPureSignalForCurrentTx;
            _fullDuplexRxActiveForCurrentTx = latched;
            _localRxAudioContinuousForCurrentTx = latched;
            _fullDuplexLatchPendingForCurrentTx = false;
        }
        bool fullDuplexRxActive = _fullDuplexRxActiveForCurrentTx;
        bool localRxAudioContinuous = _localRxAudioContinuousForCurrentTx;
        // A full-duplex receiver may resume after a transient keyed starvation,
        // but once continuity is lost the post-TX drain must finish before local
        // RX resumes. Continuous full-duplex audio bypasses that artificial gap.
        bool allowLocalRxDuringSuppression =
            fullDuplexRxActive && (activelyKeyed || localRxAudioContinuous);
        int audioSampleCount = engine.ReadAudio(channel, audioBuf);
        // Per-RX mute (Thetis chkMUT): RX1 stays the audio clock-master so the
        // mix/output timing is unchanged, but its samples are zeroed so only the
        // other (unmuted) receivers are heard. TX monitor overrides RX audio
        // below, so muting an RX never silences the monitor.
        //
        // Full duplex: while keyed (or draining post-TX), DUP keeps every
        // unmuted receiver flowing through the mix. With DUP off, or while
        // PureSignal owns RXA for feedback, TX suppression excludes them all.
        bool rx1Muted = IsReceiverMuted(state, 0)
            || (suppressRxAudioForTx && !allowLocalRxDuringSuppression);
        if (rx1Muted && audioSampleCount > 0)
            audioBuf.AsSpan(0, audioSampleCount).Clear();

        // Secondary-receiver audio. Per-receiver mute is the sole audibility
        // control (Thetis chkMUT, the #894 model): every enabled secondary is
        // mixed into the RX1-clocked output bus, and muting a receiver — RX1
        // (above) or any secondary (here) — removes its contribution. This
        // subsumes the old Rx2AudioMode (Both/RX1/RX2) routing:
        //   hear both → nothing muted;  RX1 only → mute the secondaries;
        //   RX2 only  → mute RX1.
        // The legacy Rx2AudioMode field is retained for wire compatibility but no
        // longer gates audio: its UI control was removed in #894, so a stale
        // persisted Rx2AudioMode=Rx1/Rx2 used to strand every enabled secondary
        // silent with no way to recover. Drain EVERY enabled secondary each tick
        // (incl. muted) so no WDSP audio ring can back up and trip the rx-ingest
        // "consumer behind" selfcheck (IQ ingest stays lossless regardless).
        int mixSliceCount = 0;
        for (int ri = 1; ri < MaxReceivers; ri++)
        {
            if (!SecondaryReceiverEnabled(ri, state)) continue;
            var sec = _secondaryRx[ri];
            int secChan = Volatile.Read(ref sec.ChannelId);
            if (secChan < 0) continue;

            // RX1 is the audio clock-master: read at most audioSampleCount samples
            // from each secondary so the mixed stream is fed at exactly the RX
            // sample rate. Draining fully and mixing max() over-feeds the sink —
            // per-tick counts jitter independently, so E[max] exceeds the true
            // rate (~5% on a G2), saturating the output ring → overrun → dual-RX
            // clicking (#787). The unread remainder stays buffered for the next
            // tick, so no secondary audio is dropped. With RX1 silent
            // (audioSampleCount==0) read nothing this tick.
            int want = Math.Min(audioSampleCount, sec.AudioBuf.Length);
            int n = want > 0 ? engine.ReadAudio(secChan, sec.AudioBuf.AsSpan(0, want)) : 0;
            if (n > 0)
            {
                // Ramp the first blocks of a freshly opened channel so RX2-on
                // enters the additive mix (below) smoothly instead of dumping an
                // unconverged full-scale block. Applied before the plugin tap and
                // the mix so every consumer sees the same faded audio.
                int fadeRemaining = Volatile.Read(ref sec.FadeInSamplesRemaining);
                if (fadeRemaining > 0)
                {
                    int fadeNext = ApplyRxPostTxFadeIn(
                        sec.AudioBuf.AsSpan(0, n),
                        fadeRemaining,
                        SecondaryRxOpenFadeInSamples);
                    Volatile.Write(ref sec.FadeInSamplesRemaining, fadeNext);
                }
                _productPluginAudio?.PublishRxAudio(
                    ri, AudioOutputRateHz, sec.AudioBuf.AsSpan(0, n));
            }
            // A muted secondary is still drained above (so its ring can't back up)
            // but excluded from the mix entirely — it must neither add signal nor
            // affect the additive MixRxAudioN bus. With DUP on, every unmuted
            // secondary remains audible while keyed/draining; with DUP off,
            // every secondary drops out during suppression, same as RX1 above.
            if (IsReceiverMuted(state, ri)
                || (suppressRxAudioForTx && !allowLocalRxDuringSuppression))
                continue;
            _mixSlices[mixSliceCount++] = new RxAudioSlice(sec.AudioBuf, n);
        }

        // Kiwi slice: an INDEPENDENT remote receiver that shares the final
        // output cadence but stays outside the local-radio DSP lane. Drain at most
        // audioSampleCount samples so it's fed at exactly the RX rate (its own
        // clock differs from the radio ADC; the source caps buffered latency to
        // bound that drift), then add it after local processing. When the slice
        // is disabled/muted Active is false and this is one bool
        // read. RX1 silent (audioSampleCount==0) reads nothing this tick.
        bool externalRxActive = _externalRxAudioSource.Active;
        int externalRxCount = 0;
        if (externalRxActive)
        {
            // Half-duplex boards may stop producing local RX audio while keyed.
            // Keep the independently clocked Kiwi draining at the normal 30 Hz
            // output cadence even when RX1's read count is zero.
            int requested = audioSampleCount > 0
                ? audioSampleCount
                : MonitorInjectSilentBlockSamples;
            int want = Math.Min(requested, _kiwiMixBuf.Length);
            externalRxCount = _externalRxAudioSource.Read(_kiwiMixBuf.AsSpan(0, want));
        }

        // RX1's own samples were already zeroed above when it is muted or TX
        // suppression is active without DUP; tell the mixer to drop RX1 from
        // the bus too. Every live, unmuted secondary still sums in normally.
        audioSampleCount = MixRxAudioN(
            audioBuf, audioSampleCount, _mixSlices.AsSpan(0, mixSliceCount),
            rx1Muted: rx1Muted);

        // Operator RX master mute, read ONCE this tick so every branch below agrees.
        // While muted, real RX audio (and CW sidetone) is published normally and the
        // sinks drop it — the mute is preserved. The Recorder's local-monitor lane is
        // the sole exception: it must stay audible on the PC output, so when muted we
        // do NOT mix it into the RX frame here and instead route it, recorder-only,
        // through the mute-exempt lane after the RX-publish block (see below).
        bool rxAudioMuted = _rxAudioMute?.IsMuted ?? false;
        bool externalRxPreserved = suppressRxAudioForTx && externalRxCount > 0;
        bool liveLocalRxAudioBeforeExternalRouting = audioSampleCount > 0;
        if (suppressRxAudioForTx && localRxAudioContinuous &&
            (!liveLocalRxAudioBeforeExternalRouting || externalRxPreserved))
        {
            localRxAudioContinuous = false;
            _localRxAudioContinuousForCurrentTx = false;
        }
        // Keep Kiwi outside the local-radio DSP/plugin/fade lane. That lane can
        // be nonlinear and stateful, so subtracting raw Kiwi afterward cannot
        // recover its contribution. Seed a silent output-sized local block when
        // Kiwi is the only available clock source; Kiwi is added immediately
        // before the final limiter below.
        //
        // Deliberately keyed off the raw suppressRxAudioForTx flag rather than
        // the latched DUP allowance above. This preserves the existing
        // "prefer Kiwi, drop the whole local mix" behaviour while TX/drain is
        // active. The common non-Kiwi case is unaffected because this is a
        // no-op whenever externalRxCount == 0.
        audioSampleCount = PrepareLocalRxForExternalOutput(
            audioBuf,
            audioSampleCount,
            externalRxCount,
            suppressRxAudioForTx);
        // With DUP active, the bus can carry live local audio even while the
        // keyed/post-TX suppression clock is running. Only fall back to the
        // synthesized suppressed/sidetone block when no local audio is present.
        bool liveLocalRxAudio = audioSampleCount > 0;
        bool suppressPublishedRxAudio =
            suppressRxAudioForTx && !liveLocalRxAudio && !externalRxPreserved;

        if (audioSampleCount > 0)
        {
            SanitizeMixedRxAudioBuffer(audioBuf.AsSpan(0, audioSampleCount));
            rxAudioRmsForMeter = Rms(audioBuf.AsSpan(0, audioSampleCount));

            // MOX-edge fade-out envelope. The post-TX fade-in is applied to the
            // final post-processed RX block below so plugins/modems/sidetone do
            // not reintroduce an abrupt edge after this early demod-stage ramp.
            if (_rxFadeOutPending)
            {
                if (externalRxPreserved || localRxAudioContinuous)
                {
                    // Consume the local-radio fade edge without applying it to
                    // independent Kiwi samples or local RX audio that has
                    // remained continuous since key-down.
                    _rxFadeOutPending = false;
                }
                else
                {
                    int n = Math.Min(RxFadeSamples, audioSampleCount);
                    for (int i = 0; i < n; i++)
                    {
                        float ramp = 1f - (float)(i + 1) / n;
                        audioBuf[i] *= ramp;
                    }
                    if (audioSampleCount > n)
                        Array.Clear(audioBuf, n, audioSampleCount - n);
                    _rxFadeOutPending = false;
                }
            }

            // The TX voice-processing audio chain (Compressor/EQ/VST etc.)
            // stays TX-only by design (operator decision 2026-04-30) — those
            // plugins are tuned for the mic path and share TXA-side instances.
            // RX audio plugins are a SEPARATE chain, declared by the
            // rx.post-demod manifest slot, wired through _rxAudioPluginHandler
            // below. The two never share plugin instances or IIR state.

            if (externalRxPreserved && (!txMonitorOn || _txMonitorMeterOnly))
            {
                // Preserve the established local-TX suppressed lane (silence
                // plus optional sidetone) and add Kiwi only at its output.
                // Do not advance local RX plugins/decoder/squelch merely because
                // an independent external receiver is enabled.
                PublishTxSuppressedAudio(
                    audioBuf,
                    audioSampleCount,
                    nowMs,
                    state.Squelch ?? new SquelchConfig(),
                    _kiwiMixBuf,
                    externalRxCount);
                MarkTxSuppressedAudioBlockPublished();
            }
            else if (ShouldPublishNormalRxAudio(txMonitorOn, suppressPublishedRxAudio, _txMonitorMeterOnly))
            {
                // FM squelch is owned by WDSP's FMSQ noise detector upstream;
                // the managed adaptive audio-RMS gate is invalid for FM (open-
                // channel hiss reads as high power). Resolve to the effective
                // config so the gate stays fully open in FM instead of fighting
                // FMSQ or gating on hiss.
                var squelch = ResolveManagedSquelchForMode(state.Squelch, state.Mode);
                UpdateAdaptiveSquelchMeter(
                    _adaptiveSquelch,
                    squelch,
                    AudioRmsToFallbackDbm(rxAudioRmsForMeter));

                // RX audio plugin insert (rx.post-demod slot, e.g. a CW SCAF
                // audio filter). Runs in place over the demodulated band audio
                // AFTER the MOX fade and BEFORE the sidetone mix, so the filter
                // shapes received audio without distorting the clean local
                // sidetone. Null handler (no RX plugin attached) is the common
                // case and a no-op — the RX path stays bit-identical.
                // FreeDV digital-voice insert (RX0 only). The radio runs USB
                // underneath, so audioBuf currently holds the received FreeDV
                // modem signal; when FreeDV is the active mode the modem
                // demodulates+decodes it back to speech in place (same sample
                // count, internally buffered, silence until sync). Runs BEFORE
                // the RX audio plugin + squelch so those shape decoded speech.
                _audioModem.SyncMode((byte)state.Mode);
                if (_audioModem.Active)
                {
                    if (audioSampleCount > 0)
                    {
                        _audioModem.ProcessRx(audioBuf.AsSpan(0, audioSampleCount));
                        // AF (listening) volume for FreeDV is applied HERE, on the
                        // decoded speech. WDSP's panel gain ran on the pre-decode
                        // modem audio that ProcessRx just discarded, so without
                        // this the AF slider has no effect on FreeDV volume. Placed
                        // before the RX audio plugin + squelch to match normal-mode
                        // ordering (WDSP applies AF before those managed inserts).
                        ApplyFreeDvAfGain(
                            audioBuf.AsSpan(0, audioSampleCount),
                            EffectiveAfGainDb(state.RxAfGainDb, state.Rx1AfGainDb));
                    }
                }

                if (_productAudio.Active && audioSampleCount > 0)
                    _productAudio.ProcessRx(audioBuf.AsSpan(0, audioSampleCount));

                var rxAudioHandler = _rxAudioPluginHandler;
                if (rxAudioHandler is not null && audioSampleCount > 0)
                    rxAudioHandler(audioBuf.AsSpan(0, audioSampleCount), audioSampleCount, AudioOutputRateHz);

                ApplyAdaptiveSquelch(
                    audioBuf.AsSpan(0, audioSampleCount),
                    squelch,
                    _adaptiveSquelch);

                // Slow post-WDSP perceived-loudness correction. Positive makeup
                // is fail-closed unless the independent RX0 passband estimator
                // has fresh evidence of a resolved RF signal, and is vetoed near
                // ADC overload. Thus post-AGC noise alone can never open it. Cuts
                // and the instantaneous peak guard remain active for loud arrivals.
                // Evidence is RX0-specific, so do not boost the shared mixed bus
                // when a secondary/Kiwi slice is present; per-RX evidence belongs
                // before a future per-receiver mixer, not inferred after summing.
                if (mixSliceCount == 0 && !externalRxActive)
                {
                    bool loudnessBoostAllowed =
                        RxConstantLoudnessBoostAllowed(Environment.TickCount64);
                    // The operator AF control precedes this stage. Shift the
                    // reference by the same requested dB so constant-loudness
                    // makeup preserves AF changes 1:1 instead of cancelling them.
                    double appliedAfDb = _audioModem.Active
                        ? 20.0 * Math.Log10(Math.Max(_freeDvAfGainLinear, 1.0e-9))
                        : _appliedRxAfGainDb;
                    ApplyRxAudioLeveler(
                        audioBuf.AsSpan(0, audioSampleCount),
                        ref _rxAudioLeveler,
                        enabled: state.RxLevelerEnabled,
                        rfSignalResolved: loudnessBoostAllowed,
                        adcOverloadRisk: !loudnessBoostAllowed,
                        levelReferenceOffsetDb: appliedAfDb,
                        config: state.RxLeveler);
                }
                else
                {
                    // A shared RMS controller would make one receiver duck the
                    // others. Keep the mixed bus untouched and let its existing
                    // final soft limiter handle coincident peaks.
                    _rxAudioLeveler = default;
                }

                // CW sidetone is mixed (+=) into the RX block so every
                // downstream sink — browser WS, native audio, TCI audio
                // stream — hears it on the same bus as band RX. The MOX
                // fade above silences the RXA contribution while keying;
                // when the sidetone source is idle, RenderInto returns
                // false immediately without touching the buffer.
                _sidetone?.RenderInto(audioBuf.AsSpan(0, audioSampleCount));

                // Mix any queued local-playback monitor audio (e.g. the Recorder
                // plugin playing a clip back while not transmitting) into the RX
                // block, so it reaches every sink in browser and desktop modes
                // alike. No-op (one volatile read) when nothing is queued.
                // Skipped while master-muted: the RX frame must stay pure so the
                // mute still silences it, and the recorder is instead drained onto
                // the mute-exempt lane below. Not skipping here would sum recorder
                // into the RX frame that the sink then drops => recorder inaudible.
                if (!rxAudioMuted)
                    MixMonitorInject(audioBuf.AsSpan(0, audioSampleCount));
                // Preserve the established local-radio order: limit the fully
                // processed local block first, then apply its post-TX fade.
                LimitRxAudioBuffer(audioBuf.AsSpan(0, audioSampleCount));
                int postTxFadeRemaining = Volatile.Read(ref _rxPostTxFadeInSamplesRemaining);
                // Local audio that remained continuous through TX and drain
                // must not receive a second fade. If it starved or Kiwi
                // preempted it, retain the raised-cosine soft resume.
                if (postTxFadeRemaining > 0 && localRxAudioContinuous)
                {
                    Volatile.Write(ref _rxPostTxFadeInSamplesRemaining, 0);
                    _localRxAudioContinuousForCurrentTx = false;
                }
                else if (postTxFadeRemaining > 0)
                {
                    int next = ApplyRxPostTxFadeIn(
                        audioBuf.AsSpan(0, audioSampleCount),
                        postTxFadeRemaining,
                        RxPostTxFadeInSamples);
                    Volatile.Write(ref _rxPostTxFadeInSamplesRemaining, next);
                }

                // Kiwi is independent of the local radio, so it bypasses local
                // RX plugins, squelch, MOX fades, and the post-TX settle fade.
                // A final safety limit is needed only after adding that external
                // contribution to the already-limited local output.
                if (externalRxCount > 0)
                {
                    audioSampleCount = MixExternalRxIntoTxMonitor(
                        audioBuf,
                        audioSampleCount,
                        _kiwiMixBuf,
                        externalRxCount);
                    LimitRxAudioBuffer(audioBuf.AsSpan(0, audioSampleCount));
                }

                double finalAudioRms = Rms(audioBuf.AsSpan(0, audioSampleCount));
                double finalAudioPeak = PeakAbs(audioBuf.AsSpan(0, audioSampleCount));

                var audioFrame = new AudioFrame(
                    Seq: ++_audioSeq,
                    TsUnixMs: nowMs,
                    RxId: 0,
                    Channels: 1,
                    SampleRateHz: (uint)AudioOutputRateHz,
                    SampleCount: (ushort)audioSampleCount,
                    Samples: new ReadOnlyMemory<float>(audioBuf, 0, audioSampleCount));
                CaptureAudioDiagnostics("rx", in audioFrame, finalAudioRms, finalAudioPeak, txMonitorOn, squelch);
                PublishAudio(in audioFrame);
                _productPluginAudio?.PublishRxAudio(
                    0, AudioOutputRateHz, audioBuf.AsSpan(0, audioSampleCount));
                RxAudioAvailable?.Invoke(0, AudioOutputRateHz, new ReadOnlyMemory<float>(audioBuf, 0, audioSampleCount));
                if (externalRxPreserved || (suppressRxAudioForTx && liveLocalRxAudio))
                {
                    // Advance the local-radio post-TX drain here too: normal
                    // RX audio published while suppressRxAudioForTx is still
                    // set means either Kiwi was preserved or DUP carried live
                    // local audio. The drain clock must keep ticking so the
                    // suppression state reaches zero after MOX-off even on a
                    // single-receiver full-duplex station.
                    MarkTxSuppressedAudioBlockPublished();
                }
            }
            else if (!txMonitorOn && suppressPublishedRxAudio)
            {
                PublishTxSuppressedAudio(
                    audioBuf,
                    audioSampleCount,
                    nowMs,
                    state.Squelch ?? new SquelchConfig());
                MarkTxSuppressedAudioBlockPublished();
            }
        }
        else if (!txMonitorOn && suppressPublishedRxAudio)
        {
            PublishTxSuppressedAudio(
                audioBuf,
                MonitorInjectSilentBlockSamples,
                nowMs,
                state.Squelch ?? new SquelchConfig());
            MarkTxSuppressedAudioBlockPublished();
        }
        else if (ShouldPublishNormalRxAudio(txMonitorOn, suppressPublishedRxAudio, _txMonitorMeterOnly) && !rxAudioMuted && MonitorBacklog > 0)
        {
            // FIX 4: RX produced no audio this tick (RX1 muted, or no band audio)
            // yet a local clip is playing back through the monitor-inject ring.
            // The audioSampleCount>0 path above — which mixes and publishes the
            // monitor inject — was skipped, so the clip would be silent even
            // though status says "playing". Synthesize a full-size silent RX
            // block, mix the queued playback into it, and publish so the operator
            // hears it on EVERY sink. STRICT no-op when the ring is empty (the
            // MonitorBacklog>0 guard is a single volatile read), so normal /
            // muted-RX behaviour is byte-identical when nothing is playing.
            // Deliberately does NOT fire RxAudioAvailable: that tap feeds RX
            // capture, which must stay byte-identical (no synthetic silence).
            int monBlock = Math.Min(audioBuf.Length, MonitorInjectSilentBlockSamples);
            Array.Clear(audioBuf, 0, monBlock);
            MixMonitorInject(audioBuf.AsSpan(0, monBlock));
            LimitRxAudioBuffer(audioBuf.AsSpan(0, monBlock));

            var injectFrame = new AudioFrame(
                Seq: ++_audioSeq,
                TsUnixMs: nowMs,
                RxId: 0,
                Channels: 1,
                SampleRateHz: (uint)AudioOutputRateHz,
                SampleCount: (ushort)monBlock,
                Samples: new ReadOnlyMemory<float>(audioBuf, 0, monBlock));
            PublishAudio(in injectFrame);
        }

        // RX master mute + Recorder local playback. The RX-publish block above kept
        // the RX frame PURE (recorder was not mixed in while muted), so the sinks
        // dropped it and the radio is genuinely muted — RX and CW sidetone silenced
        // on the PC output and both onboard speakers, exactly as before. Now drain
        // the monitor-inject ring into a recorder-ONLY block and publish it on the
        // mute-EXEMPT lane so the operator still hears their own playback on the PC
        // output. This is the SOLE monitor-ring consumer while muted (the in-frame
        // mix and the FIX-4 drain are both gated off by rxAudioMuted), so the ring
        // is drained exactly once per tick — no double-drain, no starvation. Like
        // FIX 4 it deliberately does NOT fire RxAudioAvailable: the RX-capture tap
        // (TCI / Recorder RX capture) must stay recorder-free and byte-identical.
        if (rxAudioMuted && ShouldPublishNormalRxAudio(txMonitorOn, suppressPublishedRxAudio, _txMonitorMeterOnly) && MonitorBacklog > 0)
        {
            int monBlock = Math.Min(audioBuf.Length, MonitorInjectSilentBlockSamples);
            Array.Clear(audioBuf, 0, monBlock);
            MixMonitorInject(audioBuf.AsSpan(0, monBlock));
            LimitRxAudioBuffer(audioBuf.AsSpan(0, monBlock));

            var exemptFrame = new AudioFrame(
                Seq: ++_audioSeq,
                TsUnixMs: nowMs,
                RxId: 0,
                Channels: 1,
                SampleRateHz: (uint)AudioOutputRateHz,
                SampleCount: (ushort)monBlock,
                Samples: new ReadOnlyMemory<float>(audioBuf, 0, monBlock));
            PublishExemptAudio(in exemptFrame);
        }

        if (txMonitorOn)
        {
            // Drain whatever the monitor RXA produced this tick. The buffer
            // shape matches the RX path (mono float32 @ 48 kHz) so it slots
            // into the same AudioFrame format with no front-end change. When
            // the chain is idle (no MOX, no mic) the monitor channel produces
            // silence, which is the correct behaviour for "preview mode but
            // operator isn't talking".
            int monCount = engine.ReadTxMonitorAudio(audioBuf.AsSpan());
            if (monCount > 0)
            {
                SanitizeAudioBuffer(audioBuf.AsSpan(0, monCount));
                double finalAudioRms = Rms(audioBuf.AsSpan(0, monCount));
                double finalAudioPeak = PeakAbs(audioBuf.AsSpan(0, monCount));

                var monFrame = new AudioFrame(
                    Seq: ++_audioSeq,
                    TsUnixMs: nowMs,
                    RxId: 0,
                    Channels: 1,
                    SampleRateHz: (uint)AudioOutputRateHz,
                    SampleCount: (ushort)monCount,
                    Samples: new ReadOnlyMemory<float>(audioBuf, 0, monCount));
                CaptureAudioDiagnostics(
                    _txMonitorMeterOnly ? "tx-monitor-meter-only" : "tx-monitor",
                    in monFrame,
                    finalAudioRms,
                    finalAudioPeak,
                    txMonitorOn,
                    state.Squelch ?? new SquelchConfig());
                // TX-air tap source: the processed transmit audio (what goes on
                // the air). Read-only fan-out to IRxAudioTapPlugin/ITxAudioTapPlugin
                // taps; null subscriber list = no cost. Fire it before Kiwi is
                // added to the operator monitor so the air tap stays TX-only.
                TxMonitorAudioAvailable?.Invoke(0, AudioOutputRateHz, new ReadOnlyMemory<float>(audioBuf, 0, monCount));
                // Monitor volume is an operator playback preference, not a
                // TX-chain control. Apply it only after diagnostics and the
                // TX-air tap have observed the unmodified signal. External RX
                // is mixed below and therefore keeps its own AF level.
                ApplyTxMonitorVolume(audioBuf.AsSpan(0, monCount), TxMonitorVolumeDb);
            }

            // Meter-only monitor (Auto Tune) intentionally has no monitor
            // broadcast; the ordinary RX path above has already carried Kiwi.
            // For the audible TX monitor, retain Kiwi alongside the local
            // sidetone/processed monitor instead of letting TX replace it.
            if (!_txMonitorMeterOnly)
            {
                int publishCount = monCount;
                // MON can remain enabled before, during, and after a keying
                // cycle, so Kiwi belongs in this lane whenever it has samples,
                // not only while the local-RX suppression latch is set. Respect
                // master RX mute: only the local monitor is mute-exempt.
                bool includeExternalRx = ShouldMixExternalRxIntoTxMonitor(
                    externalRxCount,
                    _txMonitorMeterOnly,
                    rxAudioMuted);
                if (includeExternalRx)
                {
                    publishCount = MixExternalRxIntoTxMonitor(
                        audioBuf,
                        monCount,
                        _kiwiMixBuf,
                        externalRxCount);
                    LimitRxAudioBuffer(audioBuf.AsSpan(0, publishCount));
                }

                if (publishCount > 0)
                {
                    var publishFrame = new AudioFrame(
                        Seq: monCount > 0 && !includeExternalRx ? _audioSeq : ++_audioSeq,
                        TsUnixMs: nowMs,
                        RxId: 0,
                        Channels: 1,
                        SampleRateHz: (uint)AudioOutputRateHz,
                        SampleCount: (ushort)publishCount,
                        Samples: new ReadOnlyMemory<float>(audioBuf, 0, publishCount));
                    if (rxAudioMuted)
                        PublishTxMonitorExemptAudio(in publishFrame);
                    else
                        PublishTxMonitorAudio(in publishFrame);
                }
            }
        }

        if (++_rxMeterTickMod >= RxMeterTickModulus && !_radio.IsProtocol3Active)
        {
            // Under P3 the WDSP RX channel is never fed IQ, so this meter block
            // would broadcast the "no data" floor (the −250 dBm S-meter bug).
            // The real RX meter rides the sidecar display frames and lands via
            // PublishProtocol3RxMeters below.
            _rxMeterTickMod = 0;
            double transverterMeterCorrectionDb =
                _radio.TransverterMeterCorrectionDb(state.VfoHz);
            double operatorSMeterOffsetDb = OperatorSMeterOffsetDb();
            double baseRxCalOffsetDb = RadioCalibrations.RxMeterOffsetDb(
                _radio.EffectiveBoardKind,
                _radio.EffectiveOrionMkIIVariant)
                + transverterMeterCorrectionDb
                + RxAttenuatorMeterOffsetDb(state);
            double rxCalOffsetDb = AddOperatorSMeterOffset(
                baseRxCalOffsetDb,
                operatorSMeterOffsetDb);

            // Prefer WDSP's S-meter when it is ticking. The native production-
            // sequence regression proves S_AV escapes its -400 startup sentinel
            // after IQ flows; retain the post-demod fallback for startup and any
            // genuinely unavailable native-meter interval.
            double rawDbm = engine.GetRxaSignalDbm(channel);
            double dbm;
            if (double.IsFinite(rawDbm) && rawDbm > -399.0)
            {
                dbm = ApplyRxMeterCalibration(rawDbm, rxCalOffsetDb);
            }
            else
            {
                // 0 dBFS audio ~= S9+ signal; calibrate against ambient band
                // noise later. Empirical offset of -50 dBm puts typical 20m
                // band noise near S2/S3 instead of pinning at S0.
                double rms = double.IsFinite(rxAudioRmsForMeter) ? rxAudioRmsForMeter : 0.0;
                dbm = AudioRmsToFallbackDbm(
                    rms,
                    RxFallbackMeterCorrectionDb(
                        state,
                        transverterMeterCorrectionDb,
                        operatorSMeterOffsetDb));
            }
            if (!double.IsFinite(dbm)) dbm = -160.0;
            _hub.Broadcast(new RxMeterFrame((float)dbm));
            RxMeterUpdated?.Invoke(channel, dbm);

            // Additive 0x19 broadcast (RxMetersV2Frame). Carries the full
            // set of WDSP RXA stage readings so the configurable Meters
            // Panel can render any of them; older clients that only know
            // 0x14 ignore this frame. Same 5 Hz cadence as 0x14 above.
            //
            var rx = engine.GetRxStageMeters(channel);
            var v2 = BuildRxMetersV2(rx, rxCalOffsetDb);
            // Feed Auto-AGC the Thetis-faithful tracked noise floor (#806):
            // a gated quiet-bin mean with 2 s attack + fast-attack, ported from
            // Thetis display.cs processNoiseFloor. NaN = no settled floor this
            // tick (fast-attack settling / TX / no spectrum) — the servo holds,
            // exactly as Thetis's timer skips ticks whose IsNoiseFloorGood is
            // false. Only pay for the snapshot copy + gated mean when Auto-AGC
            // is actually engaged — the servo early-returns otherwise anyway.
            double spectrumFloorDbm = double.NaN;
            if (state.AutoAgcEnabled)
            {
                double tracked = UpdateAutoAgcNoiseFloorDbm(
                    state, dbm, _keyed, Environment.TickCount64, out bool fromSpectrum);
                if (double.IsFinite(tracked))
                {
                    // Spectrum bins are raw (no cal of their own): put the floor
                    // on the calibrated dBm scale. The S-meter fallback scalar is
                    // already calibrated — adding the offset again would double it.
                    spectrumFloorDbm = fromSpectrum ? tracked + rxCalOffsetDb : tracked;
                }
            }
            _radio.HandleRxMetersForAutoAgc(dbm, spectrumFloorDbm, v2.AdcPk, v2.AgcGain, Environment.TickCount64);
            lock (_rxMeterDiagLock)
            {
                _diagRxMetersValid = true;
                _diagRxMetersMs = (long)nowMs;
                _diagRxMetersChannelId = channel;
                _diagRxDbm = dbm;
                _diagRxMeters = v2;
            }
            _hub.Broadcast(v2);
            RxMetersV2Updated?.Invoke(channel, v2);

            // Per-receiver S-meter (#1664). Everything above reads only the
            // primary WDSP channel, so the UI S-meter always tracked RX1 no
            // matter which receiver the operator had focused and tuned.
            // Broadcast a receiver-tagged RxMetersV2 for every live secondary
            // receiver (RxId = receiver index; RX1 is RxId 0, carried above) so
            // the frontend can render the focused receiver's own passband
            // reading. Only the tagged v2 frame is emitted here — the legacy
            // 0x14 frame, Auto-AGC servo, passband SNR, and the live-diagnostics
            // snapshot stay bound to RX1.
            for (int ri = 1; ri < MaxReceivers; ri++)
            {
                if (!SecondaryReceiverEnabled(ri, state)) continue;
                int secChan = Volatile.Read(ref _secondaryRx[ri].ChannelId);
                if (secChan < 0) continue;
                var (_, secVfoHz, _, _, _) = SecondaryRxParams(state, ri);
                double secBaseCalOffsetDb = RadioCalibrations.RxMeterOffsetDb(
                    _radio.EffectiveBoardKind,
                    _radio.EffectiveOrionMkIIVariant)
                    + _radio.TransverterMeterCorrectionDb(secVfoHz)
                    + RxAttenuatorMeterOffsetDb(state, ReceiverAdcSource(state, ri));
                double secCalOffsetDb = AddOperatorSMeterOffset(
                    secBaseCalOffsetDb,
                    operatorSMeterOffsetDb);
                var secRx = engine.GetRxStageMeters(secChan);
                var secV2 = BuildRxMetersV2(secRx, secCalOffsetDb) with { RxId = (byte)ri };
                _hub.Broadcast(secV2);
                RxMetersV2Updated?.Invoke(secChan, secV2);
            }

            // Passband SNR uses WDSP's independent average-power PSD
            // output. Never substitute the visual waterfall floor: its peak
            // detector/log averaging and subscriber-driven drain are unsuitable
            // for a signal report.
            bool snrFresh = engine.TryGetRxSnrPowerSpectrum(
                channel, _snrPowerBuf, out var snrInfo);
            if (snrFresh && snrInfo.IsValid)
                Array.Reverse(_snrPowerBuf, 0, snrInfo.PixelCount);
            var engineMode = RadioService.EffectiveEngineMode(state.Mode, state.VfoHz);
            var (snrFilterLow, snrFilterHigh) = SignedRxFilterFor(state, engineMode);
            long effectiveVfoHz = CwOffset.EffectiveLoHz(state.Mode, state.VfoHz)
                + (state.RitEnabled ? state.RitHz : 0L);
            long analyzerCenterHz = state.RadioLoHz > 0 ? state.RadioLoHz : state.VfoHz;
            double hzPerPixel = snrInfo.IsValid
                ? (double)snrInfo.SampleRateHz / snrInfo.PixelCount
                : double.NaN;
            var snrResult = _snrEstimator.Update(
                snrInfo.IsValid ? _snrPowerBuf.AsSpan(0, snrInfo.PixelCount) : ReadOnlySpan<float>.Empty,
                hzPerPixel,
                effectiveVfoHz + snrFilterLow - analyzerCenterHz,
                effectiveVfoHz + snrFilterHigh - analyzerCenterHz,
                new InPassbandSnrEstimator.MeasurementKey(
                    RxId: 0,
                    SampleRateHz: snrInfo.SampleRateHz,
                    PixelCount: snrInfo.PixelCount,
                    AnalyzerGeneration: snrInfo.Generation,
                    AnalyzerCenterHz: analyzerCenterHz,
                    EffectiveVfoHz: effectiveVfoHz,
                    FilterLowHz: snrFilterLow,
                    FilterHighHz: snrFilterHigh),
                Environment.TickCount64,
                fresh: snrFresh,
                keyed: _keyed);
            UpdateRxConstantLoudnessEvidence(
                snrResult, v2.SignalAv, rxCalOffsetDb, v2.AdcPk,
                snrFresh, Environment.TickCount64);
            var quality = BuildRxSignalQualityFrame(0, snrResult, rxCalOffsetDb);
            _hub.Broadcast(quality);
            RxSignalQualityUpdated?.Invoke(quality);
        }
        return true;
        }
        finally { _tickGate.Exit(); }
    }

    /// <summary>
    /// Raised when an RXA stage-meter snapshot is broadcast (approximately
    /// 5 Hz, alongside <see cref="RxMeterUpdated"/>). Arguments:
    /// (channelId, frame). Test seam — the broadcast itself is a no-op
    /// when no clients are attached, so this event lets unit tests
    /// observe the encoded frame without instantiating a WebSocket.
    /// </summary>
    public event Action<int, RxMetersV2Frame>? RxMetersV2Updated;

    /// <summary>Test/diagnostic seam for the measured passband-quality frame.</summary>
    public event Action<RxSignalQualityFrame>? RxSignalQualityUpdated;

    /// <summary>
    /// Protocol-3 RX meter ingress (S-meter fix): the sidecar frame forwarder
    /// calls this with the n9dsp per-channel meter readings that ride the
    /// display frames. Publishes through the exact same outputs as the WDSP
    /// meter tick — 0x14 RxMeterFrame, 0x19 RxMetersV2Frame, the CAT/TCI
    /// RxMeterUpdated event, Auto-AGC, and the live-diagnostics snapshot — so
    /// every meter consumer works identically under P3. The WDSP tick's own
    /// meter block is suppressed while P3 is active.
    /// <paramref name="dbfsRaw"/> is n9dsp's uncalibrated dBFS reading; the
    /// per-board RX meter offset is applied here, same as the P2 path.
    /// </summary>
    public void PublishProtocol3RxMeters(
        int channel,
        double dbfsRaw,
        double agcGainDb,
        double adcHeadroomDb)
    {
        if (!_radio.IsProtocol3Active) return;
        var state = _radio.Snapshot();
        long? receiverVfoHz = Protocol3MeterReceiverVfoHz(state, channel);
        byte? receiverAdcSource = Protocol3MeterReceiverAdcSource(state, channel);
        double operatorSMeterOffsetDb = OperatorSMeterOffsetDb();
        double baseRxCalOffsetDb = RadioCalibrations.RxMeterOffsetDb(
            _radio.EffectiveBoardKind,
            _radio.EffectiveOrionMkIIVariant)
            + (receiverVfoHz is long vfoHz
                ? _radio.TransverterMeterCorrectionDb(vfoHz)
                : 0.0)
            + (receiverAdcSource is byte adcSource
                ? RxAttenuatorMeterOffsetDb(state, adcSource)
                : 0.0);
        double rxCalOffsetDb = AddOperatorSMeterOffset(
            baseRxCalOffsetDb,
            operatorSMeterOffsetDb);
        double dbm = ApplyRxMeterCalibration(dbfsRaw, rxCalOffsetDb);
        if (!double.IsFinite(dbm)) dbm = -160.0;
        float adcPk = (float)(double.IsFinite(adcHeadroomDb) ? -adcHeadroomDb : -200.0);
        float agc = (float)(double.IsFinite(agcGainDb) ? agcGainDb : 0.0);
        // Post-AGC envelope estimate: signal + inserted AGC gain. n9dsp does
        // not export a discrete envelope tap yet; this keeps the Meters Panel
        // fields plausible until one lands.
        float env = (float)Math.Min(dbm + agc, 0.0);
        var v2 = new RxMetersV2Frame(
            SignalPk: (float)dbm,
            SignalAv: (float)dbm,
            AdcPk: adcPk,
            AdcAv: adcPk,
            AgcGain: agc,
            AgcEnvPk: env,
            AgcEnvAv: env,
            // Protocol 3 does not feed its display IQ through the local WDSP
            // analyzer, so there is no honest max-bin value to publish.
            SignalMaxBin: -200f);

        _hub.Broadcast(new RxMeterFrame((float)dbm));
        RxMeterUpdated?.Invoke(channel, dbm);
        // P3 has no panadapter spectrum, so the tracker takes its S-meter
        // fallback branch (gated + attack-smoothed) instead of the raw
        // signalDbm the old loop consumed directly. dbm here is ALREADY
        // cal-offset (ApplyRxMeterCalibration above), so no offset is added
        // to the tracked value — unlike the raw-bin main path.
        var p3State = _radio.Snapshot();
        double p3Floor = double.NaN;
        if (p3State.AutoAgcEnabled)
        {
            double tracked = UpdateAutoAgcNoiseFloorDbm(
                p3State, dbm, _keyed, Environment.TickCount64, out _);
            if (double.IsFinite(tracked))
                p3Floor = tracked;
        }
        _radio.HandleRxMetersForAutoAgc(dbm, p3Floor, v2.AdcPk, v2.AgcGain, Environment.TickCount64);
        lock (_rxMeterDiagLock)
        {
            _diagRxMetersValid = true;
            _diagRxMetersMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _diagRxMetersChannelId = channel;
            _diagRxDbm = dbm;
            _diagRxMeters = v2;
        }
        _hub.Broadcast(v2);
        RxMetersV2Updated?.Invoke(channel, v2);
        // Protocol 3 currently has no normalized raw-spectrum passband source.
        // Explicitly clear quality rather than falling back to a display floor.
        Volatile.Write(ref _rxLevelerRfSignalResolved, 0);
        Volatile.Write(ref _rxLevelerAdcOverloadRisk, 0);
        _rxLevelerRfAcquireHits = 0;
        _rxLevelerRfReleaseMisses = 0;
        Interlocked.Exchange(ref _rxLevelerRfEvidenceMs, Environment.TickCount64);
        var unavailableQuality = RxSignalQualityFrame.Unavailable((byte)Math.Clamp(channel, 0, byte.MaxValue));
        _hub.Broadcast(unavailableQuality);
        RxSignalQualityUpdated?.Invoke(unavailableQuality);
    }

    internal static long? Protocol3MeterReceiverVfoHz(StateDto state, int channel)
    {
        if (channel == 0) return state.VfoHz;
        var receivers = state.Receivers;
        if (receivers is null) return null;
        for (int i = 0; i < receivers.Count; i++)
        {
            var receiver = receivers[i];
            if (receiver.Index == channel && receiver.Enabled)
                return receiver.VfoHz;
        }
        return null;
    }

    internal static byte? Protocol3MeterReceiverAdcSource(StateDto state, int channel)
    {
        if (channel == 0) return PrimaryReceiverAdcSource(state);
        var receivers = state.Receivers;
        if (receivers is null) return null;
        for (int i = 0; i < receivers.Count; i++)
        {
            var receiver = receivers[i];
            if (receiver.Index == channel && receiver.Enabled)
                return receiver.AdcSource;
        }
        return null;
    }

    internal static RxSignalQualityFrame BuildRxSignalQualityFrame(
        byte rxId,
        InPassbandSnrEstimator.Result result,
        double rxCalibrationDb)
    {
        if (!result.IsValid || !double.IsFinite(rxCalibrationDb))
            return RxSignalQualityFrame.Unavailable(rxId);

        return new RxSignalQualityFrame(
            RxId: rxId,
            SnrDb: (float)result.SnrDb,
            SignalOnlyDbm: (float)(result.SignalOnlyDb + rxCalibrationDb),
            IntegratedNoiseDbm: (float)(result.IntegratedNoiseDb + rxCalibrationDb),
            Confidence: (float)Math.Clamp(result.Confidence, 0.0, 1.0));
    }

    private void UpdateRxConstantLoudnessEvidence(
        InPassbandSnrEstimator.Result quality,
        double currentSignalDbm,
        double rxCalibrationDb,
        float adcPkDbfs,
        bool spectrumFresh,
        long nowMs)
    {
        long previousEvidenceMs = Interlocked.Read(ref _rxLevelerRfEvidenceMs);
        bool evidenceCurrent = RxConstantLoudnessEvidenceIsCurrent(
            previousEvidenceMs, nowMs);
        if (!evidenceCurrent)
        {
            Volatile.Write(ref _rxLevelerRfSignalResolved, 0);
            _rxLevelerRfAcquireHits = 0;
            _rxLevelerRfReleaseMisses = 0;
        }

        bool adcRisk = !float.IsFinite(adcPkDbfs) ||
            adcPkDbfs <= RxConstantLoudnessAdcUnavailableDbfs ||
            adcPkDbfs > RxConstantLoudnessAdcVetoDbfs;
        bool wasResolved = Volatile.Read(ref _rxLevelerRfSignalResolved) != 0;
        bool measurementAllowsBoost = spectrumFresh &&
            RxConstantLoudnessEvidenceAllowsBoost(
                quality, currentSignalDbm, rxCalibrationDb, adcPkDbfs, wasResolved);
        var decision = AdvanceRxConstantLoudnessEvidence(
            spectrumFresh,
            measurementAllowsBoost,
            adcRisk,
            wasResolved,
            _rxLevelerRfAcquireHits,
            _rxLevelerRfReleaseMisses);

        _rxLevelerRfAcquireHits = decision.AcquireHits;
        _rxLevelerRfReleaseMisses = decision.ReleaseMisses;
        Volatile.Write(ref _rxLevelerRfSignalResolved, decision.Resolved ? 1 : 0);
        Volatile.Write(ref _rxLevelerAdcOverloadRisk, adcRisk ? 1 : 0);
        // A processed rejection reports RefreshAge too, but only resolved or
        // acquiring evidence (and the immediate ADC veto) may extend usable age.
        if (decision.RefreshAge &&
            (decision.Resolved || decision.AcquireHits > 0 || adcRisk))
            Interlocked.Exchange(ref _rxLevelerRfEvidenceMs, nowMs);
    }

    internal static (bool Resolved, int AcquireHits, int ReleaseMisses, bool RefreshAge)
        AdvanceRxConstantLoudnessEvidence(
            bool spectrumFresh,
            bool measurementAllowsBoost,
            bool adcRisk,
            bool wasResolved,
            int acquireHits,
            int releaseMisses)
    {
        if (adcRisk)
            return (false, 0, 0, true);
        if (!spectrumFresh)
            return (wasResolved, Math.Max(0, acquireHits), Math.Max(0, releaseMisses), false);

        if (!wasResolved)
        {
            if (!measurementAllowsBoost)
                return (false, 0, 0, true);

            int hits = Math.Max(0, acquireHits) + 1;
            return hits >= RxConstantLoudnessAcquireFrames
                ? (true, 0, 0, true)
                : (false, hits, 0, true);
        }

        if (measurementAllowsBoost)
            return (true, 0, 0, true);

        int misses = Math.Max(0, releaseMisses) + 1;
        return misses < RxConstantLoudnessReleaseFrames
            ? (true, 0, misses, true)
            : (false, 0, 0, true);
    }

    internal static bool RxConstantLoudnessEvidenceIsCurrent(
        long evidenceMs,
        long nowMs) =>
        evidenceMs != long.MinValue &&
        nowMs >= evidenceMs &&
        nowMs - evidenceMs <= RxConstantLoudnessEvidenceMaxAgeMs;

    internal static bool RxConstantLoudnessEvidenceAllowsBoost(
        InPassbandSnrEstimator.Result quality,
        double currentSignalDbm,
        double rxCalibrationDb,
        float adcPkDbfs,
        bool wasResolved = false)
    {
        if (!quality.IsValid ||
            quality.Confidence < RxConstantLoudnessMinConfidence ||
            !double.IsFinite(currentSignalDbm) ||
            currentSignalDbm <= RxConstantLoudnessMeterUnavailableDbm ||
            !double.IsFinite(rxCalibrationDb) ||
            !float.IsFinite(adcPkDbfs) ||
            adcPkDbfs <= RxConstantLoudnessAdcUnavailableDbfs ||
            adcPkDbfs > RxConstantLoudnessAdcVetoDbfs)
            return false;

        // The passband estimator is deliberately averaged for stable reports.
        // Cross-check its noise estimate with the current stage meter so a
        // vanished station cannot keep old averaged SNR alive and open makeup.
        double currentExcessDb = currentSignalDbm -
            (quality.IntegratedNoiseDb + rxCalibrationDb);
        double thresholdDb = wasResolved
            ? RxConstantLoudnessReleaseTotalExcessDb
            : RxConstantLoudnessMinCurrentTotalExcessDb;
        return currentExcessDb >= thresholdDb;
    }

    private bool RxConstantLoudnessBoostAllowed(long nowMs)
    {
        long evidenceMs = Interlocked.Read(ref _rxLevelerRfEvidenceMs);
        return RxConstantLoudnessEvidenceIsCurrent(evidenceMs, nowMs) &&
            Volatile.Read(ref _rxLevelerRfSignalResolved) != 0 &&
            Volatile.Read(ref _rxLevelerAdcOverloadRisk) == 0;
    }

    /// <summary>
    /// Protocol-3 final RX audio diagnostics. P3 audio bypasses the WDSP tick and
    /// is published by <see cref="Protocol3SidecarFrameForwarder"/>, but live DSP
    /// diagnostics still need the same final-audio freshness/RMS/peak evidence as
    /// the P2 path. This is read-only telemetry; sinks are still fanned out by the
    /// P3 forwarder.
    /// </summary>
    public void PublishProtocol3RxAudioDiagnostics(in AudioFrame frame)
    {
        if (!_radio.IsProtocol3Active) return;
        if (frame.RxId != 0 ||
            frame.Channels != 1 ||
            frame.SampleRateHz != AudioOutputRateHz ||
            frame.SampleCount == 0)
        {
            return;
        }

        var samples = frame.Samples.Span;
        int count = Math.Min(frame.SampleCount, samples.Length);
        if (count <= 0) return;
        var block = samples[..count];
        double rms = Rms(block);
        double peak = PeakAbs(block);
        bool txMonitorRequested = _radio.Snapshot().TxMonitorEnabled;

        lock (_audioDiagLock)
        {
            _diagAudioValid = true;
            _diagAudioFrameMs = (long)frame.TsUnixMs;
            _diagAudioSeq = frame.Seq;
            _diagAudioFrameCount++;
            _diagAudioSource = "p3-rx";
            _diagAudioSampleRateHz = checked((int)frame.SampleRateHz);
            _diagAudioSampleCount = count;
            _diagAudioRms = rms;
            _diagAudioPeak = peak;
            _diagAudioTxMonitorRequested = txMonitorRequested;
            _diagAudioSquelchEnabled = false;
            _diagAudioSquelchOpen = true;
            _diagAudioSquelchTailActive = false;
            _diagAudioSquelchGain = 1.0;
            _diagAudioLevelerValid = false;
            _diagAudioLevelerInputRmsDbfs = double.NaN;
            _diagAudioLevelerOutputRmsDbfs = double.NaN;
            _diagAudioLevelerInputPeakDbfs = double.NaN;
            _diagAudioLevelerOutputPeakDbfs = double.NaN;
            _diagAudioLevelerDesiredGainDb = double.NaN;
            _diagAudioLevelerAppliedGainDb = double.NaN;
            _diagAudioLevelerGainDeltaDb = double.NaN;
            _diagAudioLevelerPeakHeadroomDb = double.NaN;
            _diagAudioLevelerPreLimitPeakDbfs = double.NaN;
            _diagAudioLevelerOutputLimitReductionDb = double.NaN;
            _diagAudioLevelerOutputLimitSampleCount = 0;
            _diagAudioLevelerPauseHoldBlocks = 0;
            _diagAudioLevelerBoostSlewLimited = false;
            _diagAudioLevelerPeakLimited = false;
            _diagAudioLevelerOutputLimited = false;
            _diagAudioSquelchMode = "off";
            _diagAudioSquelchGateSource = "disabled";
            _diagAudioSquelchOpenKnown = true;
            _diagAudioMonitorBacklogSamples = 0;
            _diagAudioSinkCount = _audioSinks.Length;
        }
    }

    /// <summary>
    /// Build the wire frame from a raw <see cref="RxStageMeters"/>
    /// snapshot, applying <paramref name="calOffsetDb"/> only to the
    /// dBm-scale fields (Signal*, AgcEnv*). ADC* is dBFS (raw ADC,
    /// board-independent) and AgcGain is dB of insertion gain — both get
    /// the raw value. Exposed for unit tests so the encoding rule can be
    /// asserted without spinning up a hub or pipeline tick.
    /// </summary>
    public static RxMetersV2Frame BuildRxMetersV2(in RxStageMeters rx, double calOffsetDb)
    {
        float cal = (float)calOffsetDb;
        return new RxMetersV2Frame(
            SignalPk: ApplyRxMeterCalibration(rx.SignalPk, cal),
            SignalAv: ApplyRxMeterCalibration(rx.SignalAv, cal),
            AdcPk: rx.AdcPk,
            AdcAv: rx.AdcAv,
            AgcGain: rx.AgcGain,
            AgcEnvPk: ApplyRxMeterCalibration(rx.AgcEnvPk, cal),
            AgcEnvAv: ApplyRxMeterCalibration(rx.AgcEnvAv, cal),
            SignalMaxBin: ApplyRxMeterCalibration(rx.SignalMaxBin, cal));
    }

    /// <summary>
    /// Hardware attenuation lowers raw ADC/DSP readings but not the signal at
    /// the antenna connector. Add the effective manual + Auto-ATT value back
    /// to S-meter/spectrum dBm fields, matching Thetis's step-ATT correction.
    /// ADC dBFS remains deliberately uncorrected so overload headroom stays
    /// truthful.
    /// </summary>
    internal static double RxAttenuatorMeterOffsetDb(StateDto state) =>
        Math.Clamp(state.AttenDb + state.AttOffsetDb, 0, 31);

    internal static double RxFallbackMeterCorrectionDb(
        StateDto state,
        double transverterMeterCorrectionDb,
        double operatorOffsetDb = 0.0) =>
        AddOperatorSMeterOffset(
            transverterMeterCorrectionDb + RxAttenuatorMeterOffsetDb(state),
            operatorOffsetDb);

    internal static double AddOperatorSMeterOffset(
        double existingCalibrationDb,
        double operatorOffsetDb) =>
        existingCalibrationDb + operatorOffsetDb;

    internal static double RxAttenuatorMeterOffsetDb(StateDto state, byte meteredAdcSource) =>
        (meteredAdcSource == PrimaryReceiverAdcSource(state))
            ? RxAttenuatorMeterOffsetDb(state)
            : 0.0;

    private static byte PrimaryReceiverAdcSource(StateDto state)
    {
        if (state.Receivers is { } receivers)
        {
            for (int i = 0; i < receivers.Count; i++)
            {
                if (receivers[i].Index == 0) return receivers[i].AdcSource;
            }
        }
        return 0;
    }

    private static double ApplyRxMeterCalibration(double value, double calOffsetDb) =>
        value <= -199.5 ? value : value + calOffsetDb;

    private static float ApplyRxMeterCalibration(float value, float calOffsetDb) =>
        value <= -199.5f ? value : value + calOffsetDb;
}

public sealed record DspNrRuntimeSnapshot(
    bool WdspActive,
    bool WdspNativeLoadable,
    bool WdspEmnrPost2Available,
    bool WdspNr4SbnrAvailable,
    string Nr4Readiness,
    string RequestedNrMode,
    string EffectiveNrMode);

internal sealed record DspRxChainDiagnosticsDto(
    int SchemaVersion,
    string Status,
    string Mode,
    int FilterLowHz,
    int FilterHighHz,
    string? FilterPresetName,
    string AgcMode,
    double AgcTopDb,
    bool AutoAgcEnabled,
    bool RxLevelerEnabled,
    double AgcOffsetDb,
    double EffectiveAgcTopDb,
    bool SquelchEnabled,
    bool SquelchAdaptive,
    int SquelchLevel,
    string RequestedNrMode,
    string EffectiveNrMode,
    bool AnfEnabled,
    bool SnbEnabled,
    bool NbpNotchesEnabled,
    bool EffectiveNbpNotchesRun,
    string NbMode,
    double NbThreshold,
    int ManualNotchCount,
    int ActiveManualNotchCount,
    bool WdspActive,
    bool WdspNativeLoadable,
    bool WdspEmnrPost2Available,
    bool WdspNr4SbnrAvailable,
    string Nr4Readiness,
    bool AppliedNrMatchesRequested,
    bool AppliedAgcMatchesRequested,
    bool AppliedSquelchMatchesRequested,
    string[] ActiveFeatures,
    string[] QualityReasons,
    string DiagnosticRecommendation);

internal sealed record RxMetersDiagnosticsDto(
    int SchemaVersion,
    string Status,
    string Source,
    bool Fresh,
    bool Stale,
    long? AgeMs,
    int ChannelId,
    double? RxDbm,
    double? SignalPkDbm,
    double? SignalAvDbm,
    double? AdcPkDbfs,
    double? AdcAvDbfs,
    double? AdcHeadroomDb,
    double? AgcGainDb,
    double? AgcEnvPkDbm,
    double? AgcEnvAvDbm,
    bool SignalUsable,
    bool AdcUsable,
    bool AgcEnvelopeUsable,
    string DiagnosticRecommendation);

internal sealed record RxDynamicRangeActionDto(
    string Id,
    string Label,
    string Status,
    string Notes);

internal sealed record RxDynamicRangeDiagnosticsDto(
    int SchemaVersion,
    string Status,
    string Tone,
    bool Fresh,
    bool Stale,
    long? AgeMs,
    string Source,
    int SampleRateHz,
    int AttenDb,
    int AttOffsetDb,
    int EffectiveAttenDb,
    bool PreampOn,
    bool AutoAttEnabled,
    bool AdcProtectionEnabled,
    bool AdcOverloadWarning,
    int AdcOverloadLevel,
    double TargetHeadroomMinDb,
    double TargetHeadroomMaxDb,
    double? RxDbm,
    double? SignalPkDbm,
    double? AdcPkDbfs,
    double? AdcHeadroomDb,
    double? AgcGainDb,
    bool HeadroomOptimal,
    bool OverloadRisk,
    bool WeakSignalOpportunity,
    bool FrontEndUnderused,
    string[] Reasons,
    RxDynamicRangeActionDto[] Actions,
    string DiagnosticRecommendation);

internal sealed record RxListenabilityDiagnosticsDto(
    int SchemaVersion,
    string Status,
    string Tone,
    bool SignalPresent,
    bool AudioRecovered,
    string Blocker,
    string Recommendation);

internal sealed record AudioPathDiagnosticsDto(
    int SchemaVersion,
    string Status,
    string Source,
    bool Fresh,
    bool Stale,
    long? AgeMs,
    long FramesBroadcast,
    uint LastSeq,
    int SampleRateHz,
    int SampleCount,
    double? RmsLinear,
    double? PeakLinear,
    double? RmsDbfs,
    double? PeakDbfs,
    bool TxMonitorRequested,
    bool SquelchEnabled,
    bool SquelchOpen,
    bool SquelchTailActive,
    double? SquelchGateGain,
    bool RxAudioLevelerEnabled,
    double? RxAudioLevelerInputRmsDbfs,
    double? RxAudioLevelerOutputRmsDbfs,
    double? RxAudioLevelerInputPeakDbfs,
    double? RxAudioLevelerOutputPeakDbfs,
    double? RxAudioLevelerDesiredGainDb,
    double? RxAudioLevelerAppliedGainDb,
    double? RxAudioLevelerGainDeltaDb,
    double? RxAudioLevelerPeakHeadroomDb,
    double? RxAudioLevelerPreLimitPeakDbfs,
    double? RxAudioLevelerOutputLimitReductionDb,
    int? RxAudioLevelerOutputLimitSampleCount,
    int? RxAudioLevelerPauseHoldBlocks,
    bool? RxAudioLevelerBoostSlewLimited,
    bool? RxAudioLevelerPeakLimited,
    bool? RxAudioLevelerOutputLimited,
    string SquelchMode,
    string SquelchGateSource,
    bool SquelchOpenKnown,
    long MonitorBacklogSamples,
    int AudioSinkCount,
    string DiagnosticRecommendation);
