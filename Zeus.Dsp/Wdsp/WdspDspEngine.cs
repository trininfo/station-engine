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
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Zeus.Contracts;

namespace Zeus.Dsp.Wdsp;

public sealed class WdspDspEngine : IDspEngine, ITxAudioPluginHost
{
    // A 512-sample host exchange and DSP partition gives RXA a 10.67 ms
    // scheduling quantum at 48 kHz. Protocol clients already partial-frame IQ
    // into the engine, so the radio packet geometry remains unchanged.
    private const int RxaInSize = 512;
    private const int RxaDspSize = 512;

    // TXA profile varies by protocol — OpenTxChannel picks the right one:
    //   P1 (48 kHz DAC) : in=512@48k, dsp=512@48k, out=512@48k, CFIR off
    //   P2 (192 kHz DAC): in=256@48k, dsp=512@96k, out=1024@192k, CFIR on
    // pihpsdr transmitter.c:954-997 (protocol switch → buffer_size / dsp_rate /
    // ratio) and Thetis audio.cs:1800-1809 (SampleRateTX + SetTXACFIRRun)
    // define these exactly. Zeus was previously hard-coded to the P1 profile
    // regardless of protocol, which on P2 left the G2 DUC starved (it runs
    // at 192 kHz but we fed 48 kHz) and generated 8-10 kHz close-in spurs
    // on TUN and MOX.
    private const int TxaInSizeP1 = 512;
    private const int TxaDspSizeP1 = 512;
    private const int TxaOutSizeP1 = 512;
    private const int TxaInSizeP2 = 256;
    private const int TxaDspSizeP2 = 512;
    private const int TxaOutSizeP2 = 1024;

    // Latched values chosen at OpenTxChannel time; ProcessTxBlock uses them
    // to size the mic / iq spans. Default to the P1 profile so tests and
    // bring-up code that open TXA without specifying a protocol still work.
    private int _txaInSize = TxaInSizeP1;
    private int _txaDspSize = TxaDspSizeP1;
    private int _txaOutSize = TxaOutSizeP1;
    private int _txaInputRateHz = 48_000;
    private int _txaDspRateHz = 48_000;
    private int _txaOutputRateHz = 48_000;
    private bool _txaCfirRun;

    // Leveler max-gain ceiling in dB applied at TXA init. Thetis ships
    // 15 dB (radio.cs:2981 tx_leveler_max_gain = 15.0). Zeus used to ship
    // 5 dB — which turned out to be the WDSP C-init default (TXA.c:169
    // 1.778 linear), not a considered choice. With Compressor off the
    // Leveler is the only makeup stage, so 5 dB left operators 10+ dB
    // below Thetis-equivalent modulation on the air.
    //
    // Operator (kb2uka) requested 8 dB: his external analog rack already
    // provides significant preamp and pre-DSP conditioning, so a smaller
    // Leveler ceiling sounds cleaner than the Thetis stock 15 dB on his
    // setup. Operators without an external rack can push it up to 15 via
    // POST /api/tx/leveler-max-gain.
    internal const double DefaultLevelerMaxGainDb = 8.0;

    // Legacy aliases — RXA-side code still references these. Kept = RxaInSize
    // / RxaDspSize so existing callsites (audio outSamples math, channel
    // structs, etc.) don't have to change.
    private const int InSize = RxaInSize;
    private const int DspSize = RxaDspSize;
    private const int DspRate = 48_000;
    private const int OutputRate = 48_000;
    private const int MaxFftSize = 262_144;
    private const int AnalyzerFftSize = 16_384;
    private const int SnrAnalyzerPixelWidth = RxSnrSpectrumInfo.MaxPixelCount;
    private const int AnalyzerFps = 30;
    private const double MaxBinTauSeconds = 0.5;
    private const int AnalyzerWindow = 2;
    private const double AnalyzerKaiserPi = 14.0;
    private const double AnalyzerKeepTime = 0.1;
    private const int MaxWdspNativeChannels = 32; // native/wdsp/comm.h MAX_CHANNELS

    private static readonly string[] EmnrPost2RequiredExports =
    [
        nameof(NativeMethods.SetRXAEMNRpost2Run),
        nameof(NativeMethods.SetRXAEMNRpost2Factor),
        nameof(NativeMethods.SetRXAEMNRpost2Nlevel),
        nameof(NativeMethods.SetRXAEMNRpost2Taper),
        nameof(NativeMethods.SetRXAEMNRpost2Rate),
    ];

    private static readonly string[] SbnrRequiredExports =
    [
        nameof(NativeMethods.SetRXASBNRRun),
        nameof(NativeMethods.SetRXASBNRPosition),
        nameof(NativeMethods.SetRXASBNRreductionAmount),
        nameof(NativeMethods.SetRXASBNRsmoothingFactor),
        nameof(NativeMethods.SetRXASBNRwhiteningFactor),
        nameof(NativeMethods.SetRXASBNRnoiseRescale),
        nameof(NativeMethods.SetRXASBNRpostFilterThreshold),
        nameof(NativeMethods.SetRXASBNRnoiseScalingType),
    ];

    private static readonly string[] Nr3RnnrRequiredExports =
    [
        nameof(NativeMethods.SetRXARNNRRun),
        nameof(NativeMethods.SetRXARNNRPosition),
        nameof(NativeMethods.RNNRloadModel),
    ];

    private static readonly string[] NnrRequiredExports =
    [
        nameof(NativeMethods.SetRXANNRRun),
        nameof(NativeMethods.SetRXANNRPosition),
        nameof(NativeMethods.SetRXANNRModel),
        nameof(NativeMethods.SetRXANNRMaskFloor),
        nameof(NativeMethods.SetRXANNRAlpha),
        nameof(NativeMethods.SetRXANNRAlphaKnee),
        nameof(NativeMethods.SetRXANNRTau),
        nameof(NativeMethods.SetRXANNRMaxGain),
        nameof(NativeMethods.SetRXANNRSmooth),
    ];

    private static readonly string[] EngineVersionRequiredExports =
    [
        nameof(NativeMethods.GetWDSPVersion),
    ];

    private static bool AllNativeExportsAvailable(string[] symbolNames)
    {
        if (!WdspNativeLoader.TryProbe()) return false;
        for (int i = 0; i < symbolNames.Length; i++)
        {
            if (!WdspNativeLoader.TryProbeExport(symbolNames[i]))
                return false;
        }
        return true;
    }

    public static bool NativeLibraryLoadable => WdspNativeLoader.TryProbe();

    public static bool EmnrPost2Available => AllNativeExportsAvailable(EmnrPost2RequiredExports);

    public static bool Nr4SbnrAvailable => AllNativeExportsAvailable(SbnrRequiredExports);

    public static bool Nr3RnnrAvailable => AllNativeExportsAvailable(Nr3RnnrRequiredExports);

    public static bool NnrAvailable => AllNativeExportsAvailable(NnrRequiredExports);

    /// <summary>
    /// The loaded libwdsp exports <c>GetWDSPVersion</c>. Older builds may not,
    /// which is a capability gap rather than a fault — see
    /// <see cref="ResolveEngineVersion"/>.
    /// </summary>
    public static bool EngineVersionAvailable => AllNativeExportsAvailable(EngineVersionRequiredExports);

    /// <summary>
    /// Version of the libwdsp actually loaded into this process. Resolved once
    /// and cached: the answer cannot change for the lifetime of the process, and
    /// the probe behind it dlopens the library.
    ///
    /// Never throws. A libwdsp that will not load, or that predates the
    /// <c>GetWDSPVersion</c> export, comes back as a capability state rather than
    /// an exception, so a stale engine cannot turn into a crash at channel open.
    /// </summary>
    public static WdspEngineVersion ResolveEngineVersion() => LazyEngineVersion.Value;

    private static readonly Lazy<WdspEngineVersion> LazyEngineVersion = new(
        static () => WdspEngineVersion.Resolve(
            static () => NativeLibraryLoadable,
            static () => EngineVersionAvailable,
            NativeMethods.GetWDSPVersion),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private static double FiniteOrZero(double value) =>
        double.IsFinite(value) ? value : 0.0;

    private static double FiniteOrFallback(double value, double fallback) =>
        double.IsFinite(value) ? value : FiniteOrZero(fallback);

    private static double RoundDiag(double value, int digits = 3) =>
        Math.Round(FiniteOrZero(value), digits);

    private static double LinearToDb(double value) =>
        value > 1.0e-12 && double.IsFinite(value)
            ? 20.0 * Math.Log10(value)
            : -240.0;

    private enum RxaMode
    {
        LSB = 0, USB = 1, DSB = 2, CWL = 3, CWU = 4,
        FM = 5, AM = 6, DIGU = 7, SPEC = 8, DIGL = 9,
        SAM = 10, DRM = 11,
    }

    // Audio ring holds ~1 s of mono float32 @ 48 kHz (producer: worker thread after fexchange0,
    // consumer: ReadAudio caller on pipeline thread). Drops oldest when over capacity.
    private const int AudioRingCapacity = OutputRate;

    /// <summary>
    /// Per-RX-channel ingest health, latched once per ~1 s window by
    /// <see cref="EmitRxDiag"/> and read lock-free by the diagnostics provider
    /// (immutable record swapped behind a volatile field). All "PerWindow"
    /// counts are over the last completed ~1 s window. This is the realtime
    /// overflow/underrun signal for high-sample-rate / multi-DDC operation:
    /// <c>DroppedPerWindow &gt; 0</c> means the worker fell behind and IQ frames
    /// were dropped (drop-oldest); <c>WorkerMaxMs</c> approaching the per-frame
    /// budget (1000·InSize/SampleRateHz) means the channel is CPU-bound.
    /// </summary>
    public sealed record RxChannelHealth(
        int ChannelId,
        int SampleRateHz,
        int QueueDepth,
        int QueueCapacity,
        long FramesInPerWindow,
        long QueueFullPerWindow,
        long DroppedPerWindow,
        long WorkerFramesPerWindow,
        double WorkerAvgMs,
        double WorkerMaxMs,
        int AudioRingDepth,
        long AudioOverrunPerWindow,
        long AgeMs);

    private sealed class ChannelState
    {
        public required int Id;
        public required int Generation;
        public required bool IsDisplayOnly;
        public required int SampleRateHz;
        public required int PixelWidth;
        public required int OutDoubles;
        public required Thread Worker;
        public required BlockingCollection<double[]> InQueue;
        // Rate-scaled bound of InQueue (frames). Captured so diagnostics can show
        // depth-vs-capacity; see ComputeInQueueCapacity.
        public required int InQueueCapacity;
        public readonly ConcurrentQueue<double[]> FreeFrames = new();
        public double[] PartialFrame = new double[2 * InSize];
        public int PartialFill;
        public readonly object FillGate = new();
        public volatile bool Stopped;
        public CancellationTokenSource Cts = new();
        public int SpectrumRun = 1;
        // Set true after the worker runs Spectrum0 at least once. WDSP's GetPixels
        // reads the analyzer's snapped pixel buffer, which is null until the first
        // Spectrum0 — calling it before then dereferences null (native 0xc0000005).
        // P3 opens an RX WDSP channel to drive the local TXA chain but never feeds
        // it IQ (RX comes from the sidecar), so its analyzer never snaps; the
        // display drain must skip it. Also guards the first tick after any open.
        public volatile bool AnalyzerHasSnapped;
        public required int SnrAnalyzerId;
        public required long SnrAnalyzerGeneration;
        public volatile bool SnrAnalyzerHasSnapped;
        public readonly float[] AudioRing = new float[AudioRingCapacity];
        public int AudioHead;
        public int AudioCount;
        public readonly object AudioGate = new();
        // Bandpass tracked as unsigned magnitudes so SetMode can re-sign per mode
        // (WDSP wants negative f_low/f_high for LSB-family, positive for USB-family).
        public int FilterLowAbsHz = 150;
        public int FilterHighAbsHz = 2850;
        public RxaMode CurrentMode = RxaMode.USB;
        public BandpassWindow FilterWindow = BandpassWindow.Normal;
        public FilterPhaseMode FilterPhase = FilterPhaseMode.Linear;
        public readonly object FilterProfileGate = new();
        // Hardware-NCO-relative offset of the tuned passband. The analyzer
        // sees pre-shift IQ, so its max-bin detector must move by this amount
        // while the demodulator shifts the same signal back to baseband.
        public int CtunShiftHz;
        // Thetis "AGC Top" max-gain setting in dB. 90 matches the Thetis
        // default (radio.cs:1021 rx_agc_max_gain); the /api/agcGain endpoint can
        // override at runtime.
        public double AgcTopDb = 90.0;
        // AGC mode last applied via SetAgc / ApplyAgcDefaults. MED matches the
        // open-time default; surfaced so callers / tests can read back the mode.
        public AgcMode CurrentAgcMode = AgcMode.Med;
        // RX squelch config last applied via SetSquelch. Default off/adaptive
        // so a fresh channel matches Thetis (all squelch off) while the UI
        // defaults to the server-side noise-floor gate once enabled.
        // Re-asserted on every SetMode so a mode change moves fixed-mode
        // run/threshold to the new stage.
        public SquelchConfig CurrentSquelch = new();
        // Read by RunWorker to gate xanbEXT/xnobEXT; writes only from SetNoiseReduction.
        // Single-writer on the pipeline thread + word-sized read on the worker = safe
        // without a lock (worst case: one extra frame at the old setting on toggle).
        public volatile NbMode CurrentNbMode = NbMode.Off;
        public volatile NrMode CurrentNrMode = NrMode.Off;
        // Zoom level (1..32). Changing it re-calls SetAnalyzer with shifted
        // fscLin/fscHin; the worker's Spectrum0 and the pixel drain's GetPixels
        // take this lock so they never interleave with an in-flight reconfig.
        public int ZoomLevel = 1;
        // Per-channel analyzer FFT size. Defaults to the engine-wide
        // _rxAnalyzerFftSize at open; SetRxDisplayFftSize raises it for
        // high-resolution display-only consumers (wideband detail DDC), and
        // every zoom-driven SetAnalyzer reconfig re-reads this field so the
        // override survives zoom changes.
        public int AnalyzerFftSize;
        public readonly object AnalyzerLock = new();

        // --- RX ingest health telemetry (issue zeus-gdc7; now permanent) ---
        // Live overflow/underrun counters for high-sample-rate operation, reset
        // each 1 Hz emit in EmitRxDiag. Cheap Interlocked increments on the hot
        // paths; no allocation. These are the realtime signal that the worker is
        // keeping up with the IQ frame rate at 768/1536 kHz with all DDCs active.
        public long DiagFramesIn;          // frames handed to InQueue this window
        public long DiagEnqueueFull;       // enqueues that found the bounded InQueue full (worker fell behind)
        public long DiagDroppedOldest;     // frames dropped (oldest evicted) to keep the RX thread non-blocking — bounded, deliberate glitch
        public long DiagWorkerFrames;      // frames the worker processed this window
        public long DiagWorkerTotalTicks;  // Σ per-frame fexchange0+Spectrum0 Stopwatch ticks
        public long DiagWorkerMaxTicks;    // max single-frame processing ticks
        public long DiagAudioOverrun;      // PushAudio writes that overwrote an unread sample (ring full → discontinuity)
        public long DiagLastLogTicks;      // Stopwatch timestamp of last 1 Hz emit
        // Latched once per ~1 s by EmitRxDiag; read lock-free by the diagnostics
        // provider via SnapshotRxChannels. Immutable record behind a volatile
        // reference → no torn reads, no lock on the snapshot path.
        public volatile RxChannelHealth? LastHealth;
    }

    // Each RX channel hands 512-sample IQ frames from the realtime RX sink
    // thread to its WDSP worker through a bounded queue. The frame RATE scales
    // with the RX sample rate (≈47/s at 48 kHz … ≈1500/s at 1536 kHz), so a
    // fixed frame count gave a 32× smaller TIME cushion at the top of the
    // ladder. ComputeInQueueCapacity sizes the queue to hold ~InQueueTargetMs
    // of IQ regardless of rate, floored at the legacy 32 frames and capped to
    // bound memory/latency (each frame is 2*InSize doubles ≈ 16 KB, so the
    // ceiling is ≈4 MB per channel — comfortable even with all DDCs running).
    private const int InQueueFloorFrames = 32;
    private const int InQueueCeilFrames = 256;
    private const double InQueueTargetMs = 80.0;

    private static int ComputeInQueueCapacity(int sampleRateHz)
    {
        double framesPerSec = (double)sampleRateHz / InSize;
        int target = (int)Math.Ceiling(framesPerSec * (InQueueTargetMs / 1000.0));
        return Math.Clamp(target, InQueueFloorFrames, InQueueCeilFrames);
    }

    /// <summary>
    /// Feed policy for <see cref="FeedIq"/> when a channel's hand-off queue is
    /// full. Default (<c>false</c>) is non-blocking <b>drop-oldest</b>: the
    /// realtime RX sink thread must never block, because a stall cascades into
    /// kernel UDP drops. Set <c>true</c> only for <b>offline / faster-than-realtime
    /// bulk feeders</b> (unit tests, file/fixture replay) that push IQ with no
    /// network pacing and need <b>lossless</b> delivery — it restores the
    /// self-pacing blocking back-pressure the realtime path deliberately avoids.
    /// MUST stay <c>false</c> for any live radio.
    /// </summary>
    public bool BlockingIqFeed { get; init; }

    private readonly ConcurrentDictionary<int, ChannelState> _channels = new();
    // WDSP's channel/analyzer table is a single PROCESS-GLOBAL array (comm.h
    // MAX_CHANNELS = 32), so slot reservation must be process-wide, not per
    // engine instance. During a connect hand-off two WdspDspEngine instances are
    // briefly alive (the new engine is built before TeardownEngine disposes the
    // old one). With per-instance sets both would reserve id 0, 1, ... over the
    // SAME global channels — so disposing the old engine's channels/TXA would
    // destroy the new engine's, and the next WDSP call (GetPixels, SetPSHWPeak,
    // ...) hits a freed slot (native 0xc0000005). Static reservation gives the
    // new engine ids the old one still holds, so teardown only frees its own.
    // Every reserved slot is released on Dispose/StopChannel, so this cannot leak.
    private static readonly object _nativeSlotLock = new();
    private static readonly HashSet<int> _reservedNativeSlots = new();
    // WDSP wraps process-global native state that is not protected across engine
    // instances: FFTW planner/wisdom tables plus the RNNR model and instance list.
    // Serialize the infrequent calls that create, replan, reload, or tear down
    // those resources; steady-state DSP execution remains fully concurrent.
    private static readonly object _nativeLifecycleLock = new();
    private readonly ILogger _log;
    private int _disposed;
    private int _channelGeneration;
    private static long s_snrAnalyzerGeneration;

    // TXA lifecycle is disjoint from RXA's (no analyzer, no audio ring, no NB)
    // so we don't register it in _channels. _txaLock serializes OpenTxChannel
    // vs SetMox vs teardown — all three are rare, so a plain lock is fine.
    private readonly object _txaLock = new();
    // Counter throttles fexchange2-error logging so a persistent wire-protocol
    // mismatch doesn't flood the log. First 8 errors are visible then suppressed.
    private int _txFexchangeErrLogged;
    // Same throttle for TX-audio plugin handler exceptions — first 4 visible,
    // then suppressed. The handler should never throw, but a buggy plugin
    // shouldn't take down TX or flood the log.
    private int _txPluginErrLogged;
    private int? _txaChannelId;
    private bool _txaNativeOwned;
    private readonly IWdspTxControlNative _txControlNative;
    private readonly Func<int, int, int, int> _setTxMonitorChannelState;
    // Tracked so SetTxMode can re-sign bandpass bounds (LSB family wants negative,
    // USB family positive) the same way RXA does through ApplyBandpassForMode.
    private RxaMode _txCurrentMode = RxaMode.USB;
    private bool _txDigitalBypass;
    private bool _txRogerBeepBypass;
    private bool _txInjectedAudioBypass;
    // Operator configs last applied to TXA. Digital TX modes gate the effective
    // run bits only; these cached configs keep voice-mode restore exact.
    private TxPhaseRotatorConfig _txPhaseRotatorConfig = new();
    private CfcConfig _cfcConfig = CfcConfig.Default;
    // Operator's Compressor on/off, last applied via SetTxLeveling. Digital TX
    // and roger-beep bypasses force only the effective run bit off; this cache
    // preserves the operator's intent for exact voice-mode restore while the
    // configured compressor gain remains untouched. Seeded false to match the
    // TXA-open TxLevelingConfig default. Written/read under _txaLock.
    private bool _txCompressorEnabled;
    // Operator's requested CESSB state. Effective native run additionally
    // requires the speech compressor and a voice path. WDSP orders osctrl
    // before iqc, so PureSignal corrects the already CESSB-shaped reference,
    // matching Thetis where the two controls remain independent.
    private bool _txCessbEnabled;
    private int _txCessbBandwidthHz = 3000;
    // 0 unknown, 1 available, -1 missing (older bundled/system libwdsp).
    private int _cessbBandwidthExportState;
    // Operator's Leveler on/off, last applied via SetTxLeveling. The TUN and
    // two-tone paths force the Leveler St=0 while keyed and restore it on
    // un-key; they read this so the restore lands on the operator's setting
    // (1 when enabled, 0 when disabled) rather than hardcoding "on". Seeded to
    // true to match the TXA-open default (Leveler St=1). Written/read under
    // _txaLock alongside the other PostGen state.
    private bool _txLevelerEnabled = true;
    // True while TUN or two-tone is keyed and forcing the Leveler off. Read by
    // ApplyTxLevelingLocked so an operator changing leveling settings mid-key
    // can't re-enable the Leveler on the tune/test tone — it stays off until
    // un-key, when the restore re-arms it from _txLevelerEnabled. Under _txaLock.
    private bool _txLevelerForcedOff;
    // TwoTone arm-state cache. SetTwoTone records the operator-supplied freqs
    // (positive Hz) here when arming; SetTxMode reads them back so a mid-test
    // mode change re-asserts the sideband-correct signed freqs onto PostGen.
    // gen.c xgen mode-1 emits e^(-jωt) — positive freq always lands LSB-side
    // of carrier, so USB-family modes need a sign flip to put the tones inside
    // the displayed bandpass. See gen.c:241-242 and Thetis setup.cs:11097-11101
    // (chkInvertTones, gated behind a checkbox there; we auto-sign per mode).
    private double _twoToneF1Hz;
    private double _twoToneF2Hz;
    private bool _twoToneArmed;
    // Latest per-stage TX peak meters, published atomically at the end of each
    // ProcessTxBlock. The reader (TxMetersService, 10 Hz during MOX) sees a
    // consistent snapshot without blocking the DSP thread. null until first TX
    // block runs or after TXA closes; GetTxStageMeters() returns
    // TxStageMeters.Silent in that case.
    private TxStageMeters? _latestTxStageMeters;
    private readonly object _txMeterPublishLock = new();

    // Latest per-stage RX meters, published atomically each time
    // GetRxStageMeters is called from the pipeline tick. The reader sees a
    // consistent snapshot across all 7 RXA indices plus max-bin without racing against a
    // concurrent re-read. Mirrors the TX path's _latestTxStageMeters /
    // _txMeterPublishLock pattern. The lock is uncontended in steady state —
    // GetRxStageMeters runs from the pipeline tick at 5 Hz; if a future
    // caller polls from a second thread the snapshot field still gives them
    // a coherent set rather than a half-updated tuple.
    private RxStageMeters _latestRxStageMeters = RxStageMeters.Silent;
    private readonly object _rxMeterPublishLock = new();

    // TX panadapter analyzer. Separate WDSP `disp` slot from RXA's, fed with
    // the post-CFIR IQ from ProcessTxBlock so the operator can see the on-air
    // signal during MOX / TUN. The analyzer runs at the TXA output rate
    // (48 kHz on P1, 192 kHz on P2 post-CFIR) and uses fscLin/fscHin bin
    // clipping to display the same frequency span as the RXA analyzer —
    // matches pihpsdr transmitter.c:2323-2324. See issue #81.
    //
    // `_txDispLock` serializes Spectrum0 feed (from ProcessTxBlock), GetPixels
    // (from TryGetTxDisplayPixels), and SetAnalyzer reconfig (from SetZoom) —
    // same pattern as ChannelState.AnalyzerLock on the RX side.
    private readonly object _txDispLock = new();
    private int _txDispPixelWidth;
    private int _txDispUsedPixelWidth;
    private int _txDispZoomLevel = 1;
    private int _txDispRxSampleRateHz;
    private float[]? _txDispScratchPixels;
    private bool _txDispAlive;

    // Configurable TX display analyzer params (live TX waterfall feature).
    // Display-only — they shape the transmitted-signal panadapter/waterfall
    // FFT, never the air. Defaults mirror the historical constants so an
    // unconfigured engine renders byte-identically to before. _txFftSize /
    // _txWinType are read on the TX + PS-FB analyzer config path; _txAvgTauSec
    // is the visual log-recursive smoothing tau (guarded by _txDispLock for the
    // TXA reconfig). See ConfigureTxDisplayAnalyzer.
    private volatile int _txFftSize = AnalyzerFftSize;
    private volatile int _txWinType = AnalyzerWindow;
    private double _txAvgTauSec = TxAvgTauSec;

    // PureSignal feedback display analyzer (issue #121). Optional second WDSP
    // disp slot fed from FeedPsFeedbackBlock's rxI/rxQ — i.e. the post-PA
    // signal observed via the radio's loopback ADC. When the operator turns on
    // the "Monitor PA output" toggle (StateDto.PsMonitorEnabled) AND PS is
    // armed AND calcc reports correcting=true, DspPipelineService.Tick reads
    // pixels from this analyzer instead of the post-CFIR TX analyzer so the
    // panadapter shows the actual on-air RF rather than the predistorted
    // baseband. Lifecycle is paired with SetPsEnabled(true/false): we open
    // the disp slot when PS arms and tear it down when PS disarms so the
    // WDSP analyzer table doesn't leak.
    //
    // Pixel width / zoom / matched RX sample rate are inherited from the TX
    // analyzer at arm time so display frames slot in with no resize when the
    // toggle flips. If the TX analyzer is unavailable, inherit a real RX
    // display channel's geometry. The private TX-monitor RXA is deliberately
    // excluded because it is opened at 1024 pixels and is never serialized as a
    // DisplayFrame.
    private readonly object _psFbDispLock = new();
    private int? _psFbDispId;
    private int _psFbDispPixelWidth;
    private int _psFbDispUsedPixelWidth;
    private int _psFbDispZoomLevel = 1;
    private int _psFbDispRxSampleRateHz;
    private float[]? _psFbDispScratchPixels;
    private bool _psFbDispAlive;
    private long _psFbFeedCount;

    // PureSignal state. _psLock serializes the WDSP PS setters (which mutate
    // shared state inside calcc.c) and FeedPsFeedbackBlock. _psInfoBuf is
    // pinned once and reused on every GetPSInfo call.
    private readonly object _psLock = new();
    // Thetis's SetXXAEQProfile frees and reallocates the stage's F/G/Q with
    // no native lock — safe there, where one UI thread makes every call.
    // Here a REST thread reading the response curve can race a pipeline
    // thread writing a profile, so every entry point that touches those
    // arrays (both profile setters and both draw getters) takes this first.
    private readonly object _eqLock = new();

    /* ---- DEXP, the TX downward expander -----------------------------
     *
     * WDSP holds these in a global pdexp[] and creates none by itself, so
     * the engine owns the one TX instance: its buffer, its lifetime and the
     * lock that keeps the audio thread away from create/destroy. Every
     * SetDEXP* setter dereferences pdexp[id] with no null check, so nothing
     * may be pushed before _dexpCreated.
     *
     * The id is a SLOT, taken per instance, never a constant. pdexp[] is
     * process-global and there can be two engines alive at once: the
     * offline preview wraps its own WdspDspEngine, and connecting builds a
     * new one before the preview is disposed. With a fixed id 0 the
     * preview's DestroyDexpLocked freed the NEW engine's expander, and the
     * new engine's next SetDEXP* ran on freed memory — 0xC0000005 in
     * SetDEXPDetectorTau on every radio connect. WDSP channels avoid this
     * with the native slot allocator; the expander now has its own.
     */
    private const int DexpSlots = 4;                         // WDSP: DEXP pdexp[4]
    private static readonly bool[] s_dexpSlotUsed = new bool[DexpSlots];
    private static readonly object s_dexpSlotLock = new();
    private int _dexpId = -1;                                // this instance's slot while created
    private readonly object _dexpLock = new();
    private unsafe double* _dexpBuf;          // interleaved complex, _dexpSize samples
    private bool _dexpCreated;
    private int _dexpSize;
    private int _dexpRateHz;
    private TxDexpConfig _dexpConfig = TxDexpConfig.Default;
    // Read on the audio thread before taking the lock at all, so a disabled
    // expander costs one volatile read per block.
    private volatile bool _dexpActive;
    private volatile bool _psEnabled;
    private bool _psAuto = true;
    private bool _psSingle;
    private double _psHwPeak = 0.4072;   // P1 default; RadioService overrides at connect
    private double _psMoxDelaySec = 0.2;
    private double _psLoopDelaySec = 0.0;
    private double _psAmpDelayNs = 150.0;
    private const int WdspPsDelayWholeSamplePositions = 9601;
    private const int PsFeedbackBlockSize = 1024;
    // PS feedback IQ sample rate. 192 kHz on every P2 path — the paired
    // DDC0/DDC1 scheme (G2 / Saturn / ANAN-7000) and the HermesC10 keyed
    // time-mux burst both run the feedback DDC at a fixed 192 kHz — and on
    // the HL2 P1 path (shipped behaviour, untouched). On Hermes-family
    // single-ADC P1 paths (HermesC10 4-DDC, HermesII 2-DDC) feedback rides
    // the normal EP6 stream at the WIRE rate (all P1 DDCs share the one
    // global rate), so DspPipelineService
    // overrides this via SetPsFeedbackRateHz before OpenTxChannel — telling
    // WDSP 192 kHz while feeding it 48/96/384 kHz mis-scales calcc's
    // mox/loop-delay sample counts and amp-delay lines and the fit never
    // converges. (Thetis instead force-hops the whole radio to 192 kHz
    // during PS TX, console.cs:8487-8506; piHPSDR ties the feedback rate to
    // the radio rate, receiver.c:1590-1596 — we follow piHPSDR.) Used by
    // SetPSFeedbackRate at TXA open and by the PS-feedback display
    // analyzer's bin-clip config.
    private int _psFeedbackRateHz = 192_000;

    /// <summary>
    /// Override the PS feedback sample rate (Hz) BEFORE <see cref="OpenTxChannel"/>
    /// runs. Hermes-family single-ADC P1 only — see the
    /// <see cref="_psFeedbackRateHz"/> comment. No-op after TXA open (the
    /// value is latched into WDSP at open; P1 rate changes tear down and
    /// rebuild the whole engine, so the seam re-runs on every re-rate).
    /// </summary>
    public void SetPsFeedbackRateHz(int rateHz)
    {
        if (rateHz <= 0) return;
        _psFeedbackRateHz = rateHz;
    }
    private readonly int[] _psInfoBuf = new int[16];
    // GetPSDisp writes into caller memory, so these are sized from calcc's
    // own constants and reused rather than allocated per call. Guarded by
    // _psLock along with _psInfoBuf.
    private readonly double[] _psDispX = new double[PsCurve.NativeSampleCount];
    private readonly double[] _psDispYm = new double[PsCurve.NativeSampleCount];
    private readonly double[] _psDispYc = new double[PsCurve.NativeSampleCount];
    private readonly double[] _psDispYs = new double[PsCurve.NativeSampleCount];
    private readonly double[] _psDispXmCor = new double[PsCurve.CurvePoints];
    private readonly double[] _psDispYmCor = new double[PsCurve.CurvePoints];
    private readonly double[] _psDispXaCor = new double[PsCurve.CurvePoints];
    private readonly double[] _psDispYaCor = new double[PsCurve.CurvePoints];
    // Edge-triggered state-transition log target. 255 is an out-of-range
    // sentinel so the first observed state always logs (LRESET..LTURNON
    // = 0..9 per calcc.c:543-552). Updated under _psLock.
    private byte _lastLoggedPsState = 255;
    // Pscc-call counter — incremented in FeedPsFeedbackBlock after psccF.
    // At 192 kHz / 1024-sample blocks we expect ~187 calls/sec while keyed.
    // Periodic log at every 100th call lets the operator confirm feedback is
    // arriving from the radio without flooding when PS is idle.
    private long _psFeedCount;
    private double _psMaxTxEnvelope;
    // Bring-up diagnostic — emit info[] every Nth GetPsStageMeters tick so the
    // calcc state machine is visible in the server log without flooding.
    // Drop alongside the wdsp.psSeed log once PS is confirmed stable.
    private int _psInfoLogCounter;
    private int _txOverdriveLogCounter;

    // TX Monitor — private RXA channel that demodulates the post-CFIR / post-
    // RSMPOUT TX IQ (the wire signal about to hit the radio) back to mono
    // baseband audio at 48 kHz, so the operator can preview the full TX chain
    // (mic → EQ → Leveler → VST → CFC → ALC → bandpass) at the actual TX
    // bandwidth profile, with or without keying. Equivalent to Thetis MON,
    // implemented as a parallel demod rather than a tap inside TXA so the
    // bandwidth filter shape is honoured exactly.
    //
    // The channel is opened lazily on first SetTxMonitorEnabled(true) once
    // OpenTxChannel has chosen the IQ rate (48 kHz P1 / 192 kHz P2). It stays
    // open for the engine lifetime; toggling monitor off just stops feeding
    // and stops draining. Mode + filter are synced from SetTxMode/SetTxFilter
    // so the preview matches the on-air bandwidth.
    //
    // _monitorRequested is the operator's intent (REST toggle); _monitorChannelId
    // becomes non-null once the channel is actually open. ProcessTxBlock feeds
    // IQ when both are set; ReadTxMonitorAudio drains regardless of the request
    // flag (the ring drains naturally when feed stops).
    private readonly object _monitorLock = new();
    private int? _monitorChannelId;
    private volatile bool _monitorRequested;
    private RxaMode _monitorMode = RxaMode.USB;
    private int _monitorFilterLow = 150;
    private int _monitorFilterHigh = 2850;
    private const double TxMonitorFixedAgcGainDb = 0.0;
    // RXAbp1Check applies gain=2 whenever the AM demodulator is running
    // (native/wdsp/RXA.c). Cancel that receive-chain compensation only on the
    // private TX monitor so AM/SAM preview retains the same headroom as the
    // suppressed-carrier modes instead of clipping before playback volume.
    private const double TxMonitorAmCompensationDb = -6.020599913279624;

    // The first ordinary RXA opened by the pipeline is RX1, the channel whose
    // state MOX transitions must preserve/restore. TX Monitor later opens a
    // second, private RXA through OpenChannelCore, and RX2+ may add more public
    // channels. Selecting the first ConcurrentDictionary key is therefore not
    // an identity: after the monitor opened it could resume/stop that private
    // channel while leaving RX1 stopped after a PureSignal over.
    private int _primaryRxaChannelId = -1;

    // Tracked engine-side MOX so SetTxMonitorEnabled can decide whether to
    // flip TXA state independently. SetMox writes this under _txaLock; the
    // helpers below read it under _txaLock too. Without this the "monitor
    // on while MOX off" path leaves TXA quiescent (state=0) — fexchange2
    // returns without filling iout/qout and the monitor RXA hears silence
    // or stack garbage.
    private bool _moxOn;
    // PureSignal still damps RXA on key-down. Remember that transition across
    // an emergency mid-over PS disarm so key-up always restores RXA.
    private bool _rxaStoppedForCurrentMox;
    // Tracked engine-side TXA state-bit so the helper can flip idempotently
    // and avoid double-priming. TXA opens at state=0; SetChannelState walks
    // it through 1 / 0 transitions explicitly.
    private bool _txaRunning;

    // Manual notch filter (MNF) state — global to the RX path. _manualNotches
    // is the authoritative list (absolute RF Hz); the engine rewrites the WDSP
    // notch database from it and re-applies on every channel (re)open, so it
    // survives a sample-rate / mode-driven channel rebuild. _notchTuneFreqHz is
    // the last LO fed by the pipeline; notches are positioned relative to it.
    // _notchDbUnavailable latches if the bundled libwdsp predates the notch-DB
    // exports (mirrors the SBNR guard) so we don't spam the worker with throws.
    private readonly object _notchLock = new();
    private readonly List<NotchDto> _manualNotches = new();
    private double _notchTuneFreqHz;
    private bool _notchDbUnavailable;
    private readonly int _rxAnalyzerFftSize;

    public WdspDspEngine(ILogger<WdspDspEngine>? logger = null, int rxAnalyzerFftSize = AnalyzerFftSize)
        : this(logger, new WdspTxControlNative(), registerNativeResolver: true, rxAnalyzerFftSize)
    {
    }

    internal WdspDspEngine(
        ILogger<WdspDspEngine>? logger,
        IWdspTxControlNative txControlNative,
        bool registerNativeResolver,
        int rxAnalyzerFftSize = AnalyzerFftSize,
        Func<int, int, int, int>? setTxMonitorChannelState = null)
    {
        _log = logger ?? NullLogger<WdspDspEngine>.Instance;
        _txControlNative = txControlNative ?? throw new ArgumentNullException(nameof(txControlNative));
        _setTxMonitorChannelState = setTxMonitorChannelState ?? NativeMethods.SetChannelState;
        _rxAnalyzerFftSize = NormalizeRxAnalyzerFftSize(rxAnalyzerFftSize);
        if (registerNativeResolver)
        {
            WdspNativeLoader.EnsureResolverRegistered();
        }
        // WdspWisdomInitializer registers a process-wide gate when the hosting
        // singleton is constructed. Native calls that can create FFTW plans wait
        // on that gate before entering WDSP. Tests and tools that construct a
        // bare engine with no registered initializer keep the historical slow
        // first-open behaviour.
    }

    internal void OpenTxChannelForTests(
        int txaChannelId,
        RxMode mode = RxMode.USB,
        bool compressorEnabled = false,
        bool running = false)
    {
        lock (_txaLock)
        {
            _txaChannelId = txaChannelId;
            _txaNativeOwned = false;
            _txCurrentMode = MapMode(mode);
            _txCompressorEnabled = compressorEnabled;
            _txaRunning = running;
        }
    }

    internal int? TxMonitorChannelIdForTests => _monitorChannelId;

    internal void SetCessbForTests(bool enabled, int bandwidthHz)
    {
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            _txCessbEnabled = enabled;
            _txCessbBandwidthHz = bandwidthHz is 4000 ? 4000 : 3000;
            TrySetCessbBandwidth(txa, _txCessbBandwidthHz);
            ApplyTxCompressorAndCessbRuns(txa);
        }
    }

    internal AgcMode? GetChannelAgcModeForTests(int channelId) =>
        _channels.TryGetValue(channelId, out var state) ? state.CurrentAgcMode : null;

    private int ReserveNativeSlot()
    {
        lock (_nativeSlotLock)
        {
            for (int id = 0; id < MaxWdspNativeChannels; id++)
            {
                if (_reservedNativeSlots.Add(id)) return id;
            }
        }

        throw new InvalidOperationException("No free WDSP native channel/analyzer slots are available.");
    }

    internal static void RunNativeLifecycleCriticalSection(Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        lock (_nativeLifecycleLock)
            action();
    }

    private void ReleaseNativeSlot(int id)
    {
        lock (_nativeSlotLock)
            _reservedNativeSlots.Remove(id);
    }

    private void TryCleanupNativeResource(int id, string resource, Action<int> cleanup)
    {
        try
        {
            cleanup(id);
        }
        catch (Exception ex)
        {
            _log.LogDebug(ex, "wdsp.cleanup {Resource} failed id={Id}", resource, id);
        }
    }

    private void ReleaseFailedNativeOpen(
        int id,
        bool nativeChannelOpened,
        bool analyzerOpened,
        bool anbExtOpened = false,
        bool nobExtOpened = false)
    {
        // No AnalyzerLock is needed: failed opens never publish a live channel/worker for this id.
        RunNativeLifecycleCriticalSection(() =>
        {
            if (analyzerOpened)
                TryCleanupNativeResource(id, nameof(NativeMethods.DestroyAnalyzer), NativeMethods.DestroyAnalyzer);
            if (anbExtOpened)
                TryCleanupNativeResource(id, nameof(NativeMethods.DestroyAnbEXT), NativeMethods.DestroyAnbEXT);
            if (nobExtOpened)
                TryCleanupNativeResource(id, nameof(NativeMethods.DestroyNobEXT), NativeMethods.DestroyNobEXT);
            if (nativeChannelOpened)
                TryCleanupNativeResource(id, nameof(NativeMethods.CloseChannel), NativeMethods.CloseChannel);
        });
        ReleaseNativeSlot(id);
    }

    public int OpenChannel(int sampleRateHz, int pixelWidth)
    {
        int id = OpenChannelCore(sampleRateHz, pixelWidth, displayOnly: false);
        Interlocked.CompareExchange(ref _primaryRxaChannelId, id, -1);
        ReevaluateTxDisplayGeometry();
        return id;
    }

    public int OpenRxDisplayChannel(int sampleRateHz, int pixelWidth) =>
        OpenChannelCore(sampleRateHz, pixelWidth, displayOnly: true);

    private int OpenChannelCore(int sampleRateHz, int pixelWidth, bool displayOnly = false)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        if (pixelWidth <= 0) throw new ArgumentOutOfRangeException(nameof(pixelWidth));
        if (sampleRateHz <= 0) throw new ArgumentOutOfRangeException(nameof(sampleRateHz));
        WdspWisdomInitializer.WaitUntilReady();

        int id = ReserveNativeSlot();
        bool nativeChannelOpened = false;
        bool anbExtOpened = false;
        bool nobExtOpened = false;
        bool analyzerOpened = false;
        bool snrAnalyzerOpened = false;
        bool workerStarted = false;
        ChannelState? openedState = null;

        try
        {
            int outSamples = (int)((long)InSize * OutputRate / sampleRateHz);
            int outDoubles = Math.Max(2, outSamples * 2);

            // Thetis pattern: open channel quiescent (state=0), apply all config,
            // then explicitly transition to state=1 with SetChannelState at the end.
            // Mirrors cmaster.c:80 (// initial state = 0) and rxa.cs:63
            // (WDSP.SetChannelState(chid + 0, 1, 0); // main rcvr ON). A fresh
            // channel opened at state=1 does set the exchange bit correctly
            // in-vitro, but runtime observation shows it can land clear — SAv/ADC
            // pin at -400 — suggesting the open→configure window allows the flag
            // to be stomped. Opening at 0 and flipping on last guarantees exchange
            // is set after all setters have run.
            // FFTW's planner and its in-process wisdom table are process-global
            // and are not safe for concurrent plan creation. Different engine
            // instances can open channels on separate host-start tasks, so keep
            // native plan-building calls behind one process-wide gate.
            RunNativeLifecycleCriticalSection(() => NativeMethods.OpenChannel(
                    channel: id,
                    in_size: InSize,
                    dsp_size: DspSize,
                    input_samplerate: sampleRateHz,
                    dsp_rate: DspRate,
                    output_samplerate: OutputRate,
                    type: 0,
                    state: 0,
                    tdelayup: 0.010,
                    tslewup: 0.025,
                    tdelaydown: 0.0,
                    tslewdown: 0.010,
                    bfo: 1));
            nativeChannelOpened = true;

            NativeMethods.SetRXABandpassWindow(id, 1);
            NativeMethods.SetRXABandpassRun(id, 1);
            NativeMethods.SetRXAAMDSBMode(id, 0);
            NativeMethods.SetRXAPanelRun(id, 1);
            // select=3 → route both I and Q into the panel. Without this WDSP
            // demodulates a single real-valued channel and can't separate sidebands
            // (LSB/USB become audibly identical mush).
            NativeMethods.SetRXAPanelSelect(id, 3);
            NativeMethods.SetRXAPanelBinaural(id, 0);
            NativeMethods.SetRXAPanelGain1(id, 1.0);
            NativeMethods.SetRXAMode(id, (int)RxaMode.USB);
            NativeMethods.SetRXABandpassFreqs(id, 150.0, 2850.0);
            NativeMethods.RXANBPSetFreqs(id, 150.0, 2850.0);
            NativeMethods.SetRXASNBAOutputBandwidth(id, 150.0, 2850.0);

            ApplyAgcDefaults(id);
            ApplySquelchDefaults(id);

            // Pre-RXA blankers: create run=0 so the setters / xanbEXT slots are
            // allocated before any SetNoiseReduction call touches them (EXT
            // setters deref panb[id]/pnob[id]). Create-time knob values are
            // passed through here too so the struct is self-consistent on return,
            // but the authoritative knob state comes from ApplyNbDefaults right
            // after — same approach a future advanced-NB panel will take.
            NativeMethods.CreateAnbEXT(
                id: id, run: 0, buffsize: InSize, samplerate: sampleRateHz,
                tau: NrDefaults.NbTau, hangtime: NrDefaults.NbHangtime,
                advtime: NrDefaults.NbAdvtime, backtau: NrDefaults.NbBacktau,
                threshold: NrDefaults.NbDefaultThresholdScaled);
            anbExtOpened = true;
            NativeMethods.CreateNobEXT(
                id: id, run: 0, mode: 0, buffsize: InSize, samplerate: sampleRateHz,
                slewtime: NrDefaults.NbTau, hangtime: NrDefaults.NbHangtime,
                advtime: NrDefaults.NbAdvtime, backtau: NrDefaults.NbBacktau,
                threshold: NrDefaults.NbDefaultThresholdScaled);
            nobExtOpened = true;
            ApplyNbDefaults(id);

            NativeMethods.XCreateAnalyzer(id, out int rc, MaxFftSize, 1, 1, null);
            if (rc != 0) throw new InvalidOperationException($"XCreateAnalyzer failed rc={rc}");
            analyzerOpened = true;

            ConfigureAnalyzer(id, sampleRateHz, InSize, pixelWidth, zoomLevel: 1, _rxAnalyzerFftSize, AnalyzerWindow, AnalyzerKaiserPi);
            ConfigureDisplayAveraging(id);

            // RX channel slots are process-global, so the corresponding display
            // slots 32..63 are unique across engine instances and remain below
            // WDSP's dMAX_DISPLAYS (72). They do not consume native channels.
            int snrAnalyzerId = MaxWdspNativeChannels + id;
            NativeMethods.XCreateAnalyzer(snrAnalyzerId, out rc, MaxFftSize, 1, 1, null);
            if (rc != 0) throw new InvalidOperationException($"XCreateAnalyzer(SNR) failed rc={rc}");
            snrAnalyzerOpened = true;
            ConfigureSnrAnalyzer(snrAnalyzerId, sampleRateHz);

            int inQueueCapacity = ComputeInQueueCapacity(sampleRateHz);
            var state = new ChannelState
            {
                Id = id,
                Generation = Interlocked.Increment(ref _channelGeneration),
                IsDisplayOnly = displayOnly,
                SampleRateHz = sampleRateHz,
                PixelWidth = pixelWidth,
                SnrAnalyzerId = snrAnalyzerId,
                SnrAnalyzerGeneration = Interlocked.Increment(ref s_snrAnalyzerGeneration),
                OutDoubles = outDoubles,
                InQueue = new BlockingCollection<double[]>(boundedCapacity: inQueueCapacity),
                InQueueCapacity = inQueueCapacity,
                Worker = null!,
                AnalyzerFftSize = _rxAnalyzerFftSize,
            };
            openedState = state;

            var worker = new Thread(() => RunWorker(state))
            {
                IsBackground = true,
                Name = $"WdspDsp-{id}",
                Priority = ThreadPriority.AboveNormal,
            };
            state.Worker = worker;

            _channels[id] = state;
            ConfigureMaxBinDetector(state);
            worker.Start();
            workerStarted = true;

            // Thetis rxa.cs:63 — "main rcvr ON". The OpenChannel call above used
            // state=0 so the slew.upflag / ch_upslew / exchange-bit initialisation
            // block in channel.c:94-99 did NOT run. SetChannelState(id, 1, 0) is
            // the canonical transition: it sets slew.upflag, ch_upslew, clears
            // exec_bypass, and sets exchange (channel.c:278-283). After this
            // returns, fexchange0's `if (_InterlockedAnd (&ch[channel].exchange, 1))`
            // guard (iobuffs.c:484) will be satisfied and xrxa → xmeter will run.
            NativeMethods.SetChannelState(id, 1, 0);

            // Re-apply any manual notches to the freshly-opened channel. A sample-
            // rate or mode change rebuilds the WDSP channel with an empty notch DB;
            // the engine holds the authoritative list so notches persist across it.
            ApplyNotchesToChannel(id);

            return id;
        }
        catch
        {
            if (workerStarted && openedState is not null)
            {
                _channels.TryRemove(id, out _);
                StopChannel(openedState);
            }
            else
            {
                _channels.TryRemove(id, out _);
                if (openedState is not null)
                {
                    openedState.InQueue.Dispose();
                    openedState.Cts.Dispose();
                }
                if (snrAnalyzerOpened)
                    RunNativeLifecycleCriticalSection(() => NativeMethods.DestroyAnalyzer(MaxWdspNativeChannels + id));
                ReleaseFailedNativeOpen(id, nativeChannelOpened, analyzerOpened, anbExtOpened, nobExtOpened);
            }
            throw;
        }
    }

    public void CloseChannel(int channelId)
    {
        if (!_channels.TryRemove(channelId, out var state)) return;
        Interlocked.CompareExchange(ref _primaryRxaChannelId, -1, channelId);
        StopChannel(state);
    }

    public void CloseRxDisplayChannel(int channelId) => CloseChannel(channelId);

    private bool TrySnapshotRxDisplayGeometry(out int pixelWidth, out int sampleRateHz, out int zoomLevel)
    {
        pixelWidth = 0;
        sampleRateHz = 0;
        zoomLevel = 1;

        int? monitorId = _monitorChannelId;
        foreach (var kv in _channels)
        {
            if (monitorId == kv.Key) continue;
            var state = kv.Value;
            if (state.IsDisplayOnly) continue;
            if (state.Stopped) continue;
            if (state.PixelWidth <= 0 || state.SampleRateHz <= 0) continue;

            pixelWidth = state.PixelWidth;
            sampleRateHz = state.SampleRateHz;
            zoomLevel = Math.Max(1, state.ZoomLevel);
            return true;
        }

        return false;
    }

    internal bool TrySnapshotRxDisplayGeometryForTesting(
        out int pixelWidth,
        out int sampleRateHz,
        out int zoomLevel) =>
        TrySnapshotRxDisplayGeometry(out pixelWidth, out sampleRateHz, out zoomLevel);

    internal int TxDisplayZoomLevelForTesting
    {
        get { lock (_txDispLock) return _txDispZoomLevel; }
    }

    internal int PsFeedbackDisplayZoomLevelForTesting
    {
        get { lock (_psFbDispLock) return _psFbDispZoomLevel; }
    }

    private void ReevaluateTxDisplayGeometry()
    {
        if (_disposed != 0) return;
        if (!TrySnapshotRxDisplayGeometry(out int pixelWidth, out int rxRate, out int zoomLevel))
            return;

        // Serializes against OpenTxChannelInternal / TXA close so the display
        // analyzer cannot be double-created or destroyed mid-reconfigure.
        lock (_txaLock)
        {
            lock (_txDispLock)
            {
                if (_txaChannelId is int txa)
                {
                    bool geometryUnchanged =
                        _txDispRxSampleRateHz == rxRate &&
                        _txDispPixelWidth == pixelWidth &&
                        _txDispZoomLevel == zoomLevel;

                    if (!geometryUnchanged || !_txDispAlive)
                    {
                        if (TryComputeTxAnalyzerGeometry(_txaDspRateHz, rxRate, zoomLevel, pixelWidth, _txFftSize, out double clipPerSide, out int usedWidth))
                        {
                            if (!_txDispAlive)
                            {
                                NativeMethods.XCreateAnalyzer(txa, out int rc, MaxFftSize, 1, 1, null);
                                if (rc != 0)
                                {
                                    _log.LogWarning(
                                        "wdsp.txDisplay.reconfigure XCreateAnalyzer rc={Rc} — TX panadapter will fall back to RX trace",
                                        rc);
                                    return;
                                }
                            }

                            ConfigureAnalyzer(txa, _txaDspRateHz, _txaDspSize, clipPerSide, usedWidth, _txFftSize, _txWinType, AnalyzerKaiserPi);
                            ConfigureDisplayAveragingTau(txa, _txAvgTauSec);
                            _txDispPixelWidth = pixelWidth;
                            _txDispUsedPixelWidth = usedWidth;
                            _txDispRxSampleRateHz = rxRate;
                            _txDispZoomLevel = zoomLevel;
                            _txDispScratchPixels = PrepareDisplayScratch(_txDispScratchPixels, pixelWidth, usedWidth);
                            _txDispAlive = true;
                            _log.LogInformation(
                                "wdsp.txDisplay.reconfigure pix={Pix} usedPix={UsedPix} rxRate={RxRate} txDsp={TxDspRate} zoom={Zoom}",
                                pixelWidth, usedWidth, rxRate, _txaDspRateHz, zoomLevel);
                        }
                        else
                        {
                            if (_txDispAlive)
                            {
                                RunNativeLifecycleCriticalSection(() => NativeMethods.DestroyAnalyzer(txa));
                            }
                            _txDispPixelWidth = pixelWidth;
                            _txDispUsedPixelWidth = 0;
                            _txDispRxSampleRateHz = rxRate;
                            _txDispZoomLevel = zoomLevel;
                            _txDispScratchPixels = null;
                            _txDispAlive = false;
                            _log.LogWarning(
                                "wdsp.txDisplay.reconfigure skipped — rx={RxRate} txDsp={TxDspRate} not an integer multiple in either direction; panadapter will fall back to RX trace (and to PS-feedback pixels while keyed with PS armed)",
                                rxRate, _txaDspRateHz);
                        }
                    }
                }
            }
        }

        lock (_psFbDispLock)
        {
            if (_psFbDispAlive && _psFbDispId is int psFb)
            {
                if (TryConfigureTxAnalyzer(psFb, _psFeedbackRateHz, PsFeedbackBlockSize, rxRate, pixelWidth, zoomLevel, _txFftSize, _txWinType, AnalyzerKaiserPi, out int usedWidth))
                {
                    _psFbDispPixelWidth = pixelWidth;
                    _psFbDispUsedPixelWidth = usedWidth;
                    _psFbDispRxSampleRateHz = rxRate;
                    _psFbDispZoomLevel = zoomLevel;
                    _psFbDispScratchPixels = PrepareDisplayScratch(_psFbDispScratchPixels, pixelWidth, usedWidth);
                    _log.LogInformation(
                        "wdsp.psFb.reconfigure pix={Pix} usedPix={UsedPix} rxRate={RxRate} psFbRate={PsFbRate} zoom={Zoom}",
                        pixelWidth, usedWidth, rxRate, _psFeedbackRateHz, zoomLevel);
                }
                else
                {
                    _log.LogWarning(
                        "wdsp.psFb.reconfigure skipped — rx={RxRate} psFb={PsFbRate} not an integer multiple in either direction; retaining existing PS-feedback display geometry",
                        rxRate, _psFeedbackRateHz);
                }
            }
        }
    }

    public bool IsRxChannelOpen(int channelId) =>
        _channels.TryGetValue(channelId, out var state) && !state.Stopped;

    public void FeedIq(int channelId, ReadOnlySpan<double> interleavedIqSamples)
    {
        if (!_channels.TryGetValue(channelId, out var state)) return;
        if (state.Stopped) return;

        int offset = 0;
        while (offset < interleavedIqSamples.Length)
        {
            lock (state.FillGate)
            {
                int need = state.PartialFrame.Length - state.PartialFill;
                int take = Math.Min(need, interleavedIqSamples.Length - offset);
                interleavedIqSamples.Slice(offset, take).CopyTo(state.PartialFrame.AsSpan(state.PartialFill));
                state.PartialFill += take;
                offset += take;

                if (state.PartialFill == state.PartialFrame.Length)
                {
                    double[] frame = state.PartialFrame;
                    if (!state.FreeFrames.TryDequeue(out var next))
                        next = new double[2 * InSize];
                    state.PartialFrame = next;
                    state.PartialFill = 0;
                    if (!state.InQueue.IsAddingCompleted)
                    {
                        state.DiagFramesIn++;
                        try
                        {
                            if (BlockingIqFeed)
                            {
                                // Lossless back-pressure for offline / faster-than-
                                // realtime bulk feeders (tests, fixture/file replay):
                                // block until the worker makes room so no frame is
                                // dropped. NEVER used on the realtime RX path — see
                                // BlockingIqFeed.
                                state.InQueue.Add(frame);
                            }
                            // Non-blocking hand-off with drop-OLDEST (default). FeedIq
                            // runs on the realtime P1/P2 RX sink thread; a blocking Add
                            // would stall UDP intake whenever the worker falls behind,
                            // and a stalled intake lets the kernel socket buffer
                            // overflow → dropped packets → sequence gaps (the cascading
                            // failure this guard prevents). Instead, when the queue is
                            // full we evict the OLDEST queued frame and keep the newest,
                            // so display/audio latency stays bounded and the glitch is a
                            // single counted dropped frame rather than a stall. Pairs
                            // with the rate-scaled capacity (ComputeInQueueCapacity).
                            else if (!state.InQueue.TryAdd(frame))
                            {
                                state.DiagEnqueueFull++;
                                if (state.InQueue.TryTake(out var stale))
                                {
                                    state.DiagDroppedOldest++;
                                    state.FreeFrames.Enqueue(stale);
                                }
                                if (!state.InQueue.TryAdd(frame))
                                    state.FreeFrames.Enqueue(frame);
                            }
                        }
                        catch (InvalidOperationException)
                        {
                            // CompleteAdding raced the enqueue — recycle.
                            state.FreeFrames.Enqueue(frame);
                        }
                    }
                    else
                    {
                        state.FreeFrames.Enqueue(frame);
                    }
                }
            }
        }
    }

    public void SetMode(int channelId, RxMode mode)
    {
        if (!_channels.TryGetValue(channelId, out var state)) return;
        var mapped = MapMode(mode);
        // Whole-radio state notifications can repeat the current mode while an
        // unrelated control (notably a secondary VFO) is moving. Reapplying the
        // same mode is unnecessary and would discard queued demodulated audio
        // below, producing a short interruption on every tuning event.
        lock (state.FilterProfileGate)
        {
            if (state.CurrentMode == mapped) return;
            RunNativeLifecycleCriticalSection(() =>
            {
                NativeMethods.SetRXAMode(channelId, (int)mapped);
                state.CurrentMode = mapped;
                ApplyBandpassForMode(state);
            });
        }
        _log.LogInformation("wdsp.setMode channel={Id} mode={Mode}", channelId, mapped);
        ConfigureMaxBinDetector(state);
        // Re-assert squelch on the stage matching the new mode and clear the
        // old one — squelch is mode-aware (SSQL/AMSQ/FMSQ) per Thetis §5.
        ApplySquelchLocked(state);
        // Drop up to ~1 s of already-demodulated audio queued with the old mode so
        // the user hears the new sideband immediately after clicking instead of
        // finishing the tail of the wrong one. AudioHead stays put; the read
        // position is derived from Head - Count, so zeroing Count is enough.
        lock (state.AudioGate) { state.AudioCount = 0; }
    }

    public void SetFilter(int channelId, int lowHz, int highHz)
    {
        if (!_channels.TryGetValue(channelId, out var state)) return;
        // Normalize to positive magnitudes; mode dictates the sign via ApplyBandpassForMode.
        int lo = Math.Abs(lowHz);
        int hi = Math.Abs(highHz);
        if (hi < lo) (lo, hi) = (hi, lo);
        lock (state.FilterProfileGate)
        {
            state.FilterLowAbsHz = lo;
            state.FilterHighAbsHz = hi;
            ApplyBandpassForMode(state);
        }
        ConfigureMaxBinDetector(state);
    }

    public void SetVfoHz(int channelId, long vfoHz)
    {
        // VFO lives in VfoService above Protocol1Client (doc 07 §1.5) — WDSP has no
        // tuner; frequency translation happens at the protocol seam.
    }

    public void SetCtunShift(int channelId, int shiftHz)
    {
        if (!_channels.TryGetValue(channelId, out var state)) return;
        // Mirrors Thetis radio.cs:1419-1420. Note the negation: Thetis tracks
        // an `rx_osc = -(dial - centre)` then calls SetRXAShiftFreq(-osc), so
        // the net argument is (dial - centre) = our shiftHz. Same goes to
        // the nbp0 stage that enforces SSB sideband.
        lock (state.FilterProfileGate)
        {
            RunNativeLifecycleCriticalSection(() =>
            {
                NativeMethods.SetRXAShiftFreq(channelId, shiftHz);
                NativeMethods.RXANBPSetShiftFrequency(channelId, shiftHz);
                NativeMethods.SetRXAShiftRun(channelId, shiftHz != 0 ? 1 : 0);
                state.CtunShiftHz = shiftHz;
            });
        }
        ConfigureMaxBinDetector(state);
    }

    public void SetRxDisplayFastAttack(int channelId, bool fast)
    {
        if (!_channels.TryGetValue(channelId, out var state)) return;
        // RX display ONLY — channelId is the RXA channel whose analyzer was
        // created in OpenChannel (XCreateAnalyzer(id, ...)). The TX analyzer
        // (_txaChannelId) and PS-feedback analyzer keep TxAvgTauSec untouched,
        // so a retune while keyed never disturbs the TX trace or PS monitor.
        // AnalyzerLock mirrors SetZoom: SetDisplayAvBackmult races Spectrum0
        // (worker) and GetPixels (pipeline tick) otherwise.
        lock (state.AnalyzerLock)
        {
            ConfigureDisplayAveragingTau(channelId, fast ? FastAttackTauSec : DefaultAvgTauSec);
        }
    }

    public void SetAgcTop(int channelId, double topDb)
    {
        if (!_channels.TryGetValue(channelId, out var state)) return;
        state.AgcTopDb = topDb;
        NativeMethods.SetRXAAGCTop(channelId, topDb);
        _log.LogInformation("wdsp.setAgcTop channel={Id} topDb={TopDb:F1}", channelId, topDb);
    }

    public void SetAgcThresh(int channelId, double threshDbm)
    {
        if (!_channels.TryGetValue(channelId, out var state)) return;
        // WDSP converts the dBm threshold using the spectrum analyzer FFT size
        // and sample rate. Passing the 1024-sample exchange block here places
        // the AGC knee 12.04 dB away from a 16384-point analyzer. Thetis passes
        // the actual analyzer FFT size, including per-channel overrides.
        NativeMethods.SetRXAAGCThresh(channelId, threshDbm, state.AnalyzerFftSize, state.SampleRateHz);
        _log.LogInformation(
            "wdsp.setAgcThresh channel={Id} threshDbm={Thresh:F1} size={Size} rate={Rate}",
            channelId, threshDbm, state.AnalyzerFftSize, state.SampleRateHz);
    }

    public double GetAgcTop(int channelId)
    {
        if (!_channels.TryGetValue(channelId, out _)) return 0.0;
        double top = 0.0;
        NativeMethods.GetRXAAGCTop(channelId, ref top);
        return top;
    }

    public double GetAgcThresh(int channelId)
    {
        if (!_channels.TryGetValue(channelId, out var state)) return 0.0;
        double thresh = 0.0;
        NativeMethods.GetRXAAGCThresh(channelId, ref thresh, state.AnalyzerFftSize, state.SampleRateHz);
        return thresh;
    }

    public void SetAgc(int channelId, AgcConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!_channels.TryGetValue(channelId, out var state)) return;
        ApplyAgcCore(channelId, cfg);
        state.CurrentAgcMode = cfg.Mode;
        _log.LogInformation(
            "wdsp.setAgc channel={Id} mode={Mode} slope={Slope} decayMs={Decay} hangMs={Hang} hangThr={Thr} fixedDb={Fixed}",
            channelId, cfg.Mode, cfg.Slope, cfg.DecayMs, cfg.HangMs, cfg.HangThreshold, cfg.FixedGainDb);
    }

    public void SetSquelch(int channelId, SquelchConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!_channels.TryGetValue(channelId, out var state)) return;
        state.CurrentSquelch = cfg;
        ApplySquelchLocked(state);
        _log.LogInformation(
            "wdsp.setSquelch channel={Id} enabled={Enabled} level={Level} mode={Mode}",
            channelId, cfg.Enabled, cfg.Level, state.CurrentMode);
    }

    public void SetRxAfGainDb(int channelId, double db)
    {
        if (!_channels.TryGetValue(channelId, out _)) return;
        // WDSP's SetRXAPanelGain1 takes a linear multiplier on the post-demod
        // audio panel (panel.c:66). 0 dB ≡ 1.0 linear, which is the value
        // OpenChannel installs at line 237 — so a fresh channel that never
        // sees this call behaves exactly as before. Thetis wires its master
        // AF slider the same way (audio.cs:218-224, `SetRXAPanelGain1(rxa,
        // Math.Pow(10.0, db/20.0))`).
        double linear = Math.Pow(10.0, db / 20.0);
        NativeMethods.SetRXAPanelGain1(channelId, linear);
        _log.LogInformation("wdsp.setRxAfGain channel={Id} db={Db:F1} linear={Linear:F4}", channelId, db, linear);
    }

    private bool TrySetRxDisplayZoom(int channelId, int level, out bool changed)
    {
        SyntheticDspEngine.ValidateZoomLevel(level);
        changed = false;
        if (!_channels.TryGetValue(channelId, out var state)) return false;
        // Analyzer reconfig can race with Spectrum0 (worker) and GetPixels
        // (pipeline tick); the lock is the simpler option of the two team-lead
        // flagged. Briefly holds both producer and consumer while WDSP rebuilds
        // its bin mapping. Clients may still see one transient frame on the
        // wire — the averaging recovers in ~tau (≈100 ms) after the switch.
        lock (state.AnalyzerLock)
        {
            if (state.ZoomLevel != level)
            {
                state.ZoomLevel = level;
                ConfigureAnalyzer(channelId, state.SampleRateHz, InSize, state.PixelWidth, level, state.AnalyzerFftSize, AnalyzerWindow, AnalyzerKaiserPi);
                changed = true;
            }
        }

        return true;
    }

    /// <summary>
    /// Raise (or restore) the analyzer FFT size for a single RX display
    /// channel. The wideband detail DDC uses this to reach pixel-perfect bin
    /// density at deep zoom (65,536 points ≈ 2.9 Hz bins at 192 kHz) without
    /// touching the operator-facing panadapter FFT default. Zoom reconfigs
    /// re-read <see cref="ChannelState.AnalyzerFftSize"/>, so the override is
    /// sticky. Out-of-range values snap to the 16,384 default via
    /// <see cref="NormalizeRxAnalyzerFftSize"/>.
    /// </summary>
    public void SetRxDisplayFftSize(int channelId, int fftSize)
    {
        if (_disposed != 0) return;
        int normalized = NormalizeRxAnalyzerFftSize(fftSize);
        if (!_channels.TryGetValue(channelId, out var state)) return;
        // Same race story as TrySetRxDisplayZoom: SetAnalyzer rebuilds the
        // bin mapping while Spectrum0/GetPixels may be mid-frame.
        lock (state.AnalyzerLock)
        {
            if (state.AnalyzerFftSize == normalized) return;
            state.AnalyzerFftSize = normalized;
            ConfigureAnalyzer(channelId, state.SampleRateHz, InSize, state.PixelWidth, state.ZoomLevel, normalized, AnalyzerWindow, AnalyzerKaiserPi);
        }
        _log.LogInformation("wdsp.setRxDisplayFftSize channel={Id} fftSize={FftSize}", channelId, normalized);
    }

    public void SetRxDisplayZoom(int channelId, int level)
    {
        if (!TrySetRxDisplayZoom(channelId, level, out bool changed) || !changed) return;
        _log.LogInformation("wdsp.setRxDisplayZoom channel={Id} level={Level}", channelId, level);
    }

    public void SetZoom(int channelId, int level)
    {
        if (!TrySetRxDisplayZoom(channelId, level, out bool changed) || !changed) return;

        // Mirror zoom onto the TX analyzer so the TX panadapter span stays
        // lock-step with RX — otherwise keying mid-zoom would show a different
        // frequency window on MOX. No-op when TX analyzer is off.
        int? txaIdToReconfig = null;
        lock (_txDispLock)
        {
            if (_txDispAlive && _txaChannelId is int txa)
            {
                _txDispZoomLevel = level;
                txaIdToReconfig = txa;
                if (TryConfigureTxAnalyzer(txa, _txaDspRateHz, _txaDspSize, _txDispRxSampleRateHz, _txDispPixelWidth, level, _txFftSize, _txWinType, AnalyzerKaiserPi, out int usedWidth))
                {
                    _txDispUsedPixelWidth = usedWidth;
                    _txDispScratchPixels = PrepareDisplayScratch(_txDispScratchPixels, _txDispPixelWidth, usedWidth);
                }
            }
        }

        // Mirror zoom onto the PS-FB analyzer when it's open, same reasoning
        // as the TX analyzer: keep the PA-output trace span lock-step with the
        // RX panadapter so toggling the PS-Monitor view doesn't shift the axis.
        lock (_psFbDispLock)
        {
            if (_psFbDispAlive && _psFbDispId is int psFb)
            {
                _psFbDispZoomLevel = level;
                if (TryConfigureTxAnalyzer(psFb, _psFeedbackRateHz, PsFeedbackBlockSize, _psFbDispRxSampleRateHz, _psFbDispPixelWidth, level, _txFftSize, _txWinType, AnalyzerKaiserPi, out int usedWidth))
                {
                    _psFbDispUsedPixelWidth = usedWidth;
                    _psFbDispScratchPixels = PrepareDisplayScratch(_psFbDispScratchPixels, _psFbDispPixelWidth, usedWidth);
                }
            }
        }

        _log.LogInformation("wdsp.setZoom channel={Id} level={Level} txDisp={TxDisp}",
            channelId, level, txaIdToReconfig?.ToString() ?? "off");
    }

    public void SetNoiseReduction(int channelId, NrConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (!_channels.TryGetValue(channelId, out var state)) return;
        RunNativeLifecycleCriticalSection(() => ApplyNoiseReductionLocked(channelId, cfg, state));
    }

    private void ApplyNoiseReductionLocked(int channelId, NrConfig cfg, ChannelState state)
    {

        // Mutually-exclusive NR button. When switching to a mode, re-apply its
        // Thetis defaults before toggling Run=1 — matches Thetis setup.cs order
        // (configure, then enable) and keeps "toggle off then back on" at parity
        // even if a future caller changes the knobs between toggles.
        switch (cfg.NrMode)
        {
            case NrMode.Anr:
                TrySetNnrRun(channelId, 0);
                NativeMethods.SetRXAEMNRRun(channelId, 0);
                TrySetSbnrRun(channelId, 0);
                TrySetRnnrRun(channelId, 0);
                NativeMethods.SetRXAANRVals(channelId, NrDefaults.AnrTaps, NrDefaults.AnrDelay, NrDefaults.AnrGain, NrDefaults.AnrLeakage);
                NativeMethods.SetRXAANRPosition(channelId, NrDefaults.Position);
                NativeMethods.SetRXAANRRun(channelId, 1);
                break;
            case NrMode.Emnr:
                NativeMethods.SetRXAANRRun(channelId, 0);
                TrySetNnrRun(channelId, 0);
                TrySetSbnrRun(channelId, 0);
                TrySetRnnrRun(channelId, 0);
                // Core EMNR algorithm selectors (gain method, NPE method, AE
                // filter) plus the optional Trained-method T1/T2 tuning. All
                // operator-tunable; null fields fall back to NrDefaults so the
                // engine state stays Thetis-equivalent when nothing's set yet.
                ApplyNr2Core(channelId, cfg);
                // post2 comfort-noise injection. emnr.c:981–1023 generates a
                // smoothed noise floor that masks residual EMNR warble — the
                // psychoacoustic mechanism behind Thetis's noticeably smoother
                // NR2 hiss.
                ApplyNr2Post2(channelId, cfg);
                NativeMethods.SetRXAEMNRRun(channelId, 1);
                break;
            case NrMode.Sbnr:
                // NR4 — libspecbleach spectral bleaching. Disable the other
                // post-RXA NR paths first (mutual exclusion), then push the
                // operator-tuned (or Thetis-default) parameters before flipping
                // Run=1. Wrapped in TrySetSbnr* so a libwdsp build that
                // pre-dates Phase 1 (no SBNR exports) leaves the channel in
                // NR-off rather than crashing the worker.
                NativeMethods.SetRXAANRRun(channelId, 0);
                TrySetNnrRun(channelId, 0);
                TrySetEmnrPost2Run(channelId, 0);
                NativeMethods.SetRXAEMNRRun(channelId, 0);
                TrySetRnnrRun(channelId, 0);
                ApplyNr4Sbnr(channelId, cfg);
                break;
            case NrMode.Rnnr:
                // NR3 — RNNoise. Disable the other post-RXA NR paths, then
                // enable RNNR. The model is loaded process-globally via
                // RNNRloadModel at install/startup (LoadNr3Model), NOT here —
                // a per-channel call would thrash the shared model. Guarded by
                // TrySetRnnr* so a libwdsp without NR3 exports (WDSP_WITH_NR3
                // OFF) leaves the channel NR-off instead of crashing. If no
                // model is loaded, rnnr.c's create_rnnr left rnnoise_create
                // NULL, so xrnnr passes audio through untouched — inert, safe.
                NativeMethods.SetRXAANRRun(channelId, 0);
                TrySetNnrRun(channelId, 0);
                TrySetEmnrPost2Run(channelId, 0);
                NativeMethods.SetRXAEMNRRun(channelId, 0);
                TrySetSbnrRun(channelId, 0);
                TrySetRnnrPosition(channelId, NrDefaults.Position);
                TrySetRnnrRun(channelId, 1);
                break;
            case NrMode.Nnr:
                NativeMethods.SetRXAANRRun(channelId, 0);
                TrySetEmnrPost2Run(channelId, 0);
                NativeMethods.SetRXAEMNRRun(channelId, 0);
                TrySetSbnrRun(channelId, 0);
                TrySetRnnrRun(channelId, 0);
                TrySetNnrPosition(channelId, NrDefaults.Position);
                ApplyNnr(channelId, cfg);
                TrySetNnrRun(channelId, 1);
                break;
            default:
                NativeMethods.SetRXAANRRun(channelId, 0);
                TrySetEmnrPost2Run(channelId, 0);
                NativeMethods.SetRXAEMNRRun(channelId, 0);
                TrySetSbnrRun(channelId, 0);
                TrySetRnnrRun(channelId, 0);
                TrySetNnrRun(channelId, 0);
                break;
        }
        state.CurrentNrMode = cfg.NrMode;
        if (cfg.AnfEnabled)
        {
            NativeMethods.SetRXAANFVals(channelId, NrDefaults.AnfTaps, NrDefaults.AnfDelay, NrDefaults.AnfGain, NrDefaults.AnfLeakage);
            NativeMethods.SetRXAANFPosition(channelId, NrDefaults.Position);
            NativeMethods.SetRXAANFRun(channelId, 1);
        }
        else
        {
            NativeMethods.SetRXAANFRun(channelId, 0);
        }

        NativeMethods.SetRXASNBARun(channelId, cfg.SnbEnabled ? 1 : 0);
        // The notch-bandpass run flag gates BOTH the NBP toggle and the manual
        // notch database — so keep it on whenever active manual notches exist,
        // otherwise a routine NR change would silently disable the operator's
        // EMF notches. (RXANBPSetNotchesRun is the single WDSP gate for both.)
        bool anyActiveNotch;
        lock (_notchLock) anyActiveNotch = _manualNotches.Exists(static n => n.Active);
        NativeMethods.RXANBPSetNotchesRun(channelId, (cfg.NbpNotchesEnabled || anyActiveNotch) ? 1 : 0);

        // Mutually-exclusive pre-RXA blanker. Update threshold on whichever
        // path we're about to run (or both paths when switching off → on → the
        // dormant side keeps a stale value, harmless while its Run=0). UI slider
        // is 0..100; Thetis multiplies by 0.165 before passing to WDSP.
        double scaledThreshold = cfg.NbThreshold * NrDefaults.NbThresholdScale;
        switch (cfg.NbMode)
        {
            case NbMode.Nb1:
                NativeMethods.SetEXTNOBRun(channelId, 0);
                NativeMethods.SetEXTANBThreshold(channelId, scaledThreshold);
                NativeMethods.SetEXTANBRun(channelId, 1);
                break;
            case NbMode.Nb2:
                NativeMethods.SetEXTANBRun(channelId, 0);
                NativeMethods.SetEXTNOBThreshold(channelId, scaledThreshold);
                NativeMethods.SetEXTNOBRun(channelId, 1);
                break;
            default:
                NativeMethods.SetEXTANBRun(channelId, 0);
                NativeMethods.SetEXTNOBRun(channelId, 0);
                break;
        }

        // RunWorker gate. Toggled after the Run flags above so the worker
        // doesn't call xanbEXT/xnobEXT between "dispatch starts running NB1"
        // and "we remember we're NB1 mode" — same reason SetNoiseReduction
        // runs Run=0 on the other side before Run=1 on this side.
        state.CurrentNbMode = cfg.NbMode;

        _log.LogInformation(
            "wdsp.setNoiseReduction channel={Id} nr={Nr} anf={Anf} snb={Snb} notches={Notches} nb={Nb} thr={Thr:F2}",
            channelId, cfg.NrMode, cfg.AnfEnabled, cfg.SnbEnabled, cfg.NbpNotchesEnabled,
            cfg.NbMode, scaledThreshold);
    }


    public void SetNotches(IReadOnlyList<NotchDto> notches)
    {
        ArgumentNullException.ThrowIfNull(notches);
        RunNativeLifecycleCriticalSection(() =>
        {
            lock (_notchLock)
            {
                _manualNotches.Clear();
                _manualNotches.AddRange(notches);
                // Re-apply to every open RX channel (there is normally one). The
                // copy under the lock means the per-channel WDSP rewrite reads a
                // stable snapshot even if another SetNotches races in.
                foreach (var id in _channels.Keys)
                    ApplyNotchesToChannelLocked(id);
            }
        });
        _log.LogInformation("wdsp.setNotches count={Count}", notches.Count);
    }

    public void SetNotchTuneFrequencyHz(double loHz)
    {
        RunNativeLifecycleCriticalSection(() =>
        {
            lock (_notchLock)
            {
                if (loHz == _notchTuneFreqHz) return;
                _notchTuneFreqHz = loHz;
                if (_notchDbUnavailable) return;
                try
                {
                    foreach (var id in _channels.Keys)
                        NativeMethods.RXANBPSetTuneFrequency(id, loHz);
                }
                catch (EntryPointNotFoundException)
                {
                    MarkNotchDbUnavailable();
                }
            }
        });
    }

    // Re-apply the current notch list to one channel. Public-facing callers
    // take _notchLock first; OpenChannel calls the lock-acquiring wrapper.
    private void ApplyNotchesToChannel(int channelId)
    {
        RunNativeLifecycleCriticalSection(() =>
        {
            lock (_notchLock) ApplyNotchesToChannelLocked(channelId);
        });
    }

    // Rewrite WDSP's notch database for `channelId` from _manualNotches: clear
    // whatever is there, add the current set, then gate the run flag on whether
    // any notch is active. Must be called under _notchLock. Wrapped so a libwdsp
    // build without the notch-DB exports degrades to a no-op instead of killing
    // the worker (same posture as the SBNR guard).
    private void ApplyNotchesToChannelLocked(int channelId)
    {
        if (_notchDbUnavailable) return;
        try
        {
            // Position notches against the last LO we were told about.
            NativeMethods.RXANBPSetTuneFrequency(channelId, _notchTuneFreqHz);

            int count = 0;
            NativeMethods.RXANBPGetNumNotches(channelId, ref count);
            for (int i = count - 1; i >= 0; i--)
                NativeMethods.RXANBPDeleteNotch(channelId, i);

            bool anyActive = false;
            for (int i = 0; i < _manualNotches.Count; i++)
            {
                var n = _manualNotches[i];
                anyActive |= n.Active;
                NativeMethods.RXANBPAddNotch(channelId, i, n.CenterHz, n.WidthHz, n.Active ? 1 : 0);
            }

            NativeMethods.RXANBPSetNotchesRun(channelId, anyActive ? 1 : 0);
        }
        catch (EntryPointNotFoundException)
        {
            MarkNotchDbUnavailable();
        }
    }

    private void MarkNotchDbUnavailable()
    {
        if (_notchDbUnavailable) return;
        _notchDbUnavailable = true;
        _log.LogWarning(
            "wdsp.notchDb unavailable — bundled libwdsp does not export the manual-notch functions; manual notches disabled");
    }

    // NR2 (EMNR) core algorithm selectors. Pushed on every NR config update
    // (not just at NrMode-switch time) so the operator can change Gain Method
    // / NPE Method / AE Filter from the inline panel and see effect without a
    // mode cycle. T1/T2 are unconditionally written when GainMethod=3 — WDSP
    // only consults them in the Trained-gain code path (emnr.c:1226–1276).
    private static void ApplyNr2Core(int channelId, NrConfig cfg)
    {
        int gainMethod = cfg.EmnrGainMethod ?? NrDefaults.EmnrGainMethod;
        int npeMethod = cfg.EmnrNpeMethod ?? NrDefaults.EmnrNpeMethod;
        bool aeRun = cfg.EmnrAeRun ?? (NrDefaults.EmnrAeRun != 0);

        NativeMethods.SetRXAEMNRgainMethod(channelId, gainMethod);
        NativeMethods.SetRXAEMNRnpeMethod(channelId, npeMethod);
        NativeMethods.SetRXAEMNRaeRun(channelId, aeRun ? 1 : 0);
        NativeMethods.SetRXAEMNRPosition(channelId, NrDefaults.Position);

        if (gainMethod == 3)
        {
            NativeMethods.SetRXAEMNRtrainZetaThresh(channelId, cfg.EmnrTrainT1 ?? NrDefaults.EmnrTrainT1);
            NativeMethods.SetRXAEMNRtrainT2(channelId, cfg.EmnrTrainT2 ?? NrDefaults.EmnrTrainT2);
        }
    }

    // NR2 (EMNR) post2 comfort-noise tunables. Configures all five params
    // before flipping post2Run so the post-processing stage starts coherent.
    // Null fields fall back to NrDefaults so the operator's "leave it default"
    // choice (cleared field) is honoured at write time without baking the
    // current default into the persisted config. Wrapped in try/catch so a
    // libwdsp.so built before the post2 exports landed (or a stale system
    // copy shadowing the bundled one) leaves NR2 running without comfort-noise
    // instead of crashing the worker.
    private void ApplyNr2Post2(int channelId, NrConfig cfg)
    {
        try
        {
            NativeMethods.SetRXAEMNRpost2Factor(channelId, cfg.EmnrPost2Factor ?? NrDefaults.EmnrPost2Factor);
            NativeMethods.SetRXAEMNRpost2Nlevel(channelId, cfg.EmnrPost2Nlevel ?? NrDefaults.EmnrPost2Nlevel);
            NativeMethods.SetRXAEMNRpost2Rate(channelId, cfg.EmnrPost2Rate ?? NrDefaults.EmnrPost2Rate);
            NativeMethods.SetRXAEMNRpost2Taper(channelId, cfg.EmnrPost2Taper ?? NrDefaults.EmnrPost2Taper);
            bool runOn = cfg.EmnrPost2Run ?? (NrDefaults.EmnrPost2Run != 0);
            NativeMethods.SetRXAEMNRpost2Run(channelId, runOn ? 1 : 0);
        }
        catch (EntryPointNotFoundException ex)
        {
            _log.LogWarning(
                "wdsp.emnr.post2.unavailable channel={Id} reason=\"libwdsp does not export SetRXAEMNRpost2* — bundled .so is being shadowed by an older system copy, or the build pre-dates post2 support\" detail={Msg}",
                channelId, ex.Message);
        }
    }

    // NR4 (SBNR / libspecbleach) parameter push + Run=1. Native setters take
    // float; we downcast at the seam. Wrapped in TrySet* because a libwdsp
    // built without Phase 1 of issue #79 will throw EntryPointNotFoundException
    // here — the operator gets NR-off behaviour instead of a worker crash.
    private void ApplyNr4Sbnr(int channelId, NrConfig cfg)
    {
        try
        {
            NativeMethods.SetRXASBNRPosition(channelId, cfg.Nr4Position ?? NrDefaults.Nr4Position);
            NativeMethods.SetRXASBNRreductionAmount(channelId, (float)(cfg.Nr4ReductionAmount ?? NrDefaults.Nr4ReductionAmount));
            NativeMethods.SetRXASBNRsmoothingFactor(channelId, (float)(cfg.Nr4SmoothingFactor ?? NrDefaults.Nr4SmoothingFactor));
            NativeMethods.SetRXASBNRwhiteningFactor(channelId, (float)(cfg.Nr4WhiteningFactor ?? NrDefaults.Nr4WhiteningFactor));
            NativeMethods.SetRXASBNRnoiseRescale(channelId, (float)(cfg.Nr4NoiseRescale ?? NrDefaults.Nr4NoiseRescale));
            NativeMethods.SetRXASBNRpostFilterThreshold(channelId, (float)(cfg.Nr4PostFilterThreshold ?? NrDefaults.Nr4PostFilterThreshold));
            NativeMethods.SetRXASBNRnoiseScalingType(channelId, cfg.Nr4NoiseScalingType ?? NrDefaults.Nr4NoiseScalingType);
            NativeMethods.SetRXASBNRRun(channelId, 1);
        }
        catch (EntryPointNotFoundException ex)
        {
            _log.LogWarning(
                "wdsp.sbnr.unavailable channel={Id} reason=\"libwdsp build does not export SBNR symbols (Phase 1 of issue #79 not yet shipped)\" detail={Msg}",
                channelId, ex.Message);
        }
    }

    private void TrySetSbnrRun(int channelId, int run)
    {
        try { NativeMethods.SetRXASBNRRun(channelId, run); }
        catch (EntryPointNotFoundException) { /* libwdsp pre-Phase-1; SBNR is a no-op */ }
    }

    // Same shape as TrySetSbnrRun for the post2 Run=0 calls we issue when
    // switching away from NR2. A stale libwdsp.so on the operator's machine
    // (e.g. an older copy in /usr/local/lib shadowing the bundled .so) would
    // otherwise throw EntryPointNotFoundException straight up the worker.
    private void TrySetEmnrPost2Run(int channelId, int run)
    {
        try { NativeMethods.SetRXAEMNRpost2Run(channelId, run); }
        catch (EntryPointNotFoundException) { /* libwdsp lacks post2; nothing to turn off */ }
    }

    // NR3 (RNNoise) Run/Position guards — same shape as the SBNR ones. A
    // libwdsp built with WDSP_WITH_NR3=OFF (the stub) does not export RNNR
    // symbols, so every call is wrapped: NR3 then behaves as a no-op rather
    // than crashing the worker.
    private void TrySetRnnrRun(int channelId, int run)
    {
        try { NativeMethods.SetRXARNNRRun(channelId, run); }
        catch (EntryPointNotFoundException) { /* libwdsp lacks NR3; nothing to toggle */ }
    }

    private void TrySetRnnrPosition(int channelId, int position)
    {
        try { NativeMethods.SetRXARNNRPosition(channelId, position); }
        catch (EntryPointNotFoundException) { /* libwdsp lacks NR3; position is moot */ }
    }

    private void TrySetNnrRun(int channelId, int run)
    {
        try { NativeMethods.SetRXANNRRun(channelId, run); }
        catch (EntryPointNotFoundException) { /* pre-2.10 libwdsp; NNR is unavailable */ }
    }

    private void TrySetNnrPosition(int channelId, int position)
    {
        try { NativeMethods.SetRXANNRPosition(channelId, position); }
        catch (EntryPointNotFoundException) { /* pre-2.10 libwdsp; position is moot */ }
    }

    private static void ApplyNnr(int channelId, NrConfig cfg)
    {
        try
        {
            NativeMethods.SetRXANNRModel(channelId, Math.Clamp(cfg.NnrModel ?? 0, 0, 1));
            NativeMethods.SetRXANNRMaskFloor(channelId, Math.Clamp(cfg.NnrMaskFloorDb ?? -25.0, -60.0, 0.0));
            NativeMethods.SetRXANNRAlpha(channelId, Math.Clamp(cfg.NnrAlpha ?? 1.0, 0.0, 4.0));
            NativeMethods.SetRXANNRAlphaKnee(channelId, Math.Clamp(cfg.NnrKneeDb ?? 10.0, 0.0, 40.0));
            NativeMethods.SetRXANNRTau(channelId, Math.Clamp(cfg.NnrTauSeconds ?? 2.0, 0.05, 30.0));
            NativeMethods.SetRXANNRMaxGain(channelId, Math.Clamp(cfg.NnrMaxGainDb ?? 12.0, 0.0, 24.0));
            NativeMethods.SetRXANNRSmooth(channelId,
                Math.Clamp(cfg.NnrAttackMs ?? 0.0, 0.0, 500.0),
                Math.Clamp(cfg.NnrReleaseMs ?? 0.0, 0.0, 500.0));
        }
        catch (EntryPointNotFoundException) { /* pre-2.10 libwdsp; NNR remains off */ }
    }

    // Loads (or, with a null/empty path, clears) the process-global RNNoise
    // model used by every RNNR channel. Called by the server when the operator
    // installs/removes a model and once at startup if one is already installed.
    // Returns false when libwdsp lacks NR3 support so the caller can surface
    // "NR3 unavailable on this build" instead of silently succeeding. Zeus
    // builds rnnoise without a baked-in model, so clearing the path leaves NR3
    // inert (audio passes through) rather than falling back to a stock model.
    public Nr3ModelLoadResult LoadNr3Model(string? modelFilePath)
    {
        lock (_nativeLifecycleLock)
        {
            try
            {
                NativeMethods.RNNRloadModel(modelFilePath ?? string.Empty);
            }
            catch (EntryPointNotFoundException ex)
            {
                _log.LogWarning(
                    "wdsp.rnnr.unavailable reason=\"libwdsp build does not export RNNR symbols (WDSP_WITH_NR3=OFF — xiph/rnnoise not vendored)\" detail={Msg}",
                    ex.Message);
                return Nr3ModelLoadResult.Unavailable;
            }

            if (string.IsNullOrEmpty(modelFilePath))
            {
                _log.LogInformation("wdsp.rnnr.loadModel path=\"(none — NR3 inert)\"");
                return Nr3ModelLoadResult.Cleared;
            }

            // Verify the model actually parsed. RNNRmodelLoaded is an additive export;
            // older libwdsp builds lack it, in which case we can't verify and assume
            // success (the prior behaviour — no regression).
            bool? loaded = null;
            try { loaded = NativeMethods.RNNRmodelLoaded() != 0; }
            catch (EntryPointNotFoundException) { /* old libwdsp — can't verify */ }

            if (loaded == false)
            {
                _log.LogWarning(
                    "wdsp.rnnr.loadModel.failed path=\"{Path}\" reason=\"rnnoise_model_from_filename returned NULL (incompatible/corrupt weights)\"",
                    modelFilePath);
                return Nr3ModelLoadResult.LoadFailed;
            }

            _log.LogInformation("wdsp.rnnr.loadModel path=\"{Path}\" verified={Verified}",
                modelFilePath, loaded.HasValue);
            return Nr3ModelLoadResult.Loaded;
        }
    }

    // Post-RXA NR defaults — sourced from Thetis setup.designer.cs + radio.cs.
    // UI-space scaling (gain × 1e-6, leakage × 1e-3) is already resolved: these
    // are the post-scale values WDSP actually receives. See docs/prd/10-noise-reduction.md.
    private static class NrDefaults
    {
        public const int AnrTaps = 64;
        public const int AnrDelay = 16;
        public const double AnrGain = 1e-4;
        public const double AnrLeakage = 0.1;
        public const int AnfTaps = 64;
        public const int AnfDelay = 16;
        public const double AnfGain = 1e-4;
        public const double AnfLeakage = 0.1;
        public const int EmnrGainMethod = 2;
        public const int EmnrNpeMethod = 0;
        public const int EmnrAeRun = 1;
        public const int Position = 1;

        // Thetis Setup → DSP "Trained" T1/T2 NUDs (setup.designer.cs:43330 /
        // 43298). Pushed raw via SetRXAEMNRtrainZetaThresh / SetRXAEMNRtrainT2
        // (setup.cs:29384 / 32308). Only consulted by WDSP when
        // EmnrGainMethod=3 (Trained gain method).
        //   T1: udDSPNR2trainThresh.Value = -0.5  (range -5..5, step 0.1)
        //   T2: udDSPNR2trainT2.Value     =  0.2  (range 0.02..0.3, step 0.01)
        // NB: Thetis packs the T2 NUD as decimal{2,0,0,scale=1} = 0.2 — earlier
        // Zeus read it as 2.0 (a 10× error). It is 0.2.
        public const double EmnrTrainT1 = -0.5;
        public const double EmnrTrainT2 = 0.2;

        // post2 defaults sourced from Thetis radio.cs:2103/2122/2160 (raw
        // NumericUpDown values 0..100, default 15/15/12). The /100 scaling
        // happens INSIDE WDSP at emnr.c:1035/1042/1050, so the wire value is
        // the Thetis slider raw — not the post-divide internal value WDSP
        // ends up storing. (post2Rate has no /100 in WDSP, so 5.0 is correct
        // as-is.) Earlier Zeus defaults of 0.15 were 100× too small once WDSP
        // divided again, leaving comfort-noise effectively silent.
        //
        // Run defaults ON — this is a DELIBERATE Zeus divergence, not Thetis
        // parity: Thetis's "Noise post proc" checkbox ships OFF
        // (setup.designer.cs chkNR2PostProc_enable_rx1, no Checked=true). Zeus
        // enables post2 by default because the comfort-noise injection masks
        // the musical-warble artifacts of frequency-domain EMNR, which the
        // maintainers consider an improvement over the Thetis baseline.
        public const int EmnrPost2Run = 1;
        public const double EmnrPost2Factor = 15.0;
        public const double EmnrPost2Nlevel = 15.0;
        public const double EmnrPost2Rate = 5.0;
        public const int EmnrPost2Taper = 12;

        // NR4 (SBNR / libspecbleach) defaults — sourced from Thetis radio.cs
        // :2350-2462 (rx_nr4_* private fields). Native setters take float;
        // we keep them as double here and downcast at the P/Invoke seam to
        // match the rest of the contract surface.
        public const double Nr4ReductionAmount = 10.0;
        public const double Nr4SmoothingFactor = 0.0;
        public const double Nr4WhiteningFactor = 0.0;
        public const double Nr4NoiseRescale = 2.0;
        // Thetis Setup → DSP "SNRthresh" NUD default (setup.designer.cs:42132).
        // The radio.cs field-init is 0.0 but Setup pushes the NUD value (-10) at
        // first paint, so the operator's effective default is -10. WDSP's own
        // create_sbnr also seeds -10 (sbnr.c:84), so the ON-startup state in
        // Thetis is -10 across the board. Aligning here gives Zeus the same
        // first-run behaviour.
        public const double Nr4PostFilterThreshold = -10.0;
        public const int Nr4NoiseScalingType = 0;
        public const int Nr4Position = 1;


        // NB1/NB2 runtime-steady-state params — what Thetis actually runs with
        // once radio.cs's NB property setters have fired (tau=advtime=hangtime
        // = 5e-5, threshold = 0.165 × UI=20 = 3.3). backtau has no property
        // setter in Thetis, so it keeps cmaster.c's create-time value of 0.05.
        // Applied through Set* setters post-create (see ApplyNbDefaults) so
        // a future advanced-NB panel can reuse the same code path.
        public const double NbTau = 5e-5;
        public const double NbHangtime = 5e-5;
        public const double NbAdvtime = 5e-5;
        public const double NbBacktau = 0.05;
        public const double NbThresholdScale = 0.165;
        public const double NbDefaultThresholdScaled = 3.3;
    }

    public int ReadAudio(int channelId, Span<float> output)
    {
        if (!_channels.TryGetValue(channelId, out var state))
        {
            output.Clear();
            return 0;
        }

        // RX ingest health is emitted from the channel worker (RunWorker) now,
        // not here — so a receiver whose audio isn't read out (e.g. RX3+ in
        // multi-DDC, where only RX1/RX2 audio is currently mixed) still reports
        // per-DDC health. Emitting here would gate health on audio consumption.

        lock (state.AudioGate)
        {
            int n = Math.Min(output.Length, state.AudioCount);
            if (n == 0) return 0;

            int tail = (state.AudioHead - state.AudioCount + AudioRingCapacity) % AudioRingCapacity;
            int firstChunk = Math.Min(n, AudioRingCapacity - tail);
            state.AudioRing.AsSpan(tail, firstChunk).CopyTo(output);
            int remainder = n - firstChunk;
            if (remainder > 0)
                state.AudioRing.AsSpan(0, remainder).CopyTo(output.Slice(firstChunk));

            state.AudioCount -= n;
            return n;
        }
    }

    // TEMP diagnostics (zeus-gdc7): emit a 1 Hz per-channel snapshot of the
    // realtime RX path health — input frames, would-block enqueues, worker
    // per-frame timing, audio-ring depth + overruns. Lets a live G2 session
    // tell apart "worker can't keep up" (queueFull>0 / high workerMaxMs) from
    // "consumer/tick stall" (queueFull==0 but audioOverrun>0 / ringDepth low).
    // Called once per channel per ReadAudio (≈30 Hz); gated to log at ~1 Hz.
    private void EmitRxDiag(ChannelState state)
    {
        long now = System.Diagnostics.Stopwatch.GetTimestamp();
        long last = state.DiagLastLogTicks;
        if (last != 0 && now - last < System.Diagnostics.Stopwatch.Frequency) return;
        state.DiagLastLogTicks = now;
        if (last == 0) return; // first call only seeds the timer

        long framesIn = Interlocked.Exchange(ref state.DiagFramesIn, 0);
        long enqueueFull = Interlocked.Exchange(ref state.DiagEnqueueFull, 0);
        long droppedOldest = Interlocked.Exchange(ref state.DiagDroppedOldest, 0);
        long workerFrames = Interlocked.Exchange(ref state.DiagWorkerFrames, 0);
        long workerTotalTicks = Interlocked.Exchange(ref state.DiagWorkerTotalTicks, 0);
        long workerMaxTicks = Interlocked.Exchange(ref state.DiagWorkerMaxTicks, 0);
        long audioOverrun = Interlocked.Exchange(ref state.DiagAudioOverrun, 0);

        double ticksToMs = 1000.0 / System.Diagnostics.Stopwatch.Frequency;
        double workerAvgMs = workerFrames > 0 ? workerTotalTicks * ticksToMs / workerFrames : 0;
        double workerMaxMs = workerMaxTicks * ticksToMs;
        int queueDepth = state.InQueue.Count;
        int audioRingDepth;
        lock (state.AudioGate) audioRingDepth = state.AudioCount;

        _log.LogInformation(
            "wdsp.rxdiag ch={Id} rate={RateHz} framesIn={FramesIn} queueDepth={QueueDepth}/{QueueCap} " +
            "queueFull={QueueFull} dropped={Dropped} workerFrames={WorkerFrames} workerAvgMs={WorkerAvgMs:F2} " +
            "workerMaxMs={WorkerMaxMs:F2} audioRingDepth={AudioRingDepth} audioOverrun={AudioOverrun}",
            state.Id, state.SampleRateHz, framesIn, queueDepth, state.InQueueCapacity,
            enqueueFull, droppedOldest, workerFrames, workerAvgMs, workerMaxMs,
            audioRingDepth, audioOverrun);

        // Latch the same window for on-demand diagnostics (live /api/diagnostics
        // surface + 0x36 health push). Immutable record swap — lock-free read.
        state.LastHealth = new RxChannelHealth(
            ChannelId: state.Id,
            SampleRateHz: state.SampleRateHz,
            QueueDepth: queueDepth,
            QueueCapacity: state.InQueueCapacity,
            FramesInPerWindow: framesIn,
            QueueFullPerWindow: enqueueFull,
            DroppedPerWindow: droppedOldest,
            WorkerFramesPerWindow: workerFrames,
            WorkerAvgMs: workerAvgMs,
            WorkerMaxMs: workerMaxMs,
            AudioRingDepth: audioRingDepth,
            AudioOverrunPerWindow: audioOverrun,
            AgeMs: 0);
    }

    /// <summary>
    /// Lock-free snapshot of every open RX channel's latest ingest-health window
    /// (see <see cref="RxChannelHealth"/>). Allocation-light and free of any
    /// realtime/WDSP work — safe to call from the diagnostics request thread.
    /// <c>AgeMs</c> is filled in here from the latch timestamp so callers can
    /// tell a live window from a stalled one. Channels with no completed window
    /// yet are omitted.
    /// </summary>
    public IReadOnlyList<RxChannelHealth> SnapshotRxChannels()
    {
        long nowTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        double ticksToMs = 1000.0 / System.Diagnostics.Stopwatch.Frequency;
        var list = new List<RxChannelHealth>(_channels.Count);
        foreach (var kv in _channels)
        {
            var h = kv.Value.LastHealth;
            if (h is null) continue;
            long stamp = kv.Value.DiagLastLogTicks;
            long ageMs = stamp != 0 ? (long)((nowTicks - stamp) * ticksToMs) : 0;
            list.Add(h with { AgeMs = ageMs });
        }
        return list;
    }

    private static void PushAudio(ChannelState state, ReadOnlySpan<double> interleavedStereo, int monoSampleCount)
    {
        lock (state.AudioGate)
        {
            for (int i = 0; i < monoSampleCount; i++)
            {
                // interleavedStereo is [L0, R0, L1, R1, ...]; take the left channel as mono.
                state.AudioRing[state.AudioHead] = (float)interleavedStereo[i * 2];
                state.AudioHead = (state.AudioHead + 1) % AudioRingCapacity;
                if (state.AudioCount < AudioRingCapacity)
                    state.AudioCount++;
                else
                    state.DiagAudioOverrun++; // TEMP diag (zeus-gdc7): oldest unread sample overwritten → discontinuity
                // Otherwise the oldest sample has been overwritten — head advance already did it.
            }
        }
    }

    // Thetis rxaMeterType.RXA_S_AV = 1 (console/dsp.cs:876-884) — raw
    // average signal strength in dBm, smoothed by WDSP's internal meter tau.
    // The per-board S-meter calibration offset is applied by
    // DspPipelineService, where the effective board / variant are known.
    // Returns a large negative (~−200) before any frame has been exchanged.
    private const int RxaMeterSAv = 1;

    private DateTime _lastRxMeterLogUtc;
    public double GetRxaSignalDbm(int channelId)
    {
        if (!_channels.ContainsKey(channelId)) return -200.0;
        double sAv = NativeMethods.GetRXAMeter(channelId, RxaMeterSAv);
        // Debug aid: if S_AV reads the "meter-didn't-run" sentinel (-400),
        // fall through to ADC_AV (index 3) which runs earlier in xrxa and
        // tells us whether the pipeline is exchanging at all. Pass the raw
        // value through on the sentinel path so the caller's `<= -399.0`
        // check still fires instead of being shifted by the cal.
        if (sAv <= -399.0)
        {
            double adcAv = NativeMethods.GetRXAMeter(channelId, 3);
            _log.LogInformation("wdsp.getRxaMeter sAv={SAv:F1} adcAv={AdcAv:F1} (sentinel)", sAv, adcAv);
            return sAv;
        }
        // Diagnostic 2026-04-18: log the live sAv at 1 Hz so we can see RX
        // signal level over time and pinpoint when it dies (e.g. after
        // MOX-on/off transition). Extended to read all four wcp-AGC indices —
        // smeter sits BEFORE the AGC stage in xrxa (RXA.c:645 vs 662), so if
        // sAv is -400 but agcAv/agcGain are real, the chain is alive through
        // AGC and the dead zone is between adcmeter and smeter (xbpsnbain or
        // xnbp). Conversely, if all are -400, xrxa itself is not running.
        var now = DateTime.UtcNow;
        if (now - _lastRxMeterLogUtc >= TimeSpan.FromSeconds(1))
        {
            _lastRxMeterLogUtc = now;
            // Indices per WDSP RXA.h:47-57 enum rxaMeterType.
            double adcAv = NativeMethods.GetRXAMeter(channelId, 3);   // RXA_ADC_AV
            // WDSP meters volts/out_target; the applied insertion multiplier is
            // its reciprocal. Negate the dB value, as Thetis does, so positive
            // means boost and negative means gain reduction everywhere above
            // this native seam.
            double agcGain = -NativeMethods.GetRXAMeter(channelId, 4); // RXA_AGC_GAIN
            double agcAv = NativeMethods.GetRXAMeter(channelId, 6);   // RXA_AGC_AV
            _log.LogInformation(
                "wdsp.rx.meter sAv={SAv:F1} adcAv={AdcAv:F1} agcGain={AgcGain:F1} agcAv={AgcAv:F1}",
                sAv, adcAv, agcGain, agcAv);
        }
        return sAv;
    }

    // Full RXA meter snapshot — fetches all 7 ring indices plus analyzer
    // max-bin in one pass and
    // publishes under _rxMeterPublishLock so callers see a consistent set.
    // Indices per WDSP RXA.h:47-57 enum rxaMeterType:
    //   0  RXA_S_PK     1  RXA_S_AV     (signal peak / avg, dBm)
    //   2  RXA_ADC_PK   3  RXA_ADC_AV   (ADC input peak / avg, dBFS)
    //   4  RXA_AGC_GAIN              (AGC insertion gain, signed dB)
    //   5  RXA_AGC_PK   6  RXA_AGC_AV   (AGC envelope peak / avg, dBm)
    //
    // This helper is the canonical source for the 0x19 RxMetersV2Frame
    // broadcast in DspPipelineService. The pre-existing diagnostic reads
    // inside GetRxaSignalDbm (RXA_ADC_AV, RXA_AGC_GAIN, RXA_AGC_AV at 1 Hz)
    // are deliberately retained — they log a different cadence and tell us
    // whether the chain is alive when sAv is at sentinel.
    //
    // Cal offset is NOT applied here. The caller decides whether to add it
    // before serializing, so unit tests can assert raw WDSP output and a
    // future per-board calibration table can plug in at the broadcast seam
    // without re-touching this method. See plan §2.1 / §2.3.
    public RxStageMeters GetRxStageMeters(int channelId)
    {
        if (!_channels.ContainsKey(channelId)) return RxStageMeters.Silent;
        var snap = new RxStageMeters(
            SignalPk: (float)NativeMethods.GetRXAMeter(channelId, 0),
            SignalAv: (float)NativeMethods.GetRXAMeter(channelId, 1),
            AdcPk: (float)NativeMethods.GetRXAMeter(channelId, 2),
            AdcAv: (float)NativeMethods.GetRXAMeter(channelId, 3),
            AgcGain: (float)-NativeMethods.GetRXAMeter(channelId, 4),
            AgcEnvPk: (float)NativeMethods.GetRXAMeter(channelId, 5),
            AgcEnvAv: (float)NativeMethods.GetRXAMeter(channelId, 6),
            SignalMaxBin: (float)NativeMethods.GetDetectMaxBin(channelId));
        lock (_rxMeterPublishLock) { _latestRxStageMeters = snap; }
        return snap;
    }

    public bool TryGetDisplayPixels(int channelId, DisplayPixout which, Span<float> dbOut)
    {
        if (which == DisplayPixout.SnrPower) return false;
        if (!_channels.TryGetValue(channelId, out var state)) return false;
        if (dbOut.Length != state.PixelWidth)
            throw new ArgumentException($"expected span of {state.PixelWidth}", nameof(dbOut));

        lock (state.AnalyzerLock)
        {
            // The pixel drain runs on the DspPipelineService tick thread and can
            // race a concurrent StopChannel (disconnect, sample-rate/mode rebuild).
            // GetPixels on a torn-down WDSP analyzer slot is a native use-after-free
            // (0xc0000005). StopChannel sets Stopped and performs DestroyAnalyzer
            // under this same lock, so re-checking Stopped here makes the drain and
            // the teardown mutually exclusive and closes the crash window.
            if (state.Stopped || !state.AnalyzerHasSnapped) return false;
            NativeMethods.GetPixels(channelId, (int)which, ref MemoryMarshal.GetReference(dbOut), out int flag);
            return flag == 1;
        }
    }

    public bool TryGetRxSnrPowerSpectrum(
        int channelId, Span<float> dbOut, out RxSnrSpectrumInfo info)
    {
        info = default;
        if (!_channels.TryGetValue(channelId, out var state)) return false;
        info = new RxSnrSpectrumInfo(
            SnrAnalyzerPixelWidth, state.SampleRateHz, state.SnrAnalyzerGeneration);
        if (dbOut.Length < SnrAnalyzerPixelWidth)
            throw new ArgumentException($"expected span of at least {SnrAnalyzerPixelWidth}", nameof(dbOut));

        lock (state.AnalyzerLock)
        {
            if (state.Stopped || !state.SnrAnalyzerHasSnapped) return false;
            var pixels = dbOut[..SnrAnalyzerPixelWidth];
            NativeMethods.GetPixels(
                state.SnrAnalyzerId, (int)DisplayPixout.SnrPower,
                ref MemoryMarshal.GetReference(pixels), out int flag);
            return flag == 1;
        }
    }

    public bool TryGetTxDisplayPixels(DisplayPixout which, Span<float> dbOut)
    {
        if (_disposed != 0) return false;
        lock (_txDispLock)
        {
            if (!_txDispAlive) return false;
            if (_txaChannelId is not int txa) return false;
            int expectedWidth = _txDispPixelWidth;
            if (dbOut.Length != expectedWidth)
                return false;
            int usedWidth = _txDispUsedPixelWidth > 0 ? _txDispUsedPixelWidth : expectedWidth;
            if (usedWidth >= expectedWidth)
            {
                NativeMethods.GetPixels(txa, (int)which, ref MemoryMarshal.GetReference(dbOut), out int fullFlag);
                return fullFlag == 1;
            }

            float[]? scratch = _txDispScratchPixels;
            if (scratch is null || scratch.Length < usedWidth)
                return false;
            NativeMethods.GetPixels(txa, (int)which, ref scratch[0], out int scratchFlag);
            if (scratchFlag != 1) return false;
            CopyPaddedDisplayPixels(scratch.AsSpan(0, usedWidth), dbOut);
            return true;
        }
    }

    /// <summary>PureSignal feedback panadapter pixels — sourced from the
    /// post-PA loopback IQ pumped through FeedPsFeedbackBlock. Returns false
    /// when the PS-FB analyzer is not open (PS disarmed, or engine disposed;
    /// the analyzer opens on arm even when the TX display analyzer could not —
    /// it inherits RX geometry in that case). Two callers in Tick: the
    /// PS-Monitor path gates on <c>PsEnabled &amp;&amp; PsMonitorEnabled
    /// &amp;&amp; PsCorrecting</c> so a pre-correction transient doesn't show
    /// splatter when the operator has a better source; the keyed LAST-RESORT
    /// path (single-ADC time-mux burst, no TX/RX pixels available) gates only
    /// on <c>PsEnabled</c> — the true post-PA signal, splatter and all, beats
    /// a frozen display.</summary>
    public bool TryGetPsFeedbackDisplayPixels(DisplayPixout which, Span<float> dbOut)
    {
        if (_disposed != 0) return false;
        lock (_psFbDispLock)
        {
            if (!_psFbDispAlive) return false;
            if (_psFbDispId is not int id) return false;
            int expectedWidth = _psFbDispPixelWidth;
            if (dbOut.Length != expectedWidth)
                return false;
            int usedWidth = _psFbDispUsedPixelWidth > 0 ? _psFbDispUsedPixelWidth : expectedWidth;
            if (usedWidth >= expectedWidth)
            {
                NativeMethods.GetPixels(id, (int)which, ref MemoryMarshal.GetReference(dbOut), out int fullFlag);
                return fullFlag == 1;
            }

            float[]? scratch = _psFbDispScratchPixels;
            if (scratch is null || scratch.Length < usedWidth)
                return false;
            NativeMethods.GetPixels(id, (int)which, ref scratch[0], out int scratchFlag);
            if (scratchFlag != 1) return false;
            CopyPaddedDisplayPixels(scratch.AsSpan(0, usedWidth), dbOut);
            return true;
        }
    }

    private static void CopyPaddedDisplayPixels(ReadOnlySpan<float> usedPixels, Span<float> fullPixels)
    {
        float floor = usedPixels[0];
        for (int i = 1; i < usedPixels.Length; i++)
        {
            if (usedPixels[i] < floor)
                floor = usedPixels[i];
        }

        int left = (fullPixels.Length - usedPixels.Length) / 2;
        fullPixels.Slice(0, left).Fill(floor);
        usedPixels.CopyTo(fullPixels.Slice(left, usedPixels.Length));
        fullPixels.Slice(left + usedPixels.Length).Fill(floor);
    }

    private static float[]? PrepareDisplayScratch(float[]? current, int fullWidth, int usedWidth)
    {
        if (usedWidth >= fullWidth)
            return current;
        return current is not null && current.Length >= usedWidth ? current : new float[usedWidth];
    }

    // Power-of-two FFT sizes the TX display analyzer accepts. Mirrors
    // DisplaySettingsStore.TxFftSizes; an out-of-range request snaps to the
    // 16384 default so a malformed value can never reach SetAnalyzer.
    private static int NormalizeTxFftSize(int fftSize) => fftSize switch
    {
        2048 or 4096 or 8192 or 16384 or 32768 or 65536 => fftSize,
        _ => AnalyzerFftSize,
    };

    internal static int NormalizeRxAnalyzerFftSize(int fftSize) => fftSize switch
    {
        // Extended to MaxFftSize so per-channel high-resolution consumers
        // (SetRxDisplayFftSize, wideband detail) can use the full analyzer
        // range XCreateAnalyzer already allocates for.
        2048 or 4096 or 8192 or 16384 or 32768 or 65536 or 131072 or MaxFftSize => fftSize,
        _ => AnalyzerFftSize,
    };

    internal static double NormalizeTxDisplayAvgTauSec(double avgTauSec) =>
        double.IsFinite(avgTauSec) && avgTauSec >= 0.0 && avgTauSec <= 2.0
            ? avgTauSec
            : TxAvgTauSec;

    public void ConfigureTxDisplayAnalyzer(int fftSize, int windowType, double avgTauSec)
    {
        if (_disposed != 0) return;

        // Defense in depth — the store validates too, but the engine is also
        // reachable from tests and future callers. Snap anything invalid to the
        // historical defaults rather than feeding garbage to SetAnalyzer.
        int fft = NormalizeTxFftSize(fftSize);
        int win = (windowType >= 0 && windowType <= 11) ? windowType : AnalyzerWindow;
        double tau = NormalizeTxDisplayAvgTauSec(avgTauSec);

        _txFftSize = fft;
        _txWinType = win;

        // Reconfigure the live TX analyzer in place (when open). SetAnalyzer is
        // serialized against the Spectrum0 feed and GetPixels by _txDispLock,
        // matching SetZoom's reconfig path.
        lock (_txDispLock)
        {
            _txAvgTauSec = tau;
            if (_txDispAlive && _txaChannelId is int txa)
            {
                if (TryConfigureTxAnalyzer(txa, _txaDspRateHz, _txaDspSize, _txDispRxSampleRateHz, _txDispPixelWidth, _txDispZoomLevel, fft, win, AnalyzerKaiserPi, out int usedWidth))
                {
                    _txDispUsedPixelWidth = usedWidth;
                    _txDispScratchPixels = PrepareDisplayScratch(_txDispScratchPixels, _txDispPixelWidth, usedWidth);
                }
                ConfigureDisplayAveragingTau(txa, tau);
            }
        }

        // The PS-feedback analyzer shares the TX display span — keep it in sync
        // so toggling PS-Monitor doesn't change FFT resolution / smoothing.
        lock (_psFbDispLock)
        {
            if (_psFbDispAlive && _psFbDispId is int psFb)
            {
                if (TryConfigureTxAnalyzer(psFb, _psFeedbackRateHz, PsFeedbackBlockSize, _psFbDispRxSampleRateHz, _psFbDispPixelWidth, _psFbDispZoomLevel, fft, win, AnalyzerKaiserPi, out int usedWidth))
                {
                    _psFbDispUsedPixelWidth = usedWidth;
                    _psFbDispScratchPixels = PrepareDisplayScratch(_psFbDispScratchPixels, _psFbDispPixelWidth, usedWidth);
                }
                ConfigureDisplayAveragingTau(psFb, tau);
            }
        }

        _log.LogInformation("wdsp.configureTxDisplay fft={Fft} win={Win} tauMs={Tau:F0}", fft, win, tau * 1000.0);
    }

    public void ResetDisplayPixelBuffers()
    {
        if (_disposed != 0) return;

        foreach (var state in _channels.Values.ToArray())
        {
            lock (state.AnalyzerLock)
            {
                if (state.Stopped) continue;
                NativeMethods.ResetPixelBuffers(state.Id);
                NativeMethods.ResetPixelBuffers(state.SnrAnalyzerId);
                state.AnalyzerHasSnapped = false;
                state.SnrAnalyzerHasSnapped = false;
            }
        }

        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is int txDisp)
        {
            lock (_txDispLock)
            {
                if (_txDispAlive)
                    NativeMethods.ResetPixelBuffers(txDisp);
            }
        }
    }

    public int OpenTxChannel(int outputRateHz = 48_000)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        WdspWisdomInitializer.WaitUntilReady();

        int txaIdForReturn = OpenTxChannelInternal(outputRateHz);

        // If the operator had already toggled monitor on before TXA opened
        // (e.g. on a deferred protocol-2 connect path), open the channel now
        // that the IQ rate is known. EnsureMonitorChannelOpen short-circuits
        // when the channel is already open or the request flag is clear, so
        // calling it here is cheap on the common path. Done outside _txaLock
        // so the monitor lock-acquisition order stays one-way.
        if (_monitorRequested)
        {
            EnsureMonitorChannelOpen();
        }
        return txaIdForReturn;
    }

    private int OpenTxChannelInternal(int outputRateHz)
    {
        lock (_txaLock)
        {
            if (_txaChannelId is int existing) return existing;

            // Pick the TXA profile from the requested output rate. P1's DAC
            // runs at 48 kHz; the G2 on P2 expects 192 kHz. Any other value
            // falls back to the P1 profile (treated as "not a supported P2
            // rate" and keeps us off the air until the connect path specifies
            // one we know about).
            if (outputRateHz == 192_000)
            {
                _txaInSize = TxaInSizeP2;
                _txaDspSize = TxaDspSizeP2;
                _txaOutSize = TxaOutSizeP2;
                _txaInputRateHz = 48_000;
                _txaDspRateHz = 96_000;
                _txaOutputRateHz = 192_000;
                _txaCfirRun = true;
            }
            else
            {
                _txaInSize = TxaInSizeP1;
                _txaDspSize = TxaDspSizeP1;
                _txaOutSize = TxaOutSizeP1;
                _txaInputRateHz = 48_000;
                _txaDspRateHz = 48_000;
                _txaOutputRateHz = 48_000;
                _txaCfirRun = false;
            }

            int id = ReserveNativeSlot();
            bool nativeChannelOpened = false;
            bool txAnalyzerOpened = false;

            try
            {
                // type: 1 (TX), state: 0 (stays quiescent until SetMox). Rates
                // chosen above so P1 keeps its 48/48/48 shape (rated power
                // confirmed on Hermes) and P2 matches pihpsdr transmitter.c's
                // 48/96/192 profile with ratio=4 so the G2 DUC sees samples at
                // its expected 192 kHz clock.
                RunNativeLifecycleCriticalSection(() => NativeMethods.OpenChannel(
                        channel: id,
                        in_size: _txaInSize,
                        dsp_size: _txaDspSize,
                        input_samplerate: _txaInputRateHz,
                        dsp_rate: _txaDspRateHz,
                        output_samplerate: _txaOutputRateHz,
                        type: 1,
                        state: 0,
                        tdelayup: 0.010,
                        tslewup: 0.025,
                        tdelaydown: 0.0,
                        tslewdown: 0.010,
                        bfo: 1));
                nativeChannelOpened = true;

                // SSB USB default + 150-2850 passband: wider than the classic SSB
                // 300-2700 to keep low-frequency voice energy through the chain
                // (task C.0 spec). Phase-C mic ingest drives fexchange2 once
                // SetMox(true) flips the TXA state to 1; until then the TXA sits
                // at state=0 and consumes nothing.
                _txControlNative.SetTXAMode(id, (int)RxaMode.USB);
                _txCurrentMode = RxaMode.USB;
                // Default passband matches the stock SSB TX width. DspPipelineService
                // re-asserts this from the live StateDto (TxFilterLowHz/HighHz)
                // immediately after OpenTxChannel so operator-edited widths survive
                // a protocol switch / engine reopen.
                NativeMethods.SetTXABandpassFreqs(id, 150.0, 2850.0);
                NativeMethods.SetTXABandpassWindow(id, 1);
                // Intentionally NOT calling SetTXABandpassRun(id, 1): despite the
                // name it sets bp1.run (the compressor-only aux bandpass), not bp0,
                // and bp1 ships with stale LSB-direction coefs that reject the USB
                // mic on first MOX — that's the "TX 0 W until mode toggle" symptom
                // the operator saw return after this branch first restored the call.
                // bp0 is always on from create_bandpass; nothing to enable here.
                NativeMethods.SetTXAPanelRun(id, 1);
                NativeMethods.SetTXAPanelGain1(id, 1.0);
                // pihpsdr transmitter.c:1298 routes mic to both I and Q via
                // PanelSelect=2 ("Mic I sample"). Without this, WDSP's default
                // may leave Q unassigned, allowing a secondary signal path to
                // leak into the TXA output.
                NativeMethods.SetTXAPanelSelect(id, 2);

                // Explicitly disable the PreGen stage and zero its state — pihpsdr
                // transmitter.c:1293-1296 does this on every TXA open. WDSP's
                // create_channel does not guarantee these defaults, and a residual
                // non-zero PreGen tone shows up alongside the PostGen tune carrier
                // as a second discrete frequency on the air (reported as
                // "2-tone-like output" during TUN on the G2 MkII).
                NativeMethods.SetTXAPreGenMode(id, 0);
                NativeMethods.SetTXAPreGenToneMag(id, 0.0);
                NativeMethods.SetTXAPreGenToneFreq(id, 0.0);
                NativeMethods.SetTXAPreGenRun(id, 0);

                // Clamp PostGen off at open time too — TUN will re-enable it via
                // SetTxTune. Same rationale: WDSP state from a previous channel
                // open can leak through if we don't zero it.
                NativeMethods.SetTXAPostGenRun(id, 0);

                // Explicit clean-slate TX chain state. WDSP initializes these
                // "off" at channel-create, but asserting them makes the baseline
                // deterministic and independent of the library build. Leveler is
                // Thetis-factory-ON (radio.cs:3018 tx_leveler_on = true) and is
                // enabled here to match that default — a disabled Leveler stage
                // also leaves GetTXAMeter(LVLR_PK) stuck at WDSP's -400 silence
                // sentinel, which made the frontend LVLR bar look broken. Other
                // optional stages (Compressor, CFC, PHROT, EQ, AMSQ) remain OFF
                // until they're wired to operator UI and tuned — enabling them
                // with library-default parameters can mask or create distortion.
                // ALC stays on (see SetTXAALCSt below; never 0). AMSQ is the mic
                // noise gate and shouldn't shape SSB audio. CESSB (osctrl)
                // defaults OFF and is enabled only by persisted operator state.
                // ALC run state — MUST stay ON (never 0). Disabling it silences the
                // SSB modulator (NativeMethods.SetTXAALCSt warning). ApplyTxLeveling
                // below sets the ALC max-gain/decay but deliberately never touches
                // this St; assert it here so the baseline is unambiguous.
                NativeMethods.SetTXAALCSt(id, 1);
                // ALC attack — not part of the operator TxLevelingConfig (Thetis
                // doesn't surface it). 1 ms matches both pihpsdr
                // (transmitter.c:1290) and the WDSP factory Thetis inherits
                // (TXA.c:319). A slower 2 ms attack missed plosive onset and the
                // follow-up ALC chop sounded "brittle." Set once at open.
                NativeMethods.SetTXAALCAttack(id, 1);
                // Leveler max-gain default. WDSP's create_wcpagc ships with
                // max_gain = 1.778 linear (≈ +5 dB) at TXA.c:169; we assert the
                // value explicitly so the baseline stays deterministic and the
                // init log confirms what the Leveler's headroom is set to.
                // +5 dB matches the W1AEX / softerhardware community default
                // (milder than Thetis's +15 dB stock — see task #13 notes).
                // Operator-settable at runtime via POST /api/tx/leveler-max-gain;
                // intentionally NOT part of TxLevelingConfig.
                NativeMethods.SetTXALevelerTop(id, DefaultLevelerMaxGainDb);
                // ALC max-gain/decay, Leveler St/decay, Compressor run/gain all come
                // from the TxLevelingConfig defaults so a fresh channel and a runtime
                // SetTxLeveling use identical WDSP calls. Defaults: ALC 3 dB/10 ms
                // (Thetis database.cs:4596 + TXA.c attack/decay), Leveler ON/100 ms
                // (radio.cs:3018 tx_leveler_on + radio.cs leveler decay 100),
                // Compressor OFF/0 dB. DspPipelineService force-applies the operator's
                // persisted config on top via SetTxLeveling at channel open.
                ApplyTxLevelingLocked(id, new TxLevelingConfig());
                _txControlNative.SetTXACFCOMPRun(id, 0);
                ApplyTxPhaseRotatorLocked(id, new TxPhaseRotatorConfig());
                // ApplyTxLevelingLocked above also asserted the default-off
                // CESSB state after configuring the compressor.
                NativeMethods.SetTXAEQRun(id, 0);
                NativeMethods.SetTXAAMSQRun(id, 0);

                // CFIR compensates the sinc droop introduced by the TXA upsample
                // to the output rate. Thetis (audio.cs:1808) turns it ON for P2,
                // OFF for P1; pihpsdr (transmitter.c:1288) does the same. Wiring
                // this on P1 would over-correct the flat 48k chain and tilt the
                // passband, so it's conditional on the P2 profile.
                if (_txaCfirRun)
                {
                    NativeMethods.SetTXACFIRRun(id, 1);
                }

                _txaChannelId = id;
                _txaNativeOwned = true;

                // PureSignal seed. The TXA channel already owns `calcc.p` and
                // `iqc.p0/p1` as a side effect of create_txa() (TXA.c:405,424);
                // these setters tune the WDSP state machine to safe defaults so
                // arming PS later just needs SetPSRunCal(1) + SetPSControl mode-on.
                //
                // HW-peak is *not* set here — RadioService.SetPsHwPeak runs after
                // discovery so the right value (P1=0.4072 / G2=0.6121 / ANAN-7000
                // =0.2899) is applied per actual connected radio. The 0.4072 in
                // `_psHwPeak` is just a neutral default.
                //
                // See `docs/lessons/wdsp-init-gotchas.md`: setters before state-
                // flip is the load-bearing pattern. PS setters are independent of
                // `SetChannelState`, so they're safe to run unconditionally at
                // TXA open time.
                NativeMethods.SetPSFeedbackRate(id, _psFeedbackRateHz);
                NativeMethods.SetPSMoxDelay(id, _psMoxDelaySec);
                NativeMethods.SetPSLoopDelay(id, _psLoopDelaySec);
                _psAmpDelayNs = ClampPsAmpDelayNs(_psAmpDelayNs, _psFeedbackRateHz);
                _ = NativeMethods.SetPSTXDelay(id, _psAmpDelayNs * 1e-9);
                NativeMethods.SetPSHWPeak(id, _psHwPeak);
                NativeMethods.SetPSControl(id, 1, 0, 0, 0);   // RESET state
                                                              // SetPSRunCal stays 0 until the operator arms PS.
                                                              // Bring-up diagnostic — drop once PS is confirmed stable on rack.
                _log.LogInformation(
                    "wdsp.psSeed hwPeak={Peak:F4} feedbackRate={FbRate}",
                    _psHwPeak, _psFeedbackRateHz);

                // TX panadapter analyzer — issue #81. Match the first RXA's pixel
                // width and zoom so the TX trace renders into the same widget
                // without a span change on MOX. If no RXA exists yet (shouldn't
                // happen in practice — RadioService opens RX before TX), skip
                // analyzer creation and leave _txDispAlive false so the server
                // falls back to the RX pixels during MOX.
                int rxPixelWidth = 0;
                int rxSampleRateHz = 0;
                int rxZoom = 1;
                if (TrySnapshotRxDisplayGeometry(out rxPixelWidth, out rxSampleRateHz, out rxZoom))
                {
                    // Analyzer disp index reuses the TXA channel id — WDSP keeps
                    // channels and analyzers in separate arrays so the collision
                    // between RXA's channel=0 / disp=0 and TXA's channel=id /
                    // disp=id is purely in our bookkeeping, not in the library.
                    NativeMethods.XCreateAnalyzer(id, out int txRc, MaxFftSize, 1, 1, null);
                    if (txRc == 0)
                    {
                        txAnalyzerOpened = true;
                        bool configured;
                        lock (_txDispLock)
                        {
                            _txDispPixelWidth = rxPixelWidth;
                            _txDispZoomLevel = rxZoom;
                            _txDispRxSampleRateHz = rxSampleRateHz;
                            // Configure for the SIPHON tap point (xsiphon position
                            // in xtxa — BEFORE iqc/cfir/rsmpout). dsp_rate / dsp_size
                            // describe the IQ at that stage. Pulling pre-iqc samples
                            // gives the operator's clean voice spectrum on the
                            // panadapter, matching Thetis (cmaster.cs:544-545,
                            // TXA.c:586). Pre-fix the analyzer was configured at
                            // the OUTPUT (post-cfir/rsmpout) rate and got fed the
                            // predistorted IQ — cosmetically dirty by design.
                            configured = TryConfigureTxAnalyzer(id, _txaDspRateHz, _txaDspSize, rxSampleRateHz, rxPixelWidth, rxZoom, _txFftSize, _txWinType, AnalyzerKaiserPi, out int usedWidth);
                            if (configured)
                            {
                                _txDispUsedPixelWidth = usedWidth;
                                _txDispScratchPixels = PrepareDisplayScratch(_txDispScratchPixels, rxPixelWidth, usedWidth);
                                ConfigureDisplayAveragingTau(id, _txAvgTauSec);
                                _txDispAlive = true;
                            }
                        }
                        if (!configured)
                        {
                            // Rate relationship doesn't support bin-clip (e.g. TX narrower
                            // than RX, or non-integer ratio). Destroy the unused analyzer
                            // slot and leave _txDispAlive false so the panadapter falls
                            // back to the RX analyzer on MOX.
                            RunNativeLifecycleCriticalSection(() => NativeMethods.DestroyAnalyzer(id));
                            txAnalyzerOpened = false;
                            // Log the DSP rate — that's what the rate rule actually
                            // compares (the analyzer taps the SIPHON at dsp_rate,
                            // not the output rate), so at RX 192k this reads
                            // "tx=96000", making the failed 96k-vs-192k relation
                            // visible instead of a baffling "192000 vs 192000".
                            _log.LogWarning(
                                "wdsp.openTxChannel tx-analyzer skipped — rx={RxRate} txDsp={TxDspRate} not an integer multiple in either direction; panadapter will fall back to RX trace (and to PS-feedback pixels while keyed with PS armed)",
                                rxSampleRateHz, _txaDspRateHz);
                        }
                    }
                    else
                    {
                        _log.LogWarning(
                            "wdsp.openTxChannel tx-analyzer XCreateAnalyzer rc={Rc} — TX panadapter will fall back to RX trace",
                            txRc);
                    }
                }

                _log.LogInformation(
                    "wdsp.openTxChannel id={Id} rates={InRate}/{DspRate}/{OutRate} sizes={InSz}/{OutSz} cfir={Cfir} chain=[alc=1 lvlr=1 lvlrMax={LvlrMax:F1}dB cpdr=0 cfc=0 phrot=0 osctrl=0 eq=0 amsq=0] bp=150..2850 panelGain=1.0 txDisp={TxDisp}(pix={Pix} rxRate={RxRate} txRate={TxRate} zoom={Zoom})",
                    id, _txaInputRateHz, _txaDspRateHz, _txaOutputRateHz,
                    _txaInSize, _txaOutSize, _txaCfirRun ? 1 : 0, DefaultLevelerMaxGainDb,
                    _txDispAlive ? "on" : "off", _txDispPixelWidth, _txDispRxSampleRateHz, _txaOutputRateHz, _txDispZoomLevel);

                // The expander's block size and rate come from the TX
                // channel, so a re-open with a different profile has to
                // rebuild it. No-op when nothing has changed.
                lock (_dexpLock)
                {
                    EnsureDexpLocked(_txaInSize, _txaInputRateHz);
                    if (_dexpCreated)
                    {
                        ApplyDexpLocked(_dexpConfig);
                        _dexpActive = _dexpConfig.Enabled;
                    }
                }
                return id;
            }
            catch
            {
                lock (_txDispLock)
                {
                    if (txAnalyzerOpened)
                    {
                        RunNativeLifecycleCriticalSection(() =>
                            TryCleanupNativeResource(id, nameof(NativeMethods.DestroyAnalyzer), NativeMethods.DestroyAnalyzer));
                        txAnalyzerOpened = false;
                    }
                    _txDispAlive = false;
                    _txDispPixelWidth = 0;
                    _txDispUsedPixelWidth = 0;
                    _txDispRxSampleRateHz = 0;
                    _txDispZoomLevel = 1;
                    _txDispScratchPixels = null;
                }

                if (_txaChannelId == id)
                {
                    _txaChannelId = null;
                    _txaNativeOwned = false;
                }
                // Failed TX open means no TXA channel exists, so keyed state must match engine reality.
                _txaRunning = false;
                _moxOn = false;
                lock (_txMeterPublishLock) { _latestTxStageMeters = null; }

                ReleaseFailedNativeOpen(id, nativeChannelOpened, analyzerOpened: false);
                throw;
            }
        }
    }

    public void SetMox(bool moxOn)
    {
        bool stopRxForPureSignal;
        lock (_psLock)
            stopRxForPureSignal = _psEnabled;
        SetMox(moxOn, stopRxForPureSignal);
    }

    public void SetMox(bool moxOn, bool stopRxForPureSignal)
    {
        if (_disposed != 0) return;

        int txaId;
        int rxaId;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            txaId = txa;

            // MOX acts on RX1 only. The monitor RXA and RX2+ are also present in
            // _channels, so dictionary enumeration cannot identify RX1.
            rxaId = Volatile.Read(ref _primaryRxaChannelId);
            if (rxaId < 0
                || !_channels.TryGetValue(rxaId, out var primary)
                || primary.Stopped)
                return;
        }

        // Display-DUP keeps RXA running through ordinary MOX so its analyzer
        // and averaging history remain continuous. RX audio is suppressed by
        // DspPipelineService after it drains the live RXA output. PureSignal
        // retains the established RXA damp/down transition because its keyed
        // feedback routing is intentionally unchanged.
        //
        // TX-monitor wrinkle: when MOX falls but monitor is on, TXA must
        // stay running so fexchange2 keeps producing IQ for the monitor
        // demod path. We re-derive TXA target = (MOX || monitor) so the
        // monitor path doesn't go silent when the operator releases MOX.
        int rxaPrior = -1, txaPrior = -1;
        bool wantTxa = moxOn || _monitorRequested;
        if (moxOn)
        {
            _moxOn = true;
            if (stopRxForPureSignal)
            {
                rxaPrior = NativeMethods.SetChannelState(rxaId, 0, 1);
                _rxaStoppedForCurrentMox = true;
            }
            else
            {
                _rxaStoppedForCurrentMox = false;
            }
            if (!_txaRunning)
            {
                txaPrior = NativeMethods.SetChannelState(txaId, 1, 0);
                _txaRunning = true;
            }
            // No priming: Thetis (console.cs:31375) does not prime — bfo=1
            // semantics already make the first fexchange wait for real output.
            // PureSignal's MOX flag is asserted separately after the radio wire
            // MOX bit goes high, so calcc's MOX-delay timer is measured from
            // the hardware keying edge instead of from TXA warm-up.
        }
        else
        {
            _moxOn = false;
            // Drop the PS MOX flag *before* the TXA state-flip so the iqc
            // stage sees "no longer transmitting" while the chain is still
            // alive — same ordering pihpsdr uses (transmitter.c:2422-2444).
            NativeMethods.SetPSMox(txaId, 0);
            // Only damp TXA if the preview path doesn't need it. When
            // monitor is on, TXA stays at state=1 so the chain keeps
            // producing IQ to be demodulated by the monitor RXA channel.
            if (_txaRunning && !wantTxa)
            {
                txaPrior = NativeMethods.SetChannelState(txaId, 0, 1);
                _txaRunning = false;
            }
            if (_rxaStoppedForCurrentMox)
            {
                rxaPrior = NativeMethods.SetChannelState(rxaId, 1, 0);
                _rxaStoppedForCurrentMox = false;
                // PERF_PASS_3_DEBUG: t2 — WDSP RXA brought back up. Uncommitted.
                _log.LogInformation("wdsp.rxa.up ts={Ts}",
                    System.Diagnostics.Stopwatch.GetTimestamp());
            }
            // Unkeying: clear the stage-meter snapshot so UI doesn't latch the
            // last-during-TX reading while idle. The next MOX-on will publish
            // fresh data on its first ProcessTxBlock.
            lock (_txMeterPublishLock) { _latestTxStageMeters = null; }
        }
        // Diagnostic 2026-04-18: capture the prior-state return of every
        // SetChannelState call so we can detect cases where the requested
        // transition was a no-op (prior == new) — that's the failure mode that
        // looks like "RX audio doesn't come back after MOX-off".
        _log.LogInformation(
            "wdsp.setMox on={Mox} rxa={Rxa} (prior {RxaPrior}) txa={Txa} (prior {TxaPrior})",
            moxOn, rxaId, rxaPrior, txaId, txaPrior);
    }

    public void SetPsMox(bool moxOn)
    {
        if (_disposed != 0) return;
        lock (_psLock)
        {
            int? txa;
            lock (_txaLock) txa = _txaChannelId;
            if (txa is int txaId)
                NativeMethods.SetPSMox(txaId, moxOn ? 1 : 0);
        }
        _log.LogInformation("wdsp.setPsMox on={Mox}", moxOn);
    }

    public TxStageMeters GetTxStageMeters()
    {
        lock (_txMeterPublishLock)
        {
            return _latestTxStageMeters ?? TxStageMeters.Silent;
        }
    }

    public int TxBlockSamples => _txaInSize;
    public int TxOutputSamples => _txaOutSize;

    public void SetTxPanelGain(double linearGain)
    {
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            NativeMethods.SetTXAPanelGain1(txa, linearGain);
        }
        _log.LogInformation("wdsp.setTxPanelGain linear={Gain:F3}", linearGain);
    }

    /// <summary>
    /// Pre-WDSP TX-audio plugin hook. Implementer (Zeus.Server.Hosting's
    /// <c>AudioPluginBridge</c>) wraps the host's audio-plugin chain and
    /// installs this delegate at startup. Pass <c>null</c> to detach.
    /// Volatile single-pointer read on the audio thread; no virtual
    /// dispatch into Zeus.Plugins.Host from Zeus.Dsp.
    /// </summary>
    private volatile TxAudioPluginHandler? _txAudioPluginHandler;

    /// <summary>Install / detach the realtime TX-audio plugin handler. Safe to call
    /// from any thread; the audio thread sees the new value on its next block.</summary>
    public void SetTxAudioPluginHandler(TxAudioPluginHandler? handler)
        => _txAudioPluginHandler = handler;

    /// <summary>True iff a handler is currently installed. Used by Zeus.Server.Hosting
    /// to surface "audio plugin active" in <c>/api/capabilities</c>.</summary>
    public bool HasTxAudioPluginHandler => _txAudioPluginHandler is not null;

    public void SetTxLevelerMaxGain(double maxGainDb)
    {
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            NativeMethods.SetTXALevelerTop(txa, maxGainDb);
        }
        _log.LogInformation("wdsp.setTxLevelerMaxGain dB={Db:F1}", maxGainDb);
    }

    // TX leveling (Thetis parity §6.1-6.3): ALC max-gain + decay, Leveler
    // on/off + decay, Compressor on/off + gain. The TXA channel is a singleton
    // (_txaChannelId), so the IDspEngine channelId arg is accepted for interface
    // parity but the TXA channel is what we drive — same convention as
    // SetTxLevelerMaxGain. The Leveler MAX-GAIN ("top") is intentionally NOT
    // touched here — it stays on SetTxLevelerMaxGain. ALC St is NEVER touched
    // (it stays at the init St=1; disabling it silences the SSB modulator).
    public void SetTxLeveling(int channelId, TxLevelingConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            ApplyTxLevelingLocked(txa, cfg);
        }
        _log.LogInformation(
            "wdsp.setTxLeveling alcMaxGainDb={Alc:F1} alcDecayMs={AlcDecay} levelerEnabled={Lvlr} levelerDecayMs={LvlrDecay} compEnabled={Comp} compGainDb={CompGain:F1} cessbEnabled={Cessb} cessbBandwidthHz={CessbBandwidth}",
            cfg.AlcMaxGainDb, cfg.AlcDecayMs, cfg.LevelerEnabled, cfg.LevelerDecayMs,
            cfg.CompressorEnabled, cfg.CompressorGainDb, cfg.CessbEnabled,
            cfg.CessbBandwidthHz);
    }

    // Shared apply for SetTxLeveling and the TXA-open init block so a fresh
    // channel and a runtime change use identical WDSP calls for the same config.
    // Caller holds _txaLock. NEVER touches SetTXAALCSt — ALC run stays ON
    // (init St=1) or the SSB modulator emits zero IQ. Records the operator's
    // Leveler on/off so the TUN / two-tone restore re-arms it correctly.
    private void ApplyTxLevelingLocked(int txa, TxLevelingConfig cfg)
    {
        NativeMethods.SetTXAALCMaxGain(txa, cfg.AlcMaxGainDb);
        NativeMethods.SetTXAALCDecay(txa, cfg.AlcDecayMs);
        // While keyed for TUN/two-tone the Leveler is forced off; don't let a
        // mid-key settings change re-enable it on the tune tone. Still record the
        // operator's intent below so the un-key restore lands correctly.
        _txControlNative.SetTXALevelerSt(
            txa,
            (!_txLevelerForcedOff && !_txRogerBeepBypass
                && !_txInjectedAudioBypass && cfg.LevelerEnabled) ? 1 : 0);
        NativeMethods.SetTXALevelerDecay(txa, cfg.LevelerDecayMs);
        _txCompressorEnabled = cfg.CompressorEnabled;
        NativeMethods.SetTXACompressorGain(txa, cfg.CompressorGainDb);
        _txCessbEnabled = cfg.CessbEnabled;
        _txCessbBandwidthHz = cfg.CessbBandwidthHz is 4000 ? 4000 : 3000;
        TrySetCessbBandwidth(txa, _txCessbBandwidthHz);
        ApplyTxCompressorAndCessbRuns(txa);
        _txLevelerEnabled = cfg.LevelerEnabled;
    }

    // TX phase rotator (Thetis DSP->CFC->PhaseRot parity): all-pass speech
    // phase redistribution plus an explicit mic-polarity reverse flag. Reverse
    // is applied by WDSP before the `run` branch, so it remains meaningful even
    // when the all-pass rotation itself is disabled.
    public void SetTxPhaseRotator(int channelId, TxPhaseRotatorConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            ApplyTxPhaseRotatorLocked(txa, cfg);
        }
        _log.LogInformation(
            "wdsp.setTxPhaseRotator enabled={Enabled} cornerHz={Corner} stages={Stages} reverse={Reverse} autoMode={AutoMode}",
            cfg.Enabled, cfg.CornerHz, cfg.Stages, cfg.Reverse, cfg.AutoMode);
    }

    public void ResetTxPhaseRotatorAuto(int channelId)
    {
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            NativeMethods.SetTXAPHROTAutoReset(txa);
        }
        _log.LogInformation("wdsp.resetTxPhaseRotatorAuto");
    }

    public TxPhaseRotatorAsymmetry? GetTxPhaseRotatorAsymmetry(int channelId)
    {
        if (_disposed != 0) return null;
        double inPos, inNeg, inRatio, outPos, outNeg, outRatio, currentFc, autoStep;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return null;
            NativeMethods.GetTXAPHROTAsymmetry(
                txa,
                out inPos,
                out inNeg,
                out inRatio,
                out outPos,
                out outNeg,
                out outRatio,
                out currentFc,
                out autoStep);
        }
        _log.LogDebug(
            "wdsp.txPhaseRotatorAsymmetry raw inPos={InPos:F6} inNeg={InNeg:F6} inRatio={InRatio:F6} outPos={OutPos:F6} outNeg={OutNeg:F6} outRatio={OutRatio:F6} fc={Fc:F1} autoStep={AutoStep:F1}",
            inPos, inNeg, inRatio, outPos, outNeg, outRatio, currentFc, autoStep);
        return new TxPhaseRotatorAsymmetry(
            PeakToDb(inPos),
            PeakToDb(inNeg),
            RatioToPercent(inRatio),
            PeakToDb(outPos),
            PeakToDb(outNeg),
            RatioToPercent(outRatio),
            currentFc,
            autoStep);
    }

    private static bool IsDigitalTxMode(RxaMode mode) =>
        mode is RxaMode.DIGU or RxaMode.DIGL;

    private int EffectiveTxRun(bool configuredEnabled) =>
        configuredEnabled
        && !IsDigitalTxMode(_txCurrentMode)
        && !_txDigitalBypass
        && !_txRogerBeepBypass
        && !_txInjectedAudioBypass
            ? 1
            : 0;

    // Caller holds _txaLock. Set shape before Run so enabling from OFF never
    // exposes a partial phase-rotator profile to a live TXA block.
    private void ApplyTxPhaseRotatorLocked(int txa, TxPhaseRotatorConfig cfg)
    {
        _txPhaseRotatorConfig = cfg;
        _txControlNative.SetTXAPHROTReverse(txa, cfg.Reverse ? 1 : 0);
        _txControlNative.SetTXAPHROTCorner(txa, cfg.CornerHz);
        _txControlNative.SetTXAPHROTNstages(txa, cfg.Stages);
        _txControlNative.SetTXAPHROTAutoMode(txa, cfg.AutoMode ? 1 : 0);
        _txControlNative.SetTXAPHROTRun(txa, EffectiveTxRun(cfg.Enabled));
    }

    private static double PeakToDb(double peak) =>
        peak > 0.0 && double.IsFinite(peak)
            ? Math.Max(-120.0, 20.0 * Math.Log10(peak))
            : -120.0;

    private static double RatioToPercent(double ratio) =>
        double.IsFinite(ratio) ? Math.Clamp(ratio * 100.0, 0.0, 100.0) : 0.0;

    // Caller holds _txaLock. Digital-mode bypass gates only the CFC master;
    // profile/scalar/post-EQ settings stay exactly as the operator configured.
    private void ApplyCfcMasterRunLocked(int txa, CfcConfig cfg) =>
        _txControlNative.SetTXACFCOMPRun(txa, EffectiveTxRun(cfg.Enabled));

    // Apply the coupled COMP -> CESSB topology in a transition-safe order.
    // When bypassing, osctrl goes OFF before COMP so no block can see standalone
    // CESSB. When enabling, COMP goes ON first and osctrl follows. Shape/gain
    // settings remain untouched. Callers may hold either _txaLock or _psLock;
    // taking _txaLock here makes the effective-state snapshot and ordered
    // native writes atomic with respect to concurrent TX-settings changes.
    private void ApplyTxCompressorAndCessbRuns(int txa)
    {
        lock (_txaLock)
        {
            int compressorRun = EffectiveTxRun(_txCompressorEnabled);
            int cessbRun = compressorRun != 0 && _txCessbEnabled ? 1 : 0;
            RunNativeLifecycleCriticalSection(() =>
            {
                if (cessbRun == 0)
                {
                    _txControlNative.SetTXAosctrlRun(txa, 0);
                    _txControlNative.SetTXACompressorRun(txa, compressorRun);
                }
                else
                {
                    _txControlNative.SetTXACompressorRun(txa, 1);
                    _txControlNative.SetTXAosctrlRun(txa, 1);
                }
            });
        }
    }

    private void TrySetCessbBandwidth(int txa, int bandwidthHz)
    {
        // Every pre-feature libwdsp already creates osctrl at 3000 Hz. Avoid
        // probing the optional export for that baseline so old binaries remain
        // fully compatible. Once a successful 4 kHz write proves the export is
        // present, a later change back to 3 kHz must be forwarded normally.
        int availability = Volatile.Read(ref _cessbBandwidthExportState);
        if (availability < 0 || (availability == 0 && bandwidthHz == 3000))
            return;

        try
        {
            _txControlNative.SetTXAosctrlBandwidth(txa, bandwidthHz);
            Volatile.Write(ref _cessbBandwidthExportState, 1);
        }
        catch (EntryPointNotFoundException ex)
        {
            if (Interlocked.Exchange(ref _cessbBandwidthExportState, -1) >= 0)
            {
                _log.LogWarning(
                    "wdsp.cessb.bandwidth.unavailable requestedHz={Bandwidth} reason=\"libwdsp does not export SetTXAosctrlBandwidth; using native 3000 Hz default\" detail={Message}",
                    bandwidthHz, ex.Message);
            }
        }
    }

    public void SetTxTune(bool on)
    {
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            // Thetis console.cs:31806-31829 (chkTUN_CheckedChanged, non-pulse
            // branch): mode=0 single tone at ±cw_pitch offset, mag=MAX_TONE_MAG.
            // wdsp/gen.c:221-241: mode 0 = tone, mode 1 = two-tone (summed).
            // Two-tone produces a difference-frequency beat envelope, which
            // shows up on the forward-power meter as jitter — that's why the
            // old mode=1 reading "jumped like Parkinson's". The tone is offset
            // by cw_pitch so it lands in the TXA sideband passband set by
            // ApplyTxBandpassForMode (a 0 Hz tone sits on the suppressed-carrier
            // null for SSB). Sign mirrors Thetis's sideband rule.
            if (on)
            {
                // CW TX parks the hardware NCO one pitch away from the dial,
                // so the tune excitation must cancel that offset. Non-CW
                // modes already park the NCO on the dial and remain zero-beat.
                // Thetis console.cs chkTUN_CheckedChanged uses the same signed
                // CW pitch with PostGen mode 0 (single tone).
                double toneFreq = _txCurrentMode switch
                {
                    RxaMode.CWL => -CwDefaults.PitchHz,
                    RxaMode.CWU => +CwDefaults.PitchHz,
                    _ => 0.0,
                };
                const double toneMag = 0.99999;
                _txControlNative.SetTXAPostGenMode(txa, 0);
                _txControlNative.SetTXAPostGenToneFreq(txa, toneFreq);
                _txControlNative.SetTXAPostGenToneMag(txa, toneMag);
                _txControlNative.SetTXAPostGenRun(txa, 1);
                // Disable Leveler while TUN is keyed. pihpsdr sidesteps the
                // AGC-pumping AM envelope by keeping Leveler off
                // (transmitter.c:2612 — state = compressor||cfc, both off
                // on tune). We restore Leveler on TUN-off so mic MOX keeps
                // its current Thetis-matching behavior.
                _txLevelerForcedOff = true;
                _txControlNative.SetTXALevelerSt(txa, 0);
                _log.LogInformation("wdsp.setTxTune on=true mode=singletone freq={Freq:F0} mag={Mag:F5} leveler=off", toneFreq, toneMag);
            }
            else
            {
                _txControlNative.SetTXAPostGenRun(txa, 0);
                // Restore the Leveler to the operator's setting, not a hardcoded
                // "on" — if the operator disabled the Leveler via SetTxLeveling,
                // un-keying TUN must leave it disabled.
                _txLevelerForcedOff = false;
                _txControlNative.SetTXALevelerSt(
                    txa,
                    (_txLevelerEnabled && !_txRogerBeepBypass
                        && !_txInjectedAudioBypass) ? 1 : 0);
                _log.LogInformation("wdsp.setTxTune on=false leveler={Leveler}",
                    _txLevelerEnabled ? "on" : "off");
            }
        }
    }

    public void SetTxMode(RxMode mode)
    {
        if (_disposed != 0) return;
        var mapped = MapMode(mode);
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            RunNativeLifecycleCriticalSection(() => _txControlNative.SetTXAMode(txa, (int)mapped));
            _txCurrentMode = mapped;
            ApplyTxPhaseRotatorLocked(txa, _txPhaseRotatorConfig);
            ApplyCfcMasterRunLocked(txa, _cfcConfig);
            ApplyTxCompressorAndCessbRuns(txa);
            // TXA bandpass is now operator-controlled — DspPipelineService
            // asserts SetTxFilter after SetTxMode using the per-mode-family
            // memory in RadioService. No auto-apply here.
            //
            // TwoTone is sideband-sensitive. If a TwoTone test is mid-flight
            // when the operator changes mode, re-assert PostGen freqs with the
            // new sign so the tones stay inside the displayed bandpass. Sign
            // convention matches Thetis (Setup.cs:11096): negate for LSB-family,
            // positive for USB-family. Mag and run flag stay as last set.
            if (_twoToneArmed)
            {
                bool lsbFamily = mapped == RxaMode.LSB
                              || mapped == RxaMode.CWL
                              || mapped == RxaMode.DIGL;
                double signedF1 = lsbFamily ? -_twoToneF1Hz : _twoToneF1Hz;
                double signedF2 = lsbFamily ? -_twoToneF2Hz : _twoToneF2Hz;
                NativeMethods.SetTXAPostGenTTFreq(txa, signedF1, signedF2);
                _log.LogInformation(
                    "wdsp.setTxMode twoTone re-signed f1={F1} f2={F2} signedF1={SF1} signedF2={SF2} mode={Mode}",
                    _twoToneF1Hz, _twoToneF2Hz, signedF1, signedF2, mapped);
            }
        }
        // Mirror the mode onto the monitor channel so the preview demodulates
        // with the same sideband / modulation as the on-air signal.
        lock (_monitorLock)
        {
            _monitorMode = mapped;
            if (_monitorChannelId is int monId)
            {
                SetMode(monId, mode);
                ApplyTxMonitorAgc(monId, mapped);
            }
        }
        _log.LogInformation("wdsp.setTxMode mode={Mode}", mapped);
    }

    public void SetTxAmCarrierLevel(double carrierLevel)
    {
        if (_disposed != 0) return;
        if (!double.IsFinite(carrierLevel))
            throw new ArgumentOutOfRangeException(nameof(carrierLevel));
        double clamped = Math.Clamp(carrierLevel, 0.0, 0.5 * Math.Sqrt(1.25));
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            _txControlNative.SetTXAAMCarrierLevel(txa, clamped);
        }
        _log.LogInformation("wdsp.setTxAmCarrierLevel level={Level:F3}", clamped);
    }

    public void SetTxDigitalBypass(bool bypass)
    {
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txDigitalBypass == bypass) return;
            _txDigitalBypass = bypass;
            if (_txaChannelId is int txa)
            {
                ApplyTxPhaseRotatorLocked(txa, _txPhaseRotatorConfig);
                ApplyCfcMasterRunLocked(txa, _cfcConfig);
                ApplyTxCompressorAndCessbRuns(txa);
            }
        }
        _log.LogInformation("wdsp.setTxDigitalBypass bypass={Bypass}", bypass);
    }

    public void SetTxInjectedAudioBypass(bool bypass)
    {
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txInjectedAudioBypass == bypass) return;
            _txInjectedAudioBypass = bypass;
            if (_txaChannelId is int txa)
            {
                ApplyTxPhaseRotatorLocked(txa, _txPhaseRotatorConfig);
                ApplyCfcMasterRunLocked(txa, _cfcConfig);
                ApplyTxCompressorAndCessbRuns(txa);
                _txControlNative.SetTXALevelerSt(
                    txa,
                    (_txLevelerEnabled && !_txLevelerForcedOff
                        && !_txRogerBeepBypass && !bypass) ? 1 : 0);
            }
        }
        _log.LogInformation("wdsp.setTxInjectedAudioBypass bypass={Bypass}", bypass);
    }

    public void SetTxRogerBeepBypass(bool bypass)
    {
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txRogerBeepBypass == bypass) return;
            _txRogerBeepBypass = bypass;
            if (_txaChannelId is int txa)
            {
                ApplyTxPhaseRotatorLocked(txa, _txPhaseRotatorConfig);
                ApplyCfcMasterRunLocked(txa, _cfcConfig);
                ApplyTxCompressorAndCessbRuns(txa);
                _txControlNative.SetTXALevelerSt(
                    txa,
                    (_txLevelerEnabled && !_txLevelerForcedOff
                        && !_txInjectedAudioBypass && !bypass) ? 1 : 0);
            }
        }
        _log.LogInformation("wdsp.setTxRogerBeepBypass bypass={Bypass}", bypass);
    }

    public void SetTxFilter(int lowHz, int highHz)
    {
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            // Same zero-width guard as the RX path (issue #1028): TX passes its
            // signed pair straight to WDSP with no abs-fold, so a degenerate
            // width would silence the on-air signal. The monitor mirror below
            // routes through SetFilter -> ApplyBandpassForMode, which floors on
            // its own.
            var (txLow, txHigh) = FloorPassbandWidth(lowHz, highHz);
            RunNativeLifecycleCriticalSection(() => NativeMethods.SetTXABandpassFreqs(txa, txLow, txHigh));
        }
        // Mirror the filter onto the monitor channel so the preview stays at
        // the same bandwidth as the on-air signal. Stash the values regardless
        // of whether the monitor channel is open yet — EnsureMonitorChannelOpen
        // reads them at lazy-open time.
        lock (_monitorLock)
        {
            _monitorFilterLow = lowHz;
            _monitorFilterHigh = highHz;
            if (_monitorChannelId is int monId)
            {
                SetFilter(monId, lowHz, highHz);
            }
        }
        _log.LogInformation("wdsp.setTxFilter low={Low} high={High}", lowHz, highHz);
    }

    // SSB filter rectangularity (issue #871). The audible shoulder/skirt
    // steepness of the SSB bandpass is governed by the FIR *tap count* (nc),
    // NOT by the fir.c window family (Blackman-Harris 4- vs 7-term, which differ
    // only ~90 dB down in the stopband — inaudible on voice; that was the
    // original #883 mechanism and the operator heard no change). More taps =>
    // narrower transition => harder/rectangular shoulder (Icom-like); fewer
    // taps => wider transition => rounder/flat shoulder (Yaesu-like). This is
    // exactly Thetis's "Filter Size" lever. The window family stays at WDSP's
    // open-time BH-7 default for best stopband. SetRX/TXABandpassNC rebuild the
    // FIR impulse in-place inside csDSP, so it is safe during live audio.
    //
    // Enum values identify absolute tap requests. WDSP requires nc >= its DSP
    // partition and an integer multiple of it. The 1024-sample RXA partition
    // therefore preserves every operator-visible value exactly.
    internal static int ResolveBandpassNc(BandpassWindow shape, int size)
    {
        int requestedNc = shape switch
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
        int nc = Math.Max(requestedNc, size);
        return ((nc + size - 1) / size) * size;
    }

    public void SetRxBandpassWindow(int channelId, BandpassWindow window)
    {
        if (_disposed != 0) return;
        if (!_channels.TryGetValue(channelId, out var state)) return;
        lock (state.FilterProfileGate)
        {
            state.FilterWindow = window;
            ApplyRxFilterProfile(state);
        }
        int nc = ResolveBandpassNc(window, RxaDspSize);
        _log.LogInformation("wdsp.setRxBandpassShape ch={Ch} shape={Win} nc={Nc} size={Size}",
            channelId, window, nc, RxaDspSize);
    }

    public void SetTxBandpassWindow(BandpassWindow window)
    {
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            int nc = ResolveBandpassNc(window, _txaDspSize);
            WdspWisdomInitializer.WaitUntilReady();
            RunNativeLifecycleCriticalSection(() => NativeMethods.SetTXABandpassNC(txa, nc));
            _log.LogInformation("wdsp.setTxBandpassShape txa={Txa} shape={Win} nc={Nc} size={Size}",
                txa, window, nc, _txaDspSize);
        }
    }

    public void SetRxFilterPhase(int channelId, FilterPhaseMode phase)
    {
        if (!_channels.TryGetValue(channelId, out var state)) return;
        if (!Enum.IsDefined(phase)) throw new ArgumentOutOfRangeException(nameof(phase));
        lock (state.FilterProfileGate)
        {
            state.FilterPhase = phase;
            ApplyRxFilterProfile(state);
        }
    }

    private static void ApplyRxFilterProfile(ChannelState state)
    {
        int masterNc = ResolveBandpassNc(state.FilterWindow, RxaDspSize);
        int cleanupNc = Math.Max(2048, RxaDspSize);
        int mp = state.FilterPhase == FilterPhaseMode.Minimum ? 1 : 0;
        WdspWisdomInitializer.WaitUntilReady();
        RunNativeLifecycleCriticalSection(() =>
        {
            int oldState = NativeMethods.SetChannelState(state.Id, 0, 1);
            try
            {
                if (mp != 0) NativeMethods.SetRXABandpassNC(state.Id, cleanupNc);
                NativeMethods.RXANBPSetMP(state.Id, mp);
                NativeMethods.RXABPSNBASetMP(state.Id, mp);
                NativeMethods.SetRXABandpassMP(state.Id, mp);
                NativeMethods.RXANBPSetNC(state.Id, masterNc);
                NativeMethods.RXABPSNBASetNC(state.Id, masterNc);
                NativeMethods.SetRXABandpassNC(state.Id, mp != 0 ? cleanupNc : masterNc);
            }
            finally
            {
                NativeMethods.SetChannelState(state.Id, oldState, 0);
            }
        });
    }

    public void SetTxFilterPhase(FilterPhaseMode phase)
    {
        if (!Enum.IsDefined(phase)) throw new ArgumentOutOfRangeException(nameof(phase));
        if (_disposed != 0) return;
        lock (_txaLock)
        {
            if (_txaChannelId is not int channelId) return;
            RunNativeLifecycleCriticalSection(() =>
            {
                int oldState = NativeMethods.SetChannelState(channelId, 0, 1);
                try { NativeMethods.SetTXABandpassMP(channelId, phase == FilterPhaseMode.Minimum ? 1 : 0); }
                finally { NativeMethods.SetChannelState(channelId, oldState, 0); }
            });
        }
    }

    /// <summary>Operator-facing TX-monitor toggle. When true, the engine opens
    /// (or reuses) a private RXA channel and feeds it the post-CFIR/RSMPOUT TX
    /// IQ produced inside <see cref="ProcessTxBlock"/>; the demodulated mono
    /// audio is available via <see cref="ReadTxMonitorAudio"/>. The channel is
    /// only opened once TXA exists; calling this before <see cref="OpenTxChannel"/>
    /// stores the request and the channel opens on the next OpenTxChannel
    /// (or the first ProcessTxBlock with a valid TXA, whichever runs first).
    /// Toggling off keeps the channel allocated but stops feeding it, so the
    /// next on-toggle is instant.</summary>
    public void SetTxMonitorEnabled(bool enabled)
    {
        if (_disposed != 0) return;
        _monitorRequested = enabled;
        if (enabled)
        {
            EnsureMonitorChannelOpen();
        }
        ClearMonitorAudioRing();

        // Flip TXA's run state if the preview path's requirement diverges
        // from MOX's. Without this the chain stays quiescent (state=0) when
        // the operator hits monitor with MOX off, fexchange2 returns without
        // filling iout/qout, and the monitor RXA gets silence (or stack
        // garbage from the uninitialised IQ buffer). RXA stays put — the
        // operator still wants to NOT hear the band when monitor is on, but
        // we don't damp RXA either, since the AudioFrame substitution in
        // DspPipelineService.Tick handles the "RX muted" UX cleanly.
        int? txaPrior = null;
        bool nowRunning;
        lock (_txaLock)
        {
            if (_txaChannelId is int txa && !_moxOn && enabled)
            {
                if (!_txaRunning)
                {
                    txaPrior = _setTxMonitorChannelState(txa, 1, 0);
                    _txaRunning = true;
                }
            }
            // Preview-off deliberately leaves an already-running TXA warm.
            // The mic ingest gate stops calling ProcessTxBlock as soon as
            // _monitorRequested becomes false, so a damped SetChannelState(0)
            // cannot complete on this path: dmode=1 blocks the radio packet
            // thread waiting for a later fexchange2, while dmode=0 leaves
            // downflag armed and the next preview's exchange clears itself
            // back to state=0. Keeping the idle channel armed costs no DSP
            // work because it receives no blocks, makes the next Preview
            // instant, and lets the normal MOX-off lifecycle perform the only
            // damped stop while TX ingest is still clocking the fade.
            nowRunning = _txaRunning;
        }
        _log.LogInformation(
            "wdsp.setTxMonitor requested={Enabled} channelId={Id} txaRunning={Running}{Prior}",
            enabled, _monitorChannelId, nowRunning,
            txaPrior is int p ? $" (txa prior={p})" : "");
    }

    /// <summary>Drain demodulated TX-monitor audio into <paramref name="output"/>.
    /// Returns the number of mono float32 samples written, 0 when monitor is
    /// off or the channel hasn't been opened yet. Same shape as
    /// <see cref="ReadAudio"/> but routes to the private monitor channel.</summary>
    public int ReadTxMonitorAudio(Span<float> output)
    {
        if (_disposed != 0) return 0;
        if (!_monitorRequested) return 0;
        int? id = _monitorChannelId;
        if (id is null) return 0;
        return ReadAudio(id.Value, output);
    }

    private void ClearMonitorAudioRing()
    {
        int? id = _monitorChannelId;
        if (id is null) return;
        if (!_channels.TryGetValue(id.Value, out var state)) return;
        lock (state.AudioGate) { state.AudioCount = 0; }
    }

    /// <summary>Volatile-read so callers can gate the audio-broadcast path
    /// without taking _monitorLock. Reflects the operator's request, not
    /// whether the channel is fully open.</summary>
    public bool IsTxMonitorOn => _monitorRequested;

    // Open the monitor RXA channel matched to the current TXA output rate. The
    // channel uses the standard OpenChannel lifecycle (state=0 → configure →
    // worker → SetChannelState(id,1,0)) so the wdsp-init-gotchas.md ordering
    // is honoured. Mode + filter are synced from the latched TX values so the
    // preview starts at the right bandwidth profile from the first sample.
    //
    // No-op if the monitor channel is already open. No-op (with a deferred
    // open) if TXA isn't open yet — first OpenTxChannel will retry.
    private void EnsureMonitorChannelOpen()
    {
        lock (_monitorLock)
        {
            if (_monitorChannelId is not null) return;
            int iqRate;
            lock (_txaLock)
            {
                if (_txaChannelId is null) return;
                iqRate = _txaOutputRateHz;
            }
            // PixelWidth=1024 is plenty for the analyzer that OpenChannel
            // creates. The analyzer output is never read for the monitor
            // channel; we keep it allocated so RunWorker's Spectrum0 call
            // doesn't crash on an unallocated slot. Cost: a few KB of FFT
            // state per engine — negligible.
            int id;
            try
            {
                id = OpenChannelCore(iqRate, pixelWidth: 1024);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "wdsp.openMonitorChannel failed iqRate={Rate}", iqRate);
                return;
            }
            // Sync mode + filter to current TX state. SetMode also clears the
            // audio ring so the first preview block starts from silence.
            SetMode(id, MapRxaToRxMode(_txCurrentMode));
            SetFilter(id, _monitorFilterLow, _monitorFilterHigh);
            ApplyTxMonitorAgc(id, _txCurrentMode);
            _monitorMode = _txCurrentMode;
            _monitorChannelId = id;
            _log.LogInformation(
                "wdsp.openMonitorChannel id={Id} iqRate={Rate} mode={Mode} filter=[{Lo},{Hi}]",
                id, iqRate, _txCurrentMode, _monitorFilterLow, _monitorFilterHigh);
        }
    }

    private void ApplyTxMonitorAgc(int id, RxaMode mode)
    {
        double gainDb = mode is RxaMode.AM or RxaMode.SAM
            ? TxMonitorAmCompensationDb
            : TxMonitorFixedAgcGainDb;
        ApplyAgcCore(id, new AgcConfig(AgcMode.Fixed, FixedGainDb: gainDb));
        NativeMethods.SetRXAAGCTop(id, gainDb);
        if (_channels.TryGetValue(id, out var state))
        {
            state.CurrentAgcMode = AgcMode.Fixed;
            state.AgcTopDb = gainDb;
        }
    }

    // RxaMode → RxMode reverse lookup so the monitor channel can be configured
    // via the public SetMode API (which takes the contract enum). The forward
    // mapping lives at MapMode(RxMode); kept inline here since the inverse is
    // only needed by the monitor seam.
    private static RxMode MapRxaToRxMode(RxaMode m) => m switch
    {
        RxaMode.LSB => RxMode.LSB,
        RxaMode.USB => RxMode.USB,
        RxaMode.CWL => RxMode.CWL,
        RxaMode.CWU => RxMode.CWU,
        RxaMode.AM => RxMode.AM,
        RxaMode.FM => RxMode.FM,
        RxaMode.SAM => RxMode.SAM,
        RxaMode.DSB => RxMode.DSB,
        RxaMode.DIGL => RxMode.DIGL,
        RxaMode.DIGU => RxMode.DIGU,
        _ => RxMode.USB,
    };

    public void SetTwoTone(bool on, double freq1, double freq2, double mag)
    {
        if (_disposed != 0) return;
        // Clamp to safe ranges. Audio passband 50..5000; mag 0..1 linear.
        if (freq1 < 50.0) freq1 = 50.0;
        if (freq1 > 5000.0) freq1 = 5000.0;
        if (freq2 < 50.0) freq2 = 50.0;
        if (freq2 > 5000.0) freq2 = 5000.0;
        if (mag < 0.0) mag = 0.0;
        if (mag > 1.0) mag = 1.0;
        lock (_txaLock)
        {
            if (_txaChannelId is not int txa) return;
            if (on)
            {
                // Sideband sign — matches Thetis (Setup.cs:11096, chkInvertTones
                // default ON): USB-family takes positive freqs (tones above
                // carrier), LSB-family takes negated freqs so the tones land in
                // the displayed bandpass on the correct side. Zeus previously had
                // this inverted (flipped USB instead of LSB), which put the tones
                // on the wrong sideband — outside the visible passband on USB.
                // Cache the operator-supplied (unsigned) freqs so SetTxMode can
                // re-assert with the correct sign on a mid-test mode change.
                bool lsbFamily = _txCurrentMode == RxaMode.LSB
                              || _txCurrentMode == RxaMode.CWL
                              || _txCurrentMode == RxaMode.DIGL;
                double signedF1 = lsbFamily ? -freq1 : freq1;
                double signedF2 = lsbFamily ? -freq2 : freq2;
                _twoToneF1Hz = freq1;
                _twoToneF2Hz = freq2;
                _twoToneArmed = true;
                // PostGen mode=1 = two-tone summed (gen.c:221-241).
                NativeMethods.SetTXAPostGenMode(txa, 1);
                NativeMethods.SetTXAPostGenTTFreq(txa, signedF1, signedF2);
                NativeMethods.SetTXAPostGenTTMag(txa, mag, mag);
                NativeMethods.SetTXAPostGenRun(txa, 1);
                // Same Leveler-off pattern SetTxTune uses; the test signal
                // doesn't need voice-energy AGC and Leveler can pump on the
                // discrete tones.
                _txLevelerForcedOff = true;
                _txControlNative.SetTXALevelerSt(txa, 0);
                _log.LogInformation(
                    "wdsp.setTwoTone on=true f1={F1} f2={F2} signedF1={SF1} signedF2={SF2} mag={Mag:F3} mode={Mode}",
                    freq1, freq2, signedF1, signedF2, mag, _txCurrentMode);
            }
            else
            {
                _twoToneArmed = false;
                NativeMethods.SetTXAPostGenRun(txa, 0);
                // Restore the Leveler to the operator's setting (see SetTxTune)
                // rather than hardcoding "on".
                _txLevelerForcedOff = false;
                _txControlNative.SetTXALevelerSt(
                    txa,
                    (_txLevelerEnabled && !_txRogerBeepBypass
                        && !_txInjectedAudioBypass) ? 1 : 0);
                _log.LogInformation(
                    "wdsp.setTwoTone on=false f1={F1} f2={F2} mag={Mag:F3} leveler={Leveler}",
                    freq1, freq2, mag, _txLevelerEnabled ? "on" : "off");
            }
        }
    }

    public void SetPsHwPeak(double hwPeak)
    {
        if (_disposed != 0) return;
        if (hwPeak <= 0.0 || hwPeak > 2.0) return;   // bogus value, ignore
        lock (_psLock)
        {
            _psHwPeak = hwPeak;
            int? txa;
            lock (_txaLock) txa = _txaChannelId;
            if (txa is int id)
            {
                NativeMethods.SetPSHWPeak(id, hwPeak);
            }
        }
        _log.LogInformation("wdsp.setPsHwPeak peak={Peak:F4}", hwPeak);
    }

    public void SetPsHold(bool hold)
    {
        if (_disposed != 0) return;
        lock (_psLock)
        {
            int? txa;
            lock (_txaLock) txa = _txaChannelId;
            if (txa is not int id) return;
            // hold → SetPSRunCal(0): stop calcc re-fitting (state machine parks),
            // iqc keeps applying the current correction (no turn-off ramp).
            // resume → SetPSRunCal(1).
            NativeMethods.SetPSRunCal(id, hold ? 0 : 1);
        }
        _log.LogInformation("wdsp.setPsHold hold={Hold} (runcal={Run})", hold, hold ? 0 : 1);
    }

    public void SetPsControl(bool autoCal, bool singleCal)
    {
        if (_disposed != 0) return;
        lock (_psLock)
        {
            _psAuto = autoCal;
            _psSingle = singleCal;
            int? txa;
            lock (_txaLock) txa = _txaChannelId;
            if (txa is not int id) return;
            // (reset, mancal, automode, turnon) — see Thetis PSForm.cs.
            // Single takes precedence over auto when both true.
            int reset = 0;
            int mancal = singleCal ? 1 : 0;
            int automode = (autoCal && !singleCal) ? 1 : 0;
            int turnon = 0;
            if (!autoCal && !singleCal)
            {
                // Both off → reset / idle.
                reset = 1;
            }
            NativeMethods.SetPSControl(id, reset, mancal, automode, turnon);
        }
        _log.LogInformation("wdsp.setPsControl auto={Auto} single={Single}", autoCal, singleCal);
    }

    public void SetPsAdvanced(double moxDelaySec, double loopDelaySec,
                              double ampDelayNs, double hwPeak)
    {
        if (_disposed != 0) return;
        double safeAmpDelayNs = ClampPsAmpDelayNs(ampDelayNs, _psFeedbackRateHz);
        lock (_psLock)
        {
            int? txa;
            lock (_txaLock) txa = _txaChannelId;
            int id = txa ?? -1;

            _psMoxDelaySec = moxDelaySec;
            if (id >= 0) NativeMethods.SetPSMoxDelay(id, moxDelaySec);
            _psLoopDelaySec = loopDelaySec;
            if (id >= 0) NativeMethods.SetPSLoopDelay(id, loopDelaySec);
            _psAmpDelayNs = safeAmpDelayNs;
            if (id >= 0) _ = NativeMethods.SetPSTXDelay(id, safeAmpDelayNs * 1e-9);
            // mi0bot: PSForm.cs PSpeak_TextChanged calls
            // puresignal.SetPSHWPeak(_txachannel, _PShwpeak) unconditionally on
            // every TextChanged, with no equality guard against the prior
            // value. Mirror that here so the operator can re-push the same
            // value to clear a rejected-fit state without typing a different
            // value first. Range check stays (mi0bot stops the
            // operator at the WinForms NUD min/max).
            if (hwPeak > 0.0 && hwPeak <= 2.0)
            {
                _psHwPeak = hwPeak;
                if (id >= 0) NativeMethods.SetPSHWPeak(id, hwPeak);
            }
        }
        _log.LogInformation(
            "wdsp.setPsAdvanced mox={Mox:F3}s loop={Loop:F3}s amp={Amp:F1}ns peak={Peak:F4}",
            moxDelaySec, loopDelaySec, safeAmpDelayNs, hwPeak);
    }

    internal static double ClampPsAmpDelayNs(double ampDelayNs, int psFeedbackRateHz)
    {
        if (double.IsNaN(ampDelayNs))
            return 150.0;
        if (ampDelayNs < 0.0)
            return 0.0;
        if (psFeedbackRateHz <= 0)
            return ampDelayNs;

        double maxNs = Math.Floor(
            (WdspPsDelayWholeSamplePositions - 1) * 1_000_000_000.0 / psFeedbackRateHz);
        return ampDelayNs > maxNs ? maxNs : ampDelayNs;
    }

    public void SetPsEnabled(bool enabled)
    {
        if (_disposed != 0) return;
        lock (_psLock)
        {
            int? txa;
            lock (_txaLock) txa = _txaChannelId;
            if (txa is not int id)
            {
                _psEnabled = false;
                return;
            }

            if (enabled)
            {
                _psEnabled = true;
                // Re-assert the operator's COMP/CESSB topology before PS
                // starts. WDSP runs osctrl before iqc, so PureSignal corrects
                // the CESSB-shaped reference instead of replacing that stage.
                ApplyTxCompressorAndCessbRuns(id);
                // Reset diagnostic counters so the first state transition
                // and the first 100 pscc blocks log on every fresh arm.
                _lastLoggedPsState = 255;
                Interlocked.Exchange(ref _psFeedCount, 0);
                NativeMethods.SetPSRunCal(id, 1);
                int mancal = _psSingle ? 1 : 0;
                int automode = (_psAuto && !_psSingle) ? 1 : 0;
                // reset=1 forces a clean LRESET transit so a re-arm after a
                // single-cal cycle (which can leave the SM in LSTAYON) starts
                // a fresh fit (Thetis PSForm.cs:645,661).
                NativeMethods.SetPSControl(id, 1, mancal, automode, 0);
                // Open the PS-feedback display analyzer (issue #121). Inherits
                // pixel width / zoom / matched RX rate from the TX analyzer so
                // the PS-Monitor pan/wf frames slot into the same widget the
                // TX analyzer is rendering into. Skipped when TX analyzer is
                // off (no RXA, or P1 P2 rate-ratio mismatch) — the toggle
                // becomes a no-op in that case and Tick keeps falling through
                // to the existing TX/RX trace.
                OpenPsFeedbackAnalyzer(id);
            }
            else
            {
                // Tear down the PS-FB analyzer first so a stale GetPixels
                // call from Tick doesn't race with WDSP cleaning up the slot.
                ClosePsFeedbackAnalyzer();
                // pihpsdr shutdown gotcha (transmitter.c:2422-2444): when
                // disabling PS while NOT keyed, push 7 zero-IQ blocks through
                // psccF so the calcc state machine advances to LRESET cleanly
                // and doesn't latch a stale curve in iqc on re-arm.
                //
                // ONLY when not transmitting. Mid-MOX (operator aborting PS
                // during a TX), the live feedback FB pump is still writing real
                // samples into psccF; interleaving 7 manual zero blocks races
                // that stream and can wedge calcc in LCALC. While keyed the
                // live feedback advances calcc on its own, so the manual drain
                // is both unnecessary and harmful — skip it.
                if (!_moxOn)
                {
                    var zeros = new float[PsFeedbackBlockSize];
                    for (int i = 0; i < 7; i++)
                    {
                        NativeMethods.psccF(id, PsFeedbackBlockSize, zeros, zeros, zeros, zeros, 0, 0);
                    }
                }
                NativeMethods.SetPSRunCal(id, 0);
                NativeMethods.SetPSControl(id, 1, 0, 0, 0);
                _psEnabled = false;
                // PS and CESSB are independent, but re-assert the operator's
                // effective topology after both native PS controls are down.
                ApplyTxCompressorAndCessbRuns(id);
            }
        }
        _log.LogInformation("wdsp.setPsEnabled enabled={Enabled}", enabled);
    }

    // Open / configure the PS-feedback display analyzer. Caller holds
    // _psLock. Mirrors the TX analyzer's pixel width / zoom / matched RX
    // sample rate so DspPipelineService.Tick can pick between TX-pixels and
    // PS-FB-pixels per tick without a buffer resize.
    private void OpenPsFeedbackAnalyzer(int txaId)
    {
        // Snapshot TX-display geometry under its own lock — we need it whether
        // or not _txDispAlive is true, but the values are only meaningful when
        // it is.
        bool txAlive;
        int pixelWidth;
        int rxRate;
        int zoom;
        lock (_txDispLock)
        {
            txAlive = _txDispAlive;
            pixelWidth = _txDispPixelWidth;
            rxRate = _txDispRxSampleRateHz;
            zoom = _txDispZoomLevel;
        }
        if (!txAlive || pixelWidth <= 0)
        {
            // TX display analyzer isn't alive — at RX rates above the TXA DSP
            // rate, inherit the first RXA's geometry instead: the PS-FB source
            // runs at PsFeedbackSampleRateHz (192k), so its rate rule may still
            // pass where the TX analyzer's can't.
            // On a single-ADC time-mux board (HermesC10 / ANAN-G2E) this is
            // load-bearing, not cosmetic: a keyed PS burst diverts the board's
            // ONLY DDC to feedback, the RX analyzer starves, and with no TX
            // analyzer this PS-FB analyzer is the one live spectrum source —
            // without it the panadapter/waterfall freeze for the whole
            // transmission (G2E field report, #960 rework bench).
            if (!TrySnapshotRxDisplayGeometry(out pixelWidth, out rxRate, out zoom))
            {
                _log.LogInformation(
                    "wdsp.psFb.open skip — no TX display analyzer and no RX channel geometry to inherit");
                return;
            }
            _log.LogInformation(
                "wdsp.psFb.open inheriting RX geometry (txDisp not alive): pix={Pix} rxRate={RxRate} zoom={Zoom}",
                pixelWidth, rxRate, zoom);
        }

        lock (_psFbDispLock)
        {
            if (_psFbDispAlive) return;

            WdspWisdomInitializer.WaitUntilReady();
            int psFbId = ReserveNativeSlot();

            NativeMethods.XCreateAnalyzer(psFbId, out int rc, MaxFftSize, 1, 1, null);
            if (rc != 0)
            {
                ReleaseNativeSlot(psFbId);
                _log.LogWarning("wdsp.psFb.open XCreateAnalyzer rc={Rc} — PS-Monitor will fall back to TX trace", rc);
                return;
            }
            bool configured = TryConfigureTxAnalyzer(psFbId, _psFeedbackRateHz, PsFeedbackBlockSize, rxRate, pixelWidth, zoom, _txFftSize, _txWinType, AnalyzerKaiserPi, out int usedWidth);
            if (!configured)
            {
                NativeMethods.DestroyAnalyzer(psFbId);
                ReleaseNativeSlot(psFbId);
                _log.LogWarning(
                    "wdsp.psFb.open skipped — rx={RxRate} psFb={PsFbRate} not an integer multiple in either direction; PS-Monitor will fall back to TX trace",
                    rxRate, _psFeedbackRateHz);
                return;
            }
            ConfigureDisplayAveragingTau(psFbId, _txAvgTauSec);
            _psFbDispId = psFbId;
            _psFbDispPixelWidth = pixelWidth;
            _psFbDispUsedPixelWidth = usedWidth;
            _psFbDispRxSampleRateHz = rxRate;
            _psFbDispZoomLevel = zoom;
            _psFbDispScratchPixels = PrepareDisplayScratch(_psFbDispScratchPixels, pixelWidth, usedWidth);
            _psFbDispAlive = true;
            _log.LogInformation(
                "wdsp.psFb.open id={Id} pix={Pix} rxRate={RxRate} psFbRate={PsFbRate} zoom={Zoom}",
                psFbId, pixelWidth, rxRate, _psFeedbackRateHz, zoom);
        }
    }

    // Tear down the PS-feedback display analyzer. Caller holds _psLock so
    // FeedPsFeedbackBlock can't race in mid-Spectrum0; combined with
    // _psFbDispLock around GetPixels / Spectrum0 this keeps the analyzer slot
    // safe to destroy.
    private void ClosePsFeedbackAnalyzer()
    {
        lock (_psFbDispLock)
        {
            if (!_psFbDispAlive) return;
            if (_psFbDispId is int id)
            {
                try
                {
                    NativeMethods.DestroyAnalyzer(id);
                    _log.LogInformation("wdsp.psFb.close id={Id}", id);
                }
                finally
                {
                    ReleaseNativeSlot(id);
                }
            }
            _psFbDispId = null;
            _psFbDispAlive = false;
            _psFbDispPixelWidth = 0;
            _psFbDispUsedPixelWidth = 0;
            _psFbDispRxSampleRateHz = 0;
            _psFbDispZoomLevel = 1;
            _psFbDispScratchPixels = null;
        }
    }

    public void FeedPsFeedbackBlock(ReadOnlySpan<float> txI, ReadOnlySpan<float> txQ,
                                    ReadOnlySpan<float> rxI, ReadOnlySpan<float> rxQ)
    {
        if (_disposed != 0) return;
        if (txI.Length != PsFeedbackBlockSize ||
            txQ.Length != PsFeedbackBlockSize ||
            rxI.Length != PsFeedbackBlockSize ||
            rxQ.Length != PsFeedbackBlockSize)
        {
            // Don't throw — log once and drop, so a transient sizing mismatch
            // upstream (DDC re-config edge) doesn't crash the pipeline.
            _log.LogWarning("wdsp.feedPsFeedback block sizes mismatch; expected {Expected}", PsFeedbackBlockSize);
            return;
        }
        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is not int id) return;

        // psccF takes float[] (not Span). Allocate fresh — caller may reuse
        // its buffers immediately after this returns.
        var bufTxI = txI.ToArray();
        var bufTxQ = txQ.ToArray();
        var bufRxI = rxI.ToArray();
        var bufRxQ = rxQ.ToArray();

        lock (_psLock)
        {
            // mox/solidmox args are ignored by psccF (calcc.c:846); SetPSMox
            // is the source of truth and is driven from SetMox above.
            NativeMethods.psccF(id, PsFeedbackBlockSize, bufTxI, bufTxQ, bufRxI, bufRxQ, 0, 0);
            long n = Interlocked.Increment(ref _psFeedCount);
            if (n % 100 == 1)
            {
                // Confirms paired packets are reaching the engine. If this
                // line never appears while keyed + PS armed, the wire path
                // (Protocol2Client paired-packet decode) isn't running.
                _log.LogInformation("wdsp.pscc fed {N} blocks", n);
            }
        }

        // Feed the PS-feedback display analyzer with the same rxI/rxQ block
        // (post-PA loopback IQ). DspPipelineService.Tick reads from this
        // analyzer when PsMonitorEnabled is on, surfacing the actual on-air
        // signal instead of the predistorted TX-modulator IQ. Q is negated
        // for the same WDSP analyzer convention used on the RX and TX paths
        // (see ProcessTxBlock: `txSpectrumIq[2*i + 1] = -qout[i]`); without
        // it the PS-Monitor view would render with sidebands flipped about
        // the carrier.
        if (_psFbDispAlive)
        {
            int? psFbId = null;
            lock (_psFbDispLock)
            {
                if (_psFbDispAlive) psFbId = _psFbDispId;
            }
            if (psFbId is int fbDisp)
            {
                Span<double> psSpectrumIq = stackalloc double[2 * PsFeedbackBlockSize];
                for (int i = 0; i < PsFeedbackBlockSize; i++)
                {
                    psSpectrumIq[2 * i] = bufRxI[i];
                    psSpectrumIq[2 * i + 1] = -bufRxQ[i];
                }
                lock (_psFbDispLock)
                {
                    if (_psFbDispAlive && _psFbDispId == fbDisp)
                    {
                        NativeMethods.Spectrum0(1, fbDisp, 0, 0, ref psSpectrumIq[0]);
                        long n = ++_psFbFeedCount;
                        if (n == 1 || n % 200 == 0)
                        {
                            _log.LogInformation("wdsp.psFb.fed n={N} blocks", n);
                        }
                    }
                }
            }
        }
    }

    /// <summary>calcc's display curves — see <see cref="PsCurve"/>.</summary>
    /// <remarks>
    /// Only worth calling on a CalibrationAttempts edge: calcc refreshes
    /// these buffers at the end of an accepted calc() pass and they are
    /// otherwise the same arrays as last time. Returns null when PS is not
    /// armed, when there is no TXA, or before the first accepted fit —
    /// disp.nsamps stays 0 until then, which is how "no curve yet" is told
    /// apart from a flat one.
    /// </remarks>
    public PsCurve? GetPsCurve()
    {
        if (_disposed != 0) return null;
        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is not int id) return null;
        if (!_psEnabled) return null;

        int nsamps, cpts, attempts;
        double phsRefDeg;
        float[] magCor, phaseDeg, scatterX, scatterMag, scatterPhase;

        lock (_psLock)
        {
            unsafe
            {
                fixed (double* px = _psDispX)
                fixed (double* pym = _psDispYm)
                fixed (double* pyc = _psDispYc)
                fixed (double* pys = _psDispYs)
                fixed (double* pxm = _psDispXmCor)
                fixed (double* pymc = _psDispYmCor)
                fixed (double* pxa = _psDispXaCor)
                fixed (double* pyac = _psDispYaCor)
                {
                    int n = 0, c = 0;
                    double phs = 0.0;
                    NativeMethods.GetPSDisp(id, px, pym, pyc, pys, pxm, pymc, pxa, pyac,
                                            &n, &c, &phs);
                    nsamps = n;
                    cpts = c;
                    phsRefDeg = phs;
                }
                fixed (int* p = _psInfoBuf)
                {
                    NativeMethods.GetPSInfo(id, (IntPtr)p);
                }
                attempts = _psInfoBuf[5];
            }

            // A calcc that has never completed a fit reports nsamps 0 and
            // leaves the curve buffers at their malloc0 zeros. Zeros would
            // draw as a perfectly corrected amplifier, which is the most
            // misleading thing this could possibly show.
            if (nsamps <= 0 || cpts <= 0) return null;
            if (cpts > PsCurve.CurvePoints) cpts = PsCurve.CurvePoints;
            if (nsamps > PsCurve.NativeSampleCount) nsamps = PsCurve.NativeSampleCount;

            magCor = new float[cpts];
            phaseDeg = new float[cpts];
            for (int k = 0; k < cpts; k++)
            {
                magCor[k] = (float)_psDispYmCor[k];
                phaseDeg[k] = (float)_psDispYaCor[k];
            }

            // Every Nth sample, so the kept points stay spread across
            // calcc's buckets instead of clustering in the first ones.
            int stride = Math.Max(1, nsamps / PsCurve.ScatterPoints);
            int keep = Math.Min(PsCurve.ScatterPoints, (nsamps + stride - 1) / stride);
            scatterX = new float[keep];
            scatterMag = new float[keep];
            scatterPhase = new float[keep];
            const double Rad2Deg = 180.0 / Math.PI;
            for (int k = 0, i = 0; k < keep && i < nsamps; k++, i += stride)
            {
                scatterX[k] = (float)_psDispX[i];
                scatterMag[k] = (float)_psDispYm[i];
                scatterPhase[k] = (float)(Rad2Deg * Math.Atan2(_psDispYs[i], _psDispYc[i]));
            }
        }

        return new PsCurve(magCor, phaseDeg, scatterX, scatterMag, scatterPhase,
                           (float)phsRefDeg, attempts);
    }

    public PsStageMeters GetPsStageMeters()
    {
        if (_disposed != 0) return PsStageMeters.Silent;
        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is not int id) return PsStageMeters.Silent;
        // Skip the GetPSInfo P/Invoke when PS isn't armed — saves a per-tick
        // jaunt into the native side and matches the wire-quiet contract for
        // the PsMeters frame.
        if (!_psEnabled) return PsStageMeters.Silent;

        // Pin the int[16] buffer for the duration of the GetPSInfo call so
        // WDSP can write into it. Re-using the same buffer between calls is
        // fine because GetPSInfo writes synchronously.
        int feedbackRaw;
        byte calState;
        bool correcting;
        double maxTx;
        int calibrationAttempts;
        lock (_psLock)
        {
            unsafe
            {
                fixed (int* p = _psInfoBuf)
                {
                    NativeMethods.GetPSInfo(id, (IntPtr)p);
                }
            }
            feedbackRaw = _psInfoBuf[4];
            correcting = _psInfoBuf[14] != 0;
            calState = (byte)Math.Clamp(_psInfoBuf[15], 0, 255);
            calibrationAttempts = _psInfoBuf[5];
            NativeMethods.GetPSMaxTX(id, out maxTx);
            _psMaxTxEnvelope = maxTx;
        }

        // CorrectionDb: until we tap GetPSDisp's curve, derive a coarse
        // proxy as 20*log10(feedbackLevel/256+eps). Replace with a real RMS
        // when we wire GetPSDisp. Safe: callers treat <=−200 as "bypassed".
        float feedback = feedbackRaw;
        float depthDb = correcting
            ? (float)(20.0 * Math.Log10(Math.Max(feedback, 1e-3) / 256.0))
            : 0f;

        // Edge-triggered state-transition log. WDSP 2.00 calcc states:
        // LRESET=0, LWAIT=1, LMOXDELAY=2, LSETUP=3, LCOLLECT=4, MOXCHECK=5,
        // LCALC=6, LDELAY=7, LSTAYON=8, LTURNON=9. info[14]=1 means
        // corrections live. info[6] is the scheck mask: bit0 new-vs-old
        // compare failed, bit1 stuck buckets / probable overdrive.
        // The 5-sec periodic log below is too sparse to catch the
        // LCOLLECT↔LRESET bounce that happens every ~50 ms when scheck fails;
        // edge-triggered surfaces every transition without flooding when
        // PS is parked (e.g. stuck at LRESET while idle).
        if (_lastLoggedPsState != calState)
        {
            _log.LogInformation(
                "wdsp.psState {Prev}->{Cur} info4={Fb} info6=0x{Sc:X4} info13={Dog} info14={Cor}",
                _lastLoggedPsState, calState,
                _psInfoBuf[4], _psInfoBuf[6], _psInfoBuf[13], _psInfoBuf[14]);
            _lastLoggedPsState = calState;
        }

        // Bring-up diagnostic — log info[0..7] + correcting/state every Nth
        // call so the calcc state machine progression is visible during a
        // rack run. With TxMetersService running at ~10 Hz, N=50 ≈ 5 s.
        // Drop once PS is confirmed working.
        if (++_psInfoLogCounter % 50 == 0)
        {
            _log.LogDebug(
                "wdsp.psInfo binfo=[{B0},{B1},{B2},{B3},{B4},{B5},{B6},{B7}] correcting={C} state={S}",
                _psInfoBuf[0], _psInfoBuf[1], _psInfoBuf[2], _psInfoBuf[3],
                _psInfoBuf[4], _psInfoBuf[5], _psInfoBuf[6], _psInfoBuf[7],
                _psInfoBuf[14], _psInfoBuf[15]);
        }

        // Hot-audio robustness diagnostic. At ~1 Hz while PS is armed, surface
        // the forward TX envelope PEAK (GetPSMaxTX, ~1.0 = at the ALC cap)
        // next to the feedback level (info4), the 2-bit scheck reject mask
        // (info6), calcc fit count (info5), state and correcting flag. On a
        // deliberately-hot over this separates the three candidate root
        // causes: env climbing >1.0 = forward limiter escaping; fb railing
        // (toward ADC saturation, ideal ~152) = feedback path saturating
        // calcc's top bins; both bounded but info6 bit0/bit1 spiking = fit
        // rejection on the top-skewed envelope PDF. Debug-level: kept as a
        // diagnostic but no longer spams ~1 Hz on every TX in a normal run.
        if (_psInfoLogCounter % 10 == 0)
        {
            _log.LogDebug(
                "wdsp.psHot env={Env:F3} fb={Fb} info6=0x{Sc:X4} cal={Cal} state={St} cor={Cor}",
                maxTx, _psInfoBuf[4], _psInfoBuf[6], _psInfoBuf[5], _psInfoBuf[15], _psInfoBuf[14]);
        }

        return new PsStageMeters(
            FeedbackLevel: feedback,
            CalState: calState,
            Correcting: correcting,
            CorrectionDb: depthDb,
            MaxTxEnvelope: (float)maxTx,
            CalibrationAttempts: calibrationAttempts);
    }

    public void ResetPs()
    {
        if (_disposed != 0) return;
        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is not int id) return;
        lock (_psLock)
        {
            // calcc consumes reset on a later feedback block. Publish reset
            // and the saved mode together, as SetPsEnabled does, so a second
            // control write cannot clear the request before it is observed.
            // LRESET clears reset itself and resumes the selected mode.
            int mancal = _psSingle ? 1 : 0;
            int automode = (_psAuto && !_psSingle) ? 1 : 0;
            NativeMethods.SetPSControl(id, 1, mancal, automode, 0);
        }
        _log.LogInformation("wdsp.resetPs auto={Auto} single={Single}", _psAuto, _psSingle);
    }

    public void SavePsCorrection(string path)
    {
        if (_disposed != 0) return;
        if (string.IsNullOrWhiteSpace(path)) return;
        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is not int id) return;
        lock (_psLock)
        {
            NativeMethods.PSSaveCorr(id, path);
        }
        _log.LogInformation("wdsp.savePsCorrection path={Path}", path);
    }

    public void RestorePsCorrection(string path)
    {
        if (_disposed != 0) return;
        if (string.IsNullOrWhiteSpace(path)) return;
        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is not int id) return;
        lock (_psLock)
        {
            NativeMethods.PSRestoreCorr(id, path);
        }
        _log.LogInformation("wdsp.restorePsCorrection path={Path}", path);
    }

    // CFC (Continuous Frequency Compressor) — issue #123. xcfcomp already
    // sits in xtxa between xeqp and xbandpass; this method just pushes the
    // operator-tuned profile + scalar params and toggles the run flag.
    //
    // Param push order matters: profile + scalars + post-EQ-run all happen
    // BEFORE the master CFCOMPRun flip. That way when the master toggles on,
    // the audio pipeline starts processing with a fully-configured stage —
    // mirrors the same "configure, then enable" Thetis pattern we use for
    // every other WDSP stage (see SetNoiseReduction NR2/NR4 ordering).
    //
    public void SetCfcConfig(CfcConfig cfg)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        if (cfg.Bands is null) throw new ArgumentException("Bands must not be null", nameof(cfg));
        if (cfg.Bands.Length != 10)
            throw new ArgumentException($"Bands must have exactly 10 entries; got {cfg.Bands.Length}", nameof(cfg));

        if (_disposed != 0) return;

        lock (_txaLock)
        {
            if (_txaChannelId is not int id) return;
        }

        var cached = CloneCfcConfig(cfg);

        // Build parallel arrays for SetTXACFCOMPprofile. WDSP sorts internally
        // (cfcomp.c:147) and clamps to [0, Nyquist], so we don't pre-validate
        // monotonicity — operators are free to type frequencies in any order.
        const int nfreqs = 10;
        double[] f = new double[nfreqs];
        double[] g = new double[nfreqs];
        double[] e = new double[nfreqs];
        for (int i = 0; i < nfreqs; i++)
        {
            var band = cached.Bands[i];
            f[i] = band.FreqHz;
            g[i] = band.CompLevelDb;
            e[i] = band.PostGainDb;
        }

        lock (_txaLock)
        {
            // Re-check inside the lock — TXA could have closed between the
            // outer lookup and here on a teardown race. Same pattern other
            // _txaLock callers use.
            if (_txaChannelId is not int id) return;

            _cfcConfig = cached;
            // Null Q keeps the classic (non-parametric) profile, which is
            // what the ten-band CfcConfig describes. SetCfcParametric is the
            // path that carries Q.
            _txControlNative.SetTXACFCOMPprofile(id, nfreqs, f, g, e, null, null);
            _txControlNative.SetTXACFCOMPPrecomp(id, cached.PreCompDb);
            _txControlNative.SetTXACFCOMPPrePeq(id, cached.PrePeqDb);
            _txControlNative.SetTXACFCOMPPeqRun(id, cached.PostEqEnabled ? 1 : 0);
            ApplyCfcMasterRunLocked(id, cached);
        }

        _log.LogInformation(
            "wdsp.setCfc enabled={Enabled} peq={Peq} precomp={Pre:F1}dB prepeq={PrePeq:F1}dB",
            cfg.Enabled, cfg.PostEqEnabled, cfg.PreCompDb, cfg.PrePeqDb);
    }

    private static CfcConfig CloneCfcConfig(CfcConfig cfg) =>
        cfg with { Bands = cfg.Bands.ToArray() };

    private DateTime _lastTxMeterLogUtc;

    /* ---- TX/RX equalizer and TX noise gate -------------------------
     *
     * Thetis's audio suite, driven through the same WDSP entry points
     * Thetis uses. Nothing here chooses where a stage sits: create_txa
     * fixes the order as gate -> EQ -> preemph -> leveler -> CFC ->
     * compressor -> ALC, with every stage reading and writing the same
     * midbuff in sequence. These methods only set parameters and run
     * flags on stages that already exist in that order.
     */

    public void SetTxEq(GraphicEqConfig cfg)
    {
        if (_disposed != 0 || cfg is null) return;
        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is not int id) return;

        // Profile before run: turning the stage on and THEN loading the
        // curve transmits one block through whatever the eqp was last left
        // holding, which on a fresh channel is WDSP's shaped default_G
        // (TXA.c:116) rather than anything the operator asked for.
        var native = cfg.ToNativeArray();
        lock (_eqLock)
        {
            unsafe
            {
                fixed (int* p = native) NativeMethods.SetTXAGrphEQ10(id, p);
            }
            NativeMethods.SetTXAEQRun(id, cfg.Enabled ? 1 : 0);
        }
        _log.LogInformation(
            "wdsp.txEq run={Run} preamp={Preamp}dB bands=[{Bands}]",
            cfg.Enabled, cfg.PreampDb, string.Join(",", cfg.BandsDb));
    }

    public void SetRxEq(int channelId, GraphicEqConfig cfg)
    {
        if (_disposed != 0 || cfg is null) return;
        if (!_channels.TryGetValue(channelId, out var state) || state.Stopped) return;
        int id = state.Id;

        var native = cfg.ToNativeArray();
        lock (_eqLock)
        {
            unsafe
            {
                fixed (int* p = native) NativeMethods.SetRXAGrphEQ10(id, p);
            }
            NativeMethods.SetRXAEQRun(id, cfg.Enabled ? 1 : 0);
        }
        _log.LogInformation(
            "wdsp.rxEq ch={Ch} run={Run} preamp={Preamp}dB",
            channelId, cfg.Enabled, cfg.PreampDb);
    }

    public void SetTxGate(TxGateConfig cfg)
    {
        if (_disposed != 0 || cfg is null) return;
        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is not int id) return;

        // Thresholds before run, for the same reason as the EQ: AMSQ is
        // constructed with an unmute level of 0.200 linear and would gate
        // against that for a block if run came first.
        NativeMethods.SetTXAAMSQThreshold(id, cfg.ThresholdDb);
        NativeMethods.SetTXAAMSQMutedGain(id, cfg.MutedGainDb);
        NativeMethods.SetTXAAMSQRun(id, cfg.Enabled ? 1 : 0);
        _log.LogInformation(
            "wdsp.txGate run={Run} thresh={Thresh:F1}dB muted={Muted:F1}dB",
            cfg.Enabled, cfg.ThresholdDb, cfg.MutedGainDb);
    }

    /* ---- parametric EQ and CFC (the Thetis WDSP port) ---------------
     *
     * The ten-band GrphEQ10 path above and these share one stage: whichever
     * was set last is what runs. That mirrors Thetis, where the legacy and
     * parametric editors drive the same eqp.
     */

    public void SetTxEqParametric(ParametricEqConfig cfg)
    {
        if (_disposed != 0 || cfg is null) return;
        int? txa;
        lock (_txaLock) txa = _txaChannelId;
        if (txa is not int id) return;

        cfg.ToNativeArrays(out var f, out var g, out var q);
        lock (_eqLock)
        {
            unsafe
            {
                fixed (double* pF = f, pG = g, pQ = q)
                {
                    // Profile before run, as everywhere else here: enabling
                    // first would transmit a block through whatever curve the
                    // stage was last left holding.
                    NativeMethods.SetTXAEQProfile(id, cfg.Points.Length, pF, pG, pQ);
                }
            }
            NativeMethods.SetTXAEQRun(id, cfg.Enabled ? 1 : 0);
        }
        _log.LogInformation(
            "wdsp.txEq.parametric run={Run} points={N} preamp={Preamp:F1}dB",
            cfg.Enabled, cfg.Points.Length, cfg.GlobalGainDb);
    }

    public void SetRxEqParametric(int channelId, ParametricEqConfig cfg)
    {
        if (_disposed != 0 || cfg is null) return;
        if (!_channels.TryGetValue(channelId, out var state) || state.Stopped) return;
        int id = state.Id;

        cfg.ToNativeArrays(out var f, out var g, out var q);
        lock (_eqLock)
        {
            unsafe
            {
                fixed (double* pF = f, pG = g, pQ = q)
                {
                    NativeMethods.SetRXAEQProfile(id, cfg.Points.Length, pF, pG, pQ);
                }
            }
            NativeMethods.SetRXAEQRun(id, cfg.Enabled ? 1 : 0);
        }
        _log.LogInformation(
            "wdsp.rxEq.parametric ch={Ch} run={Run} points={N}",
            channelId, cfg.Enabled, cfg.Points.Length);
    }

    public void SetCfcParametric(ParametricCfcConfig cfg)
    {
        if (_disposed != 0 || cfg is null) return;
        int n = cfg.Compression.Points.Length;
        if (n == 0) return;

        // ONE frequency array for both curves, taken from the compression
        // curve — Thetis's CFC form does exactly this and drops the post-EQ
        // curve's own frequencies (frmCFCConfig.setCFCProfile).
        var f = new double[n];
        var g = new double[n];
        var e = new double[n];
        var qg = new double[n];
        var qe = new double[n];
        for (int i = 0; i < n; i++)
        {
            var cp = cfg.Compression.Points[i];
            var ep = cfg.PostEq.Points[i];
            f[i] = cp.FrequencyHz;
            g[i] = cp.GainDb;
            qg[i] = cp.Q;
            e[i] = ep.GainDb;
            qe[i] = ep.Q;
        }

        lock (_txaLock)
        {
            if (_txaChannelId is not int id) return;
            _txControlNative.SetTXACFCOMPprofile(id, n, f, g, e, qg, qe);
            // Thetis's Pre-comp and Pre-PEQ are the two curves' flat gains.
            _txControlNative.SetTXACFCOMPPrecomp(id, cfg.Compression.GlobalGainDb);
            _txControlNative.SetTXACFCOMPPrePeq(id, cfg.PostEq.GlobalGainDb);
            _txControlNative.SetTXACFCOMPPeqRun(id, cfg.PostEqEnabled ? 1 : 0);
            _txControlNative.SetTXACFCOMPRun(id, cfg.Enabled ? 1 : 0);
        }
        _log.LogInformation(
            "wdsp.cfc.parametric run={Run} peq={Peq} points={N} precomp={Pre:F1}dB prepeq={PrePeq:F1}dB",
            cfg.Enabled, cfg.PostEqEnabled, n,
            cfg.Compression.GlobalGainDb, cfg.PostEq.GlobalGainDb);
    }

    /// <summary>
    /// TX downward expander — Thetis's TX noise gate, on the mic ahead of
    /// TXA (and ahead of the TX audio plugin chain, so an effect there keeps
    /// its tail rather than being expanded away).
    /// </summary>
    public void SetTxDexp(TxDexpConfig cfg)
    {
        if (_disposed != 0 || cfg is null) return;
        lock (_dexpLock)
        {
            _dexpConfig = cfg;
            EnsureDexpLocked(_txaInSize, _txaInputRateHz);
            if (!_dexpCreated) { _dexpActive = false; return; }
            ApplyDexpLocked(cfg);
            _dexpActive = cfg.Enabled;
        }
        _log.LogInformation(
            "wdsp.txDexp run={Run} thresh={Thresh:F1}dB attack={A:F0}ms hold={H:F0}ms " +
            "release={R:F0}ms ratio={Ratio:F1}dB hyst={Hyst:F1}dB scf={Scf} look={Look}",
            cfg.Enabled, cfg.ThresholdDb, cfg.AttackMs, cfg.HoldMs, cfg.ReleaseMs,
            cfg.ExpansionRatioDb, cfg.HysteresisRatioDb,
            cfg.SideChannelFilterEnabled, cfg.LookAheadEnabled);
    }

    /// <summary>Build the stage, or rebuild it when the TX block size or
    /// rate has changed under it. Allocates — control thread only.</summary>
    private static int AcquireDexpSlot()
    {
        lock (s_dexpSlotLock)
        {
            for (int i = 0; i < DexpSlots; i++)
            {
                if (s_dexpSlotUsed[i]) continue;
                s_dexpSlotUsed[i] = true;
                return i;
            }
            return -1;
        }
    }

    private static void ReleaseDexpSlot(int id)
    {
        if (id < 0 || id >= DexpSlots) return;
        lock (s_dexpSlotLock) s_dexpSlotUsed[id] = false;
    }

    private unsafe void EnsureDexpLocked(int size, int rateHz)
    {
        if (size <= 0 || rateHz <= 0) return;
        if (_dexpCreated && _dexpSize == size && _dexpRateHz == rateHz) return;

        DestroyDexpLocked();

        int slot = AcquireDexpSlot();
        if (slot < 0)
        {
            // Four engines alive at once is a leak elsewhere; run without the
            // gate rather than share a slot and corrupt another engine's.
            _log.LogWarning("wdsp.txDexp no free pdexp slot; the TX expander is off for this engine");
            return;
        }
        _dexpId = slot;

        // WDSP's buffers are interleaved complex doubles and it keeps the
        // pointer, so this is unmanaged and lives until destroy_dexp.
        nuint bytes = (nuint)(size * 2 * sizeof(double));
        _dexpBuf = (double*)NativeMemory.AlignedAlloc(bytes, 64);
        NativeMemory.Fill(_dexpBuf, bytes, 0);

        var c = _dexpConfig;
        NativeMethods.create_dexp(
            _dexpId,
            0,                              // start stopped; SetDEXPRun follows
            size,
            _dexpBuf, _dexpBuf,             // in place, as ChannelMaster does
            rateHz,
            c.DetectorTauSec, c.AttackSec, c.ReleaseSec, c.HoldSec,
            c.ExpansionRatioLinear, c.HysteresisRatioLinear, c.ThresholdLinear,
            256,                            // side-channel filter taps (Thetis's)
            0,                              // BH-4 window (Thetis's)
            c.SideChannelLowCutHz, c.SideChannelHighCutHz,
            c.SideChannelFilterEnabled ? 1 : 0,
            0,                              // run_vox: VOX is the client's job here
            c.LookAheadEnabled ? 1 : 0,
            c.LookAheadSec,
            IntPtr.Zero,                    // no VOX callback; only read when run_vox
            0,                              // anti-vox off
            size, rateHz,                   // ...but its buffer must still be sane
            0.01, 0.01);

        _dexpCreated = true;
        _dexpSize = size;
        _dexpRateHz = rateHz;
        _log.LogInformation("wdsp.txDexp created slot={Slot} size={Size} rate={Rate}", _dexpId, size, rateHz);
    }

    private void ApplyDexpLocked(TxDexpConfig c)
    {
        NativeMethods.SetDEXPDetectorTau(_dexpId, c.DetectorTauSec);
        NativeMethods.SetDEXPAttackTime(_dexpId, c.AttackSec);
        NativeMethods.SetDEXPReleaseTime(_dexpId, c.ReleaseSec);
        NativeMethods.SetDEXPHoldTime(_dexpId, c.HoldSec);
        NativeMethods.SetDEXPExpansionRatio(_dexpId, c.ExpansionRatioLinear);
        NativeMethods.SetDEXPHysteresisRatio(_dexpId, c.HysteresisRatioLinear);
        NativeMethods.SetDEXPAttackThreshold(_dexpId, c.ThresholdLinear);
        NativeMethods.SetDEXPLowCut(_dexpId, c.SideChannelLowCutHz);
        NativeMethods.SetDEXPHighCut(_dexpId, c.SideChannelHighCutHz);
        NativeMethods.SetDEXPRunSideChannelFilter(_dexpId, c.SideChannelFilterEnabled ? 1 : 0);
        NativeMethods.SetDEXPAudioDelay(_dexpId, c.LookAheadSec);
        NativeMethods.SetDEXPRunAudioDelay(_dexpId, c.LookAheadEnabled ? 1 : 0);
        NativeMethods.SetDEXPRunVox(_dexpId, 0);
        // Run last, for the same reason the EQ and gate set parameters
        // first: otherwise a block goes through the stage as it was.
        NativeMethods.SetDEXPRun(_dexpId, c.Enabled ? 1 : 0);
    }

    private unsafe void DestroyDexpLocked()
    {
        if (_dexpCreated)
        {
            NativeMethods.SetDEXPRun(_dexpId, 0);
            NativeMethods.destroy_dexp(_dexpId);
            _dexpCreated = false;
        }
        ReleaseDexpSlot(_dexpId);
        _dexpId = -1;
        if (_dexpBuf != null)
        {
            NativeMemory.AlignedFree(_dexpBuf);
            _dexpBuf = null;
        }
        _dexpSize = 0;
        _dexpRateHz = 0;
        _dexpActive = false;
    }

    /// <summary>
    /// Run the expander over one mic block. Audio thread.
    /// </summary>
    /// <remarks>
    /// Takes _dexpLock, which the audio thread would normally avoid — but
    /// the alternative is racing destroy_dexp, and the lock is uncontended
    /// except on the rare control-thread reconfigure. The volatile
    /// _dexpActive read above it means a disabled expander never gets here.
    /// </remarks>
    private unsafe bool TryRunDexp(ReadOnlySpan<float> mic, Span<float> dst, int inSize)
    {
        if (!_dexpActive) return false;
        lock (_dexpLock)
        {
            if (!_dexpCreated || _dexpBuf == null || _dexpSize != inSize) return false;
            double* b = _dexpBuf;
            for (int i = 0; i < inSize; i++)
            {
                b[2 * i + 0] = mic[i];
                b[2 * i + 1] = 0.0;
            }
            NativeMethods.xdexp(_dexpId);
            for (int i = 0; i < inSize; i++) dst[i] = (float)b[2 * i + 0];
        }
        return true;
    }

    public bool TryGetEqDraw(bool transmit, int channelId, Span<double> x, Span<double> y)
    {
        if (_disposed != 0) return false;
        if (x.Length < GraphicEqConfig.DrawPoints || y.Length < GraphicEqConfig.DrawPoints)
            return false;

        int id;
        if (transmit)
        {
            int? txa;
            lock (_txaLock) txa = _txaChannelId;
            if (txa is not int t) return false;
            id = t;
        }
        else if (_channels.TryGetValue(channelId, out var state) && !state.Stopped)
        {
            id = state.Id;
        }
        else
        {
            return false;
        }

        // GetXXXEQDraw memcpys `upts` doubles and never reports the count;
        // upts is 1024 from create_nurbs (eq.c:64) and nothing mutates it.
        // The length guard above is what keeps that from being a heap
        // overwrite if the constant and the native ever disagree.
        // Same lock as the profile setters: these read the stage's F/G/Q,
        // which a concurrent profile write frees and replaces.
        lock (_eqLock)
        {
            unsafe
            {
                fixed (double* px = x)
                fixed (double* py = y)
                {
                    if (transmit) NativeMethods.GetTXAEQDraw(id, px, py);
                    else NativeMethods.GetRXAEQDraw(id, px, py);
                }
            }
        }
        return true;
    }

    public int ProcessTxBlock(ReadOnlySpan<float> micMono, Span<float> iqInterleaved)
    {
        if (_disposed != 0) return 0;
        int inSize = _txaInSize;
        int outSize = _txaOutSize;
        if (micMono.Length != inSize)
            throw new ArgumentException($"expected mic span of {inSize}", nameof(micMono));
        if (iqInterleaved.Length != 2 * outSize)
            throw new ArgumentException($"expected iq span of {2 * outSize}", nameof(iqInterleaved));

        int txa;
        bool skipTxAudioPlugins;
        lock (_txaLock)
        {
            if (_txaChannelId is not int id) return 0;
            txa = id;
            skipTxAudioPlugins =
                _txDigitalBypass || _txRogerBeepBypass || _txInjectedAudioBypass
                || IsDigitalTxMode(_txCurrentMode);
        }

        // fexchange2 wants mutable refs to the first float of each buffer.
        // For P2, in != out (inSize=512 mic, outSize=2048 IQ). Stack-allocate
        // both — max combined footprint is 512 + 512 + 2048 + 2048 = 5120 floats
        // ≈ 20 KiB, well inside the default stack budget.
        Span<float> iin = stackalloc float[inSize];

        // TX-audio plugin seam. Zeus.Server.Hosting wires this delegate at
        // startup (via SetTxAudioPluginHandler) once PluginManager has
        // surfaced any IAudioPlugin instances. Digital TX bypasses the suite
        // while preserving the installed handler for automatic voice restore.
        // When not in digital bypass, a single volatile read of the handler is
        // performed on the realtime path; in bypass the read is skipped
        // entirely and the mic falls through to the original copy.
        // Plugins see mic-monaural float32 at _txaInputRateHz (48 kHz) at
        // the configured TXA input block size; output buffer is iin, which
        // fexchange2 consumes directly. Bit-identical to "no plugins" when
        // the handler is null or digitally bypassed.
        // The expander runs FIRST, on the raw mic — the seam Thetis's
        // ChannelMaster uses. Ahead of the plugin chain on purpose: a reverb
        // added there would otherwise have its tail expanded away by a gate
        // sitting downstream of it.
        Span<float> dexped = stackalloc float[inSize];
        ReadOnlySpan<float> micStage = TryRunDexp(micMono, dexped, inSize) ? dexped : micMono;

        var pluginHandler = skipTxAudioPlugins ? null : _txAudioPluginHandler;
        if (pluginHandler is null)
        {
            micStage.CopyTo(iin);
        }
        else
        {
            try
            {
                pluginHandler(micStage, iin, inSize, channels: 1, sampleRate: _txaInputRateHz);
            }
            catch (Exception ex)
            {
                // Audio thread: never throw upward. Degrade to pass-through.
                // The handler should never throw, but a buggy plugin or a
                // wrapper bug shouldn't take down TX.
                micStage.CopyTo(iin);
                if (++_txPluginErrLogged <= 4)
                    _log.LogWarning(ex, "wdsp.tx-plugin handler threw (suppressed after 4)");
            }
        }

        Span<float> qin = stackalloc float[inSize];
        qin.Clear();
        Span<float> iout = stackalloc float[outSize];
        Span<float> qout = stackalloc float[outSize];
        // stackalloc spans are NOT guaranteed zero-initialised across all
        // .NET configurations (SkipLocalsInit can elide the zeroing). When
        // TXA is at state=0, fexchange2 returns without writing iout/qout,
        // and we'd otherwise propagate stack garbage downstream — both into
        // the wire IQ ring and (worse) into the TX-monitor RXA channel, where
        // garbage demodulates as audible noise. Clear them so the no-process
        // path is deterministic silence.
        iout.Clear();
        qout.Clear();

        NativeMethods.fexchange2(txa, ref iin[0], ref qin[0], ref iout[0], ref qout[0], out int err);
        if (err != 0 && ++_txFexchangeErrLogged <= 8)
        {
            _log.LogWarning("wdsp.fexchange2 tx err={Err} (suppressed after 8 occurrences)", err);
        }

        float txOutPeak = 0f;
        for (int i = 0; i < outSize; i++)
        {
            iqInterleaved[2 * i] = iout[i];
            iqInterleaved[2 * i + 1] = qout[i];
            float e = iout[i] * iout[i] + qout[i] * qout[i];
            if (e > txOutPeak) txOutPeak = e;
        }
        // Overdrive probe (#559): is the ALC limiting and is the wire IQ railing?
        // outPeak≈1.0 = post-iqc IQ clipping the Int24 wire (splatter). alcGain
        // (meter 14, dB) should go NEGATIVE under overdrive = ALC reducing gain
        // (limiting); ~0 dB while the mic clips = ALC NOT limiting (the bug).
        // Debug-level: kept as a diagnostic but no longer spams ~1 Hz on every
        // TX in a normal run — the meter reads are skipped entirely when the
        // log level isn't enabled.
        if (++_txOverdriveLogCounter % 50 == 0 && _log.IsEnabled(LogLevel.Debug))
        {
            double alcGainDb = NativeMethods.GetTXAMeter(txa, 14);
            double alcPkDb = NativeMethods.GetTXAMeter(txa, 12);
            double micPkDb = NativeMethods.GetTXAMeter(txa, 0);
            _log.LogDebug(
                "wdsp.txOverdrive micPk={Mic:F1}dB alcGain={Alc:F1}dB alcPk={AlcPk:F1}dB outPeak={Out:F3}{Clip}",
                micPkDb, alcGainDb, alcPkDb, Math.Sqrt(txOutPeak),
                txOutPeak >= 0.998 * 0.998 ? " RAIL!" : "");
        }

        // TX Monitor — feed the post-CFIR / post-RSMPOUT IQ (the wire signal
        // about to hit the radio) into the private monitor RXA channel so the
        // operator can hear the actual on-air audio at the TX bandwidth
        // profile. Volatile-bool short-circuit when monitor is off; matches
        // the VST seam pattern above. Float→double conversion is required by
        // the FeedIq contract; stack-allocate to avoid GC pressure on the
        // mic-ingest hot path. Worst case is P2: outSize=2048 → 2 × 2048 ×
        // 8 bytes = 32 KiB on the stack, comfortable under the default budget.
        if (_monitorRequested && _monitorChannelId is int monId)
        {
            Span<double> monIqDouble = stackalloc double[2 * outSize];
            for (int i = 0; i < outSize; i++)
            {
                monIqDouble[2 * i] = iout[i];
                monIqDouble[2 * i + 1] = qout[i];
            }
            FeedIq(monId, monIqDouble);
        }

        // Feed the TX analyzer with the post-CFIR IQ so TryGetTxDisplayPixels
        // Feed the TX analyzer from the WDSP TXA SIPHON (xsiphon position in
        // xtxa, BEFORE iqc/cfir/rsmpout — see siphon.c, TXA.c:586) so the
        // panadapter trace shows the operator's pre-distortion voice spectrum.
        // Pre-fix this used the post-cfir iout/qout output buffer, which is
        // intentionally shaped with anti-IMD content while PS is correcting
        // and renders as visible "splatter" even when the antenna is clean
        // (issue #121). Thetis takes the same tap (cmaster.cs:544-545,
        // TXASetSipMode + TXASetSipDisplay). Sample rate / size match the
        // analyzer config: dsp_rate / dsp_size. Q is still negated to match
        // the WDSP analyzer's sideband convention (same fix as before — the
        // siphon hands back complex IQ in the same orientation as the post-
        // CFIR buffer did).
        if (_txDispAlive)
        {
            int sipSize = _txaDspSize;
            Span<float> sipBuf = stackalloc float[2 * sipSize];
            NativeMethods.TXAGetaSipF1(txa, ref sipBuf[0], sipSize);
            Span<double> txSpectrumIq = stackalloc double[2 * sipSize];
            for (int i = 0; i < sipSize; i++)
            {
                txSpectrumIq[2 * i] = sipBuf[2 * i];
                txSpectrumIq[2 * i + 1] = -sipBuf[2 * i + 1];
            }
            lock (_txDispLock)
            {
                // Re-check under the display lock to close the destroy/feed window.
                if (_txDispAlive && _txaChannelId == txa)
                    NativeMethods.Spectrum0(1, txa, 0, 0, ref txSpectrumIq[0]);
            }
        }

        // Per-stage TXA peak + average meters. Peak surfaces clipping-induced
        // crackle that averages smooth away; the average is what the operator
        // reads to judge level. Both are published so the frontend can show
        // a Thetis-style dual-needle per row. Indices per native/wdsp/TXA.h:49-66
        // txaMeterType:
        //   0  MIC_PK    1  MIC_AV
        //   2  EQ_PK     3  EQ_AV
        //   4  LVLR_PK   5  LVLR_AV   6  LVLR_GAIN
        //   7  CFC_PK    8  CFC_AV    9  CFC_GAIN
        //  10  COMP_PK  11  COMP_AV
        //  12  ALC_PK   13  ALC_AV   14  ALC_GAIN
        //  15  OUT_PK   16  OUT_AV
        double micPk = NativeMethods.GetTXAMeter(txa, 0);
        double micAv = NativeMethods.GetTXAMeter(txa, 1);
        double eqPk = NativeMethods.GetTXAMeter(txa, 2);
        double eqAv = NativeMethods.GetTXAMeter(txa, 3);
        double lvlrPk = NativeMethods.GetTXAMeter(txa, 4);
        double lvlrAv = NativeMethods.GetTXAMeter(txa, 5);
        double lvlrGain = NativeMethods.GetTXAMeter(txa, 6);
        double cfcPk = NativeMethods.GetTXAMeter(txa, 7);
        double cfcAv = NativeMethods.GetTXAMeter(txa, 8);
        double cfcGain = NativeMethods.GetTXAMeter(txa, 9);
        double compPk = NativeMethods.GetTXAMeter(txa, 10);
        double compAv = NativeMethods.GetTXAMeter(txa, 11);
        double alcPk = NativeMethods.GetTXAMeter(txa, 12);
        double alcAv = NativeMethods.GetTXAMeter(txa, 13);
        double alcGain = NativeMethods.GetTXAMeter(txa, 14);
        double outPk = NativeMethods.GetTXAMeter(txa, 15);
        double outAv = NativeMethods.GetTXAMeter(txa, 16);

        // Publish the snapshot before returning so pollers don't see a
        // partially-written set. Lock is uncontended in steady state —
        // ProcessTxBlock runs from the TX ingest thread and GetTxStageMeters
        // only from TxMetersService (10 Hz).
        // *Gain readings from WDSP are 20*log10(linear_gain) ≤ 0 when
        // reducing. Store as positive "gain reduction" dB per TxStageMeters
        // convention.
        var snap = new TxStageMeters(
            MicPk: (float)micPk,
            MicAv: (float)micAv,
            EqPk: (float)eqPk,
            EqAv: (float)eqAv,
            LvlrPk: (float)lvlrPk,
            LvlrAv: (float)lvlrAv,
            LvlrGr: (float)-lvlrGain,
            CfcPk: (float)cfcPk,
            CfcAv: (float)cfcAv,
            CfcGr: (float)-cfcGain,
            CompPk: (float)compPk,
            CompAv: (float)compAv,
            AlcPk: (float)alcPk,
            AlcAv: (float)alcAv,
            AlcGr: (float)-alcGain,
            OutPk: (float)outPk,
            OutAv: (float)outAv);
        lock (_txMeterPublishLock) { _latestTxStageMeters = snap; }

        var now = DateTime.UtcNow;
        if (now - _lastTxMeterLogUtc >= TimeSpan.FromSeconds(1))
        {
            _lastTxMeterLogUtc = now;
            double micBlockPeak = 0, ioutPeak = 0;
            for (int i = 0; i < inSize; i++)
            {
                double m = Math.Abs(iin[i]); if (m > micBlockPeak) micBlockPeak = m;
            }
            for (int i = 0; i < outSize; i++)
            {
                double oi = Math.Abs(iout[i]); double oq = Math.Abs(qout[i]);
                double ma = Math.Max(oi, oq); if (ma > ioutPeak) ioutPeak = ma;
            }
            _log.LogInformation(
                "wdsp.tx.stage micBlockPeak={MP:F3} iqBlockPeak={IP:F4} | mic pk={MicPk:F1} av={MicAv:F1} | eq pk={EqPk:F1} av={EqAv:F1} | lvlr pk={LvlrPk:F1} av={LvlrAv:F1} gr={LvlrGr:F1} | cfc pk={CfcPk:F1} av={CfcAv:F1} gr={CfcGr:F1} | alc pk={AlcPk:F1} av={AlcAv:F1} gr={AlcGr:F1} | out pk={OutPk:F1} av={OutAv:F1}",
                micBlockPeak, ioutPeak,
                micPk, micAv, eqPk, eqAv,
                lvlrPk, lvlrAv, -lvlrGain,
                cfcPk, cfcAv, -cfcGain,
                alcPk, alcAv, -alcGain,
                outPk, outAv);
        }
        return outSize;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        foreach (var key in _channels.Keys.ToArray())
        {
            if (_channels.TryRemove(key, out var state))
                StopChannel(state);
        }
        lock (_psLock)
        {
            ClosePsFeedbackAnalyzer();
        }
        lock (_txaLock)
        {
            if (_txaChannelId is int txa)
            {
                try
                {
                    if (_txaNativeOwned)
                    {
                        lock (_txDispLock)
                        {
                            if (_txDispAlive)
                            {
                                NativeMethods.DestroyAnalyzer(txa);
                            }
                            _txDispAlive = false;
                            _txDispPixelWidth = 0;
                            _txDispUsedPixelWidth = 0;
                            _txDispRxSampleRateHz = 0;
                            _txDispZoomLevel = 1;
                            _txDispScratchPixels = null;
                        }
                        RunNativeLifecycleCriticalSection(() => NativeMethods.CloseChannel(txa));
                    }
                }
                finally
                {
                    if (_txaNativeOwned)
                        ReleaseNativeSlot(txa);
                    _txaChannelId = null;
                    _txaNativeOwned = false;
                    // The expander outlives the TXA channel otherwise: WDSP
                    // keeps it in a global, and its buffer is ours to free.
                    lock (_dexpLock) DestroyDexpLocked();
                }
            }
        }
    }

    // Thetis-style log-recursive EMA on both pan and wf outputs. `tauSec` is
    // the visual smoothing time constant; with PipelineFps ticks/s the per-tick
    // retention is `exp(-1 / (fps * tau))`. Default 100 ms reads as "smooth
    // but still alive" — heavy enough to kill the per-frame jumpiness the
    // user called out, light enough that signals still pop.
    private const int PipelineFps = 30;
    private const double DefaultAvgTauSec = 0.100;
    // Heavier smoothing on TX-side traces. Voice modulation through the
    // operator's leveler/compressor/ALC has natural envelope dynamics that
    // a 100 ms tau renders as visible "splatter spreading"; 0.5 s gives the
    // Thetis-style smoothed envelope so the operator sees signal shape, not
    // every voiced/unvoiced transition.
    private const double TxAvgTauSec = 0.175;
    // Issue #597 Phase 0: retune fast-attack tau. ~20 ms at 30 fps gives
    // backmult ≈ exp(-1/(30·0.02)) ≈ 0.19 — the newest frame dominates, so
    // post-retune content settles in ~3 ticks (~100 ms) instead of the
    // 300-400 ms melt the 100 ms default produces. Mirrors Thetis's
    // fast-attack after a display center change (display.cs:6360-6383).
    private const double FastAttackTauSec = 0.020;
    private const int LogRecursiveMode = 3;
    private const int LinearRecursiveMode = 1;
    private const int AveragePowerDetectorMode = 2;
    private const double SnrPowerAvgTauSec = 0.750;

    private static void ConfigureDisplayAveraging(int disp)
        => ConfigureDisplayAveragingTau(disp, DefaultAvgTauSec);

    private static void ConfigureDisplayAveragingTau(int disp, double tauSec)
    {
        // tau=0 is the operator-facing "no smoothing" setting. A zero
        // recursive multiplier makes each output use the current frame only.
        double backmult = tauSec <= 0.0
            ? 0.0
            : Math.Exp(-1.0 / (PipelineFps * tauSec));
        for (int pixout = 0; pixout < 2; pixout++)
        {
            NativeMethods.SetDisplayAverageMode(disp, pixout, LogRecursiveMode);
            NativeMethods.SetDisplayAvBackmult(disp, pixout, backmult);
            NativeMethods.SetDisplayNumAverage(disp, pixout, 2);
        }
    }

    private static void ConfigureSnrPowerOutput(int disp, int sampleRateHz)
    {
        int pixout = (int)DisplayPixout.SnrPower;
        double backmult = Math.Exp(-1.0 / (PipelineFps * SnrPowerAvgTauSec));
        NativeMethods.SetDisplayDetectorMode(disp, pixout, AveragePowerDetectorMode);
        NativeMethods.SetDisplayAverageMode(disp, pixout, LinearRecursiveMode);
        NativeMethods.SetDisplayAvBackmult(disp, pixout, backmult);
        NativeMethods.SetDisplayNumAverage(disp, pixout, 2);
        // WDSP's analyzer starts with sample_rate=0; NormOneHz divides by it.
        // The rate must be installed before enabling one-hertz normalization.
        NativeMethods.SetDisplaySampleRate(disp, sampleRateHz);
        NativeMethods.SetDisplayNormOneHz(disp, pixout, 1);
    }

    private static void ConfigureSnrAnalyzer(int disp, int sampleRateHz)
    {
        WdspWisdomInitializer.WaitUntilReady();
        int overlap = (int)Math.Max(0,
            Math.Ceiling(AnalyzerFftSize - (double)sampleRateHz / AnalyzerFps));
        int maxW = AnalyzerFftSize + (int)Math.Min(
            AnalyzerKeepTime * sampleRateHz,
            AnalyzerKeepTime * AnalyzerFftSize * AnalyzerFps);
        int flp = 0;
        RunNativeLifecycleCriticalSection(() =>
        {
            NativeMethods.SetAnalyzer(
                disp, 3, 1, 1, ref flp, AnalyzerFftSize, InSize,
                AnalyzerWindow, AnalyzerKaiserPi, overlap, 0, 0.0, 0.0,
                SnrAnalyzerPixelWidth, 1, 0, 0.0, 0.0, maxW);
            ConfigureSnrPowerOutput(disp, sampleRateHz);
        });
    }

    // TX analyzer wrapper: maps the RX display span onto the TX/PS-feedback
    // analyzer source rate. Legacy txRate >= rxRate pairs keep the exact old
    // integer "effective zoom" clip math: P2 TX at 192 kHz vs RX at 48 kHz is
    // a 4x rate ratio; RX zoom=1 becomes effective TX zoom=4, clipping 3/8 x
    // fft_size bins off each side. When RX is wider than the TX source but is
    // still an integer multiple, WDSP can either show a centred fractional-bin
    // crop (RX zoom at least the rate ratio) or render the whole TX span into a
    // narrower pixel width that the managed read path pads back to the RX width.
    // Non-integer rate pairs still cannot be mapped without lying about the
    // axis, so callers skip the analyzer and fall back to another display
    // source.
    private static bool TryConfigureTxAnalyzer(int disp, int txSampleRateHz, int txBlockSize, int rxSampleRateHz, int pixelWidth, int rxZoomLevel,
        int fftSize, int winType, double piAlpha, out int configuredPixelWidth)
    {
        configuredPixelWidth = 0;
        if (!TryComputeTxAnalyzerGeometry(
            txSampleRateHz, rxSampleRateHz, rxZoomLevel, pixelWidth, fftSize,
            out double clipPerSide, out int nPix))
        {
            return false;
        }

        // bf_sz must match the per-Spectrum0 block size fed from ProcessTxBlock
        // (_txaOutSize: 1024 on P1, 2048 on P2). Hardcoding InSize left WDSP
        // reading only the first 1024 of each 2048-sample P2 block, aliasing at
        // (192000/1024) ≈ 188 Hz and producing a spur comb on the TUN carrier.
        ConfigureAnalyzer(disp, txSampleRateHz, txBlockSize, clipPerSide, nPix, fftSize, winType, piAlpha);
        configuredPixelWidth = nPix;
        return true;
    }

    internal static bool TryComputeTxAnalyzerGeometry(
        int txRate,
        int rxRate,
        int rxZoom,
        int pixelWidth,
        int fftSize,
        out double clipPerSide,
        out int nPix)
    {
        clipPerSide = 0.0;
        nPix = 0;
        if (txRate <= 0 || rxRate <= 0 || rxZoom <= 0 || pixelWidth <= 0 || fftSize <= 0)
            return false;

        if (txRate >= rxRate)
        {
            if (txRate % rxRate != 0)
                return false;

            int effectiveZoom = rxZoom * (txRate / rxRate);
            if (effectiveZoom > 1)
            {
                int clippedPerSide = fftSize * (effectiveZoom - 1) / (2 * effectiveZoom);
                clipPerSide = clippedPerSide;
            }
            nPix = pixelWidth;
            return true;
        }

        if (rxRate % txRate != 0)
            return false;

        int rateRatio = rxRate / txRate;
        if (rxZoom >= rateRatio)
        {
            clipPerSide = fftSize * (1.0 - (double)rateRatio / rxZoom) / 2.0;
            nPix = pixelWidth;
            return true;
        }

        int roundedWidth = (int)Math.Round(
            pixelWidth * (double)rxZoom / rateRatio,
            MidpointRounding.AwayFromZero);
        nPix = ClampAnalyzerPixelWidth(roundedWidth, pixelWidth);
        return true;
    }

    private static int ClampAnalyzerPixelWidth(int value, int pixelWidth)
    {
        int min = Math.Min(2, pixelWidth);
        if (value < min) return min;
        if (value > pixelWidth) return pixelWidth;
        return value;
    }

    // fftSize / winType / piAlpha let the TX display analyzer be reconfigured at
    // runtime (live TX waterfall feature); RX callers pass the historical
    // AnalyzerFftSize / AnalyzerWindow / AnalyzerKaiserPi constants so RX
    // behaviour is byte-identical.
    private static void ConfigureAnalyzer(int disp, int sampleRateHz, int bfSize, int pixelWidth, int zoomLevel,
        int fftSize, int winType, double piAlpha)
    {
        // fscLin/fscHin are integer bin counts to clip from the LOW and HIGH
        // ends of the full-span FFT output (analyzer.c:1253-1254, PanDisplay.cs
        // :4720-4726 in Thetis). For a centred zoom by factor L, keep
        // fft_size/L bins in the middle and clip (fft_size - fft_size/L)/2
        // from each side. At L=1 both clips are 0 (full span).
        double fscLin = 0.0, fscHin = 0.0;
        if (zoomLevel > 1)
        {
            int clippedPerSide = fftSize * (zoomLevel - 1) / (2 * zoomLevel);
            fscLin = clippedPerSide;
            fscHin = clippedPerSide;
        }
        ConfigureAnalyzer(disp, sampleRateHz, bfSize, fscLin, pixelWidth, fftSize, winType, piAlpha);
    }

    private static void ConfigureAnalyzer(int disp, int sampleRateHz, int bfSize, double clipPerSide, int nPix,
        int fftSize, int winType, double piAlpha)
    {
        WdspWisdomInitializer.WaitUntilReady();
        int overlap = (int)Math.Max(0, Math.Ceiling(fftSize - (double)sampleRateHz / AnalyzerFps));
        int maxW = fftSize + (int)Math.Min(
            AnalyzerKeepTime * sampleRateHz,
            AnalyzerKeepTime * fftSize * AnalyzerFps);
        int flp = 0;

        RunNativeLifecycleCriticalSection(() =>
        {
            NativeMethods.SetAnalyzer(
                disp: disp,
                n_pixout: 2,
                n_fft: 1,
                typ: 1,
                flp: ref flp,
                sz: fftSize,
                bf_sz: bfSize,
                win_type: winType,
                pi_alpha: piAlpha,
                ovrlp: overlap,
                clp: 0,
                fscLin: clipPerSide,
                fscHin: clipPerSide,
                n_pix: nPix,
                n_stch: 1,
                calset: 0,
                fmin: 0.0,
                fmax: 0.0,
                max_w: maxW);
            // SetAnalyzer resets ring/IQ state but preserves pixel averaging;
            // purge the old frequency mapping as Thetis does after reconfigure
            // (clsSpectrumProcessor.cs:1007).
            NativeMethods.ResetPixelBuffers(disp);
        });
    }

    private void StopChannel(ChannelState state)
    {
        try
        {
            state.Stopped = true;
            state.InQueue.CompleteAdding();
            state.Cts.Cancel();
            if (!state.Worker.Join(TimeSpan.FromSeconds(2)))
            {
                // Worker did not exit in time; fall through to teardown anyway.
            }
            state.InQueue.Dispose();
            state.Cts.Dispose();
            // Serialize native teardown against the display pixel-drain
            // (TryGetDisplayPixels → GetPixels), which reads this analyzer under the
            // same lock on the DspPipelineService tick thread. state.Stopped is set
            // at the top of StopChannel and re-checked by the drain under this lock,
            // so once we hold it the drain has either already finished or will
            // early-out instead of touching a freed WDSP slot (fixes 0xc0000005 on
            // disconnect / sample-rate rebuild). The worker thread was joined above,
            // so it can no longer contend this lock (no deadlock).
            lock (state.AnalyzerLock)
            {
                RunNativeLifecycleCriticalSection(() =>
                {
                    NativeMethods.DestroyAnalyzer(state.Id);
                    NativeMethods.DestroyAnalyzer(state.SnrAnalyzerId);
                    // Tear down EXT blankers before CloseChannel — they reference our id
                    // slot in panb[]/pnob[] and outlive CloseChannel unless destroyed here.
                    NativeMethods.DestroyAnbEXT(state.Id);
                    NativeMethods.DestroyNobEXT(state.Id);
                    NativeMethods.CloseChannel(state.Id);
                });
            }
        }
        finally
        {
            ReleaseNativeSlot(state.Id);
        }
    }

    private void RunWorker(ChannelState state)
    {
        if (OperatingSystem.IsWindows())
            RealtimeThreadPriority.PromoteCallingThreadToProAudio(_log);

        double[] audio = new double[state.OutDoubles];
        double[] spectrumIq = new double[2 * InSize];
        int monoSamples = state.OutDoubles / 2;
        try
        {
            foreach (var frame in state.InQueue.GetConsumingEnumerable(state.Cts.Token))
            {
                // TEMP diag (zeus-gdc7): time the per-frame WDSP work so we can
                // tell whether the worker is the bottleneck (slow fexchange0 /
                // Spectrum0 → queue fills → RX net thread blocks) or whether the
                // queue stays shallow and the stall is on the consumer/tick side.
                long frameStart = System.Diagnostics.Stopwatch.GetTimestamp();
                // Pre-RXA blanker. In-place is safe: xanb/xnob read a->in[i]
                // before writing a->out[i] within each iteration, so same-buffer
                // aliasing doesn't clobber unread samples. Skipped entirely when
                // both NBs are off so there's no WDSP call overhead in the common
                // path. Non-enabled side stays at Run=0, so even if the mode
                // changes mid-frame its xanb/xnob is a no-op pass-through.
                switch (state.CurrentNbMode)
                {
                    case NbMode.Nb1:
                        NativeMethods.XanbEXT(state.Id, ref frame[0], ref frame[0]);
                        break;
                    case NbMode.Nb2:
                        NativeMethods.XnobEXT(state.Id, ref frame[0], ref frame[0]);
                        break;
                }

                NativeMethods.fexchange0(
                    state.Id,
                    ref frame[0],
                    ref audio[0],
                    out _);
                // Deliver audio to the ring BEFORE taking AnalyzerLock. PushAudio
                // only touches AudioGate + audio[]; it has no dependency on
                // Spectrum0. Keeping it here means a SetZoom / SetRxDisplayFastAttack
                // holding AnalyzerLock (heavy SetAnalyzer rebuild on zoom, tau
                // reconfig on pan) cannot stall audio delivery — the ring keeps
                // draining while the worker waits its turn to write spectrum.
                PushAudio(state, audio, monoSamples);
                // Empirical fix for HL2 panadapter sideband mirror: conjugate the
                // IQ stream fed to the analyzer (I unchanged, Q negated). Audio
                // path keeps the original IQ so demod stays correct. Without this
                // the displayed spectrum appears flipped about the carrier (USB
                // energy shows left of carrier, LSB shows right) despite audio
                // and the synthetic-IQ orientation test both being correct.
                for (int i = 0; i < frame.Length; i += 2)
                {
                    spectrumIq[i] = frame[i];
                    spectrumIq[i + 1] = -frame[i + 1];
                }
                // Analyzer input side: paired with GetPixels under the same
                // lock, so SetZoom can rebuild bin mapping without a half-
                // written state being observed.
                lock (state.AnalyzerLock)
                {
                    NativeMethods.Spectrum0(state.SpectrumRun, state.Id, 0, 0, ref spectrumIq[0]);
                    NativeMethods.Spectrum0(state.SpectrumRun, state.SnrAnalyzerId, 0, 0, ref spectrumIq[0]);
                }
                // The analyzer now has a snapped pixel buffer; GetPixels is safe.
                state.AnalyzerHasSnapped = true;
                state.SnrAnalyzerHasSnapped = true;
                state.FreeFrames.Enqueue(frame);

                long frameTicks = System.Diagnostics.Stopwatch.GetTimestamp() - frameStart;
                state.DiagWorkerFrames++;
                state.DiagWorkerTotalTicks += frameTicks;
                if (frameTicks > state.DiagWorkerMaxTicks) state.DiagWorkerMaxTicks = frameTicks;

                // Latch per-DDC ingest health from the worker (the single thread
                // that processes this channel's IQ). Self-gated to ~1 Hz. Doing it
                // here rather than in ReadAudio means every fed receiver reports
                // health — including RX3+ whose audio isn't read out — which is
                // the realtime overflow/underrun signal for multi-DDC operation.
                EmitRxDiag(state);
            }
        }
        catch (OperationCanceledException) { }
        catch (InvalidOperationException) { }
    }

    // Mirror Thetis radio.cs's NB property-setter behaviour: after create_*EXT
    // seeds the struct, the setters immediately overwrite the knob state and
    // run initBlanker / init_nob once. Keeping this as a discrete config block
    // means an advanced-NB panel can reuse the same setter path with
    // user-supplied values rather than introducing a second code path.
    private static void ApplyNbDefaults(int id)
    {
        NativeMethods.SetEXTANBTau(id, NrDefaults.NbTau);
        NativeMethods.SetEXTANBHangtime(id, NrDefaults.NbHangtime);
        NativeMethods.SetEXTANBAdvtime(id, NrDefaults.NbAdvtime);
        NativeMethods.SetEXTANBBacktau(id, NrDefaults.NbBacktau);
        NativeMethods.SetEXTANBThreshold(id, NrDefaults.NbDefaultThresholdScaled);

        NativeMethods.SetEXTNOBMode(id, 0);
        NativeMethods.SetEXTNOBTau(id, NrDefaults.NbTau);
        NativeMethods.SetEXTNOBHangtime(id, NrDefaults.NbHangtime);
        NativeMethods.SetEXTNOBAdvtime(id, NrDefaults.NbAdvtime);
        NativeMethods.SetEXTNOBBacktau(id, NrDefaults.NbBacktau);
        NativeMethods.SetEXTNOBThreshold(id, NrDefaults.NbDefaultThresholdScaled);
    }

    // Applies Thetis AGC_MEDIUM defaults — the mode all HL2 users start on.
    // Without this, WDSP's AGC is off and the audio path has effectively
    // unity gain on signals with peak ~2e-5, which is inaudible.
    //
    // Routes through the shared ApplyAgcCore path so SetAgc and channel-open
    // produce identical WDSP state for the same AgcConfig. The max-gain (top)
    // stays separate — it has its own SetAgcTop / auto-AGC path — so we set the
    // 80 dB open-time baseline here, NOT inside ApplyAgcCore.
    private static void ApplyAgcDefaults(int id)
    {
        ApplyAgcCore(id, new AgcConfig(AgcMode.Med));
        NativeMethods.SetRXAAGCTop(id, 90.0);            // max gain, dB (Thetis radio.cs:1021 default)
    }

    // Canned-mode presets, verbatim from Thetis console.cs:27958-28024. Hang/Decay
    // in ms. HangThreshold: MED/FAST hard-set 100 (slider disabled); LONG/SLOW
    // leave it at the Thetis slider/init default (0). Custom returns the Med
    // baseline so a null custom field falls back somewhere sane.
    private static (int HangMs, int DecayMs, int HangThreshold) AgcPreset(AgcMode mode) => mode switch
    {
        AgcMode.Long => (2000, 2000, 0),
        AgcMode.Slow => (1000, 500, 0),
        AgcMode.Med => (0, 250, 100),
        AgcMode.Fast => (0, 50, 100),
        AgcMode.Fixed => (0, 250, 100),
        _ => (0, 250, 100),
    };

    // Pushes the AGC mode + custom/fixed params to WDSP. Shared by ApplyAgcDefaults
    // (channel open, before the ChannelState is registered) and SetAgc (runtime),
    // so both paths produce identical WDSP state. Does NOT touch SetRXAAGCTop —
    // the max-gain has its own path. Attack is the WDSP create-time default of
    // 1 ms (RXA.c: tau_attack = 0.001): Thetis NEVER calls SetRXAAGCAttack, so it
    // runs every mode at the 1 ms default — its "Attack 2ms" UI tooltip is label
    // text the code never applies. We set it explicitly to 1 to match exactly.
    private static void ApplyAgcCore(int id, AgcConfig cfg)
    {
        NativeMethods.SetRXAAGCMode(id, (int)cfg.Mode);
        NativeMethods.SetRXAAGCAttack(id, 1);

        int hangMs, decayMs, hangThreshold;
        if (cfg.Mode == AgcMode.Custom)
        {
            hangMs = cfg.HangMs ?? 250;
            decayMs = cfg.DecayMs ?? 250;
            hangThreshold = cfg.HangThreshold ?? 0;
        }
        else
        {
            (hangMs, decayMs, hangThreshold) = AgcPreset(cfg.Mode);
        }
        NativeMethods.SetRXAAGCHang(id, hangMs);
        NativeMethods.SetRXAAGCDecay(id, decayMs);
        NativeMethods.SetRXAAGCHangThreshold(id, hangThreshold);

        // Thetis's default slope is 0 (radio.cs:1110 rx_agc_slope; the
        // udDSPAGCSlope UI default is 0) and the canned-mode switch never sets
        // it, so every non-custom mode runs at slope 0 — a flat-output AGC that
        // holds all signals at the same loudness. Custom applies the operator
        // slope ×10 (Thetis setup.cs:9088). The old hard-coded 35 made every
        // mode let stronger signals stay louder, which is NOT Thetis behaviour.
        int slope = cfg.Mode == AgcMode.Custom ? (cfg.Slope ?? 0) * 10 : 0;
        NativeMethods.SetRXAAGCSlope(id, slope);

        if (cfg.Mode == AgcMode.Fixed)
            NativeMethods.SetRXAAGCFixed(id, cfg.FixedGainDb ?? 20.0);
    }

    // RX squelch tau / max-tail baselines (Thetis defaults, §5.2). Set once at
    // channel open and not operator-exposed in v1; the run + threshold are
    // driven mode-aware by ApplySquelchLocked. Runs all stages off so a fresh
    // channel is silent-squelch-free until the operator enables it.
    private static void ApplySquelchDefaults(int id)
    {
        NativeMethods.SetRXASSQLTauMute(id, 0.1);
        NativeMethods.SetRXASSQLTauUnMute(id, 0.1);
        NativeMethods.SetRXAAMSQMaxTail(id, 1.5);
        NativeMethods.SetRXASSQLRun(id, 0);
        NativeMethods.SetRXAAMSQRun(id, 0);
        NativeMethods.SetRXAFMSQRun(id, 0);
    }

    // Fixed squelch is mode-aware (Thetis §5.3): exactly one WDSP stage runs
    // based on the channel's current RX mode, the other two are forced off.
    // Adaptive squelch is handled later in DspPipelineService from the live
    // S-meter/noise floor, so WDSP fixed stages stay off when Adaptive=true.
    // Fixed mapping keeps the useful part of the slider in range while keeping
    // the low end sensitive. Level 0 is treated as fully open/pass-through;
    // above that, fixedSensitivity reshapes the curve so the operator can tune
    // how easily weak/moderate signals open the fixed WDSP squelch.
    // - SSQL: 0..0.32, shaped so low levels stay permissive
    // - AMSQ: -150..-50 dB, shaped so low levels remain very permissive
    // - FMSQ: 1.0..0.2 noise threshold, inverted so higher = tighter
    // Called from SetSquelch AND SetMode so a mode change re-asserts the
    // fixed squelch on the new stage and clears the old.
    private static void ApplySquelchLocked(ChannelState state)
    {
        int id = state.Id;
        // All squelch decisions (mode resolution, run flag, stage, threshold)
        // come from the pure PlanFixedSquelch so there is no un-resolved path
        // here to regress (issue #2101). This method is only native dispatch:
        // everything off first, then the owning stage on.
        var plan = PlanFixedSquelch(state.CurrentSquelch, MapRxaToRxMode(state.CurrentMode));

        switch (plan.Stage)
        {
            case FixedSquelchStage.Amsq:
                NativeMethods.SetRXASSQLRun(id, 0);
                NativeMethods.SetRXAFMSQRun(id, 0);
                NativeMethods.SetRXAAMSQThreshold(id, plan.Threshold);
                NativeMethods.SetRXAAMSQRun(id, plan.Run);
                break;
            case FixedSquelchStage.Fmsq:
                NativeMethods.SetRXASSQLRun(id, 0);
                NativeMethods.SetRXAAMSQRun(id, 0);
                NativeMethods.SetRXAFMSQThreshold(id, plan.Threshold);
                NativeMethods.SetRXAFMSQRun(id, plan.Run);
                break;
            default:
                NativeMethods.SetRXAAMSQRun(id, 0);
                NativeMethods.SetRXAFMSQRun(id, 0);
                NativeMethods.SetRXASSQLThreshold(id, plan.Threshold);
                NativeMethods.SetRXASSQLRun(id, plan.Run);
                break;
        }
    }

    private const double FixedSquelchMinCurve = 0.65;

    // Which native fixed-squelch stage owns a given RX mode.
    internal enum FixedSquelchStage { Ssql, Amsq, Fmsq }

    // The full fixed-squelch decision ApplySquelchLocked dispatches on. Kept as
    // a pure, testable function so the production call site (not just the ForMode
    // seam) is covered: reverting the mode resolution here drops FMSQ on FM and
    // turns the unit test red.
    internal readonly record struct FixedSquelchPlan(FixedSquelchStage Stage, int Run, double Threshold);

    // Resolve the effective config for the channel's mode, then pick the owning
    // stage, run flag, and threshold. FM always uses the fixed FMSQ noise
    // detector regardless of the stored Adaptive preference (issue #2101) — a
    // web client that left Adaptive=true still gets a working FM squelch — while
    // SSB/AM keep the operator's choice. SSB/CW/DIG/DSB families all map to SSQL.
    internal static FixedSquelchPlan PlanFixedSquelch(SquelchConfig currentSquelch, RxMode mode)
    {
        var cfg = ResolveFixedSquelchForMode(currentSquelch, mode);
        int level = Math.Clamp(cfg.Level, 0, 100);
        int run = ShouldRunFixedSquelch(cfg) ? 1 : 0;
        return mode switch
        {
            RxMode.AM or RxMode.SAM =>
                new(FixedSquelchStage.Amsq, run, MapFixedAmsqThresholdDb(level, cfg.FixedSensitivity)),
            RxMode.FM =>
                new(FixedSquelchStage.Fmsq, run, MapFixedFmsqThreshold(level, cfg.FixedSensitivity)),
            _ =>
                new(FixedSquelchStage.Ssql, run, MapFixedSsqlThreshold(level, cfg.FixedSensitivity)),
        };
    }

    // The mode resolution ApplySquelchLocked feeds to the native fixed stages:
    // FM forces the fixed FMSQ detector regardless of the stored Adaptive
    // preference (issue #2101), while SSB/AM keep the operator's choice. Exposed
    // internally so tests exercise this seam instead of re-deriving ForMode.
    internal static SquelchConfig ResolveFixedSquelchForMode(SquelchConfig currentSquelch, RxMode mode) =>
        currentSquelch.ForMode(mode);

    internal static bool ShouldRunFixedSquelch(SquelchConfig cfg) =>
        cfg.Enabled && !cfg.Adaptive && Math.Clamp(cfg.Level, 0, 100) > 0;

    internal static double MapFixedSsqlThreshold(
        int level,
        int fixedSensitivity = SquelchConfig.DefaultFixedSensitivity)
    {
        double t = FixedSquelchLevel(level);
        return Math.Pow(t, FixedSquelchCurve(fixedSensitivity)) * 0.32;
    }

    internal static double MapFixedAmsqThresholdDb(
        int level,
        int fixedSensitivity = SquelchConfig.DefaultFixedSensitivity)
    {
        double t = FixedSquelchLevel(level);
        return -150.0 + Math.Pow(t, FixedSquelchCurve(fixedSensitivity)) * 100.0;
    }

    internal static double MapFixedFmsqThreshold(
        int level,
        int fixedSensitivity = SquelchConfig.DefaultFixedSensitivity)
    {
        double t = FixedSquelchLevel(level);
        return 1.0 - Math.Pow(t, FixedSquelchCurve(fixedSensitivity)) * 0.8;
    }

    private static double FixedSquelchLevel(int level) =>
        Math.Clamp(level, 0, 100) / 100.0;

    private static double FixedSquelchSensitivity(int fixedSensitivity) =>
        Math.Clamp(
            fixedSensitivity,
            SquelchConfig.MinFixedSensitivity,
            SquelchConfig.MaxFixedSensitivity) / 100.0;

    private static double FixedSquelchCurve(int fixedSensitivity) =>
        FixedSquelchMinCurve + FixedSquelchSensitivity(fixedSensitivity);

    // WDSP bandpass takes signed frequencies: LSB-family modes live in negative
    // baseband (low=-high, high=-low), USB-family in positive. CW follows the
    // USB/LSB convention per its suffix. Other modes keep unsigned bounds since
    // their passbands span zero.
    private static (double low, double high) SignedBandpassForMode(
        RxaMode mode,
        int lowAbsHz,
        int highAbsHz)
    {
        int lo = Math.Min(Math.Abs(lowAbsHz), Math.Abs(highAbsHz));
        int hi = Math.Max(Math.Abs(lowAbsHz), Math.Abs(highAbsHz));
        double low, high;
        switch (mode)
        {
            case RxaMode.LSB:
            case RxaMode.CWL:
            case RxaMode.DIGL:
                low = -hi; high = -lo; break;
            case RxaMode.USB:
            case RxaMode.CWU:
            case RxaMode.DIGU:
                low = lo; high = hi; break;
            default:
                // AM/SAM/DSB/FM/DRM/SPEC: symmetric around 0.
                low = -hi; high = hi; break;
        }
        // Last-line guard against a zero/sub-floor-width passband reaching WDSP
        // (issue #1028). A centre-zero filter that was clamped to a tiny signed
        // width upstream can still abs-fold back to low == high here, which WDSP
        // accepts and then passes nothing through — silent receiver, no error
        // path anywhere. See FloorPassbandWidth.
        return FloorPassbandWidth(low, high);
    }

    private static void ApplyBandpassForMode(ChannelState state)
    {
        lock (state.FilterProfileGate)
        {
            var (low, high) = SignedBandpassForMode(
                state.CurrentMode,
                state.FilterLowAbsHz,
                state.FilterHighAbsHz);
            // Thetis rxa.cs:110-124: every filter change updates all three stages.
            // SetRXABandpassFreqs alone only affects bp1, which is bypassed for SSB.
            // nbp0 (RXANBPSetFreqs) is what actually carries the SSB passband.
            // In minimum-phase mode these setters can regenerate an impulse and
            // create FFTW plans, so share the process-wide planner lifecycle gate.
            RunNativeLifecycleCriticalSection(() =>
            {
                NativeMethods.SetRXABandpassFreqs(state.Id, low, high);
                NativeMethods.RXANBPSetFreqs(state.Id, low, high);
                NativeMethods.SetRXASNBAOutputBandwidth(state.Id, low, high);
            });
        }
    }

    private static void ConfigureMaxBinDetector(ChannelState state)
    {
        var (low, high) = SignedBandpassForMode(
            state.CurrentMode,
            state.FilterLowAbsHz,
            state.FilterHighAbsHz);
        low += state.CtunShiftHz;
        high += state.CtunShiftHz;

        // Keep SetupDetectMaxBin in the same C# call stream as Spectrum0,
        // SetAnalyzer, and analyzer teardown. WDSP owns synchronization with
        // its asynchronous FFT workers; this lock prevents overlapping managed
        // reconfiguration calls or a call into a display slot being destroyed.
        lock (state.AnalyzerLock)
        {
            NativeMethods.SetupDetectMaxBin(
                run: 1,
                disp: state.Id,
                ss: 0,
                lo: 0,
                rate: state.SampleRateHz,
                fLow: low,
                fHigh: high,
                tau: MaxBinTauSeconds,
                frameRate: AnalyzerFps);
        }
    }

    internal static (double low, double high) ComputeMaxBinPassband(
        RxMode mode,
        int lowHz,
        int highHz,
        int ctunShiftHz)
    {
        var (low, high) = SignedBandpassForMode(
            MapMode(mode),
            lowHz,
            highHz);
        return (low + ctunShiftHz, high + ctunShiftHz);
    }

    // WDSP's bandpass stages silently pass NOTHING for a zero-width passband
    // (low == high), taking the RX — or the TX/monitor chain — dead with no
    // diagnostic anywhere (issue #1028: an operator's bandwidth slid to zero and
    // audio dropped while every health check looked fine). This is the single
    // point every RX / RX2 / monitor / channel-open filter push funnels through
    // (and SetTxFilter applies the same guard to the TX bandpass), so flooring
    // the FINAL signed width here makes the guarantee airtight regardless of
    // mode, centre, or upstream caller — including a centre-zero filter that an
    // upstream symmetric clamp left at low == high after abs-folding. Expands
    // symmetrically about the centre so sideband placement is preserved. A no-op
    // for every legitimate width (the narrowest shipped preset, CW at 25 Hz,
    // sits well above this floor), so real filters reach WDSP byte-identical.
    internal const double MinPassbandWidthHz = 10.0;

    internal static (double low, double high) FloorPassbandWidth(double low, double high)
    {
        if (high - low >= MinPassbandWidthHz) return (low, high);
        double center = (low + high) / 2.0;
        double half = MinPassbandWidthHz / 2.0;
        return (center - half, center + half);
    }

    private static RxaMode MapMode(RxMode mode) => mode switch
    {
        RxMode.LSB => RxaMode.LSB,
        RxMode.USB => RxaMode.USB,
        RxMode.CWL => RxaMode.CWL,
        RxMode.CWU => RxaMode.CWU,
        RxMode.AM => RxaMode.AM,
        RxMode.FM => RxaMode.FM,
        RxMode.SAM => RxaMode.SAM,
        RxMode.DSB => RxaMode.DSB,
        RxMode.DIGL => RxaMode.DIGL,
        RxMode.DIGU => RxaMode.DIGU,
        _ => RxaMode.USB,
    };
}
