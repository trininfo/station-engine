// SPDX-License-Identifier: GPL-2.0-or-later

using Zeus.Contracts;
using Zeus.Dsp;

namespace Zeus.Server;

/// <summary>
/// Host-owned guard around an independently distributed Protocol 3 TX engine.
/// Optional PureSignal calls cross this boundary only when the selected
/// provider declared that capability before the sidecar and engine started.
/// The guard also freezes the validated realtime block geometry and rejects a
/// provider that reports an impossible produced count before host scratch
/// buffers can be indexed with it.
/// </summary>
internal sealed class ExternalProtocol3TxDspEngine : IDspEngine
{
    internal const int MaxTxBlockSamples = 1024;
    internal const int MaxTxOutputSamples = 2048;

    private readonly IDspEngine _inner;
    private readonly IExternalPureSignalStatusSource? _pureSignalStatusSource;
    private int _txBlockSamples;
    private int _txOutputSamples;

    internal ExternalProtocol3TxDspEngine(IDspEngine inner, bool pureSignalEnabled = false)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        PureSignalEnabled = pureSignalEnabled;
        if (pureSignalEnabled && inner is not IExternalPureSignalRouteSettingsSink)
        {
            throw new InvalidOperationException(
                "External DSP provider declared PureSignal but its engine does not implement the required radio-route settings sink.");
        }
        if (pureSignalEnabled && inner is not IExternalPureSignalStatusSource)
        {
            throw new InvalidOperationException(
                "External DSP provider declared PureSignal but its engine does not implement the required non-blocking status source.");
        }
        _pureSignalStatusSource = pureSignalEnabled
            ? (IExternalPureSignalStatusSource)inner
            : null;
    }

    internal bool PureSignalEnabled { get; }
    internal bool PureSignalArmed => GetPureSignalStatus().Armed;

    internal ExternalPureSignalStatus GetPureSignalStatus()
    {
        if (!PureSignalEnabled || _pureSignalStatusSource is null)
            return default;

        try { return _pureSignalStatusSource.GetPureSignalStatus(); }
        catch { return new ExternalPureSignalStatus(Armed: false, false, 0, 0, 0, 0, 0); }
    }

    internal void SetPureSignalRouteSettings(ExternalPureSignalRouteSettings settings)
    {
        if (PureSignalEnabled)
            ((IExternalPureSignalRouteSettingsSink)_inner).SetPureSignalRouteSettings(settings);
    }

    public int OpenChannel(int sampleRateHz, int pixelWidth) =>
        _inner.OpenChannel(sampleRateHz, pixelWidth);

    public void CloseChannel(int channelId) => _inner.CloseChannel(channelId);

    public int OpenRxDisplayChannel(int sampleRateHz, int pixelWidth) =>
        _inner.OpenRxDisplayChannel(sampleRateHz, pixelWidth);

    public void CloseRxDisplayChannel(int channelId) => _inner.CloseRxDisplayChannel(channelId);

    public void FeedIq(int channelId, ReadOnlySpan<double> interleavedIqSamples) =>
        _inner.FeedIq(channelId, interleavedIqSamples);

    public void SetMode(int channelId, RxMode mode) => _inner.SetMode(channelId, mode);
    public void SetFilter(int channelId, int lowHz, int highHz) => _inner.SetFilter(channelId, lowHz, highHz);
    public void SetVfoHz(int channelId, long vfoHz) => _inner.SetVfoHz(channelId, vfoHz);
    public void SetCtunShift(int channelId, int shiftHz) => _inner.SetCtunShift(channelId, shiftHz);
    public void SetAgcTop(int channelId, double topDb) => _inner.SetAgcTop(channelId, topDb);
    public void SetAgcThresh(int channelId, double threshDbm) => _inner.SetAgcThresh(channelId, threshDbm);
    public double GetAgcTop(int channelId) => _inner.GetAgcTop(channelId);
    public double GetAgcThresh(int channelId) => _inner.GetAgcThresh(channelId);
    public void SetAgc(int channelId, AgcConfig cfg) => _inner.SetAgc(channelId, cfg);
    public void SetSquelch(int channelId, SquelchConfig cfg) => _inner.SetSquelch(channelId, cfg);
    public void SetTxLeveling(int channelId, TxLevelingConfig cfg) => _inner.SetTxLeveling(channelId, cfg);
    public void SetTxPhaseRotator(int channelId, TxPhaseRotatorConfig cfg) => _inner.SetTxPhaseRotator(channelId, cfg);
    public void ResetTxPhaseRotatorAuto(int channelId) => _inner.ResetTxPhaseRotatorAuto(channelId);
    public TxPhaseRotatorAsymmetry? GetTxPhaseRotatorAsymmetry(int channelId) =>
        _inner.GetTxPhaseRotatorAsymmetry(channelId);
    public void SetRxDisplayFastAttack(int channelId, bool fast) => _inner.SetRxDisplayFastAttack(channelId, fast);
    public void ConfigureRxDisplayAveraging(double panTauSec, double wfTauSec) =>
        _inner.ConfigureRxDisplayAveraging(panTauSec, wfTauSec);
    public void SetRxAfGainDb(int channelId, double db) => _inner.SetRxAfGainDb(channelId, db);
    public void SetNoiseReduction(int channelId, NrConfig cfg) => _inner.SetNoiseReduction(channelId, cfg);
    public Nr3ModelLoadResult LoadNr3Model(string? modelFilePath) => _inner.LoadNr3Model(modelFilePath);
    public void SetNotches(IReadOnlyList<NotchDto> notches) => _inner.SetNotches(notches);
    public void SetNotchTuneFrequencyHz(double loHz) => _inner.SetNotchTuneFrequencyHz(loHz);
    public void SetZoom(int channelId, int level) => _inner.SetZoom(channelId, level);
    public void SetRxDisplayZoom(int channelId, int level) => _inner.SetRxDisplayZoom(channelId, level);
    public void SetRxDisplayFftSize(int channelId, int fftSize) => _inner.SetRxDisplayFftSize(channelId, fftSize);
    public int ReadAudio(int channelId, Span<float> output) => _inner.ReadAudio(channelId, output);
    public bool TryGetDisplayPixels(int channelId, DisplayPixout which, Span<float> dbOut) =>
        _inner.TryGetDisplayPixels(channelId, which, dbOut);
    public bool TryGetRxSnrPowerSpectrum(int channelId, Span<float> dbOut, out RxSnrSpectrumInfo info) =>
        _inner.TryGetRxSnrPowerSpectrum(channelId, dbOut, out info);
    public bool TryGetTxDisplayPixels(DisplayPixout which, Span<float> dbOut) =>
        _inner.TryGetTxDisplayPixels(which, dbOut);

    public bool TryGetPsFeedbackDisplayPixels(DisplayPixout which, Span<float> dbOut) =>
        PureSignalEnabled && _inner.TryGetPsFeedbackDisplayPixels(which, dbOut);

    public void ConfigureTxDisplayAnalyzer(int fftSize, int windowType, double avgTauSec) =>
        _inner.ConfigureTxDisplayAnalyzer(fftSize, windowType, avgTauSec);
    public void ResetDisplayPixelBuffers() => _inner.ResetDisplayPixelBuffers();

    public int OpenTxChannel(int outputRateHz = 48_000)
    {
        int channelId = _inner.OpenTxChannel(outputRateHz);
        int blockSamples = _inner.TxBlockSamples;
        int outputSamples = _inner.TxOutputSamples;
        long outputNumerator;
        try
        {
            outputNumerator = checked((long)blockSamples * outputRateHz);
        }
        catch (OverflowException ex)
        {
            throw new InvalidOperationException("External DSP realtime geometry overflowed host validation.", ex);
        }

        const int micRateHz = 48_000;
        bool integralRatio = outputRateHz > 0 && outputNumerator % micRateHz == 0;
        long expectedOutputSamples = integralRatio ? outputNumerator / micRateHz : -1;
        if (blockSamples is <= 0 or > MaxTxBlockSamples ||
            outputSamples is <= 0 or > MaxTxOutputSamples ||
            !integralRatio ||
            outputSamples != expectedOutputSamples)
        {
            throw new InvalidOperationException(
                $"External DSP realtime geometry {blockSamples} input / {outputSamples} output samples exceeds " +
                $"the host limits {MaxTxBlockSamples} / {MaxTxOutputSamples} or does not match " +
                $"the exact 48 kHz to {outputRateHz} Hz ratio (expected {expectedOutputSamples} output samples).");
        }

        Volatile.Write(ref _txBlockSamples, blockSamples);
        Volatile.Write(ref _txOutputSamples, outputSamples);
        return channelId;
    }

    public void SetMox(bool moxOn) => _inner.SetMox(moxOn, stopRxForPureSignal: false);
    public void SetMox(bool moxOn, bool stopRxForPureSignal) =>
        _inner.SetMox(moxOn, stopRxForPureSignal: false);
    public double GetRxaSignalDbm(int channelId) => _inner.GetRxaSignalDbm(channelId);
    public RxStageMeters GetRxStageMeters(int channelId) => _inner.GetRxStageMeters(channelId);
    public void SetTxMode(RxMode mode) => _inner.SetTxMode(mode);
    public void SetTxDigitalBypass(bool bypass) => _inner.SetTxDigitalBypass(bypass);
    public void SetTxInjectedAudioBypass(bool bypass) => _inner.SetTxInjectedAudioBypass(bypass);
    public void SetTxRogerBeepBypass(bool bypass) => _inner.SetTxRogerBeepBypass(bypass);
    public void SetTxFilter(int lowHz, int highHz) => _inner.SetTxFilter(lowHz, highHz);
    public void SetRxBandpassWindow(int channelId, BandpassWindow window) =>
        _inner.SetRxBandpassWindow(channelId, window);
    public void SetTxBandpassWindow(BandpassWindow window) => _inner.SetTxBandpassWindow(window);
    public void SetRxFilterPhase(int channelId, FilterPhaseMode phase) => _inner.SetRxFilterPhase(channelId, phase);
    public void SetTxFilterPhase(FilterPhaseMode phase) => _inner.SetTxFilterPhase(phase);

    public int ProcessTxBlock(ReadOnlySpan<float> micMono, Span<float> iqInterleaved)
    {
        int blockSamples = Volatile.Read(ref _txBlockSamples);
        int outputSamples = Volatile.Read(ref _txOutputSamples);
        if (blockSamples <= 0 || outputSamples <= 0)
            throw new InvalidOperationException("External DSP TX channel has not been validated.");
        if (micMono.Length != blockSamples || iqInterleaved.Length < 2 * outputSamples)
            throw new ArgumentException("External DSP TX block does not match the validated realtime geometry.");

        int produced = _inner.ProcessTxBlock(
            micMono,
            iqInterleaved[..(2 * outputSamples)]);
        if (produced < 0 || produced > outputSamples)
        {
            throw new InvalidOperationException(
                $"External DSP produced {produced} complex samples; validated capacity is {outputSamples}.");
        }
        return produced;
    }

    public int TxBlockSamples => Volatile.Read(ref _txBlockSamples);
    public int TxOutputSamples => Volatile.Read(ref _txOutputSamples);
    public void SetTxPanelGain(double linearGain) => _inner.SetTxPanelGain(linearGain);
    public void SetTxLevelerMaxGain(double maxGainDb) => _inner.SetTxLevelerMaxGain(maxGainDb);
    public void SetTxTune(bool on) => _inner.SetTxTune(on);
    public TxStageMeters GetTxStageMeters() => _inner.GetTxStageMeters();
    public void SetTwoTone(bool on, double freq1, double freq2, double mag) =>
        _inner.SetTwoTone(on, freq1, freq2, mag);

    public void SetPsEnabled(bool enabled)
    {
        if (!PureSignalEnabled)
            return;

        _inner.SetPsEnabled(enabled);
    }
    public void SetPsMox(bool moxOn) { if (PureSignalEnabled) _inner.SetPsMox(moxOn); }
    public void SetPsControl(bool autoCal, bool singleCal)
    { if (PureSignalEnabled) _inner.SetPsControl(autoCal, singleCal); }
    public void SetPsHold(bool hold) { if (PureSignalEnabled) _inner.SetPsHold(hold); }
    public void SetPsAdvanced(double moxDelaySec, double loopDelaySec, double ampDelayNs, double hwPeak)
    { if (PureSignalEnabled) _inner.SetPsAdvanced(moxDelaySec, loopDelaySec, ampDelayNs, hwPeak); }
    public void SetPsHwPeak(double hwPeak) { if (PureSignalEnabled) _inner.SetPsHwPeak(hwPeak); }
    public void FeedPsFeedbackBlock(
        ReadOnlySpan<float> txI,
        ReadOnlySpan<float> txQ,
        ReadOnlySpan<float> rxI,
        ReadOnlySpan<float> rxQ)
    { if (PureSignalEnabled) _inner.FeedPsFeedbackBlock(txI, txQ, rxI, rxQ); }
    public PsStageMeters GetPsStageMeters() =>
        PureSignalEnabled ? _inner.GetPsStageMeters() : PsStageMeters.Silent;
    public void ResetPs() { if (PureSignalEnabled) _inner.ResetPs(); }
    public void SavePsCorrection(string path) { if (PureSignalEnabled) _inner.SavePsCorrection(path); }
    public void RestorePsCorrection(string path) { if (PureSignalEnabled) _inner.RestorePsCorrection(path); }

    public void SetCfcConfig(CfcConfig cfg) => _inner.SetCfcConfig(cfg);
    public void SetTxMonitorEnabled(bool enabled) => _inner.SetTxMonitorEnabled(enabled);
    public int ReadTxMonitorAudio(Span<float> output) => _inner.ReadTxMonitorAudio(output);
    public bool IsTxMonitorOn => _inner.IsTxMonitorOn;
    public void Dispose() => _inner.Dispose();
}
