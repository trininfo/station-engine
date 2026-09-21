// SPDX-License-Identifier: GPL-2.0-or-later

using Microsoft.Extensions.Logging;
using Zeus.Contracts;

namespace Zeus.Dsp.Wdsp;

/// <summary>
/// Disconnected-mode DSP engine: no radio RX/audio is published, but the TXA
/// chain is a real WDSP path so Audio Suite Preview can run while off-air.
/// </summary>
public sealed class OfflinePreviewDspEngine : IDspEngine, ITxAudioPluginHost
{
    private readonly SyntheticDspEngine _control = new();
    private readonly WdspDspEngine _tx;
    private readonly object _channelLock = new();
    private readonly Dictionary<int, int> _channelMap = new();
    private int _disposed;

    public OfflinePreviewDspEngine(ILogger<WdspDspEngine>? logger = null, int rxAnalyzerFftSize = 16_384)
    {
        _tx = new WdspDspEngine(logger, rxAnalyzerFftSize);
    }

    public int OpenChannel(int sampleRateHz, int pixelWidth) =>
        OpenChannelCore(sampleRateHz, pixelWidth, displayOnly: false);

    public int OpenRxDisplayChannel(int sampleRateHz, int pixelWidth) =>
        OpenChannelCore(sampleRateHz, pixelWidth, displayOnly: true);

    private int OpenChannelCore(int sampleRateHz, int pixelWidth, bool displayOnly)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        int controlId = displayOnly
            ? _control.OpenRxDisplayChannel(sampleRateHz, pixelWidth)
            : _control.OpenChannel(sampleRateHz, pixelWidth);
        try
        {
            int txRxId = displayOnly
                ? _tx.OpenRxDisplayChannel(sampleRateHz, pixelWidth)
                : _tx.OpenChannel(sampleRateHz, pixelWidth);
            lock (_channelLock) _channelMap[controlId] = txRxId;
            return controlId;
        }
        catch
        {
            _control.CloseChannel(controlId);
            throw;
        }
    }

    public void CloseChannel(int channelId)
    {
        int txChannelId = -1;
        lock (_channelLock)
        {
            if (_channelMap.Remove(channelId, out int mapped))
                txChannelId = mapped;
        }

        _control.CloseChannel(channelId);
        if (txChannelId >= 0)
            _tx.CloseChannel(txChannelId);
    }

    public void CloseRxDisplayChannel(int channelId) => CloseChannel(channelId);

    private int TxChannelFor(int channelId)
    {
        lock (_channelLock)
            return _channelMap.TryGetValue(channelId, out int mapped) ? mapped : channelId;
    }

    public void FeedIq(int channelId, ReadOnlySpan<double> interleavedIqSamples)
    {
        // No hardware RX stream exists in offline preview mode. Deliberately do
        // not feed the WDSP RXA or publish synthetic spectrum/audio.
    }

    public void SetMode(int channelId, RxMode mode)
    {
        _control.SetMode(channelId, mode);
        _tx.SetMode(TxChannelFor(channelId), mode);
    }

    public void SetFilter(int channelId, int lowHz, int highHz)
    {
        _control.SetFilter(channelId, lowHz, highHz);
        _tx.SetFilter(TxChannelFor(channelId), lowHz, highHz);
    }

    public void SetVfoHz(int channelId, long vfoHz)
    {
        _control.SetVfoHz(channelId, vfoHz);
        _tx.SetVfoHz(TxChannelFor(channelId), vfoHz);
    }

    public void SetCtunShift(int channelId, int shiftHz)
    {
        _control.SetCtunShift(channelId, shiftHz);
        _tx.SetCtunShift(TxChannelFor(channelId), shiftHz);
    }

    public void SetAgcTop(int channelId, double topDb)
    {
        _control.SetAgcTop(channelId, topDb);
        _tx.SetAgcTop(TxChannelFor(channelId), topDb);
    }

    public void SetAgcThresh(int channelId, double threshDbm)
    {
        _control.SetAgcThresh(channelId, threshDbm);
        _tx.SetAgcThresh(TxChannelFor(channelId), threshDbm);
    }

    public double GetAgcTop(int channelId) => _tx.GetAgcTop(TxChannelFor(channelId));

    public double GetAgcThresh(int channelId) => _tx.GetAgcThresh(TxChannelFor(channelId));

    public void SetAgc(int channelId, AgcConfig cfg)
    {
        _control.SetAgc(channelId, cfg);
        _tx.SetAgc(TxChannelFor(channelId), cfg);
    }

    public void SetSquelch(int channelId, SquelchConfig cfg)
    {
        _control.SetSquelch(channelId, cfg);
        _tx.SetSquelch(TxChannelFor(channelId), cfg);
    }

    public void SetTxLeveling(int channelId, TxLevelingConfig cfg)
    {
        _control.SetTxLeveling(channelId, cfg);
        _tx.SetTxLeveling(TxChannelFor(channelId), cfg);
    }

    public void SetTxPhaseRotator(int channelId, TxPhaseRotatorConfig cfg)
    {
        _control.SetTxPhaseRotator(channelId, cfg);
        _tx.SetTxPhaseRotator(TxChannelFor(channelId), cfg);
    }

    public void ResetTxPhaseRotatorAuto(int channelId)
    {
        _control.ResetTxPhaseRotatorAuto(channelId);
        _tx.ResetTxPhaseRotatorAuto(TxChannelFor(channelId));
    }

    public TxPhaseRotatorAsymmetry? GetTxPhaseRotatorAsymmetry(int channelId) =>
        _tx.GetTxPhaseRotatorAsymmetry(TxChannelFor(channelId));

    public void SetRxDisplayFastAttack(int channelId, bool fast)
    {
        _control.SetRxDisplayFastAttack(channelId, fast);
    }

    public void SetRxAfGainDb(int channelId, double db)
    {
        _control.SetRxAfGainDb(channelId, db);
        _tx.SetRxAfGainDb(TxChannelFor(channelId), db);
    }

    public void SetNoiseReduction(int channelId, NrConfig cfg)
    {
        _control.SetNoiseReduction(channelId, cfg);
        _tx.SetNoiseReduction(TxChannelFor(channelId), cfg);
    }

    public Nr3ModelLoadResult LoadNr3Model(string? modelFilePath) => _tx.LoadNr3Model(modelFilePath);

    public void SetNotches(IReadOnlyList<NotchDto> notches)
    {
        _control.SetNotches(notches);
        _tx.SetNotches(notches);
    }

    public void SetNotchTuneFrequencyHz(double loHz)
    {
        _control.SetNotchTuneFrequencyHz(loHz);
        _tx.SetNotchTuneFrequencyHz(loHz);
    }

    public void SetZoom(int channelId, int level)
    {
        _control.SetZoom(channelId, level);
        _tx.SetZoom(TxChannelFor(channelId), level);
    }

    public void SetRxDisplayZoom(int channelId, int level)
    {
        _control.SetRxDisplayZoom(channelId, level);
        _tx.SetRxDisplayZoom(TxChannelFor(channelId), level);
    }

    public int ReadAudio(int channelId, Span<float> output)
    {
        output.Clear();
        return 0;
    }

    public bool TryGetDisplayPixels(int channelId, DisplayPixout which, Span<float> dbOut) => false;

    public bool TryGetTxDisplayPixels(DisplayPixout which, Span<float> dbOut) =>
        _tx.TryGetTxDisplayPixels(which, dbOut);

    public bool TryGetPsFeedbackDisplayPixels(DisplayPixout which, Span<float> dbOut) =>
        _tx.TryGetPsFeedbackDisplayPixels(which, dbOut);

    public void ConfigureTxDisplayAnalyzer(int fftSize, int windowType, double avgTauSec) =>
        _tx.ConfigureTxDisplayAnalyzer(fftSize, windowType, avgTauSec);

    public void ResetDisplayPixelBuffers() => _tx.ResetDisplayPixelBuffers();

    public int OpenTxChannel(int outputRateHz = 48_000) => _tx.OpenTxChannel(outputRateHz);

    public void SetMox(bool moxOn) => _tx.SetMox(moxOn);
    public void SetMox(bool moxOn, bool stopRxForPureSignal) =>
        _tx.SetMox(moxOn, stopRxForPureSignal);

    public double GetRxaSignalDbm(int channelId) => -140.0;

    public RxStageMeters GetRxStageMeters(int channelId) => RxStageMeters.Silent;

    public void SetTxMode(RxMode mode) => _tx.SetTxMode(mode);

    public void SetTxAmCarrierLevel(double carrierLevel) =>
        _tx.SetTxAmCarrierLevel(carrierLevel);

    public void SetTxDigitalBypass(bool bypass) => _tx.SetTxDigitalBypass(bypass);
    public void SetTxInjectedAudioBypass(bool bypass) => _tx.SetTxInjectedAudioBypass(bypass);
    public void SetTxRogerBeepBypass(bool bypass) => _tx.SetTxRogerBeepBypass(bypass);

    public void SetTxFilter(int lowHz, int highHz) => _tx.SetTxFilter(lowHz, highHz);

    public void SetRxBandpassWindow(int channelId, BandpassWindow window)
    {
        _control.SetRxBandpassWindow(channelId, window);
        _tx.SetRxBandpassWindow(TxChannelFor(channelId), window);
    }

    public void SetTxBandpassWindow(BandpassWindow window) => _tx.SetTxBandpassWindow(window);

    public void SetRxFilterPhase(int channelId, FilterPhaseMode phase)
    {
        _control.SetRxFilterPhase(channelId, phase);
        _tx.SetRxFilterPhase(TxChannelFor(channelId), phase);
    }

    public void SetTxFilterPhase(FilterPhaseMode phase) => _tx.SetTxFilterPhase(phase);

    public int ProcessTxBlock(ReadOnlySpan<float> micMono, Span<float> iqInterleaved) =>
        _tx.ProcessTxBlock(micMono, iqInterleaved);

    public int TxBlockSamples => _tx.TxBlockSamples;

    public int TxOutputSamples => _tx.TxOutputSamples;

    public void SetTxPanelGain(double linearGain) => _tx.SetTxPanelGain(linearGain);

    public void SetTxLevelerMaxGain(double maxGainDb) => _tx.SetTxLevelerMaxGain(maxGainDb);

    public void SetTxTune(bool on) => _tx.SetTxTune(on);

    public TxStageMeters GetTxStageMeters() => _tx.GetTxStageMeters();

    public void SetTwoTone(bool on, double freq1, double freq2, double mag) =>
        _tx.SetTwoTone(on, freq1, freq2, mag);

    public void SetPsEnabled(bool enabled) => _tx.SetPsEnabled(enabled);

    public void SetPsMox(bool moxOn) => _tx.SetPsMox(moxOn);

    public void SetPsControl(bool autoCal, bool singleCal) => _tx.SetPsControl(autoCal, singleCal);

    public void SetPsHold(bool hold) => _tx.SetPsHold(hold);

    public void SetPsAdvanced(
        double moxDelaySec,
        double loopDelaySec,
        double ampDelayNs,
        double hwPeak) =>
        _tx.SetPsAdvanced(moxDelaySec, loopDelaySec, ampDelayNs, hwPeak);

    public void SetPsHwPeak(double hwPeak) => _tx.SetPsHwPeak(hwPeak);

    public void SetPsCalccSwitches(bool pin, bool map, bool stabilize, int ints, int spi) =>
        _tx.SetPsCalccSwitches(pin, map, stabilize, ints, spi);

    public void FeedPsFeedbackBlock(
        ReadOnlySpan<float> txI,
        ReadOnlySpan<float> txQ,
        ReadOnlySpan<float> rxI,
        ReadOnlySpan<float> rxQ) =>
        _tx.FeedPsFeedbackBlock(txI, txQ, rxI, rxQ);

    public PsStageMeters GetPsStageMeters() => PsStageMeters.Silent;

    public void ResetPs() => _tx.ResetPs();

    public void SavePsCorrection(string path) => _tx.SavePsCorrection(path);

    public void RestorePsCorrection(string path) => _tx.RestorePsCorrection(path);

    public void SetCfcConfig(CfcConfig cfg)
    {
        _control.SetCfcConfig(cfg);
        _tx.SetCfcConfig(cfg);
    }

    public void SetTxMonitorEnabled(bool enabled) => _tx.SetTxMonitorEnabled(enabled);

    public int ReadTxMonitorAudio(Span<float> output) => _tx.ReadTxMonitorAudio(output);

    public bool IsTxMonitorOn => _tx.IsTxMonitorOn;

    public void SetTxAudioPluginHandler(TxAudioPluginHandler? handler) =>
        _tx.SetTxAudioPluginHandler(handler);

    public bool HasTxAudioPluginHandler => _tx.HasTxAudioPluginHandler;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _control.Dispose();
        _tx.Dispose();
    }
}
