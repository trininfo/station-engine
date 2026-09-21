// SPDX-License-Identifier: GPL-2.0-or-later

namespace Zeus.Dsp.Wdsp;

internal interface IWdspTxControlNative
{
    void SetTXAMode(int channel, int mode);
    void SetTXAAMCarrierLevel(int channel, double carrierLevel);
    void SetTXACompressorRun(int channel, int run);
    void SetTXAosctrlRun(int channel, int run);
    void SetTXAosctrlBandwidth(int channel, double bandwidth);
    void SetTXACFCOMPRun(int channel, int run);
    void SetTXACFCOMPprofile(int channel, int nfreqs, double[] f, double[] g, double[] e,
                             double[]? qg, double[]? qe);
    void SetTXACFCOMPPrecomp(int channel, double precomp);
    void SetTXACFCOMPPrePeq(int channel, double prepeq);
    void SetTXACFCOMPPeqRun(int channel, int run);
    void SetTXAPHROTRun(int channel, int run);
    void SetTXAPHROTCorner(int channel, double frequency);
    void SetTXAPHROTNstages(int channel, int nstages);
    void SetTXAPHROTAutoMode(int channel, int autoMode);
    void SetTXAPHROTReverse(int channel, int reverse);
    void SetTXALevelerSt(int channel, int state);
    void SetTXAPostGenRun(int channel, int run);
    void SetTXAPostGenMode(int channel, int mode);
    void SetTXAPostGenToneMag(int channel, double magnitude);
    void SetTXAPostGenToneFreq(int channel, double frequency);
}

internal sealed class WdspTxControlNative : IWdspTxControlNative
{
    public void SetTXAMode(int channel, int mode) =>
        NativeMethods.SetTXAMode(channel, mode);

    public void SetTXAAMCarrierLevel(int channel, double carrierLevel) =>
        NativeMethods.SetTXAAMCarrierLevel(channel, carrierLevel);

    public void SetTXACompressorRun(int channel, int run) =>
        NativeMethods.SetTXACompressorRun(channel, run);

    public void SetTXAosctrlRun(int channel, int run) =>
        NativeMethods.SetTXAosctrlRun(channel, run);

    public void SetTXAosctrlBandwidth(int channel, double bandwidth) =>
        NativeMethods.SetTXAosctrlBandwidth(channel, bandwidth);

    public void SetTXACFCOMPRun(int channel, int run) =>
        NativeMethods.SetTXACFCOMPRun(channel, run);

    public unsafe void SetTXACFCOMPprofile(int channel, int nfreqs, double[] f, double[] g, double[] e,
                                          double[]? qg, double[]? qe)
    {
        // `fixed` on a null array yields a null pointer, which is precisely
        // the "no Q factors" profile Thetis sends with its Q switch off.
        fixed (double* pF = f, pG = g, pE = e, pQg = qg, pQe = qe)
        {
            NativeMethods.SetTXACFCOMPprofile(channel, nfreqs, pF, pG, pE, pQg, pQe);
        }
    }

    public void SetTXACFCOMPPrecomp(int channel, double precomp) =>
        NativeMethods.SetTXACFCOMPPrecomp(channel, precomp);

    public void SetTXACFCOMPPrePeq(int channel, double prepeq) =>
        NativeMethods.SetTXACFCOMPPrePeq(channel, prepeq);

    public void SetTXACFCOMPPeqRun(int channel, int run) =>
        NativeMethods.SetTXACFCOMPPeqRun(channel, run);

    public void SetTXAPHROTRun(int channel, int run) =>
        NativeMethods.SetTXAPHROTRun(channel, run);

    public void SetTXAPHROTCorner(int channel, double frequency) =>
        NativeMethods.SetTXAPHROTCorner(channel, frequency);

    public void SetTXAPHROTNstages(int channel, int nstages) =>
        NativeMethods.SetTXAPHROTNstages(channel, nstages);

    public void SetTXAPHROTAutoMode(int channel, int autoMode) =>
        NativeMethods.SetTXAPHROTAutoMode(channel, autoMode);

    public void SetTXAPHROTReverse(int channel, int reverse) =>
        NativeMethods.SetTXAPHROTReverse(channel, reverse);

    public void SetTXALevelerSt(int channel, int state) =>
        NativeMethods.SetTXALevelerSt(channel, state);

    public void SetTXAPostGenRun(int channel, int run) =>
        NativeMethods.SetTXAPostGenRun(channel, run);

    public void SetTXAPostGenMode(int channel, int mode) =>
        NativeMethods.SetTXAPostGenMode(channel, mode);

    public void SetTXAPostGenToneMag(int channel, double magnitude) =>
        NativeMethods.SetTXAPostGenToneMag(channel, magnitude);

    public void SetTXAPostGenToneFreq(int channel, double frequency) =>
        NativeMethods.SetTXAPostGenToneFreq(channel, frequency);
}
