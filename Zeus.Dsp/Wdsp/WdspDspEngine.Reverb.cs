// SPDX-License-Identifier: GPL-2.0-or-later
//
// FORK-LOCAL FILE. The TX plate reverb, as a partial of the engine so the
// upstream WdspDspEngine.cs carries only the few calls into it.
//
// The reverb itself is not in this repository. It is plate_reverb.dll from
// trininfo/poseidon-plate-reverb, a NativeAOT build with a plain C ABI,
// loaded here ONLY IF PRESENT. A build without it has no reverb and says so
// (TxReverbAvailable); nothing else changes.
//
// It runs at WDSP's post-CFC insert (native/wdsp/TXA.c): pr_wdsp_insert is
// handed straight to WDSP as the hook, with the reverb's handle as its
// context, so the audio path is WDSP -> native reverb -> WDSP with no managed
// code on the DSP thread at all.

using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Zeus.Contracts;

namespace Zeus.Dsp.Wdsp;

public sealed partial class WdspDspEngine
{
    private readonly object _reverbLock = new();
    private TxReverbConfig _reverbConfig = TxReverbConfig.Default;
    private nint _reverbHandle;
    private int _reverbRateHz;
    private int _reverbInstalledOn = -1;          // TX channel carrying the hook, or -1

    private static readonly Lazy<PlateReverbLibrary?> s_reverbLib = new(PlateReverbLibrary.TryLoad);

    /// <summary>True when plate_reverb.dll was found and speaks our ABI.</summary>
    public static bool TxReverbLibraryAvailable => s_reverbLib.Value is not null;

    public bool TxReverbAvailable => TxReverbLibraryAvailable;

    public void SetTxReverb(TxReverbConfig cfg)
    {
        if (_disposed != 0 || cfg is null) return;
        int? tx;
        lock (_txaLock) tx = _txaChannelId;
        lock (_reverbLock)
        {
            _reverbConfig = cfg;
            ApplyReverbLocked(tx);
        }
        _log.LogInformation(
            "wdsp.txReverb available={Avail} enabled={On} mix={Mix:F2} wet={Wet:F1}dB decay={Decay:F2}s " +
            "pre={Pre:F0}ms lowcut={Lo:F0}Hz highcut={Hi:F0}Hz",
            TxReverbLibraryAvailable, cfg.Enabled, cfg.Mix, cfg.WetDb, cfg.DecaySeconds,
            cfg.PreDelayMs, cfg.LowCutHz, cfg.HighCutHz);
    }

    /// <summary>
    /// Bring WDSP's insert into line with the config. Never throws: it runs
    /// from the TX open path and a control request, and a missing or broken
    /// reverb must cost the reverb, not the transmitter.
    /// </summary>
    private unsafe void ApplyReverbLocked(int? txChannel)
    {
        var lib = s_reverbLib.Value;
        if (lib is null) return;
        try
        {
            var cfg = _reverbConfig;
            if (!cfg.Enabled || txChannel is not int ch)
            {
                // Off takes the hook out of WDSP altogether, so off is
                // bit-exact -- not "mix 0 through a double->float->double
                // round trip", which is -144 dB of error but not nothing.
                RemoveReverbLocked();
                return;
            }

            // The insert runs at WDSP's DSP rate (96 kHz on the P2 profile,
            // 48 on P1), not the mic's. A new rate needs a new instance.
            var rate = _txaDspRateHz;
            if (_reverbHandle == 0 || _reverbRateHz != rate)
            {
                RemoveReverbLocked();
                DestroyReverbHandleLocked();
                _reverbHandle = lib.Create(rate);
                _reverbRateHz = rate;
                if (_reverbHandle == 0)
                {
                    _log.LogWarning("wdsp.txReverb could not create an instance at {Rate} Hz", rate);
                    return;
                }
            }

            lib.Apply(_reverbHandle, cfg);

            if (_reverbInstalledOn != ch)
            {
                RemoveReverbLocked();
                NativeMethods.SetTXAPostCfcInsert(ch, lib.InsertFn, _reverbHandle);
                _reverbInstalledOn = ch;
            }
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "wdsp.txReverb apply failed; TX continues without it");
        }
    }

    /// <summary>Take the hook out of WDSP. Only while the channel is still
    /// open: SetTXAPostCfcInsert on a closed channel would enter a deleted
    /// critical section.</summary>
    private void RemoveReverbLocked()
    {
        if (_reverbInstalledOn < 0) return;
        try { NativeMethods.SetTXAPostCfcInsert(_reverbInstalledOn, 0, 0); }
        catch (Exception ex) { _log.LogWarning(ex, "wdsp.txReverb remove failed"); }
        _reverbInstalledOn = -1;
    }

    private void DestroyReverbHandleLocked()
    {
        if (_reverbHandle == 0) return;
        // Only ever after RemoveReverbLocked: WDSP must hold no pointer to it.
        s_reverbLib.Value?.Destroy(_reverbHandle);
        _reverbHandle = 0;
        _reverbRateHz = 0;
    }

    /// <summary>
    /// A key edge in either direction drops the tail, so the end of one
    /// over never rings into the start of the next -- and a TX-monitor
    /// preview's tail never bleeds into the over. A request, not a clear: it
    /// takes effect at the start of WDSP's next block, on WDSP's thread.
    /// </summary>
    private void ReverbKeyEdge()
    {
        var h = _reverbHandle;
        if (h != 0) s_reverbLib.Value?.Clear(h);
    }

    /// <summary>Called with the TX channel still open, just before it closes.</summary>
    private void ReverbBeforeTxClose()
    {
        lock (_reverbLock) RemoveReverbLocked();
    }

    /// <summary>Called once the TX channel is gone for good.</summary>
    private void ReverbAfterTxClose()
    {
        lock (_reverbLock)
        {
            _reverbInstalledOn = -1;
            DestroyReverbHandleLocked();
        }
    }

    /// <summary>The loaded library: every export resolved once, up front.</summary>
    private sealed unsafe class PlateReverbLibrary
    {
        public const int Abi = 2;

        private readonly delegate* unmanaged<float, nint> _create;
        private readonly delegate* unmanaged<nint, void> _destroy;
        private readonly delegate* unmanaged<nint, int> _clear;
        private readonly delegate* unmanaged<nint, float, int> _mix, _dry, _wet, _output, _pre, _damping,
            _lowCut, _highCut, _diffusion, _modRate, _modDepth;
        private readonly delegate* unmanaged<nint, double, int> _decaySeconds;

        public nint InsertFn { get; }
        public string Path { get; }

        private PlateReverbLibrary(nint lib, string path)
        {
            Path = path;
            nint E(string name) => NativeLibrary.GetExport(lib, name);
            _create = (delegate* unmanaged<float, nint>)E("pr_create");
            _destroy = (delegate* unmanaged<nint, void>)E("pr_destroy");
            _clear = (delegate* unmanaged<nint, int>)E("pr_clear");
            _mix = (delegate* unmanaged<nint, float, int>)E("pr_set_mix");
            _dry = (delegate* unmanaged<nint, float, int>)E("pr_set_dry");
            _wet = (delegate* unmanaged<nint, float, int>)E("pr_set_wet");
            _output = (delegate* unmanaged<nint, float, int>)E("pr_set_output");
            _pre = (delegate* unmanaged<nint, float, int>)E("pr_set_predelay_ms");
            _damping = (delegate* unmanaged<nint, float, int>)E("pr_set_damping");
            _lowCut = (delegate* unmanaged<nint, float, int>)E("pr_set_low_cut_hz");
            _highCut = (delegate* unmanaged<nint, float, int>)E("pr_set_high_cut_hz");
            _diffusion = (delegate* unmanaged<nint, float, int>)E("pr_set_diffusion");
            _modRate = (delegate* unmanaged<nint, float, int>)E("pr_set_mod_rate_hz");
            _modDepth = (delegate* unmanaged<nint, float, int>)E("pr_set_mod_depth");
            _decaySeconds = (delegate* unmanaged<nint, double, int>)E("pr_set_decay_seconds");
            InsertFn = E("pr_wdsp_insert");
        }

        /// <summary>
        /// PLATE_REVERB_LIB if set, else next to the engine. Refuses a library
        /// with a different ABI rather than calling it with the wrong
        /// meanings -- ABI 2 changed what "mix" does.
        /// </summary>
        public static PlateReverbLibrary? TryLoad()
        {
            // The reverb is only half the story: WDSP must carry the fork's
            // insert too. Only the win-x64 wdsp.dll in this repository is
            // rebuilt from our source; the ARM64 and Linux binaries are
            // upstream's and do not have it. Offering reverb controls there
            // would be a control with nothing behind it -- so no.
            if (!NativeLibrary.TryLoad(NativeMethods.LibraryName, typeof(WdspDspEngine).Assembly, null, out var wdsp)
                || !NativeLibrary.TryGetExport(wdsp, "SetTXAPostCfcInsert", out _))
                return null;

            var candidates = new List<string>();
            if (Environment.GetEnvironmentVariable("PLATE_REVERB_LIB") is { Length: > 0 } env) candidates.Add(env);
            candidates.Add(System.IO.Path.Combine(AppContext.BaseDirectory, "plate_reverb.dll"));
            foreach (var path in candidates)
            {
                if (!File.Exists(path) || !NativeLibrary.TryLoad(path, out var lib)) continue;
                try
                {
                    if (!NativeLibrary.TryGetExport(lib, "pr_abi_version", out var v)) continue;
                    if (((delegate* unmanaged<int>)v)() != Abi) continue;
                    return new PlateReverbLibrary(lib, path);
                }
                catch
                {
                    // A library missing an export is not ours to call.
                }
            }
            return null;
        }

        public nint Create(int rateHz) => _create(rateHz);
        public void Destroy(nint h) => _destroy(h);
        public void Clear(nint h) => _ = _clear(h);

        /// <summary>The config's person-facing units onto the DSP's.</summary>
        public void Apply(nint h, TxReverbConfig c)
        {
            _mix(h, (float)c.Mix);
            _dry(h, (float)TxReverbConfig.Linear(c.DryDb));
            _wet(h, (float)TxReverbConfig.Linear(c.WetDb));
            _output(h, (float)TxReverbConfig.Linear(c.OutputDb));
            _decaySeconds(h, c.DecaySeconds);
            _pre(h, (float)c.PreDelayMs);
            _damping(h, (float)c.Damping);
            _lowCut(h, (float)c.LowCutHz);
            _highCut(h, (float)c.HighCutHz);
            _diffusion(h, (float)c.Diffusion);
            _modRate(h, (float)c.ModRateHz);
            _modDepth(h, (float)c.ModDepth);
        }
    }
}
