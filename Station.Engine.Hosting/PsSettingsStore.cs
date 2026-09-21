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
// See ATTRIBUTIONS.md at the repository root for the full provenance
// statement and per-component attribution.
//
// Protocol-2 / PureSignal / Saturn-class behaviour was additionally informed
// by pihpsdr (https://github.com/dl1ycf/pihpsdr), maintained by Christoph
// Wüllen (DL1YCF); and by DeskHPSDR
// (https://github.com/dl1bz/deskhpsdr), maintained by Heiko (DL1BZ).
// Both are GPL-2.0-or-later.

using LiteDB;
using Zeus.Contracts;

namespace Zeus.Server;

// PureSignal settings persistence. Both production hosts supply
// PrefsDbPath.EngineGet(), so this store's ps_settings collection lives in
// station-engine.db alongside DspSettingsStore rather than in zeus-prefs.db.
// Stores the operator's arm intent and calibration tuning (timing delays,
// auto-attenuate, per-board HW peak) so they survive server restarts.
//
// Runtime PsEnabled still starts false in every new process. Only the distinct
// ArmIntent field records an explicit operator arm/disarm decision, and a
// persisted arm is applied through the connect-time disarm/rearm sanitize
// cycle. Legacy raw PsEnabled BSON is deliberately ignored forever. PsSingle
// is session-only, as are the MOX/TUN keying controls.
//
// `HwPeakByBoard` IS persisted as of 2026-05-16. The earlier "re-derive per
// radio at connect time" assumption clobbered operator-calibrated values
// every reconnect — on chains that don't match the per-board factory
// default (external amp sample taps, non-stock attenuator pads) the value
// can legitimately differ from the resolved default and must survive a
// restart. The dictionary is keyed by `{p1|p2}:{board}[:variant]` (variant
// only when board is `OrionMkII` and we're on P2) so each physical chain
// owns its own calibrated value.
public sealed class PsSettingsStore : IDisposable
{
    private readonly Zeus.Data.SharedLiteDatabase.Lease _dbLease;
    private readonly LiteDatabase _db;
    private readonly ILiteCollection<PsSettingsEntry> _entries;
    private readonly ILogger<PsSettingsStore> _log;

    public PsSettingsStore(ILogger<PsSettingsStore> log, string? dbPathOverride = null)
    {
        _log = log;
        var dbPath = dbPathOverride ?? PrefsDbPath.Get();

        var dir = Path.GetDirectoryName(dbPath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            Directory.CreateDirectory(dir);

        _dbLease = Zeus.Data.SharedLiteDatabase.Acquire(dbPath);
        _db = _dbLease.Database;
        _entries = _db.GetCollection<PsSettingsEntry>("ps_settings");
        _entries.EnsureIndex(x => x.ProfileId, unique: true);

        MigrateTxAttnPoison();

        _log.LogInformation("PsSettingsStore initialized at {Path}", dbPath);
    }

    /// <summary>
    /// Current TxAttnByBoard migration version. New entries must be stamped
    /// at this version (RadioService.PersistPsState does) so a store created
    /// AFTER the poison window is never wiped by a later startup's migration
    /// pass.
    /// </summary>
    public const int TxAttnMigrationCurrent = 2;

    /// <summary>
    /// Step-wise migration: TxAttnMigration &lt; 1 drops persisted HermesC10
    /// TX feedback-attenuation entries; TxAttnMigration &lt; 2 drops HermesII
    /// entries. The G2E testers builds
    /// (test-g2e-ps / test-g2e-p1-ps, PRs #1249/#1283) shipped a defective
    /// auto-acquire walk that ratcheted the attenuation to 31 dB (max — a
    /// deaf feedback ADC) and persisted it per-board, re-applied on every
    /// connect. Both field testers' stores are known-poisoned (#1248/#1285).
    /// Clearing the entries returns the board to the protected virgin-store
    /// arm (31 dB seed, then the two-tone servo walks down and re-persists a
    /// calibrated value). HwPeakByBoard is left untouched — it was never
    /// written by the broken walk and may hold an operator calibration.
    /// v2 also clears HermesII (ANAN-10E) entries produced by the same pre-
    /// hardening servo policy, without re-clearing HermesC10 entries that
    /// already passed v1. Every other board's entries survive.
    /// </summary>
    private void MigrateTxAttnPoison()
    {
        const int targetVersion = TxAttnMigrationCurrent;
        foreach (var entry in _entries.FindAll().ToList())
        {
            if (entry.TxAttnMigration >= targetVersion) continue;
            int fromVersion = entry.TxAttnMigration;
            var poisoned = new List<string>();
            int c10Cleared = fromVersion < 1
                ? RemoveTxAttnBoardKeys(entry, HpsdrBoardKind.HermesC10, poisoned)
                : 0;
            int hermesIICleared = fromVersion < 2
                ? RemoveTxAttnBoardKeys(entry, HpsdrBoardKind.HermesII, poisoned)
                : 0;
            entry.TxAttnMigration = targetVersion;
            _entries.Update(entry);
            if (poisoned.Count > 0)
            {
                _log.LogWarning(
                    "ps.migration txAttn v{FromVersion}->{ToVersion}: cleared HermesC10={C10Count} HermesII={HermesIICount} poisoned single-ADC attenuation entr{Plural} ({Keys}) — see PR #1249/#1283 field failure",
                    fromVersion, targetVersion, c10Cleared, hermesIICleared,
                    poisoned.Count == 1 ? "y" : "ies", string.Join(",", poisoned));
            }
        }
    }

    private static int RemoveTxAttnBoardKeys(
        PsSettingsEntry entry,
        HpsdrBoardKind board,
        List<string> removedKeys)
    {
        string suffix = ":" + board;
        string middle = suffix + ":";
        var keys = entry.TxAttnByBoard.Keys
            .Where(k => k.EndsWith(suffix, StringComparison.Ordinal)
                        || k.Contains(middle, StringComparison.Ordinal))
            .ToList();
        foreach (var key in keys)
        {
            entry.TxAttnByBoard.Remove(key);
            removedKeys.Add(key);
        }
        return keys.Count;
    }

    public PsSettingsEntry? Get(string profileId = "default")
        => _entries.FindOne(x => x.ProfileId == profileId);

    public void Upsert(PsSettingsEntry entry, string profileId = "default")
    {
        entry.ProfileId = profileId;
        entry.UpdatedUtc = DateTime.UtcNow;
        var existing = _entries.FindOne(x => x.ProfileId == profileId);
        if (existing is null)
        {
            _entries.Insert(entry);
        }
        else
        {
            entry.Id = existing.Id;
            _entries.Update(entry);
        }
    }

    public void Dispose() => _dbLease.Dispose();

}

public sealed class PsSettingsEntry
{
    public int Id { get; set; }
    public string ProfileId { get; set; } = string.Empty;
    // Operator arm intent, written only by an explicit RadioService.SetPs.
    // Missing means false for first-run and legacy stores. This name is
    // deliberately distinct from poisoned legacy raw PsEnabled BSON, which
    // must remain ignored forever (PureSignalNoAutoArmStartupMatrixTests).
    public bool ArmIntent { get; set; } = false;
    // Cal-mode default — Auto = continuous adapt. Persisted because operators
    // who prefer single-shot calibration (and run TwoTone manually) want that
    // selection to stick across sessions.
    public bool Auto { get; set; } = true;
    public bool AutoAttenuate { get; set; } = true;
    public double MoxDelaySec { get; set; } = 0.2;
    public double LoopDelaySec { get; set; } = 0.0;
    public double AmpDelayNs { get; set; } = 150.0;
    // Thetis PureSignal-form switches ported into the fork's calcc. Defaults
    // match StateDto's and leave the calibrator as it was; a store written
    // before these existed reads back as those defaults.
    public bool PinMode { get; set; } = true;
    public bool MapMode { get; set; } = false;
    public bool Stabilize { get; set; } = false;
    public int Ints { get; set; } = PsCalccLayout.DefaultInts;
    public int Spi { get; set; } = PsCalccLayout.DefaultSpi;
    // Feedback antenna source — Internal coupler (default) or External
    // (Bypass). Persisted so an operator who runs an external sniffer
    // doesn't have to re-pick it every session.
    public PsFeedbackSource Source { get; set; } = PsFeedbackSource.Internal;
    // Two-tone test generator settings. Persisted so an operator who has
    // dialled in custom IMD test tones (e.g. for a specific filter response
    // or PA test) doesn't have to re-enter them every session. Runtime
    // PsEnabled and TwoToneEnabled are intentionally NOT stored here; the
    // distinct ArmIntent above carries only the operator's PS arm decision,
    // while the dialled-in frequencies/magnitude persist below.
    // Defaults match tx-store.ts (700/1900/0.49) and pihpsdr.
    public double TwoToneFreq1 { get; set; } = 700.0;
    public double TwoToneFreq2 { get; set; } = 1900.0;
    public double TwoToneMag { get; set; } = 0.49;
    // Per-board HW peak overrides. Keyed by `{p1|p2}:{board}[:variant]` —
    // e.g. "p2:OrionMkII:G2", "p1:HermesLite2", "p1:Hermes". Populated by
    // RadioService.PersistPsState whenever the operator (or auto-cal)
    // changes PsHwPeak while a radio is connected; consumed by
    // ApplyPsHwPeakForConnection, which prefers a persisted entry over the
    // per-board factory default. Empty on first run — no entry means "use
    // the factory default from RadioService.ResolvePsHwPeak". See lengthy
    // header comment above for the why.
    public Dictionary<string, double> HwPeakByBoard { get; set; } = new();
    // PS TX feedback attenuation (dB) per board, same key scheme as
    // HwPeakByBoard. Keeps a hot external-tap feedback chain (e.g. an RF2K-S
    // −55 dB coupler into G2 PS-IN) out of ADC saturation by restoring the
    // converged attenuation on every connect, instead of booting at 0 dB
    // where the feedback rails and calcc can never fit. Written by
    // RadioService.PersistPsState via SetPsTxAttenuationDb when the auto-
    // attenuate dance (or a manual control) changes it; consumed on connect
    // by DspPipelineService's restore path. Empty on first run — no entry
    // means "leave the radio at 0".
    public Dictionary<string, int> TxAttnByBoard { get; set; } = new();
    // Schema migration marker for TxAttnByBoard cleanups. 0 (the LiteDB
    // default for pre-existing records) = never migrated; 1 = poisoned
    // HermesC10 entries from the PR #1249/#1283 testers builds were cleared;
    // 2 = HermesII entries from the same pre-hardening servo window were also
    // cleared.
    // See PsSettingsStore.MigrateTxAttnPoison.
    public int TxAttnMigration { get; set; }
    public DateTime UpdatedUtc { get; set; }
}
