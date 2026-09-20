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
// PureSignal curve surfaces follow Warren Pratt's (NR0V) WDSP calcc display
// buffers; see native/wdsp/calcc.c GetPSDisp.

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Zeus.Contracts;

// PureSignal correction curves — the AM/AM and AM/PM data, plus the sample
// cloud calcc fitted them through. HEADERED (16-byte WireFormat header),
// like DisplayFrame and AudioFrame, because it is a multi-kilobyte frame and
// the timestamp is what lets a client say how old a curve is.
//
// Body:
//   [0]      version   u8    1
//   [1]      flags     u8    bit0 = scatter present
//   [2..3]   points    u16   curve points, calcc's DISP_PTS (512)
//   [4..5]   scatter   u16   scatter points that follow (0 when bit0 clear)
//   [6..9]   phsRefDeg f32   PA phase shift at full drive, degrees
//   [10..13] attempts  u32   info[5] when captured
//   then points  f32  magCorrection   (calcc ym_cor)
//   then points  f32  phaseDeg        (calcc ya_cor, unwrapped, 0 at full drive)
//   then scatter f32  x               (normalised FEEDBACK magnitude)
//   then scatter f32  mag             (TX/RX magnitude ratio)
//   then scatter f32  phaseDeg
//
// Planar, not interleaved, matching DisplayFrame's pan/waterfall pair: a
// client can blit each field into a typed array in one copy.
//
// The curve x axis is implicit — point k sits at k/(points-1) of normalised
// drive, which is how calcc builds it (calcc.c:1684-1730). Sending a ramp
// would be 2 KB a frame to say something both ends already know.
//
// Emitted only on a CalibrationAttempts edge while PS is armed, because that
// is the only time calcc's display buffers change. A client that attaches
// mid-over sees no curve until the next accepted fit.
public readonly record struct PsCurveFrame(
    uint Seq,
    double TsUnixMs,
    ushort Points,
    float PhsRefDeg,
    uint CalibrationAttempts,
    ReadOnlyMemory<float> MagCorrection,
    ReadOnlyMemory<float> PhaseDeg,
    ReadOnlyMemory<float> ScatterX,
    ReadOnlyMemory<float> ScatterMag,
    ReadOnlyMemory<float> ScatterPhaseDeg)
{
    public const int BodyHeaderSize = 1 + 1 + 2 + 2 + 4 + 4;
    public const byte Version = 1;
    public const byte FlagScatter = 1 << 0;

    public ushort ScatterCount => checked((ushort)ScatterX.Length);

    public int BodyByteLength =>
        BodyHeaderSize + Points * 4 * 2 + ScatterCount * 4 * 3;

    public int TotalByteLength => WireFormat.HeaderSize + BodyByteLength;

    public void Serialize(IBufferWriter<byte> writer, byte headerFlags = 1)
    {
        if (MagCorrection.Length != Points || PhaseDeg.Length != Points)
            throw new InvalidOperationException("MagCorrection/PhaseDeg must be Points floats long.");
        int scatter = ScatterCount;
        if (ScatterMag.Length != scatter || ScatterPhaseDeg.Length != scatter)
            throw new InvalidOperationException("scatter arrays must be the same length.");

        int total = TotalByteLength;
        var span = writer.GetSpan(total);

        WireFormat.WriteHeader(
            span,
            MsgType.PsCurve,
            headerFlags,
            checked((ushort)BodyByteLength),
            Seq,
            TsUnixMs);

        var body = span.Slice(WireFormat.HeaderSize, BodyByteLength);
        body[0] = Version;
        body[1] = scatter > 0 ? FlagScatter : (byte)0;
        BinaryPrimitives.WriteUInt16LittleEndian(body.Slice(2, 2), Points);
        BinaryPrimitives.WriteUInt16LittleEndian(body.Slice(4, 2), (ushort)scatter);
        BinaryPrimitives.WriteSingleLittleEndian(body.Slice(6, 4), PhsRefDeg);
        BinaryPrimitives.WriteUInt32LittleEndian(body.Slice(10, 4), CalibrationAttempts);

        int curveBytes = Points * 4;
        int scatterBytes = scatter * 4;
        int at = BodyHeaderSize;
        MemoryMarshal.AsBytes(MagCorrection.Span).CopyTo(body.Slice(at, curveBytes)); at += curveBytes;
        MemoryMarshal.AsBytes(PhaseDeg.Span).CopyTo(body.Slice(at, curveBytes)); at += curveBytes;
        MemoryMarshal.AsBytes(ScatterX.Span).CopyTo(body.Slice(at, scatterBytes)); at += scatterBytes;
        MemoryMarshal.AsBytes(ScatterMag.Span).CopyTo(body.Slice(at, scatterBytes)); at += scatterBytes;
        MemoryMarshal.AsBytes(ScatterPhaseDeg.Span).CopyTo(body.Slice(at, scatterBytes));

        writer.Advance(total);
    }

    public static PsCurveFrame Deserialize(ReadOnlySpan<byte> bytes)
    {
        WireFormat.ReadHeader(bytes, out var msgType, out _, out var payloadLen, out var seq, out var ts);
        if (msgType != MsgType.PsCurve)
            throw new InvalidDataException($"expected PsCurve, got {msgType}");
        if (payloadLen < BodyHeaderSize)
            throw new InvalidDataException($"PsCurveFrame body needs {BodyHeaderSize} bytes, got {payloadLen}");

        var body = bytes.Slice(WireFormat.HeaderSize, payloadLen);
        byte version = body[0];
        if (version != Version)
            throw new InvalidDataException($"PsCurveFrame version {version} not understood");

        ushort points = BinaryPrimitives.ReadUInt16LittleEndian(body.Slice(2, 2));
        ushort scatter = BinaryPrimitives.ReadUInt16LittleEndian(body.Slice(4, 2));
        float phsRef = BinaryPrimitives.ReadSingleLittleEndian(body.Slice(6, 4));
        uint attempts = BinaryPrimitives.ReadUInt32LittleEndian(body.Slice(10, 4));

        int curveBytes = points * 4;
        int scatterBytes = scatter * 4;
        if (payloadLen < BodyHeaderSize + curveBytes * 2 + scatterBytes * 3)
            throw new InvalidDataException("PsCurveFrame body shorter than its own field counts");

        var mag = new float[points];
        var phase = new float[points];
        var sx = new float[scatter];
        var sm = new float[scatter];
        var sp = new float[scatter];

        int at = BodyHeaderSize;
        body.Slice(at, curveBytes).CopyTo(MemoryMarshal.AsBytes(mag.AsSpan())); at += curveBytes;
        body.Slice(at, curveBytes).CopyTo(MemoryMarshal.AsBytes(phase.AsSpan())); at += curveBytes;
        body.Slice(at, scatterBytes).CopyTo(MemoryMarshal.AsBytes(sx.AsSpan())); at += scatterBytes;
        body.Slice(at, scatterBytes).CopyTo(MemoryMarshal.AsBytes(sm.AsSpan())); at += scatterBytes;
        body.Slice(at, scatterBytes).CopyTo(MemoryMarshal.AsBytes(sp.AsSpan()));

        return new PsCurveFrame(seq, ts, points, phsRef, attempts, mag, phase, sx, sm, sp);
    }
}
