///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import {
  ProtocolTemplateCommandDefinition,
  TcpHexFieldDefinition,
  TcpHexValueType
} from '@shared/models/device.models';

/** 与 ProtocolTemplateTransportTcpDataConfiguration#fixedFieldByteLength 一致 */
function fixedFieldByteLength(f: TcpHexFieldDefinition): number {
  if (!f?.valueType) {
    return 0;
  }
  if (f.valueType === TcpHexValueType.BYTES_AS_HEX) {
    if (f.byteLength != null && f.byteLength > 0) {
      return f.byteLength;
    }
    return -1;
  }
  return tcpHexFixedTypeWidth(f.valueType);
}

function tcpHexFixedTypeWidth(vt: TcpHexValueType): number {
  switch (vt) {
    case TcpHexValueType.UINT8:
    case TcpHexValueType.INT8:
      return 1;
    case TcpHexValueType.UINT16_BE:
    case TcpHexValueType.UINT16_LE:
    case TcpHexValueType.INT16_BE:
    case TcpHexValueType.INT16_LE:
      return 2;
    case TcpHexValueType.UINT32_BE:
    case TcpHexValueType.UINT32_LE:
    case TcpHexValueType.INT32_BE:
    case TcpHexValueType.INT32_LE:
    case TcpHexValueType.FLOAT_BE:
    case TcpHexValueType.FLOAT_LE:
      return 4;
    case TcpHexValueType.DOUBLE_BE:
    case TcpHexValueType.DOUBLE_LE:
      return 8;
    default:
      return 0;
  }
}

/** 与下行组帧命令字区间 [cmdOff, cmdOff+cmdW) 是否相交 */
export function fieldOverlapsCommandSpan(f: TcpHexFieldDefinition, cmdOff: number, cmdW: number): boolean {
  const fw = fixedFieldByteLength(f);
  if (fw <= 0 || cmdW <= 0) {
    return f.byteOffset === cmdOff;
  }
  const f1 = f.byteOffset + fw;
  const c1 = cmdOff + cmdW;
  return f.byteOffset < c1 && cmdOff < f1;
}

export function tcpHexMatchValueTypeWidth(vt: TcpHexValueType | undefined): number {
  switch (vt) {
    case TcpHexValueType.UINT8:
    case TcpHexValueType.INT8:
      return 1;
    case TcpHexValueType.UINT16_BE:
    case TcpHexValueType.UINT16_LE:
    case TcpHexValueType.INT16_BE:
    case TcpHexValueType.INT16_LE:
      return 2;
    case TcpHexValueType.UINT32_BE:
    case TcpHexValueType.UINT32_LE:
    case TcpHexValueType.INT32_BE:
    case TcpHexValueType.INT32_LE:
      return 4;
    default:
      return 4;
  }
}

function hexFieldSpansOverlap(a: TcpHexFieldDefinition, b: TcpHexFieldDefinition): boolean {
  const a0 = a.byteOffset;
  const b0 = b.byteOffset;
  const alen = fixedFieldByteLength(a);
  const blen = fixedFieldByteLength(b);
  if (alen <= 0 || blen <= 0) {
    return a0 === b0;
  }
  const a1 = a0 + alen;
  const b1 = b0 + blen;
  return a0 < b1 && b0 < a1;
}

/**
 * 与 Java {@code ProtocolTemplateTransportTcpDataConfiguration.mergeTemplateAndCommandFields} 一致。
 */
export function mergeTemplateAndCommandFields(
  templateFields: TcpHexFieldDefinition[] | undefined | null,
  commandFields: TcpHexFieldDefinition[] | undefined | null
): TcpHexFieldDefinition[] {
  const cmdIn = (commandFields ?? []).filter(c => !!c);
  if (!cmdIn.length) {
    return (templateFields ?? []).filter(t => !!t).slice();
  }
  const tplIn = (templateFields ?? []).filter(t => !!t);
  if (!tplIn.length) {
    return cmdIn.slice();
  }
  const merged: TcpHexFieldDefinition[] = [];
  for (const t of tplIn) {
    let overlaps = false;
    for (const c of cmdIn) {
      if (hexFieldSpansOverlap(t, c)) {
        overlaps = true;
        break;
      }
    }
    if (!overlaps) {
      merged.push(t);
    }
  }
  merged.push(...cmdIn);
  return merged;
}

/**
 * 组帧 JSON 默认值（遥测语义：已乘 scale；0 表示「原始 0」）。
 * BYTES_AS_HEX 仅生成固定 byteLength 的全零十六进制；动态长度字段跳过（需手写）。
 */
export function defaultJsonValueForHexField(f: TcpHexFieldDefinition): unknown | undefined {
  if (!f?.key?.trim()) {
    return undefined;
  }
  const vt = f.valueType;
  if (vt === TcpHexValueType.BYTES_AS_HEX) {
    const n = f.byteLength;
    if (n != null && n > 0) {
      return '00'.repeat(n);
    }
    return undefined;
  }
  switch (vt) {
    case TcpHexValueType.UINT8:
    case TcpHexValueType.INT8:
    case TcpHexValueType.UINT16_BE:
    case TcpHexValueType.UINT16_LE:
    case TcpHexValueType.INT16_BE:
    case TcpHexValueType.INT16_LE:
    case TcpHexValueType.UINT32_BE:
    case TcpHexValueType.UINT32_LE:
    case TcpHexValueType.INT32_BE:
    case TcpHexValueType.INT32_LE:
      return 0;
    case TcpHexValueType.FLOAT_BE:
    case TcpHexValueType.FLOAT_LE:
    case TcpHexValueType.DOUBLE_BE:
    case TcpHexValueType.DOUBLE_LE:
      return 0;
    default:
      return undefined;
  }
}

/** 配置了固定线值/固定 hex 时下行组帧可不传 JSON（与后端 TcpHexFieldDefinition 一致） */
export function hexFieldHasFixedValue(f: TcpHexFieldDefinition | null | undefined): boolean {
  if (!f) {
    return false;
  }
  if (f.fixedWireIntegralValue != null && Number.isFinite(Number(f.fixedWireIntegralValue))) {
    return true;
  }
  return !!String(f.fixedBytesHex ?? '').trim();
}

/** 与后端下行组帧：参长由系统写入、JSON 可省略 */
export function writesAutoDownlinkPayloadLength(
  f: TcpHexFieldDefinition,
  cmd?: ProtocolTemplateCommandDefinition
): boolean {
  if (cmd?.downlinkPayloadLengthAuto) {
    const lk = cmd.downlinkPayloadLengthFieldKey?.trim();
    if (lk && f.key === lk) {
      return true;
    }
  }
  if (f.downlinkPayloadLengthMemberKeys?.length) {
    return true;
  }
  return f.autoDownlinkPayloadLength === true;
}

export interface DownlinkSkeletonOptions {
  /** 模板命令字偏移 */
  commandByteOffset: number;
  /** 命令匹配类型宽度（与 matchValueType 一致） */
  commandMatchValueType?: TcpHexValueType;
  /** 当前下行命令（命令级自动参长时用于省略长度键） */
  command?: ProtocolTemplateCommandDefinition;
}

/** 按合并后字段生成对象（按 byteOffset、key 排序，便于对照帧布局） */
export function buildDownlinkFieldValuesSkeleton(
  merged: TcpHexFieldDefinition[],
  opts?: DownlinkSkeletonOptions
): Record<string, unknown> {
  const cmdOff = opts?.commandByteOffset;
  const cmdW = opts ? tcpHexMatchValueTypeWidth(opts.commandMatchValueType) : 4;
  const sorted = merged
    .filter(f => f && f.key && String(f.key).trim())
    .slice()
    .sort((a, b) => a.byteOffset - b.byteOffset || String(a.key).localeCompare(String(b.key)));
  const out: Record<string, unknown> = {};
  for (const f of sorted) {
    if (writesAutoDownlinkPayloadLength(f, opts?.command)) {
      continue;
    }
    if (hexFieldHasFixedValue(f)) {
      continue;
    }
    if (cmdOff != null && fieldOverlapsCommandSpan(f, cmdOff, cmdW)) {
      continue;
    }
    const v = defaultJsonValueForHexField(f);
    if (v !== undefined) {
      out[f.key] = v;
    }
  }
  return out;
}
