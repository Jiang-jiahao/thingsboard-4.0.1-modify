///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import {
  ProtocolTemplateCommandDefinition,
  TcpHexFieldDefinition,
  TcpHexLtvTagMapping,
  TcpHexValueType,
  isTcpHexVariableByteSlice
} from '@shared/models/device.models';

function hexDigitsToBytes(hex: string): Uint8Array {
  const d = hex.replace(/\s+/g, '');
  const out = new Uint8Array(d.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = Number.parseInt(d.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

/** 与 ProtocolTemplateTransportTcpDataConfiguration#fixedFieldByteLength 一致 */
function fixedFieldByteLength(f: TcpHexFieldDefinition): number {
  if (!f?.valueType) {
    return 0;
  }
  if (isTcpHexVariableByteSlice(f.valueType)) {
    if (f.byteLength != null && f.byteLength > 0) {
      return f.byteLength;
    }
    return -1;
  }
  return tcpHexFixedTypeWidth(f.valueType);
}

export function tcpHexFixedTypeWidth(vt: TcpHexValueType): number {
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

/**
 * LTV/命令匹配等「整型 Tag 值」输入框旁的十六进制回显（按字段宽度掩码，与线上整型语义一致）。
 */
export function formatTcpHexMatchValueHexHint(n: number | null | undefined, vt: TcpHexValueType | null | undefined): string {
  if (vt == null) {
    return '';
  }
  const byteW = tcpHexMatchValueTypeWidth(vt);
  if (byteW <= 0) {
    return '';
  }
  const num = Number(n);
  if (!Number.isFinite(num)) {
    return '';
  }
  const bits = BigInt(byteW * 8);
  const mask = (1n << bits) - 1n;
  const v = BigInt(Math.trunc(num)) & mask;
  return '0x' + v.toString(16).toLowerCase().padStart(byteW * 2, '0');
}

/** 从 API/模型载入 LTV Tag 输入框：按 Tag 字段类型宽度的 `0x` 十六进制 */
export function ltvTagWireTextFromModel(
  n: number | undefined | null,
  tagFieldType?: TcpHexValueType | null
): string {
  return formatTcpHexMatchValueHexHint(n, tagFieldType ?? TcpHexValueType.UINT8);
}

/**
 * 从模型载入单行 LTV Tag：tagValueLiterallyHex 为 true/false 时强制对应 0x/十进制回显；
 * 未写入该字段的旧数据则回退到节级 unknownTagTelemetryKeyHexLiteral === true 时用 0x，否则十进制。
 */
export function ltvTagWireTextForMapping(
  m: Pick<TcpHexLtvTagMapping, 'tagValue' | 'tagValueLiterallyHex'> | undefined | null,
  tagFieldType: TcpHexValueType | null | undefined,
  sectionUnknownTagTelemetryKeyHexLiteral: boolean | undefined | null
): string {
  if (tagFieldType == null) {
    return '';
  }
  const n = m?.tagValue;
  if (n == null || !Number.isFinite(Number(n))) {
    return '';
  }
  const tri = m?.tagValueLiterallyHex;
  if (tri === true) {
    return formatTcpHexMatchValueHexHint(Number(n), tagFieldType);
  }
  if (tri === false) {
    return String(Math.trunc(Number(n)));
  }
  if (sectionUnknownTagTelemetryKeyHexLiteral === true) {
    return formatTcpHexMatchValueHexHint(Number(n), tagFieldType);
  }
  return String(Math.trunc(Number(n)));
}

/** 与保存时 LTV 节级 unknownTagTelemetryKeyHexLiteral 一致：任一行映射以 0x 字面保存则为 true。 */
export function unknownTagTelemetryKeyHexLiteralFromMappings(tagMappings: TcpHexLtvTagMapping[]): boolean {
  return tagMappings.some(m => m.tagValueLiterallyHex === true);
}

/**
 * LTV Tag 输入：允许 `0x`/`0X` 前缀十六进制，或（可选）纯十进制整数。
 */
export function parseLtvTagWireTextToNumber(raw: unknown): number | undefined {
  if (raw === null || raw === undefined) {
    return undefined;
  }
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    return Math.trunc(raw);
  }
  const s = String(raw).trim().replace(/\s+/g, '');
  if (!s) {
    return undefined;
  }
  if (/^-?\d+$/.test(s)) {
    const n = Number.parseInt(s, 10);
    return Number.isFinite(n) ? n : undefined;
  }
  if (!/^0x[0-9a-fA-F]+$/i.test(s)) {
    return undefined;
  }
  const n = Number.parseInt(s.slice(2), 16);
  return Number.isFinite(n) ? n : undefined;
}

/**
 * 与后端 {@code ProtocolTemplateHexBuildService.isRedundantPaWordField} 一致：
 * 已定义 paN_hi + paN_lo 时，paN_word 不应再作为独立可编辑/写入字段（否则与 hi/lo 叠写会多字节）。
 */
function isRedundantPaWordFieldInMerged(f: TcpHexFieldDefinition, merged: TcpHexFieldDefinition[]): boolean {
  const key = f?.key?.trim();
  if (!key) {
    return false;
  }
  const m = /^pa(\d+)_word$/.exec(key);
  if (!m) {
    return false;
  }
  const n = m[1];
  const hk = `pa${n}_hi`;
  const lk = `pa${n}_lo`;
  let hi = false;
  let lo = false;
  for (const g of merged ?? []) {
    const k = g?.key?.trim();
    if (!k) {
      continue;
    }
    if (k === hk) {
      hi = true;
    } else if (k === lk) {
      lo = true;
    }
    if (hi && lo) {
      return true;
    }
  }
  return false;
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
 * BYTES_AS_HEX 仅生成固定 byteLength 的全零十六进制；BYTES_AS_UTF8 用空串（组帧左侧补零）；动态长度字段跳过（需手写）。
 */
export function defaultJsonValueForHexField(f: TcpHexFieldDefinition): unknown | undefined {
  if (!f?.key?.trim()) {
    return undefined;
  }
  const vt = f.valueType;
  if (vt === TcpHexValueType.BYTES_AS_UTF8) {
    const n = f.byteLength;
    if (n != null && n > 0) {
      return '';
    }
    return undefined;
  }
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

/** 与后端下行组帧：整包总长由系统写入、JSON 可省略 */
export function writesAutoDownlinkTotalFrameLength(f: TcpHexFieldDefinition | null | undefined): boolean {
  return f?.autoDownlinkTotalFrameLength === true;
}

export interface DownlinkSkeletonOptions {
  /** 模板命令字偏移 */
  commandByteOffset: number;
  /** 命令匹配类型宽度（与 matchValueType 一致） */
  commandMatchValueType?: TcpHexValueType;
  /** 当前下行命令（命令级自动参长时用于省略长度键） */
  command?: ProtocolTemplateCommandDefinition;
}

/**
 * 下行组帧需用户填值的字段（与 {@link buildDownlinkFieldValuesSkeleton} 规则一致，不含自动参长/固定线值/与命令字重叠等）。
 */
export function listDownlinkEditableHexFields(
  merged: TcpHexFieldDefinition[],
  opts?: DownlinkSkeletonOptions
): TcpHexFieldDefinition[] {
  const cmdOff = opts?.commandByteOffset;
  const cmdW = opts ? tcpHexMatchValueTypeWidth(opts.commandMatchValueType) : 4;
  const sorted = merged
    .filter(f => f && f.key && String(f.key).trim())
    .slice()
    .sort((a, b) => a.byteOffset - b.byteOffset || String(a.key).localeCompare(String(b.key)));
  const out: TcpHexFieldDefinition[] = [];
  for (const f of sorted) {
    if (writesAutoDownlinkPayloadLength(f, opts?.command)) {
      continue;
    }
    if (writesAutoDownlinkTotalFrameLength(f)) {
      continue;
    }
    if (hexFieldHasFixedValue(f)) {
      continue;
    }
    if (cmdOff != null && fieldOverlapsCommandSpan(f, cmdOff, cmdW)) {
      continue;
    }
    if (defaultJsonValueForHexField(f) === undefined) {
      continue;
    }
    if (isRedundantPaWordFieldInMerged(f, sorted)) {
      continue;
    }
    out.push(f);
  }
  return out;
}

/** 组帧表单初始展示：整型/浮点为十进制；BYTES_AS_HEX 为连续小写 hex（无 0x）；BYTES_AS_UTF8 为原文 */
export function defaultDownlinkFieldInputText(
  f: TcpHexFieldDefinition,
  defaultJson: unknown
): string {
  const vt = f.valueType;
  if (vt === TcpHexValueType.BYTES_AS_UTF8 && typeof defaultJson === 'string') {
    return String(defaultJson);
  }
  if (vt === TcpHexValueType.BYTES_AS_HEX && typeof defaultJson === 'string') {
    return String(defaultJson).replace(/\s+/g, '').toLowerCase();
  }
  if (
    vt === TcpHexValueType.FLOAT_BE ||
    vt === TcpHexValueType.FLOAT_LE ||
    vt === TcpHexValueType.DOUBLE_BE ||
    vt === TcpHexValueType.DOUBLE_LE
  ) {
    if (typeof defaultJson === 'number' && Number.isFinite(defaultJson)) {
      return String(defaultJson);
    }
    return '0';
  }
  if (typeof defaultJson === 'number' && Number.isFinite(defaultJson)) {
    return integralDecimalDisplayString(vt, Math.trunc(defaultJson));
  }
  return '0';
}

function integralDecimalDisplayString(vt: TcpHexValueType, n: number): string {
  switch (vt) {
    case TcpHexValueType.UINT8:
      return String((((n % 256) + 256) % 256));
    case TcpHexValueType.INT8: {
      const b = (((n % 256) + 256) % 256);
      return String(b > 127 ? b - 256 : b);
    }
    case TcpHexValueType.UINT16_BE:
    case TcpHexValueType.UINT16_LE:
      return String((((n % 65536) + 65536) % 65536));
    case TcpHexValueType.INT16_BE:
    case TcpHexValueType.INT16_LE: {
      const w = (((n % 65536) + 65536) % 65536);
      return String(w > 32767 ? w - 65536 : w);
    }
    case TcpHexValueType.UINT32_BE:
    case TcpHexValueType.UINT32_LE:
      return String(n >>> 0);
    case TcpHexValueType.INT32_BE:
    case TcpHexValueType.INT32_LE:
      return String(n | 0);
    default:
      return '0';
  }
}

function integralWireHexNoPrefix(vt: TcpHexValueType, n: number): string {
  let u: number;
  let pad: number;
  switch (vt) {
    case TcpHexValueType.UINT8:
    case TcpHexValueType.INT8:
      u = (((Math.trunc(n) % 256) + 256) % 256);
      pad = 2;
      break;
    case TcpHexValueType.UINT16_BE:
    case TcpHexValueType.UINT16_LE:
    case TcpHexValueType.INT16_BE:
    case TcpHexValueType.INT16_LE:
      u = (((Math.trunc(n) % 65536) + 65536) % 65536);
      pad = 4;
      break;
    case TcpHexValueType.UINT32_BE:
    case TcpHexValueType.UINT32_LE:
    case TcpHexValueType.INT32_BE:
    case TcpHexValueType.INT32_LE:
      u = Math.trunc(n) >>> 0;
      pad = 8;
      break;
    default:
      u = 0;
      pad = 2;
  }
  return u.toString(16).padStart(pad, '0');
}

function floatDoubleWireHex(vt: TcpHexValueType, v: number): string {
  const nBytes = tcpHexFixedTypeWidth(vt);
  const buf = new ArrayBuffer(nBytes);
  const view = new DataView(buf);
  const le = vt === TcpHexValueType.FLOAT_LE || vt === TcpHexValueType.DOUBLE_LE;
  if (nBytes === 4) {
    view.setFloat32(0, v, le);
  } else {
    view.setFloat64(0, v, le);
  }
  let s = '';
  for (let i = 0; i < nBytes; i++) {
    s += view.getUint8(i).toString(16).padStart(2, '0');
  }
  return s;
}

/** 与输入规则一致：0x 前缀 → 0x+hex 回显；否则十进制或纯 hex 字节串 */
export type DownlinkFieldEchoMode = 'dec' | 'hex0x' | 'bytesPlain';

export type DownlinkFieldParseResult =
  | { ok: true; value: unknown; echoMode: DownlinkFieldEchoMode }
  | { ok: false; error: string };

export function formatDownlinkFieldEcho(
  value: unknown,
  vt: TcpHexValueType,
  echoMode: DownlinkFieldEchoMode,
  byteLength?: number
): string {
  if (vt === TcpHexValueType.BYTES_AS_UTF8 && typeof value === 'string') {
    return value;
  }
  if (vt === TcpHexValueType.BYTES_AS_HEX && typeof value === 'string') {
    const h = value.replace(/\s+/g, '').toLowerCase();
    if (echoMode === 'hex0x') {
      return '0x' + h;
    }
    if (echoMode === 'bytesPlain') {
      return h;
    }
    try {
      return BigInt('0x' + (h || '0')).toString(10);
    } catch {
      return h;
    }
  }
  if (
    vt === TcpHexValueType.FLOAT_BE ||
    vt === TcpHexValueType.FLOAT_LE ||
    vt === TcpHexValueType.DOUBLE_BE ||
    vt === TcpHexValueType.DOUBLE_LE
  ) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      if (echoMode === 'hex0x') {
        return '0x' + floatDoubleWireHex(vt, value);
      }
      return String(value);
    }
    return '0';
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    if (echoMode === 'hex0x') {
      return '0x' + integralWireHexNoPrefix(vt, value);
    }
    return integralDecimalDisplayString(vt, Math.trunc(value));
  }
  return '0';
}

const hexToUInt = (hex: string, maxBits: number): bigint => {
  if (!hex.length) {
    return 0n;
  }
  const bi = BigInt('0x' + hex);
  const mask = (1n << BigInt(maxBits)) - 1n;
  return bi & mask;
};

function parseDownlinkFieldFromHexDigits(
  digits: string,
  vt: TcpHexValueType,
  byteLength: number | undefined
): DownlinkFieldParseResult {
  const d = digits.replace(/\s+/g, '');
  if (d.length > 0 && !/^[0-9a-fA-F]+$/.test(d)) {
    return { ok: false, error: 'invalid hex' };
  }

  if (vt === TcpHexValueType.BYTES_AS_UTF8) {
    const n = byteLength;
    if (n == null || n <= 0) {
      return { ok: false, error: 'BYTES_AS_UTF8 needs fixed byteLength' };
    }
    if (d.length > 0 && d.length % 2 === 1) {
      return { ok: false, error: 'odd hex length' };
    }
    const want = n * 2;
    const use = d.length ? d : '00'.repeat(n);
    if (use.length !== want) {
      return { ok: false, error: `need ${want} hex digits (${n} bytes)` };
    }
    const dec = new TextDecoder('utf-8', { fatal: false });
    return { ok: true, value: dec.decode(hexDigitsToBytes(use)), echoMode: 'bytesPlain' };
  }

  if (vt === TcpHexValueType.BYTES_AS_HEX) {
    const n = byteLength;
    if (n == null || n <= 0) {
      return { ok: false, error: 'BYTES_AS_HEX needs fixed byteLength' };
    }
    if (d.length > 0 && d.length % 2 === 1) {
      return { ok: false, error: 'odd hex length' };
    }
    const want = n * 2;
    const use = d.length ? d : '00'.repeat(n);
    if (use.length !== want) {
      return { ok: false, error: `need ${want} hex digits (${n} bytes)` };
    }
    return { ok: true, value: use.toLowerCase(), echoMode: 'hex0x' };
  }

  switch (vt) {
    case TcpHexValueType.UINT8:
    case TcpHexValueType.INT8: {
      const u = d.length ? hexToUInt(d, 8) : 0n;
      if (u > 0xffn) {
        return { ok: false, error: 'max 2 hex digits (1 byte)' };
      }
      if (vt === TcpHexValueType.UINT8) {
        return { ok: true, value: Number(u), echoMode: 'hex0x' };
      }
      const n = Number(u);
      const s8 = n > 127 ? n - 256 : n;
      return { ok: true, value: s8, echoMode: 'hex0x' };
    }
    case TcpHexValueType.UINT16_BE:
    case TcpHexValueType.UINT16_LE:
    case TcpHexValueType.INT16_BE:
    case TcpHexValueType.INT16_LE: {
      const u = d.length ? hexToUInt(d, 16) : 0n;
      if (u > 0xffffn) {
        return { ok: false, error: 'max 4 hex digits (2 bytes)' };
      }
      if (
        vt === TcpHexValueType.UINT16_BE ||
        vt === TcpHexValueType.UINT16_LE
      ) {
        return { ok: true, value: Number(u), echoMode: 'hex0x' };
      }
      const n = Number(u);
      const s16 = n > 32767 ? n - 65536 : n;
      return { ok: true, value: s16, echoMode: 'hex0x' };
    }
    case TcpHexValueType.UINT32_BE:
    case TcpHexValueType.UINT32_LE:
    case TcpHexValueType.INT32_BE:
    case TcpHexValueType.INT32_LE: {
      const u = d.length ? hexToUInt(d, 32) : 0n;
      if (u > 0xffffffffn) {
        return { ok: false, error: 'max 8 hex digits (4 bytes)' };
      }
      if (
        vt === TcpHexValueType.UINT32_BE ||
        vt === TcpHexValueType.UINT32_LE
      ) {
        return { ok: true, value: Number(u), echoMode: 'hex0x' };
      }
      const n = Number(u);
      const s32 = n | 0;
      return { ok: true, value: s32, echoMode: 'hex0x' };
    }
    case TcpHexValueType.FLOAT_BE:
    case TcpHexValueType.FLOAT_LE: {
      if (d.length > 0 && d.length % 2 === 1) {
        return { ok: false, error: 'odd hex length' };
      }
      const want = 8;
      const use = d.length ? d : '0'.repeat(want);
      if (use.length !== want) {
        return { ok: false, error: 'need 8 hex digits (4 bytes)' };
      }
      const buf = new ArrayBuffer(4);
      const view = new DataView(buf);
      const le = vt === TcpHexValueType.FLOAT_LE;
      for (let i = 0; i < 4; i++) {
        view.setUint8(i, Number.parseInt(use.slice(i * 2, i * 2 + 2), 16));
      }
      return { ok: true, value: view.getFloat32(0, le), echoMode: 'hex0x' };
    }
    case TcpHexValueType.DOUBLE_BE:
    case TcpHexValueType.DOUBLE_LE: {
      if (d.length > 0 && d.length % 2 === 1) {
        return { ok: false, error: 'odd hex length' };
      }
      const want = 16;
      const use = d.length ? d : '0'.repeat(want);
      if (use.length !== want) {
        return { ok: false, error: 'need 16 hex digits (8 bytes)' };
      }
      const buf = new ArrayBuffer(8);
      const view = new DataView(buf);
      const le = vt === TcpHexValueType.DOUBLE_LE;
      for (let i = 0; i < 8; i++) {
        view.setUint8(i, Number.parseInt(use.slice(i * 2, i * 2 + 2), 16));
      }
      return { ok: true, value: view.getFloat64(0, le), echoMode: 'hex0x' };
    }
    default:
      return { ok: false, error: 'unsupported type' };
  }
}

function decimalUintToFixedBytesBe(n: bigint, nBytes: number): string {
  const max = 1n << BigInt(8 * nBytes);
  if (n < 0n || n >= max) {
    return '';
  }
  let s = '';
  for (let i = nBytes - 1; i >= 0; i--) {
    s += Number((n >> BigInt(8 * i)) & 0xffn)
      .toString(16)
      .padStart(2, '0');
  }
  return s;
}

function parseDownlinkFieldFromDecimalText(
  trimmed: string,
  vt: TcpHexValueType,
  byteLength: number | undefined
): DownlinkFieldParseResult {
  const collapsed = trimmed.replace(/\s+/g, '');

  if (vt === TcpHexValueType.BYTES_AS_UTF8) {
    const nB = byteLength;
    if (nB == null || nB <= 0) {
      return { ok: false, error: 'BYTES_AS_UTF8 needs fixed byteLength' };
    }
    if (/^[0-9a-fA-F]+$/.test(collapsed) && collapsed.length % 2 === 0 && collapsed.length === nB * 2) {
      const dec = new TextDecoder('utf-8', { fatal: false });
      return { ok: true, value: dec.decode(hexDigitsToBytes(collapsed)), echoMode: 'bytesPlain' };
    }
    const enc = new TextEncoder().encode(trimmed);
    if (enc.length > nB) {
      return { ok: false, error: `UTF-8 encodes to ${enc.length} bytes, max ${nB}` };
    }
    return { ok: true, value: trimmed, echoMode: 'dec' };
  }

  if (vt === TcpHexValueType.BYTES_AS_HEX) {
    const nB = byteLength;
    if (nB == null || nB <= 0) {
      return { ok: false, error: 'BYTES_AS_HEX needs fixed byteLength' };
    }
    if (/^\d+$/.test(collapsed)) {
      try {
        const bi = BigInt(collapsed);
        const hex = decimalUintToFixedBytesBe(bi, nB);
        if (!hex) {
          return { ok: false, error: `decimal out of range for ${nB} bytes` };
        }
        return { ok: true, value: hex, echoMode: 'dec' };
      } catch {
        return { ok: false, error: 'invalid decimal' };
      }
    }
    if (/^[0-9a-fA-F]+$/.test(collapsed)) {
      if (collapsed.length % 2 === 1) {
        return { ok: false, error: 'odd hex length' };
      }
      const want = nB * 2;
      if (collapsed.length !== want) {
        return { ok: false, error: `need ${want} hex digits (${nB} bytes)` };
      }
      return { ok: true, value: collapsed.toLowerCase(), echoMode: 'bytesPlain' };
    }
    return { ok: false, error: 'use decimal digits or even-length hex (no 0x)' };
  }

  if (
    vt === TcpHexValueType.FLOAT_BE ||
    vt === TcpHexValueType.FLOAT_LE ||
    vt === TcpHexValueType.DOUBLE_BE ||
    vt === TcpHexValueType.DOUBLE_LE
  ) {
    const x = Number(trimmed);
    if (!Number.isFinite(x)) {
      return { ok: false, error: 'invalid number' };
    }
    return { ok: true, value: x, echoMode: 'dec' };
  }

  const t = Number(trimmed);
  if (!Number.isFinite(t) || !Number.isInteger(t)) {
    return { ok: false, error: 'use decimal integer or 0x hex' };
  }

  switch (vt) {
    case TcpHexValueType.UINT8:
      if (t < 0 || t > 255) {
        return { ok: false, error: 'UINT8 range 0..255' };
      }
      return { ok: true, value: t, echoMode: 'dec' };
    case TcpHexValueType.INT8:
      if (t < -128 || t > 127) {
        return { ok: false, error: 'INT8 range -128..127' };
      }
      return { ok: true, value: t, echoMode: 'dec' };
    case TcpHexValueType.UINT16_BE:
    case TcpHexValueType.UINT16_LE:
      if (t < 0 || t > 65535) {
        return { ok: false, error: 'UINT16 range 0..65535' };
      }
      return { ok: true, value: t, echoMode: 'dec' };
    case TcpHexValueType.INT16_BE:
    case TcpHexValueType.INT16_LE:
      if (t < -32768 || t > 32767) {
        return { ok: false, error: 'INT16 range -32768..32767' };
      }
      return { ok: true, value: t, echoMode: 'dec' };
    case TcpHexValueType.UINT32_BE:
    case TcpHexValueType.UINT32_LE:
      if (t < 0 || t > 4294967295) {
        return { ok: false, error: 'UINT32 range 0..4294967295' };
      }
      return { ok: true, value: t, echoMode: 'dec' };
    case TcpHexValueType.INT32_BE:
    case TcpHexValueType.INT32_LE:
      if (t < -2147483648 || t > 2147483647) {
        return { ok: false, error: 'INT32 range' };
      }
      return { ok: true, value: t | 0, echoMode: 'dec' };
    default:
      return { ok: false, error: 'unsupported type' };
  }
}

function emptyDefaultDownlinkFieldParse(
  vt: TcpHexValueType,
  byteLength: number | undefined
): DownlinkFieldParseResult {
  if (vt === TcpHexValueType.BYTES_AS_UTF8) {
    const n = byteLength;
    if (n == null || n <= 0) {
      return { ok: false, error: 'BYTES_AS_UTF8 needs fixed byteLength' };
    }
    return { ok: true, value: '', echoMode: 'dec' };
  }
  if (vt === TcpHexValueType.BYTES_AS_HEX) {
    const n = byteLength;
    if (n == null || n <= 0) {
      return { ok: false, error: 'BYTES_AS_HEX needs fixed byteLength' };
    }
    return { ok: true, value: '00'.repeat(n), echoMode: 'dec' };
  }
  if (
    vt === TcpHexValueType.FLOAT_BE ||
    vt === TcpHexValueType.FLOAT_LE ||
    vt === TcpHexValueType.DOUBLE_BE ||
    vt === TcpHexValueType.DOUBLE_LE
  ) {
    return { ok: true, value: 0, echoMode: 'dec' };
  }
  return { ok: true, value: 0, echoMode: 'dec' };
}

/**
 * 以 0x / 0X 开头按十六进制解析；否则按十进制（整型/浮点）。
 * BYTES_AS_HEX：0x 后为线型 hex；否则纯数字按无符号十进制大端编码，或偶数位纯 hex 字串（无 0x）。
 */
export function parseDownlinkFieldInput(
  rawInput: string,
  vt: TcpHexValueType,
  byteLength?: number
): DownlinkFieldParseResult {
  const trimmed = String(rawInput ?? '').trim();
  if (trimmed === '') {
    return emptyDefaultDownlinkFieldParse(vt, byteLength);
  }
  if (/^0x/i.test(trimmed)) {
    const digits = trimmed.replace(/^0x/i, '').replace(/\s+/g, '');
    return parseDownlinkFieldFromHexDigits(digits, vt, byteLength);
  }
  return parseDownlinkFieldFromDecimalText(trimmed, vt, byteLength);
}

/** 按合并后字段生成对象（按 byteOffset、key 排序，便于对照帧布局） */
export function buildDownlinkFieldValuesSkeleton(
  merged: TcpHexFieldDefinition[],
  opts?: DownlinkSkeletonOptions
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const f of listDownlinkEditableHexFields(merged, opts)) {
    const v = defaultJsonValueForHexField(f);
    if (v !== undefined) {
      out[f.key] = v;
    }
  }
  return out;
}

/**
 * 固定线值整型：仅按十进制解析（与所选整型类型的线值一致，不用 0x/十六进制串规则）。
 */
export function parseIntegralWireTextToNumber(raw: unknown): number | undefined {
  if (raw === null || raw === undefined) {
    return undefined;
  }
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    return Math.trunc(raw);
  }
  const s = String(raw).trim();
  if (!s) {
    return undefined;
  }
  const n = Number(s);
  return Number.isFinite(n) ? Math.trunc(n) : undefined;
}

/** 从 API 载入：十进制回显。 */
export function formatFixedWireIntegralFromModel(v: number | undefined | null): string {
  if (v === undefined || v === null || !Number.isFinite(Number(v))) {
    return '';
  }
  return String(Math.trunc(Number(v)));
}

/** 失焦归一化：十进制回显。 */
export function formatIntegralWireTextEcho(rawInput: string, numericValue: number): string {
  const trimmed = String(rawInput ?? '').trim();
  if (trimmed === '') {
    return '';
  }
  if (!Number.isFinite(numericValue)) {
    return trimmed;
  }
  return String(Math.trunc(numericValue));
}

/**
 * BYTES_AS_HEX 固定内容：仅去掉空白，不按数值解析，以保留前导 0 与用户输入的字母大小写。
 */
export function normalizeFixedBytesHexWhitespace(raw: unknown): string {
  return String(raw ?? '').replace(/\s+/g, '');
}

/**
 * 从模型写入表单：字符串原样（去空白）；若历史 JSON 误存为数字则按 byteLength 左补到 2×byteLength 位 hex（仅用于恢复展示）。
 */
export function fixedBytesHexModelToFormControl(f?: TcpHexFieldDefinition): string {
  if (!f) {
    return '';
  }
  const raw: unknown = (f as { fixedBytesHex?: unknown }).fixedBytesHex;
  if (raw === null || raw === undefined || raw === '') {
    return '';
  }
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    const bl = f.byteLength;
    if (bl != null && bl > 0) {
      const n = Math.trunc(raw);
      const u = n >= 0 ? n : n >>> 0;
      return u.toString(16).padStart(bl * 2, '0');
    }
    return String(raw);
  }
  return normalizeFixedBytesHexWhitespace(raw);
}
