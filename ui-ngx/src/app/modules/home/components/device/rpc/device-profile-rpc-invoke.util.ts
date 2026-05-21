///
/// Copyright © 2016-2025 The Thingsboard Authors
///

import {
  buildDownlinkFieldValuesSkeleton,
  defaultDownlinkFieldInputText,
  defaultJsonValueForHexField,
  listDownlinkEditableHexFields,
  mergeTemplateAndCommandFields,
  formatDownlinkFieldEcho,
  parseDownlinkFieldInput
} from '@home/pages/profiles/protocol-template-downlink-fields.util';
import {
  DeviceProfileRpcBindingType,
  DeviceProfileRpcMethod,
  isTcpHexVariableByteSlice,
  ProtocolTemplateBundle,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateDefinition,
  TcpHexFieldDefinition,
  TcpHexValueType,
  TcpDeviceProfileTransportConfiguration
} from '@shared/models/device.models';

export interface DeviceRpcInvokeFieldRow {
  /** 协议模板字段 key（模块侧） */
  templateFieldKey: string;
  /** 平台侧参数名（表单键、固定参数键） */
  platformKey: string;
  valueType: TcpHexValueType;
  byteLength?: number;
  inputText: string;
}

/** paramMap 中平台名 → 模板字段 key；无映射时与 platformKey 相同 */
export function templateFieldKeyForPlatformKey(platformKey: string, paramMap?: Record<string, string>): string {
  return paramMap?.[platformKey] ?? platformKey;
}

export function protocolTemplateBundleIdFromTcpProfile(
  tcp: TcpDeviceProfileTransportConfiguration | null | undefined
): string | null {
  const id = tcp?.transportTcpDataTypeConfiguration?.protocolTemplateBundleId
    ?? tcp?.transportTcpDataTypeConfiguration?.monitoringProtocolBundleId;
  return id?.trim() || null;
}

export function findBundleCommand(
  bundle: ProtocolTemplateBundle,
  method: DeviceProfileRpcMethod
): ProtocolTemplateCommandDefinition | null {
  const cmds = bundle.protocolCommands ?? bundle.monitoringCommands ?? [];
  return cmds.find(c => c
    && c.templateId === method.templateId
    && c.commandValue === method.commandValue
    && (method.templateCommandName == null || method.templateCommandName === '' || c.name === method.templateCommandName)
  ) ?? null;
}

export function platformKeyForTemplateField(templateKey: string, paramMap?: Record<string, string>): string {
  if (!paramMap) {
    return templateKey;
  }
  for (const [pk, tk] of Object.entries(paramMap)) {
    if (tk === templateKey) {
      return pk;
    }
  }
  return templateKey;
}

export function applyParamMapToTemplateValues(
  platformValues: Record<string, unknown>,
  paramMap?: Record<string, string>
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(platformValues)) {
    const tk = paramMap?.[k] ?? k;
    out[tk] = v;
  }
  return out;
}

export function buildInvokeFieldRows(
  bundle: ProtocolTemplateBundle,
  catalogMethod: DeviceProfileRpcMethod
): { rows: DeviceRpcInvokeFieldRow[]; valuesJsonFallback: string; usesFieldInputs: boolean } {
  const cmd = findBundleCommand(bundle, catalogMethod);
  const templates = bundle.protocolTemplates ?? bundle.monitoringTemplates ?? [];
  const tpl = templates.find(t => t && t.id === catalogMethod.templateId);
  if (!cmd || !tpl) {
    return { rows: [], valuesJsonFallback: '{}', usesFieldInputs: false };
  }
  const merged = mergeTemplateAndCommandFields(tpl.hexProtocolFields, cmd.fields);
  const cmdOff = tpl.commandByteOffset ?? 12;
  const matchVt = cmd.matchValueType ?? TcpHexValueType.UINT32_LE;
  const tplCmdW = tpl.commandMatchWidth === 1 ? 1 : 4;
  const opts = {
    commandByteOffset: cmdOff,
    commandMatchValueType: matchVt,
    commandMatchWireByteWidth: isTcpHexVariableByteSlice(matchVt) ? tplCmdW : undefined,
    command: cmd
  };
  const fields = listDownlinkEditableHexFields(merged, opts);
  const skeleton = buildDownlinkFieldValuesSkeleton(merged, opts);
  const rows: DeviceRpcInvokeFieldRow[] = [];
  for (const f of fields) {
    const dj = defaultJsonValueForHexField(f);
    if (dj === undefined) {
      continue;
    }
    const templateFieldKey = f.key!;
    rows.push({
      templateFieldKey,
      platformKey: platformKeyForTemplateField(templateFieldKey, catalogMethod.paramMap),
      valueType: f.valueType,
      byteLength: f.byteLength,
      inputText: defaultDownlinkFieldInputText(f, dj)
    });
  }
  if (rows.length > 0) {
    return { rows, valuesJsonFallback: '{}', usesFieldInputs: true };
  }
  const platformSkeleton: Record<string, unknown> = {};
  for (const [tk, v] of Object.entries(skeleton)) {
    platformSkeleton[platformKeyForTemplateField(tk, catalogMethod.paramMap)] = v;
  }
  return {
    rows: [],
    valuesJsonFallback: Object.keys(platformSkeleton).length ? JSON.stringify(platformSkeleton, null, 2) : '{}',
    usesFieldInputs: false
  };
}

export function rpcParamDefaultsFromDeviceData(
  deviceData?: { rpcParamDefaults?: Record<string, unknown> } | null
): Record<string, unknown> {
  const d = deviceData?.rpcParamDefaults;
  if (!d || typeof d !== 'object' || Array.isArray(d)) {
    return {};
  }
  return { ...d };
}

export function rpcParamDefaultsByMethodFromDeviceData(
  deviceData?: { rpcParamDefaultsByMethod?: Record<string, Record<string, unknown>> } | null
): Record<string, Record<string, unknown>> {
  const m = deviceData?.rpcParamDefaultsByMethod;
  if (!m || typeof m !== 'object' || Array.isArray(m)) {
    return {};
  }
  const out: Record<string, Record<string, unknown>> = {};
  for (const [methodId, params] of Object.entries(m)) {
    if (params && typeof params === 'object' && !Array.isArray(params)) {
      out[methodId] = { ...(params as Record<string, unknown>) };
    }
  }
  return out;
}

/** 当前方法生效的固定参数：优先按方法配置，否则回退旧版全局 rpcParamDefaults */
export function resolveMethodRpcDefaults(
  deviceData?: {
    rpcParamDefaultsByMethod?: Record<string, Record<string, unknown>>;
    rpcParamDefaults?: Record<string, unknown>;
  } | null,
  methodId?: string | null
): Record<string, unknown> {
  if (!methodId) {
    return {};
  }
  const byMethod = rpcParamDefaultsByMethodFromDeviceData(deviceData);
  const perMethod = byMethod[methodId];
  if (perMethod && Object.keys(perMethod).length > 0) {
    return { ...perMethod };
  }
  const legacy = rpcParamDefaultsFromDeviceData(deviceData);
  if (Object.keys(byMethod).length === 0 && Object.keys(legacy).length > 0) {
    return { ...legacy };
  }
  return {};
}

export interface DeviceRpcMethodFieldMeta {
  valueType: TcpHexValueType;
  byteLength?: number;
}

export interface DeviceRpcFixedParamEntry {
  platformKey: string;
  valueText: string;
}

/** 当前 RPC 方法在模板/档案中声明的平台侧参数字段名 */
export function catalogMethodPlatformKeys(
  bundle: ProtocolTemplateBundle | null,
  method: DeviceProfileRpcMethod
): string[] {
  if (method.bindingType === DeviceProfileRpcBindingType.TCP_TEMPLATE && bundle) {
    const built = buildInvokeFieldRows(bundle, method);
    if (built.usesFieldInputs) {
      return built.rows.map(r => r.platformKey);
    }
    const sk = parseNativeParamsJson(built.valuesJsonFallback) ?? {};
    return Object.keys(sk);
  }
  const profile = parseNativeParamsJson(method.paramsTemplateJson ?? '') ?? {};
  return Object.keys(profile);
}

export function catalogMethodFieldMetaMap(
  bundle: ProtocolTemplateBundle | null,
  method: DeviceProfileRpcMethod
): Record<string, DeviceRpcMethodFieldMeta> {
  const meta: Record<string, DeviceRpcMethodFieldMeta> = {};
  if (method.bindingType === DeviceProfileRpcBindingType.TCP_TEMPLATE && bundle) {
    const built = buildInvokeFieldRows(bundle, method);
    for (const row of built.rows) {
      meta[row.platformKey] = { valueType: row.valueType, byteLength: row.byteLength };
    }
  }
  return meta;
}

export function fixedEntriesFromDefaults(
  defaults: Record<string, unknown>,
  fieldMeta?: Record<string, DeviceRpcMethodFieldMeta>
): DeviceRpcFixedParamEntry[] {
  return Object.entries(defaults).map(([platformKey, v]) => {
    const fm = fieldMeta?.[platformKey];
    let valueText = v === null || v === undefined ? '' : String(v);
    if (fm && v !== null && v !== undefined) {
      const echoMode = fm.valueType === TcpHexValueType.BYTES_AS_HEX ? 'bytesPlain' as const : undefined;
      valueText = formatDownlinkFieldEcho(v, fm.valueType, echoMode, fm.byteLength);
    }
    return { platformKey, valueText };
  });
}

export function formatFixedParamSummary(
  defaults: Record<string, unknown>,
  fieldMeta?: Record<string, DeviceRpcMethodFieldMeta>
): string[] {
  return fixedEntriesFromDefaults(defaults, fieldMeta).map(e => `${e.platformKey}=${e.valueText}`);
}

export function defaultsFromFixedEntries(
  entries: DeviceRpcFixedParamEntry[],
  fieldMeta?: Record<string, DeviceRpcMethodFieldMeta>
): Record<string, unknown> | null {
  const out: Record<string, unknown> = {};
  for (const e of entries) {
    const key = (e.platformKey ?? '').trim();
    if (!key) {
      continue;
    }
    const text = (e.valueText ?? '').trim();
    if (text === '') {
      continue;
    }
    const fm = fieldMeta?.[key];
    if (fm) {
      const parsed = parseDownlinkFieldInput(text, fm.valueType, fm.byteLength);
      if (parsed.ok !== true) {
        return null;
      }
      out[key] = parsed.value;
    } else {
      if (/^-?\d+$/.test(text)) {
        out[key] = Number(text);
      } else if (/^-?\d+\.\d+$/.test(text)) {
        out[key] = parseFloat(text);
      } else {
        out[key] = text;
      }
    }
  }
  return out;
}

export function parseNativeParamsJson(text: string): Record<string, unknown> | null {
  const t = (text ?? '').trim();
  if (!t) {
    return {};
  }
  try {
    const v = JSON.parse(t);
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      return v as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

export function mergeRpcPlatformValues(
  deviceDefaults: Record<string, unknown> | undefined,
  userValues: Record<string, unknown>
): Record<string, unknown> {
  if (!deviceDefaults || !Object.keys(deviceDefaults).length) {
    return { ...userValues };
  }
  return { ...deviceDefaults, ...userValues };
}

/** 将设备保存的固定参数应用到下行字段行：固定项预填且不在下发表单展示 */
export function applyDeviceRpcParamDefaults(
  rows: DeviceRpcInvokeFieldRow[],
  defaults?: Record<string, unknown>,
  fieldMeta?: Record<string, DeviceRpcMethodFieldMeta>
): { allRows: DeviceRpcInvokeFieldRow[]; editableRows: DeviceRpcInvokeFieldRow[]; fixedSummary: string[] } {
  if (!defaults || !Object.keys(defaults).length) {
    return { allRows: rows, editableRows: rows, fixedSummary: [] };
  }
  const allRows = rows.map(r => ({ ...r }));
  const editableRows: DeviceRpcInvokeFieldRow[] = [];
  const fixedSummary: string[] = [];
  for (const row of allRows) {
    if (Object.prototype.hasOwnProperty.call(defaults, row.platformKey)) {
      const v = defaults[row.platformKey];
      const fm = fieldMeta?.[row.platformKey];
      if (fm && v !== null && v !== undefined) {
        const echoMode = fm.valueType === TcpHexValueType.BYTES_AS_HEX ? 'bytesPlain' as const : undefined;
        row.inputText = formatDownlinkFieldEcho(v, fm.valueType, echoMode, fm.byteLength);
      } else {
        row.inputText = v === null || v === undefined ? '' : String(v);
      }
      const label = row.templateFieldKey ?? row.platformKey;
      fixedSummary.push(
        label !== row.platformKey
          ? `${label} (${row.platformKey})=${row.inputText}`
          : `${row.platformKey}=${row.inputText}`
      );
    } else {
      editableRows.push(row);
    }
  }
  return { allRows, editableRows, fixedSummary };
}

export function mergeNativeParams(
  templateJson: string | undefined,
  userJson: Record<string, unknown>
): Record<string, unknown> {
  let base: Record<string, unknown> = {};
  const t = (templateJson ?? '').trim();
  if (t) {
    try {
      const v = JSON.parse(t);
      if (v && typeof v === 'object' && !Array.isArray(v)) {
        base = { ...(v as Record<string, unknown>) };
      }
    } catch {
      /* ignore invalid template */
    }
  }
  return { ...base, ...userJson };
}

export function catalogMethodLabel(m: DeviceProfileRpcMethod): string {
  return m.displayName?.trim() || m.id;
}

export function isTcpTemplateBinding(m: DeviceProfileRpcMethod): boolean {
  return m.bindingType === DeviceProfileRpcBindingType.TCP_TEMPLATE;
}
