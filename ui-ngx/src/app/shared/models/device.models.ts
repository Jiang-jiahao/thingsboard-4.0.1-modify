///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { BaseData, ExportableEntity } from '@shared/models/base-data';
import { DeviceId } from './id/device-id';
import { TenantId } from '@shared/models/id/tenant-id';
import { CustomerId } from '@shared/models/id/customer-id';
import { DeviceCredentialsId } from '@shared/models/id/device-credentials-id';
import { EntitySearchQuery } from '@shared/models/relation.models';
import { DeviceProfileId } from '@shared/models/id/device-profile-id';
import { RuleChainId } from '@shared/models/id/rule-chain-id';
import { EntityInfoData, HasTenantId, HasVersion } from '@shared/models/entity.models';
import { FilterPredicateValue, KeyFilter } from '@shared/models/query/query.models';
import { TimeUnit } from '@shared/models/time/time.models';
import _moment from 'moment';
import { AbstractControl, ValidationErrors } from '@angular/forms';
import { OtaPackageId } from '@shared/models/id/ota-package-id';
import { DashboardId } from '@shared/models/id/dashboard-id';
import { DataType } from '@shared/models/constants';
import {
  getDefaultProfileClientLwM2mSettingsConfig,
  getDefaultProfileObserveAttrConfig,
  PowerMode
} from '@home/components/profile/device/lwm2m/lwm2m-profile-config.models';
import { PageLink } from '@shared/models/page/page-link';
import { isDefinedAndNotNull, isNotEmptyStr } from '@core/utils';
import { EdgeId } from '@shared/models/id/edge-id';

export enum DeviceProfileType {
  DEFAULT = 'DEFAULT',
  SNMP = 'SNMP'
}

export enum DeviceTransportType {
  DEFAULT = 'DEFAULT',
  MQTT = 'MQTT',
  COAP = 'COAP',
  LWM2M = 'LWM2M',
  SNMP = 'SNMP',
  TCP = 'TCP'
}

export enum BasicTransportType {
  HTTP = 'HTTP'
}
export type TransportType =  BasicTransportType | DeviceTransportType;
export type NetworkTransportType =  BasicTransportType | Exclude<DeviceTransportType, DeviceTransportType.DEFAULT>;

export enum TransportPayloadType {
  JSON = 'JSON',
  PROTOBUF = 'PROTOBUF'
}

export enum CoapTransportDeviceType {
  DEFAULT = 'DEFAULT',
  EFENTO = 'EFENTO'
}

export enum DeviceProvisionType {
  DISABLED = 'DISABLED',
  ALLOW_CREATE_NEW_DEVICES = 'ALLOW_CREATE_NEW_DEVICES',
  CHECK_PRE_PROVISIONED_DEVICES = 'CHECK_PRE_PROVISIONED_DEVICES',
  X509_CERTIFICATE_CHAIN = 'X509_CERTIFICATE_CHAIN'
}

export interface DeviceConfigurationFormInfo {
  hasProfileConfiguration: boolean;
  hasDeviceConfiguration: boolean;
}

export const deviceProfileTypeTranslationMap = new Map<DeviceProfileType, string>(
  [
    [DeviceProfileType.DEFAULT, 'device-profile.type-default']
  ]
);

export const deviceProfileTypeConfigurationInfoMap = new Map<DeviceProfileType, DeviceConfigurationFormInfo>(
  [
    [
      DeviceProfileType.DEFAULT,
      {
        hasProfileConfiguration: false,
        hasDeviceConfiguration: false,
      }
    ],
    [
      DeviceProfileType.SNMP,
      {
        hasProfileConfiguration: true,
        hasDeviceConfiguration: true,
      }
    ]
  ]
);

export const deviceTransportTypeTranslationMap = new Map<TransportType, string>(
  [
    [DeviceTransportType.DEFAULT, 'device-profile.transport-type-default'],
    [DeviceTransportType.MQTT, 'device-profile.transport-type-mqtt'],
    [DeviceTransportType.COAP, 'device-profile.transport-type-coap'],
    [DeviceTransportType.LWM2M, 'device-profile.transport-type-lwm2m'],
    [DeviceTransportType.SNMP, 'device-profile.transport-type-snmp'],
    [DeviceTransportType.TCP, 'device-profile.transport-type-tcp'],
    [BasicTransportType.HTTP, 'device-profile.transport-type-http']
  ]
);


export const deviceProvisionTypeTranslationMap = new Map<DeviceProvisionType, string>(
  [
    [DeviceProvisionType.DISABLED, 'device-profile.provision-strategy-disabled'],
    [DeviceProvisionType.ALLOW_CREATE_NEW_DEVICES, 'device-profile.provision-strategy-created-new'],
    [DeviceProvisionType.CHECK_PRE_PROVISIONED_DEVICES, 'device-profile.provision-strategy-check-pre-provisioned'],
    [DeviceProvisionType.X509_CERTIFICATE_CHAIN, 'device-profile.provision-strategy-x509.certificate-chain']
  ]
);

export const deviceTransportTypeHintMap = new Map<TransportType, string>(
  [
    [DeviceTransportType.DEFAULT, 'device-profile.transport-type-default-hint'],
    [DeviceTransportType.MQTT, 'device-profile.transport-type-mqtt-hint'],
    [DeviceTransportType.COAP, 'device-profile.transport-type-coap-hint'],
    [DeviceTransportType.LWM2M, 'device-profile.transport-type-lwm2m-hint'],
    [DeviceTransportType.SNMP, 'device-profile.transport-type-snmp-hint'],
    [DeviceTransportType.TCP, 'device-profile.transport-type-tcp-hint'],
    [BasicTransportType.HTTP, '']
  ]
);

export const transportPayloadTypeTranslationMap = new Map<TransportPayloadType, string>(
  [
    [TransportPayloadType.JSON, 'device-profile.transport-device-payload-type-json'],
    [TransportPayloadType.PROTOBUF, 'device-profile.transport-device-payload-type-proto']
  ]
);

export const defaultTelemetrySchema =
  'syntax ="proto3";\n' +
  'package telemetry;\n' +
  '\n' +
  'message SensorDataReading {\n' +
  '\n' +
  '  optional double temperature = 1;\n' +
  '  optional double humidity = 2;\n' +
  '  InnerObject innerObject = 3;\n' +
  '\n' +
  '  message InnerObject {\n' +
  '    optional string key1 = 1;\n' +
  '    optional bool key2 = 2;\n' +
  '    optional double key3 = 3;\n' +
  '    optional int32 key4 = 4;\n' +
  '    optional string key5 = 5;\n' +
  '  }\n' +
  '}\n';

export const defaultAttributesSchema =
  'syntax ="proto3";\n' +
  'package attributes;\n' +
  '\n' +
  'message SensorConfiguration {\n' +
  '  optional string firmwareVersion = 1;\n' +
  '  optional string serialNumber = 2;\n' +
  '}';

export const defaultRpcRequestSchema =
  'syntax ="proto3";\n' +
  'package rpc;\n' +
  '\n' +
  'message RpcRequestMsg {\n' +
  '  optional string method = 1;\n' +
  '  optional int32 requestId = 2;\n' +
  '  optional string params = 3;\n' +
  '}';

export const defaultRpcResponseSchema =
  'syntax ="proto3";\n' +
  'package rpc;\n' +
  '\n' +
  'message RpcResponseMsg {\n' +
  '  optional string payload = 1;\n' +
  '}';

export const coapDeviceTypeTranslationMap = new Map<CoapTransportDeviceType, string>(
  [
    [CoapTransportDeviceType.DEFAULT, 'device-profile.coap-device-type-default'],
    [CoapTransportDeviceType.EFENTO, 'device-profile.coap-device-type-efento']
  ]
);


export const deviceTransportTypeConfigurationInfoMap = new Map<DeviceTransportType, DeviceConfigurationFormInfo>(
  [
    [
      DeviceTransportType.DEFAULT,
      {
        hasProfileConfiguration: false,
        hasDeviceConfiguration: false,
      }
    ],
    [
      DeviceTransportType.MQTT,
      {
        hasProfileConfiguration: true,
        hasDeviceConfiguration: false,
      }
    ],
    [
      DeviceTransportType.LWM2M,
      {
        hasProfileConfiguration: true,
        hasDeviceConfiguration: true,
      }
    ],
    [
      DeviceTransportType.COAP,
      {
        hasProfileConfiguration: true,
        hasDeviceConfiguration: true,
      }
    ],
    [
      DeviceTransportType.SNMP,
      {
        hasProfileConfiguration: true,
        hasDeviceConfiguration: true
      }
    ],
    [
      DeviceTransportType.TCP,
      {
        hasProfileConfiguration: true,
        hasDeviceConfiguration: true
      }
    ]
  ]
);

export interface DefaultDeviceProfileConfiguration {
  [key: string]: any;
}

export type DeviceProfileConfigurations = DefaultDeviceProfileConfiguration;

export interface DeviceProfileConfiguration extends DeviceProfileConfigurations {
  type: DeviceProfileType;
}

export interface DefaultDeviceProfileTransportConfiguration {
  [key: string]: any;
}

export interface MqttDeviceProfileTransportConfiguration {
  deviceTelemetryTopic?: string;
  deviceAttributesTopic?: string;
  deviceAttributesSubscribeTopic?: string;
  sparkplug?: boolean;
  sendAckOnValidationException?: boolean;
  transportPayloadTypeConfiguration?: {
    transportPayloadType?: TransportPayloadType;
    enableCompatibilityWithJsonPayloadFormat?: boolean;
    useJsonPayloadFormatForDefaultDownlinkTopics?: boolean;
  };
  [key: string]: any;
}

export interface CoapClientSetting {
  powerMode?: PowerMode | null;
  edrxCycle?: number;
  pagingTransmissionWindow?: number;
  psmActivityTimer?: number;
}

export interface CoapDeviceProfileTransportConfiguration {
  coapDeviceTypeConfiguration?: {
    coapDeviceType?: CoapTransportDeviceType;
    transportPayloadTypeConfiguration?: {
      transportPayloadType?: TransportPayloadType;
      [key: string]: any;
    };
  };
  clientSettings?: CoapClientSetting;
}

export interface Lwm2mDeviceProfileTransportConfiguration {
  [key: string]: any;
}

export interface SnmpDeviceProfileTransportConfiguration {
  timeoutMs?: number;
  retries?: number;
  communicationConfigs?: SnmpCommunicationConfig[];
}

export enum SnmpSpecType {
  TELEMETRY_QUERYING = 'TELEMETRY_QUERYING',
  CLIENT_ATTRIBUTES_QUERYING = 'CLIENT_ATTRIBUTES_QUERYING',
  SHARED_ATTRIBUTES_SETTING = 'SHARED_ATTRIBUTES_SETTING',
  TO_DEVICE_RPC_REQUEST = 'TO_DEVICE_RPC_REQUEST',
  TO_SERVER_RPC_REQUEST = 'TO_SERVER_RPC_REQUEST'
}


export enum TcpTransportConnectMode {
  SERVER = 'SERVER',
  CLIENT = 'CLIENT'
}
export enum TcpTransportFramingMode {
  LINE = 'LINE',
  LENGTH_PREFIX_4 = 'LENGTH_PREFIX_4',
  LENGTH_PREFIX_2 = 'LENGTH_PREFIX_2',
  FIXED_LENGTH = 'FIXED_LENGTH'
}
export enum TcpWireAuthenticationMode {
  TOKEN = 'TOKEN',
  NONE = 'NONE'
}
export enum TcpJsonWithoutMethodMode {
  TELEMETRY_FLAT = 'TELEMETRY_FLAT',
  OPAQUE_FOR_RULE_ENGINE = 'OPAQUE_FOR_RULE_ENGINE'
}
export enum TransportTcpDataType {
  JSON = 'JSON',
  HEX = 'HEX',
  /**
   * 与 HEX 链路上一致；配置为「协议模板」（帧模板 + 上行/下行命令），后端展开为 HEX 解析。
   */
  PROTOCOL_TEMPLATE = 'PROTOCOL_TEMPLATE',
  ASCII = 'ASCII'
}

/** 将历史字符串 MONITORING_PROTOCOL 规范为 PROTOCOL_TEMPLATE */
export function normalizeTransportTcpDataType(
  v: string | TransportTcpDataType | undefined | null
): TransportTcpDataType | undefined {
  if (v == null || v === '') {
    return undefined;
  }
  if (v === 'MONITORING_PROTOCOL') {
    return TransportTcpDataType.PROTOCOL_TEMPLATE;
  }
  return v as TransportTcpDataType;
}

/** 协议模板命令方向（与后端 ProtocolTemplateCommandDirection 一致） */
export enum ProtocolTemplateCommandDirection {
  UPLINK = 'UPLINK',
  DOWNLINK = 'DOWNLINK',
  BOTH = 'BOTH'
}

/** 与 TCP HEX 解析器一致的可选整帧校验 */
export interface TcpHexChecksumDefinition {
  type: string;
  fromByte: number;
  toExclusive: number;
  checksumByteIndex: number;
}

/** 帧模板：命令偏移、默认字段等 */
export interface ProtocolTemplateDefinition {
  id: string;
  name?: string;
  commandByteOffset?: number;
  commandMatchWidth?: number;
  /** 可选：整帧校验（先于字段解析） */
  checksum?: TcpHexChecksumDefinition;
  hexProtocolFields?: TcpHexFieldDefinition[];
  hexLtvRepeating?: TcpHexLtvRepeatingConfig;
}

/** 在模板下按命令字配置方向与可选字段覆盖 */
export interface ProtocolTemplateCommandDefinition {
  templateId: string;
  name?: string;
  commandValue: number;
  matchValueType?: TcpHexValueType;
  /** 可选：第二匹配偏移（整帧 0 起），与命令字节配合区分 0xA2 类应答（如第 7 字节回显原命令） */
  secondaryMatchByteOffset?: number;
  secondaryMatchValueType?: TcpHexValueType;
  secondaryMatchValue?: number;
  direction: ProtocolTemplateCommandDirection;
  /**
   * 上行/双向：与帧模板合并后用于从设备上报帧中按偏移解析（遥测键）。
   * 下行：描述组帧时参区应写入的参数布局（键名/类型/偏移作编码契约），需由 RPC、规则链等按此构造字节；TCP 侧不解析下行。
   * 合并规则：与模板字段按字节区间合并，重叠以本命令为准。
   */
  fields?: TcpHexFieldDefinition[];
  ltvRepeating?: TcpHexLtvRepeatingConfig;
  /** 下行/双向：勾选后在组帧时自动向「长度字段」写入参区字节数 */
  downlinkPayloadLengthAuto?: boolean;
  /** 作为参长写入的字段 key（须在模板+命令合并结果中存在，且为整型） */
  downlinkPayloadLengthFieldKey?: string;
}

/** 租户级协议模板包（持久化于 protocol_template_bundle 表；旧版可曾存于 Tenant.additionalInfo，由后端迁移） */
export interface ProtocolTemplateBundle {
  id: string;
  /** 创建时间（毫秒），列表展示 */
  createdTime?: number;
  name?: string;
  protocolTemplates: ProtocolTemplateDefinition[];
  protocolCommands: ProtocolTemplateCommandDefinition[];
  /** 历史 JSON 字段名（仅读取兼容） */
  monitoringTemplates?: ProtocolTemplateDefinition[];
  monitoringCommands?: ProtocolTemplateCommandDefinition[];
}

export const PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY = 'protocolTemplateBundles';

/** 旧版 tenant.additionalInfo 键名（迁移兼容） */
export const LEGACY_PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY = 'monitoringProtocolBundles';

/** TCP HEX 帧内字段类型（与后端 TcpHexValueType 一致） */
export enum TcpHexValueType {
  UINT8 = 'UINT8',
  INT8 = 'INT8',
  UINT16_BE = 'UINT16_BE',
  UINT16_LE = 'UINT16_LE',
  INT16_BE = 'INT16_BE',
  INT16_LE = 'INT16_LE',
  UINT32_BE = 'UINT32_BE',
  UINT32_LE = 'UINT32_LE',
  INT32_BE = 'INT32_BE',
  INT32_LE = 'INT32_LE',
  /** 与后端一致；仅解析链路等内部使用。LTV Tag 映射请用 UINT_AUTO_LE（8 字节按 64 位解码）。 */
  UINT64_LE = 'UINT64_LE',
  /** @internal 勿用于 LTV Tag 映射 */
  UINT64_BE = 'UINT64_BE',
  /** @internal 勿用于 LTV Tag 映射 */
  INT64_LE = 'INT64_LE',
  /** @internal 勿用于 LTV Tag 映射 */
  INT64_BE = 'INT64_BE',
  /** 仅 LTV Tag 映射：无符号小端整型，宽度由本段 Value 字节数（1/2/4/8）决定 */
  UINT_AUTO_LE = 'UINT_AUTO_LE',
  UINT_AUTO_BE = 'UINT_AUTO_BE',
  INT_AUTO_LE = 'INT_AUTO_LE',
  INT_AUTO_BE = 'INT_AUTO_BE',
  FLOAT_BE = 'FLOAT_BE',
  FLOAT_LE = 'FLOAT_LE',
  DOUBLE_BE = 'DOUBLE_BE',
  DOUBLE_LE = 'DOUBLE_LE',
  /** 原始字节按 UTF-8 解码为字符串（与 BYTES_AS_HEX 共享变长/固定长度配置） */
  BYTES_AS_UTF8 = 'BYTES_AS_UTF8',
  BYTES_AS_HEX = 'BYTES_AS_HEX'
}

/** BYTES_AS_HEX / BYTES_AS_UTF8：变长字节切片（共享 byteLength、从帧读长度等） */
export function isTcpHexVariableByteSlice(vt: TcpHexValueType | null | undefined): boolean {
  return vt === TcpHexValueType.BYTES_AS_HEX || vt === TcpHexValueType.BYTES_AS_UTF8;
}

export interface TcpHexFieldDefinition {
  key: string;
  byteOffset: number;
  valueType: TcpHexValueType;
  /** 仅命令覆盖行：勾选表示该参数参与下行「自动参长」的字节统计 */
  includeInDownlinkPayloadLength?: boolean;
  /**
   * 为 true 时：下行组帧自动写入整包总长（默认等于最终帧字节数，含本字段；灵信类「总长含本字段」）。
   * JSON 可省略该键。合并字段中至多一个字段勾选。
   */
  autoDownlinkTotalFrameLength?: boolean;
  /**
   * 仅当 autoDownlinkTotalFrameLength 为 true：为 true 时总长不含本字段线宽（写入 buf.length - 本字段字节数）。
   */
  downlinkTotalFrameLengthExcludesLengthFieldBytes?: boolean;
  /** @deprecated 旧版；请用命令级 downlinkPayloadLengthAuto + 勾选参与参长 */
  downlinkPayloadLengthMemberKeys?: string[];
  /** 参长字段上：参区从整帧该字节偏移起算；未设则从本字段结束字节之后起算 */
  /** @deprecated 仅旧版字段级自动参长；命令级参长为勾选字段字节宽度之和，不再使用 */
  downlinkPayloadStartByteOffset?: number;
  /** @deprecated 旧版 */
  autoDownlinkPayloadLength?: boolean;
  /** @deprecated 旧版 */
  downlinkPayloadEndExclusiveByteOffset?: number;
  /** BYTES_AS_HEX / BYTES_AS_UTF8：固定字节数（与 byteLengthFromByteOffset 二选一） */
  byteLength?: number;
  /** BYTES_AS_HEX / BYTES_AS_UTF8：从整帧该字节偏移处按 byteLengthFromValueType 读出长度，再截取相应字节（非「引用别名字段」） */
  byteLengthFromByteOffset?: number;
  /** 读取动态长度时的整型类型，默认 UINT8 */
  byteLengthFromValueType?: TcpHexValueType;
  /**
   * 与 byteLengthFromByteOffset 联用：截取长度 = 读出的整型值减去该常量（≥0）。
   * 例：报文长度 = 12 + JSON 字节数时，正文起点固定偏移 16，此处填 12。
   */
  byteLengthFromIntegralSubtract?: number;
  /** @deprecated UI 已移除；后端仍兼容旧配置 */
  scale?: number;
  /** @deprecated UI 已移除；后端仍兼容旧配置 */
  bitMask?: number;
  /**
   * 整型：线型整数码（与帧内字节一致，如 UINT8 的 0xA5 为 165）。上行校验、下行自动写入，JSON 可省略。
   */
  fixedWireIntegralValue?: number;
  /**
   * BYTES_AS_HEX / BYTES_AS_UTF8（固定 byteLength）：固定线型字节以 hex 串表示。上行按字节比对；下行写入。
   */
  fixedBytesHex?: string;
}

export enum TcpHexLtvChunkOrder {
  LTV = 'LTV',
  TLV = 'TLV'
}

export enum TcpHexUnknownTagMode {
  SKIP = 'SKIP',
  EMIT_HEX = 'EMIT_HEX'
}

export interface TcpHexLtvTagMapping {
  tagValue: number;
  /** 该行 Tag 是否以 0x 字面保存；决定回显与未映射 _t 后缀是否参与十六进制模式 */
  tagValueLiterallyHex?: boolean;
  telemetryKey: string;
  valueType: TcpHexValueType;
  /** @deprecated 由 LTV Length 切出的 Value 长度决定；保存时可省略 */
  byteLength?: number;
  /** @deprecated UI 已移除 */
  scale?: number;
  /** @deprecated UI 已移除 */
  bitMask?: number;
}

export interface TcpHexLtvRepeatingConfig {
  startByteOffset: number;
  lengthFieldType: TcpHexValueType;
  tagFieldType: TcpHexValueType;
  chunkOrder?: TcpHexLtvChunkOrder;
  maxItems?: number;
  keyPrefix?: string;
  unknownTagMode?: TcpHexUnknownTagMode;
  tagMappings?: TcpHexLtvTagMapping[];
  /**
   * 为 true 时，Length 数值（仍不含 Length 字段自身）= Tag + Value 总字节数。默认 false 时 Length 仅为 Value 长度。
   */
  lengthIncludesTag?: boolean;
  /**
   * 例外：为 true 时，Length 数值把 Length 字段自身也算入（整段 L+T+V）。与常规「Length 不含自身」不同；解析优先于 lengthIncludesTag。
   */
  lengthIncludesLengthField?: boolean;
  /**
   * 未映射 Tag 且 unknownTagMode=EMIT_HEX 时，遥测键 _t 后缀是否用 0x：节级 true，或任一行 tagValueLiterallyHex 为 true 时生效。
   * 保存时由 UI 根据各映射行是否以 0x 输入自动写入（与任一行 tagValueLiterallyHex 一致）。
   */
  unknownTagTelemetryKeyHexLiteral?: boolean;
}

/** 按命令字匹配的一套解析（与后端 TcpHexCommandProfile 一致） */
export interface TcpHexCommandProfile {
  /** 可选，匹配成功后会写入遥测键 hexCmdProfile */
  name?: string;
  matchByteOffset: number;
  /** 仅整型，用于从帧中读出命令字再与 matchValue 比较 */
  matchValueType: TcpHexValueType;
  matchValue: number;
  secondaryMatchByteOffset?: number;
  secondaryMatchValueType?: TcpHexValueType;
  secondaryMatchValue?: number;
  /** 与 ltvRepeating 至少配置其一（由后端校验） */
  fields?: TcpHexFieldDefinition[];
  /** 可选：参数字段内为 LTV/TLV 列表时使用 */
  ltvRepeating?: TcpHexLtvRepeatingConfig;
}

/** 命令匹配下拉：不允许 FLOAT/DOUBLE/BYTES_AS_HEX */
export const TCP_HEX_MATCH_VALUE_TYPES: TcpHexValueType[] = [
  TcpHexValueType.UINT8,
  TcpHexValueType.INT8,
  TcpHexValueType.UINT16_BE,
  TcpHexValueType.UINT16_LE,
  TcpHexValueType.INT16_BE,
  TcpHexValueType.INT16_LE,
  TcpHexValueType.UINT32_BE,
  TcpHexValueType.UINT32_LE,
  TcpHexValueType.INT32_BE,
  TcpHexValueType.INT32_LE
];

/**
 * 帧内按字节偏移解析的字段可选类型（与后端 TcpHexFieldDefinition.validate 一致）。
 * 不含 UINT_AUTO_* / INT_AUTO_*（仅用于 LTV Tag→遥测映射）。
 */
export const TCP_HEX_FRAME_FIELD_VALUE_TYPES: TcpHexValueType[] = [
  TcpHexValueType.UINT8,
  TcpHexValueType.INT8,
  TcpHexValueType.UINT16_BE,
  TcpHexValueType.UINT16_LE,
  TcpHexValueType.INT16_BE,
  TcpHexValueType.INT16_LE,
  TcpHexValueType.UINT32_BE,
  TcpHexValueType.UINT32_LE,
  TcpHexValueType.INT32_BE,
  TcpHexValueType.INT32_LE,
  TcpHexValueType.FLOAT_BE,
  TcpHexValueType.FLOAT_LE,
  TcpHexValueType.DOUBLE_BE,
  TcpHexValueType.DOUBLE_LE,
  TcpHexValueType.BYTES_AS_UTF8,
  TcpHexValueType.BYTES_AS_HEX
];

/**
 * LTV tag mapping dropdown options: each row uses a static i18n labelKey (so mat-option can translate).
 * Types: AUTO integers, floats, HEX only; no explicit UINT64 or INT64 enums (8-byte integers use AUTO; server decodes as 64-bit).
 */
export interface TcpHexLtvTagValueOption {
  value: TcpHexValueType;
  labelKey: string;
}

export const TCP_HEX_LTV_TAG_VALUE_OPTIONS: TcpHexLtvTagValueOption[] = [
  { value: TcpHexValueType.UINT_AUTO_LE, labelKey: 'device-profile.tcp.hex-ltv-vt-UINT_AUTO_LE' },
  { value: TcpHexValueType.UINT_AUTO_BE, labelKey: 'device-profile.tcp.hex-ltv-vt-UINT_AUTO_BE' },
  { value: TcpHexValueType.INT_AUTO_LE, labelKey: 'device-profile.tcp.hex-ltv-vt-INT_AUTO_LE' },
  { value: TcpHexValueType.INT_AUTO_BE, labelKey: 'device-profile.tcp.hex-ltv-vt-INT_AUTO_BE' },
  { value: TcpHexValueType.FLOAT_BE, labelKey: 'device-profile.tcp.hex-ltv-vt-FLOAT_BE' },
  { value: TcpHexValueType.FLOAT_LE, labelKey: 'device-profile.tcp.hex-ltv-vt-FLOAT_LE' },
  { value: TcpHexValueType.DOUBLE_BE, labelKey: 'device-profile.tcp.hex-ltv-vt-DOUBLE_BE' },
  { value: TcpHexValueType.DOUBLE_LE, labelKey: 'device-profile.tcp.hex-ltv-vt-DOUBLE_LE' },
  { value: TcpHexValueType.BYTES_AS_UTF8, labelKey: 'device-profile.tcp.hex-ltv-vt-BYTES_AS_UTF8' },
  { value: TcpHexValueType.BYTES_AS_HEX, labelKey: 'device-profile.tcp.hex-ltv-vt-BYTES_AS_HEX' }
];

/** Values from {@link TCP_HEX_LTV_TAG_VALUE_OPTIONS}, same order. */
export const TCP_HEX_LTV_TAG_VALUE_TYPES: TcpHexValueType[] = TCP_HEX_LTV_TAG_VALUE_OPTIONS.map((o) => o.value);

/** 将旧版映射中的固定位宽整型折成 AUTO，便于界面只展示「宽度随本段」选项 */
export function migrateLegacyLtvTagValueType(vt: TcpHexValueType | null | undefined): TcpHexValueType {
  if (vt == null) {
    return TcpHexValueType.UINT_AUTO_LE;
  }
  switch (vt) {
    case TcpHexValueType.UINT8:
    case TcpHexValueType.UINT16_LE:
    case TcpHexValueType.UINT32_LE:
      return TcpHexValueType.UINT_AUTO_LE;
    case TcpHexValueType.UINT16_BE:
    case TcpHexValueType.UINT32_BE:
      return TcpHexValueType.UINT_AUTO_BE;
    case TcpHexValueType.INT8:
    case TcpHexValueType.INT16_LE:
    case TcpHexValueType.INT32_LE:
      return TcpHexValueType.INT_AUTO_LE;
    case TcpHexValueType.INT16_BE:
    case TcpHexValueType.INT32_BE:
      return TcpHexValueType.INT_AUTO_BE;
    case TcpHexValueType.UINT64_LE:
      return TcpHexValueType.UINT_AUTO_LE;
    case TcpHexValueType.UINT64_BE:
      return TcpHexValueType.UINT_AUTO_BE;
    case TcpHexValueType.INT64_LE:
      return TcpHexValueType.INT_AUTO_LE;
    case TcpHexValueType.INT64_BE:
      return TcpHexValueType.INT_AUTO_BE;
    case TcpHexValueType.BYTES_AS_UTF8:
      return TcpHexValueType.BYTES_AS_UTF8;
    default:
      return vt;
  }
}

export interface TransportTcpDataTypeConfiguration {
  transportTcpDataType?: TransportTcpDataType;
  /** 仅当 transportTcpDataType 为 HEX：按顺序匹配命令规则 */
  hexCommandProfiles?: TcpHexCommandProfile[];
  /** 未匹配任何命令时的回退字段解析 */
  hexProtocolFields?: TcpHexFieldDefinition[];
  /** 未匹配命令时，在固定字段之后解析的 LTV/TLV 重复段 */
  hexLtvRepeating?: TcpHexLtvRepeatingConfig;
  /** 手动 HEX：可选整帧校验 */
  checksum?: TcpHexChecksumDefinition;
  /** 协议模板负载：先配模板再配命令 */
  protocolTemplates?: ProtocolTemplateDefinition[];
  protocolCommands?: ProtocolTemplateCommandDefinition[];
  /** 可选：所选协议模板包 ID（租户「协议模板」库），便于 UI 回显 */
  protocolTemplateBundleId?: string;
  /** 历史 JSON 字段名（仅读取兼容） */
  monitoringTemplates?: ProtocolTemplateDefinition[];
  monitoringCommands?: ProtocolTemplateCommandDefinition[];
  monitoringProtocolBundleId?: string;
}
export interface TcpDeviceProfileTransportConfiguration {
  type?: DeviceTransportType;
  tcpTransportConnectMode?: TcpTransportConnectMode;
  tcpTransportFramingMode?: TcpTransportFramingMode;
  tcpFixedFrameLength?: number;
  tcpWireAuthenticationMode?: TcpWireAuthenticationMode;
  /** CLIENT：断线/建连失败后重连间隔（秒）；空=后端默认 30；0=不重连 */
  tcpOutboundReconnectIntervalSec?: number;
  /** CLIENT：最大重连次数；空或 0=不限制 */
  tcpOutboundReconnectMaxAttempts?: number;
  /** CLIENT/SERVER：超过该秒数无上行数据则断开；空或 0=不启用 */
  tcpReadIdleTimeoutSec?: number;
  tcpJsonWithoutMethodMode?: TcpJsonWithoutMethodMode;
  tcpOpaqueRuleEngineKey?: string;
  transportTcpDataTypeConfiguration?: TransportTcpDataTypeConfiguration;
}

export const SnmpSpecTypeTranslationMap = new Map<SnmpSpecType, string>([
  [SnmpSpecType.TELEMETRY_QUERYING, ' Telemetry (SNMP GET)'],
  [SnmpSpecType.CLIENT_ATTRIBUTES_QUERYING, 'Client attributes (SNMP GET)'],
  [SnmpSpecType.SHARED_ATTRIBUTES_SETTING, 'Shared attributes (SNMP SET)'],
  [SnmpSpecType.TO_DEVICE_RPC_REQUEST, 'To-device RPC request (SNMP GET/SET)'],
  [SnmpSpecType.TO_SERVER_RPC_REQUEST, 'From-device RPC request (SNMP TRAP)']
]);

export interface SnmpCommunicationConfig {
  spec: SnmpSpecType;
  mappings: SnmpMapping[];
  queryingFrequencyMs?: number;
}

export interface SnmpMapping {
  oid: string;
  key: string;
  dataType: DataType;
}

export type DeviceProfileTransportConfigurations = DefaultDeviceProfileTransportConfiguration &
                                                   MqttDeviceProfileTransportConfiguration &
                                                   CoapDeviceProfileTransportConfiguration &
                                                   Lwm2mDeviceProfileTransportConfiguration &
                                                   SnmpDeviceProfileTransportConfiguration &
                                                   TcpDeviceProfileTransportConfiguration;

export interface DeviceProfileTransportConfiguration extends DeviceProfileTransportConfigurations {
  type: DeviceTransportType;
}

export interface DeviceProvisionConfiguration {
  type: DeviceProvisionType;
  provisionDeviceSecret?: string;
  provisionDeviceKey?: string;
  certificateValue?: string;
  certificateRegExPattern?: string;
  allowCreateNewDevicesByX509Certificate?: boolean;
}

export const createDeviceProfileConfiguration = (type: DeviceProfileType): DeviceProfileConfiguration => {
  let configuration: DeviceProfileConfiguration = null;
  if (type) {
    switch (type) {
      case DeviceProfileType.DEFAULT:
        const defaultConfiguration: DefaultDeviceProfileConfiguration = {};
        configuration = {...defaultConfiguration, type: DeviceProfileType.DEFAULT};
        break;
    }
  }
  return configuration;
};

export const createDeviceConfiguration = (type: DeviceProfileType): DeviceConfiguration => {
  let configuration: DeviceConfiguration = null;
  if (type) {
    switch (type) {
      case DeviceProfileType.DEFAULT:
        const defaultConfiguration: DefaultDeviceConfiguration = {};
        configuration = {...defaultConfiguration, type: DeviceProfileType.DEFAULT};
        break;
    }
  }
  return configuration;
};

export const createDeviceProfileTransportConfiguration = (type: DeviceTransportType): DeviceProfileTransportConfiguration => {
  let transportConfiguration: DeviceProfileTransportConfiguration = null;
  if (type) {
    switch (type) {
      case DeviceTransportType.DEFAULT:
        const defaultTransportConfiguration: DefaultDeviceProfileTransportConfiguration = {};
        transportConfiguration = {...defaultTransportConfiguration, type: DeviceTransportType.DEFAULT};
        break;
      case DeviceTransportType.MQTT:
        const mqttTransportConfiguration: MqttDeviceProfileTransportConfiguration = {
          deviceTelemetryTopic: 'v1/devices/me/telemetry',
          deviceAttributesTopic: 'v1/devices/me/attributes',
          deviceAttributesSubscribeTopic: 'v1/devices/me/attributes',
          sparkplug: false,
          sparkplugAttributesMetricNames: ['Node Control/*', 'Device Control/*', 'Properties/*'],
          sendAckOnValidationException: false,
          transportPayloadTypeConfiguration: {
            transportPayloadType: TransportPayloadType.JSON,
            enableCompatibilityWithJsonPayloadFormat: false,
            useJsonPayloadFormatForDefaultDownlinkTopics: false,
          }
        };
        transportConfiguration = {...mqttTransportConfiguration, type: DeviceTransportType.MQTT};
        break;
      case DeviceTransportType.COAP:
        const coapTransportConfiguration: CoapDeviceProfileTransportConfiguration = {
          coapDeviceTypeConfiguration: {
            coapDeviceType: CoapTransportDeviceType.DEFAULT,
            transportPayloadTypeConfiguration: {transportPayloadType: TransportPayloadType.JSON}
          },
          clientSettings: {
            powerMode: PowerMode.DRX
          }
        };
        transportConfiguration = {...coapTransportConfiguration, type: DeviceTransportType.COAP};
        break;
      case DeviceTransportType.LWM2M:
        const lwm2mTransportConfiguration: Lwm2mDeviceProfileTransportConfiguration = {
          observeAttr: getDefaultProfileObserveAttrConfig(),
          bootstrap: [],
          clientLwM2mSettings: getDefaultProfileClientLwM2mSettingsConfig()
        };
        transportConfiguration = {...lwm2mTransportConfiguration, type: DeviceTransportType.LWM2M};
        break;
      case DeviceTransportType.SNMP:
        const snmpTransportConfiguration: SnmpDeviceProfileTransportConfiguration = {
          timeoutMs: 500,
          retries: 0,
          communicationConfigs: null
        };
        transportConfiguration = {...snmpTransportConfiguration, type: DeviceTransportType.SNMP};
        break;
      case DeviceTransportType.TCP:
        const tcpTransportConfiguration: TcpDeviceProfileTransportConfiguration = {
          tcpTransportConnectMode: TcpTransportConnectMode.SERVER,
          tcpTransportFramingMode: TcpTransportFramingMode.LINE,
          tcpFixedFrameLength: null,
          tcpWireAuthenticationMode: TcpWireAuthenticationMode.TOKEN,
          tcpJsonWithoutMethodMode: TcpJsonWithoutMethodMode.TELEMETRY_FLAT,
          tcpOpaqueRuleEngineKey: 'tcpOpaquePayload',
          transportTcpDataTypeConfiguration: {
            transportTcpDataType: TransportTcpDataType.JSON
          }
        };
        transportConfiguration = {...tcpTransportConfiguration, type: DeviceTransportType.TCP};
        break;
    }
  }
  return transportConfiguration;
};

export const createDeviceTransportConfiguration = (type: DeviceTransportType): DeviceTransportConfiguration => {
  let transportConfiguration: DeviceTransportConfiguration = null;
  if (type) {
    switch (type) {
      case DeviceTransportType.DEFAULT:
        const defaultTransportConfiguration: DefaultDeviceTransportConfiguration = {};
        transportConfiguration = {...defaultTransportConfiguration, type: DeviceTransportType.DEFAULT};
        break;
      case DeviceTransportType.MQTT:
        const mqttTransportConfiguration: MqttDeviceTransportConfiguration = {};
        transportConfiguration = {...mqttTransportConfiguration, type: DeviceTransportType.MQTT};
        break;
      case DeviceTransportType.COAP:
        const coapTransportConfiguration: CoapDeviceTransportConfiguration = {
          powerMode: null
        };
        transportConfiguration = {...coapTransportConfiguration, type: DeviceTransportType.COAP};
        break;
      case DeviceTransportType.LWM2M:
        const lwm2mTransportConfiguration: Lwm2mDeviceTransportConfiguration = {
          powerMode: null
        };
        transportConfiguration = {...lwm2mTransportConfiguration, type: DeviceTransportType.LWM2M};
        break;
      case DeviceTransportType.SNMP:
        const snmpTransportConfiguration: SnmpDeviceTransportConfiguration = {
          host: 'localhost',
          port: 161,
          protocolVersion: SnmpDeviceProtocolVersion.V2C,
          community: 'public'
        };
        transportConfiguration = {...snmpTransportConfiguration, type: DeviceTransportType.SNMP};
        break;
      case DeviceTransportType.TCP:
        const tcpDeviceTransportConfiguration: TcpDeviceTransportConfiguration = {
          host: '127.0.0.1',
          port: 5025
        };
        transportConfiguration = {...tcpDeviceTransportConfiguration, type: DeviceTransportType.TCP};
        break;
    }
  }
  return transportConfiguration;
};

export enum AlarmConditionType {
  SIMPLE = 'SIMPLE',
  DURATION = 'DURATION',
  REPEATING = 'REPEATING'
}

export const AlarmConditionTypeTranslationMap = new Map<AlarmConditionType, string>(
  [
    [AlarmConditionType.SIMPLE, 'device-profile.condition-type-simple'],
    [AlarmConditionType.DURATION, 'device-profile.condition-type-duration'],
    [AlarmConditionType.REPEATING, 'device-profile.condition-type-repeating']
  ]
);

export interface AlarmConditionSpec{
  type?: AlarmConditionType;
  unit?: TimeUnit;
  predicate: FilterPredicateValue<number>;
}

export interface AlarmCondition {
  condition: Array<KeyFilter>;
  spec?: AlarmConditionSpec;
}

export enum AlarmScheduleType {
  ANY_TIME = 'ANY_TIME',
  SPECIFIC_TIME = 'SPECIFIC_TIME',
  CUSTOM = 'CUSTOM'
}

export const AlarmScheduleTypeTranslationMap = new Map<AlarmScheduleType, string>(
  [
    [AlarmScheduleType.ANY_TIME, 'device-profile.schedule-any-time'],
    [AlarmScheduleType.SPECIFIC_TIME, 'device-profile.schedule-specific-time'],
    [AlarmScheduleType.CUSTOM, 'device-profile.schedule-custom']
  ]
);

export interface AlarmSchedule{
  dynamicValue?: {
    sourceAttribute: string;
    sourceType: string;
  };
  type: AlarmScheduleType;
  timezone?: string;
  daysOfWeek?: number[];
  startsOn?: number;
  endsOn?: number;
  items?: CustomTimeSchedulerItem[];
}

export interface CustomTimeSchedulerItem{
  enabled: boolean;
  dayOfWeek: number;
  startsOn: number;
  endsOn: number;
}

interface AlarmRule {
  condition: AlarmCondition;
  alarmDetails?: string;
  dashboardId?: DashboardId;
  schedule?: AlarmSchedule;
}

export { AlarmRule as DeviceProfileAlarmRule };

const alarmRuleValid = (alarmRule: AlarmRule): boolean =>
  !(!alarmRule || !alarmRule.condition || !alarmRule.condition.condition || !alarmRule.condition.condition.length);

export const alarmRuleValidator = (control: AbstractControl): ValidationErrors | null => {
  const alarmRule: AlarmRule = control.value;
  return alarmRuleValid(alarmRule) ? null : {alarmRule: true};
};

export interface DeviceProfileAlarm {
  id: string;
  alarmType: string;
  createRules: {[severity: string]: AlarmRule};
  clearRule?: AlarmRule;
  propagate?: boolean;
  propagateToOwner?: boolean;
  propagateToTenant?: boolean;
  propagateRelationTypes?: Array<string>;
}

export const deviceProfileAlarmValidator = (control: AbstractControl): ValidationErrors | null => {
  const deviceProfileAlarm: DeviceProfileAlarm = control.value;
  if (deviceProfileAlarm && deviceProfileAlarm.id && deviceProfileAlarm.alarmType &&
    deviceProfileAlarm.createRules) {
    const severities = Object.keys(deviceProfileAlarm.createRules);
    if (severities.length) {
      let alarmRulesValid = true;
      for (const severity of severities) {
        const alarmRule = deviceProfileAlarm.createRules[severity];
        if (!alarmRuleValid(alarmRule)) {
          alarmRulesValid = false;
          break;
        }
      }
      if (alarmRulesValid) {
        if (deviceProfileAlarm.clearRule && !alarmRuleValid(deviceProfileAlarm.clearRule)) {
          alarmRulesValid = false;
        }
      }
      if (alarmRulesValid) {
        return null;
      }
    }
  }
  return {deviceProfileAlarm: true};
};


export interface DeviceProfileData {
  configuration: DeviceProfileConfiguration;
  transportConfiguration: DeviceProfileTransportConfiguration;
  alarms?: Array<DeviceProfileAlarm>;
  provisionConfiguration?: DeviceProvisionConfiguration;
}

export interface DeviceProfile extends BaseData<DeviceProfileId>, HasTenantId, HasVersion, ExportableEntity<DeviceProfileId> {
  tenantId?: TenantId;
  name: string;
  description?: string;
  default?: boolean;
  type: DeviceProfileType;
  image?: string;
  transportType: DeviceTransportType;
  provisionType: DeviceProvisionType;
  provisionDeviceKey?: string;
  defaultRuleChainId?: RuleChainId;
  defaultDashboardId?: DashboardId;
  defaultQueueName?: string;
  firmwareId?: OtaPackageId;
  softwareId?: OtaPackageId;
  profileData: DeviceProfileData;
  defaultEdgeRuleChainId?: RuleChainId;
}

export interface DeviceProfileInfo extends EntityInfoData, HasTenantId {
  tenantId?: TenantId;
  type: DeviceProfileType;
  transportType: DeviceTransportType;
  image?: string;
  defaultDashboardId?: DashboardId;
}

export interface DefaultDeviceConfiguration {
  [key: string]: any;
}

export type DeviceConfigurations = DefaultDeviceConfiguration;

export interface DeviceConfiguration extends DeviceConfigurations {
  type: DeviceProfileType;
}

export interface DefaultDeviceTransportConfiguration {
  [key: string]: any;
}

export interface MqttDeviceTransportConfiguration {
  [key: string]: any;
}

export interface CoapDeviceTransportConfiguration {
  powerMode?: PowerMode | null;
  edrxCycle?: number;
  pagingTransmissionWindow?: number;
  psmActivityTimer?: number;
}

export interface Lwm2mDeviceTransportConfiguration {
  powerMode?: PowerMode | null;
  edrxCycle?: number;
  pagingTransmissionWindow?: number;
  psmActivityTimer?: number;
}

export enum SnmpDeviceProtocolVersion {
  V1 = 'V1',
  V2C = 'V2C',
  V3 = 'V3'
}

export enum SnmpAuthenticationProtocol {
  SHA_1 = 'SHA_1',
  SHA_224 = 'SHA_224',
  SHA_256 = 'SHA_256',
  SHA_384 = 'SHA_384',
  SHA_512 = 'SHA_512',
  MD5 = 'MD5'
}

export const SnmpAuthenticationProtocolTranslationMap = new Map<SnmpAuthenticationProtocol, string>([
  [SnmpAuthenticationProtocol.SHA_1, 'SHA-1'],
  [SnmpAuthenticationProtocol.SHA_224, 'SHA-224'],
  [SnmpAuthenticationProtocol.SHA_256, 'SHA-256'],
  [SnmpAuthenticationProtocol.SHA_384, 'SHA-384'],
  [SnmpAuthenticationProtocol.SHA_512, 'SHA-512'],
  [SnmpAuthenticationProtocol.MD5, 'MD5']
]);

export enum SnmpPrivacyProtocol {
  DES = 'DES',
  AES_128 = 'AES_128',
  AES_192 = 'AES_192',
  AES_256 = 'AES_256'
}

export const SnmpPrivacyProtocolTranslationMap = new Map<SnmpPrivacyProtocol, string>([
  [SnmpPrivacyProtocol.DES, 'DES'],
  [SnmpPrivacyProtocol.AES_128, 'AES-128'],
  [SnmpPrivacyProtocol.AES_192, 'AES-192'],
  [SnmpPrivacyProtocol.AES_256, 'AES-256'],
]);

export interface SnmpDeviceTransportConfiguration {
  host?: string;
  port?: number;
  protocolVersion?: SnmpDeviceProtocolVersion;
  community?: string;
  username?: string;
  securityName?: string;
  contextName?: string;
  authenticationProtocol?: SnmpAuthenticationProtocol;
  authenticationPassphrase?: string;
  privacyProtocol?: SnmpPrivacyProtocol;
  privacyPassphrase?: string;
  engineId?: string;
}


export interface TcpDeviceTransportConfiguration {
  type?: DeviceTransportType;
  host?: string;
  port?: number;
  sourceHost?: string;
  serverBindPort?: number;
}

export type DeviceTransportConfigurations = DefaultDeviceTransportConfiguration &
  MqttDeviceTransportConfiguration &
  CoapDeviceTransportConfiguration &
  Lwm2mDeviceTransportConfiguration &
  SnmpDeviceTransportConfiguration &
  TcpDeviceTransportConfiguration;

export interface DeviceTransportConfiguration extends DeviceTransportConfigurations {
  type: DeviceTransportType;
}

export interface DeviceData {
  configuration: DeviceConfiguration;
  transportConfiguration: DeviceTransportConfiguration;
}

export interface Device extends BaseData<DeviceId>, HasTenantId, HasVersion, ExportableEntity<DeviceId> {
  tenantId?: TenantId;
  customerId?: CustomerId;
  name: string;
  type?: string;
  label: string;
  firmwareId?: OtaPackageId;
  softwareId?: OtaPackageId;
  deviceProfileId?: DeviceProfileId;
  deviceData?: DeviceData;
  additionalInfo?: any;
}

export interface DeviceInfo extends Device {
  customerTitle: string;
  customerIsPublic: boolean;
  deviceProfileName: string;
  active: boolean;
}

export interface DeviceInfoFilter {
  customerId?: CustomerId;
  edgeId?: EdgeId;
  type?: string;
  deviceProfileId?: DeviceProfileId;
  active?: boolean;
}

export class DeviceInfoQuery  {

  pageLink: PageLink;
  deviceInfoFilter: DeviceInfoFilter;

  constructor(pageLink: PageLink, deviceInfoFilter: DeviceInfoFilter) {
    this.pageLink = pageLink;
    this.deviceInfoFilter = deviceInfoFilter;
  }

  public toQuery(): string {
    let query;
    if (this.deviceInfoFilter.customerId) {
      query = `/customer/${this.deviceInfoFilter.customerId.id}/deviceInfos`;
    } else if (this.deviceInfoFilter.edgeId) {
      query = `/edge/${this.deviceInfoFilter.edgeId.id}/devices`;
    } else {
      query = '/tenant/deviceInfos';
    }
    query += this.pageLink.toQuery();
    if (isNotEmptyStr(this.deviceInfoFilter.type)) {
      query += `&type=${this.deviceInfoFilter.type}`;
    } else if (this.deviceInfoFilter.deviceProfileId) {
      query += `&deviceProfileId=${this.deviceInfoFilter.deviceProfileId.id}`;
    }
    if (isDefinedAndNotNull(this.deviceInfoFilter.active)) {
      query += `&active=${this.deviceInfoFilter.active}`;
    }
    return query;
  }
}

export enum DeviceCredentialsType {
  ACCESS_TOKEN = 'ACCESS_TOKEN',
  X509_CERTIFICATE = 'X509_CERTIFICATE',
  MQTT_BASIC = 'MQTT_BASIC',
  LWM2M_CREDENTIALS = 'LWM2M_CREDENTIALS'
}

export const credentialTypeNames = new Map<DeviceCredentialsType, string>(
  [
    [DeviceCredentialsType.ACCESS_TOKEN, 'Access token'],
    [DeviceCredentialsType.X509_CERTIFICATE, 'X.509'],
    [DeviceCredentialsType.MQTT_BASIC, 'MQTT Basic'],
    [DeviceCredentialsType.LWM2M_CREDENTIALS, 'LwM2M Credentials']
  ]
);

export const credentialTypesByTransportType = new Map<DeviceTransportType, DeviceCredentialsType[]>(
  [
    [DeviceTransportType.DEFAULT, [
      DeviceCredentialsType.ACCESS_TOKEN, DeviceCredentialsType.X509_CERTIFICATE, DeviceCredentialsType.MQTT_BASIC
    ]],
    [DeviceTransportType.MQTT, [
      DeviceCredentialsType.ACCESS_TOKEN, DeviceCredentialsType.X509_CERTIFICATE, DeviceCredentialsType.MQTT_BASIC
    ]],
    [DeviceTransportType.COAP, [DeviceCredentialsType.ACCESS_TOKEN, DeviceCredentialsType.X509_CERTIFICATE]],
    [DeviceTransportType.LWM2M, [DeviceCredentialsType.LWM2M_CREDENTIALS]],
    [DeviceTransportType.SNMP, [DeviceCredentialsType.ACCESS_TOKEN]],
    [DeviceTransportType.TCP, [DeviceCredentialsType.ACCESS_TOKEN]]
  ]
);

export interface DeviceCredentials extends BaseData<DeviceCredentialsId>, HasTenantId {
  deviceId: DeviceId;
  credentialsType: DeviceCredentialsType;
  credentialsId: string;
  credentialsValue: string;
}

export interface DeviceCredentialMQTTBasic {
  clientId: string;
  userName: string;
  password: string;
}

export const getDeviceCredentialMQTTDefault = (): DeviceCredentialMQTTBasic => ({
  clientId: '',
  userName: '',
  password: ''
});

export interface DeviceSearchQuery extends EntitySearchQuery {
  deviceTypes: Array<string>;
}

export interface ClaimRequest {
  secretKey: string;
}

export enum ClaimResponse {
  SUCCESS = 'SUCCESS',
  FAILURE = 'FAILURE',
  CLAIMED = 'CLAIMED'
}

export interface ClaimResult {
  device: Device;
  response: ClaimResponse;
}

export interface PublishTelemetryCommand {
  http?: {
    http?: string;
    https?: string;
  };
  mqtt: {
    mqtt?: string;
    mqtts?: string | Array<string>;
    docker?: {
      mqtt?: string;
      mqtts?: string | Array<string>;
    };
    sparkplug?: string;
  };
  coap: {
    coap?: string;
    coaps?: string | Array<string>;
    docker?: {
      coap?: string;
      coaps?: string | Array<string>;
    };
  };
  lwm2m?: string;
  snmp?: string;
  tcp?: string;
}

export const dayOfWeekTranslations = new Array<string>(
  'device-profile.schedule-day.monday',
  'device-profile.schedule-day.tuesday',
  'device-profile.schedule-day.wednesday',
  'device-profile.schedule-day.thursday',
  'device-profile.schedule-day.friday',
  'device-profile.schedule-day.saturday',
  'device-profile.schedule-day.sunday'
);

export const timeOfDayToUTCTimestamp = (date: Date | number): number => {
  if (typeof date === 'number' || date === null) {
    return 0;
  }
  return _moment.utc([1970, 0, 1, date.getHours(), date.getMinutes(), date.getSeconds(), 0]).valueOf();
};

export const utcTimestampToTimeOfDay = (time = 0): Date => new Date(time + new Date(time).getTimezoneOffset() * 60 * 1000);

const timeOfDayToMoment = (date: Date | number): _moment.Moment => {
  if (typeof date === 'number' || date === null) {
    return _moment([1970, 0, 1, 0, 0, 0, 0]);
  }
  return _moment([1970, 0, 1, date.getHours(), date.getMinutes(), 0, 0]);
};

export const getAlarmScheduleRangeText = (startsOn: Date | number, endsOn: Date | number): string => {
  const start = timeOfDayToMoment(startsOn);
  const end = timeOfDayToMoment(endsOn);
  if (start < end) {
    return `<span><span class="nowrap">${start.format('hh:mm A')}</span> – <span class="nowrap">${end.format('hh:mm A')}</span></span>`;
  } else if (start.valueOf() === 0 && end.valueOf() === 0 || start.isSame(_moment([1970, 0])) && end.isSame(_moment([1970, 0]))) {
    return '<span><span class="nowrap">12:00 AM</span> – <span class="nowrap">12:00 PM</span></span>';
  }
  return `<span><span class="nowrap">12:00 AM</span> – <span class="nowrap">${end.format('hh:mm A')}</span>` +
    ` and <span class="nowrap">${start.format('hh:mm A')}</span> – <span class="nowrap">12:00 PM</span></span>`;
};
