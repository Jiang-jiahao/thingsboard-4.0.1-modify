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
  TCP = 'TCP',
  UDP = 'UDP',
  HTTP_PULL = 'HTTP_PULL',
  MQTT_PULL = 'MQTT_PULL'
}

export enum BasicTransportType {
  HTTP = 'HTTP',
  MQTT = 'MQTT'
}

/** HTTP 传输在 UI 中的工作模式（保存时映射为 DEFAULT 或 HTTP_PULL） */
export enum HttpTransportMode {
  PASSIVE = 'PASSIVE',
  PULL = 'PULL'
}

/** MQTT 传输在 UI 中的工作模式（保存时映射为 MQTT 或 MQTT_PULL） */
export enum MqttTransportMode {
  PASSIVE = 'PASSIVE',
  PULL = 'PULL'
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

/** Set to true to show device provisioning in device profile UI. */
export const DEVICE_PROVISIONING_UI_ENABLED = false;

/** Set to true to show gateway management UI (menu, routes, device gateway options). */
export const GATEWAY_UI_ENABLED = false;

/** Set to true to show edge management UI (menu, routes, customer edge actions). */
export const EDGE_UI_ENABLED = false;

/** Set to true to show mobile center UI (menu, routes, device/asset profile mobile dashboard). */
export const MOBILE_UI_ENABLED = false;

/** Set to true to show OTA updates UI (advanced features menu, device/profile firmware & software). */
export const OTA_UI_ENABLED = false;

/** Set to true to show version control UI (advanced features menu, entity tabs, settings, editors). */
export const VERSION_CONTROL_UI_ENABLED = false;

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
    [DeviceTransportType.UDP, 'device-profile.transport-type-udp'],
    [BasicTransportType.HTTP, 'device-profile.transport-type-http']
  ]
);

/** 设备配置向导/编辑页传输类型下拉选项（HTTP 合并主动/被动，不单独展示 HTTP_PULL） */
export const deviceProfileTransportTypeOptions: TransportType[] = [
  DeviceTransportType.DEFAULT,
  DeviceTransportType.MQTT,
  DeviceTransportType.COAP,
  DeviceTransportType.LWM2M,
  DeviceTransportType.SNMP,
  BasicTransportType.HTTP,
  DeviceTransportType.TCP,
  DeviceTransportType.UDP
];


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
    [DeviceTransportType.UDP, 'device-profile.transport-type-udp-hint'],
    [BasicTransportType.HTTP, 'device-profile.transport-type-http-hint']
  ]
);

export function isHttpTransportType(transportType: TransportType | DeviceTransportType | null | undefined): boolean {
  return transportType === DeviceTransportType.HTTP_PULL || transportType === BasicTransportType.HTTP;
}

export function isMqttTransportType(transportType: TransportType | DeviceTransportType | null | undefined): boolean {
  return transportType === DeviceTransportType.MQTT_PULL || transportType === BasicTransportType.MQTT
    || transportType === DeviceTransportType.MQTT;
}

export function toUiTransportType(transportType: DeviceTransportType | TransportType | null | undefined): TransportType | null {
  if (!transportType) {
    return null;
  }
  if (isHttpTransportType(transportType)) {
    return BasicTransportType.HTTP;
  }
  if (isMqttTransportType(transportType)) {
    return BasicTransportType.MQTT;
  }
  return transportType as TransportType;
}

/** 判断档案传输配置是否为 HTTP 主动拉取（不依赖 type 字段，父表单会剥离 type） */
export function isHttpPullProfileTransportConfiguration(
  transportConfiguration?: { type?: DeviceTransportType; httpTransportMode?: HttpTransportMode } | HttpPullDeviceProfileTransportConfiguration | null
): boolean {
  if (!transportConfiguration) {
    return false;
  }
  if (transportConfiguration.httpTransportMode === HttpTransportMode.PULL) {
    return true;
  }
  if (transportConfiguration.httpTransportMode === HttpTransportMode.PASSIVE) {
    return false;
  }
  if ('type' in transportConfiguration && transportConfiguration.type === DeviceTransportType.HTTP_PULL) {
    return true;
  }
  const cfg = transportConfiguration as HttpPullDeviceProfileTransportConfiguration;
  if (cfg.pollRequests?.length) {
    return true;
  }
  if (cfg.pollUrl) {
    return true;
  }
  if (cfg.queryingFrequencyMs != null && cfg.auth != null) {
    return true;
  }
  return false;
}

export function resolveTransportTypeForSave(uiTransportType: TransportType,
                                           transportConfiguration?: {
                                             type?: DeviceTransportType;
                                             httpTransportMode?: HttpTransportMode;
                                             mqttTransportMode?: MqttTransportMode;
                                           }): DeviceTransportType {
  if (uiTransportType === BasicTransportType.HTTP) {
    return isHttpPullProfileTransportConfiguration(transportConfiguration)
      ? DeviceTransportType.HTTP_PULL
      : DeviceTransportType.DEFAULT;
  }
  if (uiTransportType === BasicTransportType.MQTT) {
    return isMqttPullProfileTransportConfiguration(transportConfiguration)
      ? DeviceTransportType.MQTT_PULL
      : DeviceTransportType.MQTT;
  }
  return uiTransportType as DeviceTransportType;
}

/**
 * 解析档案 HTTP 传输类型（回显用）：优先 transportConfiguration 中的工作模式标记，
 * 避免 entity.transportType 仍为 DEFAULT 时把已保存的主动拉取误判为被动上报。
 */
export function resolveHttpProfileTransportTypeForDisplay(
  entityTransportType: DeviceTransportType | string | null | undefined,
  transportConfiguration?: { type?: DeviceTransportType; httpTransportMode?: HttpTransportMode } | null
): DeviceTransportType {
  if (transportConfiguration?.httpTransportMode === HttpTransportMode.PULL) {
    return DeviceTransportType.HTTP_PULL;
  }
  if (transportConfiguration?.httpTransportMode === HttpTransportMode.PASSIVE) {
    return DeviceTransportType.DEFAULT;
  }
  if (transportConfiguration?.type === DeviceTransportType.HTTP_PULL) {
    return DeviceTransportType.HTTP_PULL;
  }
  if (transportConfiguration && isHttpPullProfileTransportConfiguration(transportConfiguration)) {
    return DeviceTransportType.HTTP_PULL;
  }
  return (entityTransportType as DeviceTransportType) ?? DeviceTransportType.DEFAULT;
}

/** 从 API 加载后补齐工作模式，保证编辑页回显「主动拉取」 */
export function normalizeHttpProfileTransportConfigurationForDisplay(
  transportType: DeviceTransportType | string | null | undefined,
  transportConfiguration: DeviceProfileTransportConfiguration | Record<string, unknown> | null | undefined
): DeviceProfileTransportConfiguration | null | undefined {
  if (!transportConfiguration) {
    return transportConfiguration as null | undefined;
  }
  const effectiveTransportType = resolveHttpProfileTransportTypeForDisplay(transportType, transportConfiguration);
  const isPull = effectiveTransportType === DeviceTransportType.HTTP_PULL;
  if (!isPull) {
    return {
      ...transportConfiguration,
      type: DeviceTransportType.DEFAULT,
      httpTransportMode: HttpTransportMode.PASSIVE
    } as DeviceProfileTransportConfiguration;
  }
  return {
    ...transportConfiguration,
    type: DeviceTransportType.HTTP_PULL,
    httpTransportMode: transportConfiguration.httpTransportMode || HttpTransportMode.PULL
  } as DeviceProfileTransportConfiguration;
}

/** 保存前补齐 JSON 多态 type 与工作模式，避免后端按 DEFAULT 解析 */
export function normalizeHttpProfileTransportConfigurationForSave(
  transportType: DeviceTransportType,
  transportConfiguration: DeviceProfileTransportConfiguration | Record<string, unknown> | null | undefined
): DeviceProfileTransportConfiguration | null | undefined {
  if (!transportConfiguration) {
    return transportConfiguration as null | undefined;
  }
  const isPull = transportType === DeviceTransportType.HTTP_PULL
    || isHttpPullProfileTransportConfiguration(transportConfiguration as HttpPullDeviceProfileTransportConfiguration);
  const mode = isPull ? HttpTransportMode.PULL : HttpTransportMode.PASSIVE;
  if (isPull) {
    return {
      ...transportConfiguration,
      type: DeviceTransportType.HTTP_PULL,
      httpTransportMode: mode
    } as DeviceProfileTransportConfiguration;
  }
  const passive: DeviceProfileTransportConfiguration = {
    type: DeviceTransportType.DEFAULT,
    httpTransportMode: mode
  };
  if (transportConfiguration.routing) {
    passive.routing = transportConfiguration.routing;
  }
  return passive;
}

export function hasDeviceProfileTransportConfiguration(transportType: TransportType): boolean {
  if (transportType === BasicTransportType.HTTP || transportType === BasicTransportType.MQTT) {
    return true;
  }
  return deviceTransportTypeConfigurationInfoMap.get(transportType as DeviceTransportType)?.hasProfileConfiguration ?? false;
}

export function hasDeviceTransportConfiguration(transportType: TransportType): boolean {
  if (transportType === BasicTransportType.HTTP || transportType === BasicTransportType.MQTT) {
    return true;
  }
  return deviceTransportTypeConfigurationInfoMap.get(transportType as DeviceTransportType)?.hasDeviceConfiguration ?? false;
}

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
      DeviceTransportType.HTTP_PULL,
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
    ],
    [
      DeviceTransportType.UDP,
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
  routing?: HttpPullDeviceRoutingConfiguration;
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

export enum HttpPullAuthType {
  NONE = 'NONE',
  API_KEY = 'API_KEY',
  BASIC = 'BASIC',
  BEARER_STATIC = 'BEARER_STATIC',
  LOGIN_TOKEN = 'LOGIN_TOKEN',
  OAUTH2_CLIENT_CREDENTIALS = 'OAUTH2_CLIENT_CREDENTIALS',
  OAUTH2_PASSWORD = 'OAUTH2_PASSWORD'
}

export enum HttpPullRoutingMode {
  SINGLE_DEVICE = 'SINGLE_DEVICE',
  /** 已废弃：曾用于 MQTT 按消息路由 */
  PER_MESSAGE = 'PER_MESSAGE',
  MULTI_DEVICE = 'MULTI_DEVICE',
  AUTO = 'AUTO'
}

/** 界面可选的路由模式（不含已废弃的 AUTO） */
export const HTTP_PULL_ROUTING_MODE_OPTIONS = [
  HttpPullRoutingMode.SINGLE_DEVICE,
  HttpPullRoutingMode.MULTI_DEVICE
];

export function normalizeHttpPullRoutingMode(mode?: HttpPullRoutingMode | null): HttpPullRoutingMode {
  return mode === HttpPullRoutingMode.MULTI_DEVICE || mode === HttpPullRoutingMode.AUTO
    ? HttpPullRoutingMode.MULTI_DEVICE
    : HttpPullRoutingMode.SINGLE_DEVICE;
}

export enum HttpPullDeviceIdMatchStrategy {
  DEVICE_NAME = 'DEVICE_NAME',
  DEVICE_LABEL = 'DEVICE_LABEL',
  EXTERNAL_DEVICE_ID = 'EXTERNAL_DEVICE_ID'
}

export interface HttpPullAuthConfiguration {
  authType?: HttpPullAuthType;
  apiKeyHeader?: string;
  apiKeyValue?: string;
  apiKeyInQuery?: boolean;
  apiKeyQueryParam?: string;
  username?: string;
  password?: string;
  bearerToken?: string;
  loginUrl?: string;
  loginMethod?: string;
  loginBody?: string;
  loginHeaders?: { [key: string]: string };
  accessTokenJsonPath?: string;
  tokenHeader?: string;
  tokenPrefix?: string;
  expiresInJsonPath?: string;
  defaultTokenTtlSec?: number;
  refreshTokenJsonPath?: string;
  refreshUrl?: string;
  refreshMethod?: string;
  refreshBodyTemplate?: string;
  tokenUrl?: string;
  clientId?: string;
  clientSecret?: string;
  scope?: string;
  oauthUsername?: string;
  oauthPassword?: string;
}

export interface HttpPullDeviceRoutingConfiguration {
  routingMode?: HttpPullRoutingMode;
  responseArrayJsonPath?: string;
  deviceIdJsonPath?: string;
  deviceIdMatchStrategy?: HttpPullDeviceIdMatchStrategy;
  targetDeviceProfileId?: string;
  telemetryPayloadKey?: string;
}

export enum HttpPullPollDataType {
  TELEMETRY = 'TELEMETRY',
  CLIENT_ATTRIBUTES = 'CLIENT_ATTRIBUTES',
  SHARED_ATTRIBUTES = 'SHARED_ATTRIBUTES'
}

export interface HttpPullPollRequest {
  id?: string;
  name?: string;
  enabled?: boolean;
  pollUrl?: string;
  pollMethod?: string;
  pollBody?: string;
  pollHeaders?: { [key: string]: string };
  /** 为空时使用档案默认轮询间隔 */
  queryingFrequencyMs?: number;
  dataType?: HttpPullPollDataType;
  /** false：本请求无需携带登录凭证（如登录接口） */
  requiresAuth?: boolean;
  routing?: HttpPullDeviceRoutingConfiguration;
}

export interface HttpPullDeviceProfileTransportConfiguration {
  /** 保存/回显用：PULL=主动拉取，PASSIVE=被动上报 */
  httpTransportMode?: HttpTransportMode;
  timeoutMs?: number;
  readTimeoutMs?: number;
  queryingFrequencyMs?: number;
  pollRequests?: HttpPullPollRequest[];
  /** @deprecated 请使用 pollRequests */
  pollUrl?: string;
  pollMethod?: string;
  pollBody?: string;
  pollHeaders?: { [key: string]: string };
  auth?: HttpPullAuthConfiguration;
  routing?: HttpPullDeviceRoutingConfiguration;
}

export interface HttpPullDeviceTransportConfiguration {
  collector?: boolean;
  externalDeviceId?: string;
  /** 目标设备归属的采集器设备 ID（UUID） */
  collectorDeviceId?: string;
  pollUrlOverride?: string;
}

/** 与后端 {@code HttpPullDeviceTransportConfiguration#isCollector()} 一致 */
export function resolveHttpPullDeviceIsCollector(
  cfg: Pick<HttpPullDeviceTransportConfiguration, 'collector' | 'externalDeviceId'> | null | undefined
): boolean {
  if (!cfg) {
    return true;
  }
  if ((cfg.externalDeviceId || '').trim()) {
    return false;
  }
  return cfg.collector !== false;
}

/** 档案下同配置下可作为「归属采集器」的候选设备：未配置外部设备 ID */
export function isHttpPullCollectorCandidate(
  cfg: Pick<HttpPullDeviceTransportConfiguration, 'externalDeviceId'> | null | undefined
): boolean {
  return !(cfg?.externalDeviceId || '').trim();
}

/** 根据字段形态判断是否为 HTTP 主动拉取设备传输配置（与 Default 被动上报区分） */
export function isHttpPullDeviceTransportConfigurationShape(
  cfg: Record<string, unknown> | null | undefined
): boolean {
  if (!cfg) {
    return false;
  }
  return 'collector' in cfg || 'pollUrlOverride' in cfg || 'collectorDeviceId' in cfg;
}

/** 保存设备时解析传输 JSON 多态 type，避免误存为 DEFAULT 导致 collectorDeviceId 等字段丢失 */
export function resolveHttpDeviceTransportTypeForSave(
  transportType: DeviceTransportType | TransportType | null | undefined,
  configuration: Record<string, unknown> | null | undefined,
  httpPullProfileActive = false
): DeviceTransportType {
  if (configuration?.type === DeviceTransportType.HTTP_PULL) {
    return DeviceTransportType.HTTP_PULL;
  }
  if (httpPullProfileActive || isHttpPullDeviceTransportConfigurationShape(configuration)) {
    return DeviceTransportType.HTTP_PULL;
  }
  if (transportType === BasicTransportType.HTTP) {
    return DeviceTransportType.DEFAULT;
  }
  return (transportType as DeviceTransportType) ?? DeviceTransportType.DEFAULT;
}

/** 保存前规范化 HTTP Pull 设备传输配置，确保 type 与字段与后端 {@link HttpPullDeviceTransportConfiguration} 一致 */
export function normalizeHttpDeviceTransportConfigurationForSave(
  transportType: DeviceTransportType | TransportType | null | undefined,
  configuration: DeviceTransportConfiguration | Record<string, unknown> | null | undefined,
  httpPullProfileActive = false
): DeviceTransportConfiguration | null | undefined {
  if (!configuration) {
    return configuration as null | undefined;
  }
  const resolvedType = resolveHttpDeviceTransportTypeForSave(transportType, configuration as Record<string, unknown>, httpPullProfileActive);
  if (resolvedType !== DeviceTransportType.HTTP_PULL) {
    return { ...configuration, type: resolvedType } as DeviceTransportConfiguration;
  }
  const raw = configuration as HttpPullDeviceTransportConfiguration;
  const collector = resolveHttpPullDeviceIsCollector(raw);
  const externalDeviceId = (raw.externalDeviceId || '').trim() || undefined;
  const collectorDeviceId = (raw.collectorDeviceId || '').trim() || undefined;
  const pollUrlOverride = (raw.pollUrlOverride || '').trim() || undefined;
  return {
    type: DeviceTransportType.HTTP_PULL,
    collector,
    externalDeviceId: collector ? undefined : externalDeviceId,
    collectorDeviceId: collector ? undefined : collectorDeviceId,
    pollUrlOverride: collector ? pollUrlOverride : undefined
  };
}

export interface HttpPullProfileContext {
  routingMode: HttpPullRoutingMode;
  pollUrl: string | null;
}

/** 从档案传输配置解析路由模式（优先各拉取请求上的 routing，兼容旧版档案级 routing） */
export function resolveHttpPullProfileRoutingMode(
  raw: HttpPullDeviceProfileTransportConfiguration | null | undefined
): HttpPullRoutingMode {
  if (!raw) {
    return HttpPullRoutingMode.SINGLE_DEVICE;
  }
  const pollRequests = raw.pollRequests;
  if (pollRequests?.length) {
    for (const req of pollRequests) {
      const mode = req.routing?.routingMode ?? raw.routing?.routingMode;
      if (mode === HttpPullRoutingMode.MULTI_DEVICE || mode === HttpPullRoutingMode.AUTO) {
        return HttpPullRoutingMode.MULTI_DEVICE;
      }
    }
    return normalizeHttpPullRoutingMode(pollRequests[0]?.routing?.routingMode ?? raw.routing?.routingMode);
  }
  return normalizeHttpPullRoutingMode(raw.routing?.routingMode);
}

/** 从完整设备档案解析 HTTP 主动采集的路由模式与档案级拉取 URL */
export function extractHttpPullProfileContext(dp: DeviceProfile | null | undefined): HttpPullProfileContext | null {
  if (!dp || dp.transportType !== DeviceTransportType.HTTP_PULL) {
    return null;
  }
  const raw = dp.profileData?.transportConfiguration as HttpPullDeviceProfileTransportConfiguration | undefined;
  if (!raw) {
    return null;
  }
  const firstPoll = raw.pollRequests?.[0];
  return {
    routingMode: resolveHttpPullProfileRoutingMode(raw),
    pollUrl: firstPoll?.pollUrl ?? raw.pollUrl ?? null
  };
}

/** 从 HTTP 档案（被动上报 / DEFAULT 传输）解析数据路由模式 */
export function extractHttpPushProfileContext(dp: DeviceProfile | null | undefined): { routingMode: HttpPullRoutingMode } | null {
  if (!dp || dp.transportType === DeviceTransportType.HTTP_PULL) {
    return null;
  }
  if (toUiTransportType(dp.transportType) !== BasicTransportType.HTTP) {
    return null;
  }
  const raw = dp.profileData?.transportConfiguration as DefaultDeviceProfileTransportConfiguration | undefined;
  if (!raw) {
    return { routingMode: HttpPullRoutingMode.SINGLE_DEVICE };
  }
  const pullCfg = raw as HttpPullDeviceProfileTransportConfiguration;
  if (pullCfg.pollUrl != null) {
    return null;
  }
  return {
    routingMode: raw.routing?.routingMode ?? HttpPullRoutingMode.SINGLE_DEVICE
  };
}

const HTTP_PULL_HOST_PORT_PATTERN = /^[\w.\-]+:\d+$/;

function parseHttpPullOverrideUrl(raw: string): URL | null {
  try {
    const withScheme = /^https?:\/\//i.test(raw) || raw.includes('://') ? raw : `http://${raw}`;
    return new URL(withScheme);
  } catch {
    return null;
  }
}

/** 仅主机:端口或仅有 origin（无路径）时，拉取时与档案 URL 合并路径 */
export function shouldMergeHttpPullPollUrlWithProfile(raw: string | undefined | null): boolean {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return false;
  }
  if (HTTP_PULL_HOST_PORT_PATTERN.test(trimmed)) {
    return true;
  }
  const parsed = parseHttpPullOverrideUrl(trimmed);
  if (!parsed) {
    return false;
  }
  const path = parsed.pathname;
  return path === '' || path === '/';
}

/**
 * 运行时有效拉取 URL（与后端 HttpPullPollUrlResolver 一致）。
 */
export function resolveHttpPullPollUrlOverride(
  raw: string | undefined | null,
  profilePollUrl: string | null | undefined
): string | undefined {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return profilePollUrl || undefined;
  }
  if (!shouldMergeHttpPullPollUrlWithProfile(trimmed)) {
    return trimmed;
  }
  if (!profilePollUrl) {
    return parseHttpPullOverrideUrl(trimmed)?.origin.replace(/\/$/, '') || trimmed;
  }
  try {
    const profile = new URL(profilePollUrl);
    const override = parseHttpPullOverrideUrl(trimmed);
    if (!override) {
      return trimmed;
    }
    return `${override.protocol}//${override.host}${profile.pathname}${profile.search}`;
  } catch {
    return trimmed;
  }
}

/**
 * 表单展示：若已存 URL 的路径与当前档案一致，仅显示主机:端口，便于档案改路径后仍生效。
 */
export function formatHttpPullPollUrlOverrideForDisplay(
  stored: string | undefined | null,
  profilePollUrl: string | null | undefined
): string {
  const trimmed = stored?.trim();
  if (!trimmed) {
    return '';
  }
  if (HTTP_PULL_HOST_PORT_PATTERN.test(trimmed)) {
    return trimmed;
  }
  if (!profilePollUrl) {
    return trimmed;
  }
  try {
    const profile = new URL(profilePollUrl);
    const override = parseHttpPullOverrideUrl(trimmed);
    if (override && override.pathname === profile.pathname && override.search === profile.search) {
      return override.host;
    }
  } catch {
    /* keep trimmed */
  }
  return trimmed;
}

export enum MqttPullAuthType {
  NONE = 'NONE',
  USERNAME_PASSWORD = 'USERNAME_PASSWORD'
}

export interface MqttPullAuthConfiguration {
  authType?: MqttPullAuthType;
  username?: string;
  password?: string;
}

export interface MqttPullSubscribeRequest {
  id?: string;
  name?: string;
  enabled?: boolean;
  topic?: string;
  qos?: number;
  dataType?: HttpPullPollDataType;
  telemetryPayloadKey?: string;
}

export interface MqttPullDeviceProfileTransportConfiguration {
  mqttTransportMode?: MqttTransportMode;
  connectTimeoutMs?: number;
  keepAliveSec?: number;
  cleanSession?: boolean;
  reconnectIntervalMs?: number;
  subscribeRequests?: MqttPullSubscribeRequest[];
}

export interface MqttPullDeviceTransportConfiguration {
  brokerUrl?: string;
  clientId?: string;
  /** 对方 Server Topic 前缀。仅无人机监管等协议需要；大公博创留空 */
  topicPrefix?: string;
  auth?: MqttPullAuthConfiguration;
}

/** 判断档案传输配置是否为 MQTT 主动拉取（不依赖 type 字段，父表单会剥离 type） */
export function isMqttPullProfileTransportConfiguration(
  transportConfiguration?: { type?: DeviceTransportType; mqttTransportMode?: MqttTransportMode } | MqttPullDeviceProfileTransportConfiguration | null
): boolean {
  if (!transportConfiguration) {
    return false;
  }
  if (transportConfiguration.mqttTransportMode === MqttTransportMode.PULL) {
    return true;
  }
  if (transportConfiguration.mqttTransportMode === MqttTransportMode.PASSIVE) {
    return false;
  }
  if ('type' in transportConfiguration && transportConfiguration.type === DeviceTransportType.MQTT_PULL) {
    return true;
  }
  const cfg = transportConfiguration as MqttPullDeviceProfileTransportConfiguration;
  if (cfg.subscribeRequests?.length) {
    return true;
  }
  return cfg.connectTimeoutMs != null;
}

export function resolveMqttProfileTransportTypeForDisplay(
  entityTransportType: DeviceTransportType | string | null | undefined,
  transportConfiguration?: { type?: DeviceTransportType; mqttTransportMode?: MqttTransportMode } | null
): DeviceTransportType {
  if (transportConfiguration?.mqttTransportMode === MqttTransportMode.PULL) {
    return DeviceTransportType.MQTT_PULL;
  }
  if (transportConfiguration?.mqttTransportMode === MqttTransportMode.PASSIVE) {
    return DeviceTransportType.MQTT;
  }
  if (transportConfiguration?.type === DeviceTransportType.MQTT_PULL) {
    return DeviceTransportType.MQTT_PULL;
  }
  if (transportConfiguration && isMqttPullProfileTransportConfiguration(transportConfiguration)) {
    return DeviceTransportType.MQTT_PULL;
  }
  return (entityTransportType as DeviceTransportType) ?? DeviceTransportType.MQTT;
}

export function normalizeMqttProfileTransportConfigurationForDisplay(
  transportType: DeviceTransportType | string | null | undefined,
  transportConfiguration: DeviceProfileTransportConfiguration | Record<string, unknown> | null | undefined
): DeviceProfileTransportConfiguration | null | undefined {
  if (!transportConfiguration) {
    return transportConfiguration as null | undefined;
  }
  const effectiveTransportType = resolveMqttProfileTransportTypeForDisplay(transportType, transportConfiguration);
  const isPull = effectiveTransportType === DeviceTransportType.MQTT_PULL;
  if (!isPull) {
    return {
      ...transportConfiguration,
      type: DeviceTransportType.MQTT,
      mqttTransportMode: MqttTransportMode.PASSIVE
    } as DeviceProfileTransportConfiguration;
  }
  return {
    ...transportConfiguration,
    type: DeviceTransportType.MQTT_PULL,
    mqttTransportMode: transportConfiguration.mqttTransportMode || MqttTransportMode.PULL
  } as DeviceProfileTransportConfiguration;
}

export function normalizeMqttProfileTransportConfigurationForSave(
  transportType: DeviceTransportType,
  transportConfiguration: DeviceProfileTransportConfiguration | Record<string, unknown> | null | undefined
): DeviceProfileTransportConfiguration | null | undefined {
  if (!transportConfiguration) {
    return transportConfiguration as null | undefined;
  }
  const isPull = transportType === DeviceTransportType.MQTT_PULL
    || isMqttPullProfileTransportConfiguration(transportConfiguration as MqttPullDeviceProfileTransportConfiguration);
  const mode = isPull ? MqttTransportMode.PULL : MqttTransportMode.PASSIVE;
  if (isPull) {
    const raw = transportConfiguration as MqttPullDeviceProfileTransportConfiguration;
    const subscribeRequests = (raw.subscribeRequests || []).map(req => ({
      id: req.id,
      name: req.name,
      enabled: req.enabled,
      topic: req.topic,
      qos: req.qos,
      dataType: req.dataType,
      telemetryPayloadKey: req.telemetryPayloadKey || 'mqttPullPayload'
    }));
    return {
      ...transportConfiguration,
      type: DeviceTransportType.MQTT_PULL,
      mqttTransportMode: mode,
      subscribeRequests
    } as DeviceProfileTransportConfiguration;
  }
  return {
    ...transportConfiguration,
    type: DeviceTransportType.MQTT,
    mqttTransportMode: mode
  } as DeviceProfileTransportConfiguration;
}

export function normalizeProfileTransportConfigurationForDisplay(
  transportType: DeviceTransportType | string | null | undefined,
  transportConfiguration: DeviceProfileTransportConfiguration | Record<string, unknown> | null | undefined
): DeviceProfileTransportConfiguration | null | undefined {
  if (!transportConfiguration) {
    return transportConfiguration as null | undefined;
  }
  const uiType = toUiTransportType(transportType as DeviceTransportType);
  if (uiType === BasicTransportType.HTTP) {
    return normalizeHttpProfileTransportConfigurationForDisplay(transportType, transportConfiguration);
  }
  if (uiType === BasicTransportType.MQTT) {
    return normalizeMqttProfileTransportConfigurationForDisplay(transportType, transportConfiguration);
  }
  return transportConfiguration as DeviceProfileTransportConfiguration;
}

export function normalizeProfileTransportConfigurationForSave(
  transportType: DeviceTransportType,
  transportConfiguration: DeviceProfileTransportConfiguration | Record<string, unknown> | null | undefined
): DeviceProfileTransportConfiguration | null | undefined {
  if (!transportConfiguration) {
    return transportConfiguration as null | undefined;
  }
  const uiType = toUiTransportType(transportType);
  if (uiType === BasicTransportType.HTTP) {
    return normalizeHttpProfileTransportConfigurationForSave(transportType, transportConfiguration);
  }
  if (uiType === BasicTransportType.MQTT) {
    return normalizeMqttProfileTransportConfigurationForSave(transportType, transportConfiguration);
  }
  return transportConfiguration as DeviceProfileTransportConfiguration;
}

export function isMqttPullDeviceTransportConfigurationShape(
  cfg: Record<string, unknown> | null | undefined
): boolean {
  if (!cfg) {
    return false;
  }
  return 'brokerUrl' in cfg || 'clientId' in cfg || 'auth' in cfg;
}

export function resolveMqttDeviceTransportTypeForSave(
  transportType: DeviceTransportType | TransportType | null | undefined,
  configuration: Record<string, unknown> | null | undefined,
  mqttPullProfileActive = false
): DeviceTransportType {
  if (configuration?.type === DeviceTransportType.MQTT_PULL) {
    return DeviceTransportType.MQTT_PULL;
  }
  if (mqttPullProfileActive || isMqttPullDeviceTransportConfigurationShape(configuration)) {
    return DeviceTransportType.MQTT_PULL;
  }
  if (transportType === BasicTransportType.MQTT) {
    return DeviceTransportType.MQTT;
  }
  return (transportType as DeviceTransportType) ?? DeviceTransportType.MQTT;
}

export function normalizeMqttDeviceTransportConfigurationForSave(
  transportType: DeviceTransportType | TransportType | null | undefined,
  configuration: DeviceTransportConfiguration | Record<string, unknown> | null | undefined,
  mqttPullProfileActive = false
): DeviceTransportConfiguration | null | undefined {
  if (!configuration) {
    return configuration as null | undefined;
  }
  const resolvedType = resolveMqttDeviceTransportTypeForSave(transportType, configuration as Record<string, unknown>, mqttPullProfileActive);
  if (resolvedType !== DeviceTransportType.MQTT_PULL) {
    return { ...configuration, type: resolvedType } as DeviceTransportConfiguration;
  }
  const raw = configuration as MqttPullDeviceTransportConfiguration;
  const brokerUrl = (raw.brokerUrl || '').trim() || undefined;
  const clientId = (raw.clientId || '').trim() || undefined;
  const topicPrefix = (raw.topicPrefix || '').trim() || undefined;
  const authType = raw.auth?.authType || MqttPullAuthType.NONE;
  return {
    type: DeviceTransportType.MQTT_PULL,
    brokerUrl,
    clientId,
    topicPrefix,
    auth: {
      authType,
      username: authType === MqttPullAuthType.USERNAME_PASSWORD ? (raw.auth?.username || undefined) : undefined,
      password: authType === MqttPullAuthType.USERNAME_PASSWORD ? (raw.auth?.password || undefined) : undefined
    }
  };
}

export interface MqttPullProfileContext {
  active: true;
}

export function extractMqttPullProfileContext(dp: DeviceProfile | null | undefined): MqttPullProfileContext | null {
  if (!dp || dp.transportType !== DeviceTransportType.MQTT_PULL) {
    return null;
  }
  return { active: true };
}

/** 档案主题已是完整路径（如大公 dgb/${device.name}/...）时，设备不需要填写主题前缀 */
export function mqttPullProfileUsesAbsoluteTopics(dp: DeviceProfile | null | undefined): boolean {
  const cfg = dp?.profileData?.transportConfiguration as MqttPullDeviceProfileTransportConfiguration | undefined;
  const topics = (cfg?.subscribeRequests || []).map(r => (r.topic || '').trim()).filter(Boolean);
  return topics.some(t => t.startsWith('dgb/') || t.includes('${device.name}') || t.includes('${deviceName}'));
}

export function defaultMqttPullDeviceTopicPrefix(dp?: DeviceProfile | null): string | undefined {
  return mqttPullProfileUsesAbsoluteTopics(dp) ? undefined : 'server/chan';
}

export function withMqttPullTopicPrefixForProfile(
  transportConfiguration: DeviceTransportConfiguration | null | undefined,
  deviceProfile: DeviceProfile | null | undefined
): DeviceTransportConfiguration | null | undefined {
  if (!transportConfiguration || !deviceProfile) {
    return transportConfiguration;
  }
  const cfg = transportConfiguration as MqttPullDeviceTransportConfiguration;
  if (cfg.type && cfg.type !== DeviceTransportType.MQTT_PULL) {
    return transportConfiguration;
  }
  const current = (cfg.topicPrefix || '').trim();
  const next = defaultMqttPullDeviceTopicPrefix(deviceProfile) || '';
  const wasDefault = !current || current === 'server/chan';
  if (!wasDefault || current === next) {
    return transportConfiguration;
  }
  return { ...cfg, topicPrefix: next || undefined };
}

export function createUavMqttPlatformSubscribeRequests(): MqttPullSubscribeRequest[] {
  return [
    { name: 'system', enabled: true, topic: 'api/system', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'system' },
    { name: 'locate', enabled: true, topic: 'api/locate', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'locate' },
    { name: 'rid', enabled: true, topic: 'api/rid', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'rid' },
    { name: 'detect', enabled: true, topic: 'api/detect', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'detect' },
    { name: 'aoa', enabled: true, topic: 'api/aoa', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'aoa' },
    { name: 'jammerresult', enabled: true, topic: 'api/jammerresult', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'jammerresult' },
    { name: 'jammerResponse', enabled: true, topic: 'api/jammer/response', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'jammerResponse' }
  ];
}

export function createUavMqttPlatformRpcMethods(): DeviceProfileRpcMethod[] {
  return [
    {
      id: 'jammer',
      displayName: '单次反制',
      oneWay: false,
      timeoutMs: 20000,
      bindingType: DeviceProfileRpcBindingType.MQTT_CUSTOM,
      mqttRequestTopic: 'api/jammer',
      mqttResponseTopic: 'api/jammerresult',
      mqttPayloadTemplate:
        '{"device_id":${params.device_id},"sector_id":${params.sector_id},"emit":{"freq":${params.freq},"power":${params.power},"duration":${params.duration}}}',
      mqttQos: 1,
      paramsTemplateJson: '{"device_id":0,"sector_id":7,"freq":["2400","5800"],"power":1,"duration":10}'
    },
    {
      id: 'jammerStop',
      displayName: '停止反制',
      oneWay: false,
      timeoutMs: 15000,
      bindingType: DeviceProfileRpcBindingType.MQTT_CUSTOM,
      mqttRequestTopic: 'api/jammer',
      mqttResponseTopic: 'api/jammerresult',
      mqttPayloadTemplate:
        '{"device_id":${params.device_id},"sector_id":${params.sector_id},"emit":{"freq":${params.freq},"power":0,"duration":0}}',
      mqttQos: 1,
      paramsTemplateJson: '{"device_id":0,"sector_id":7,"freq":[]}'
    },
    {
      id: 'spoof',
      displayName: '诱骗控制',
      oneWay: false,
      timeoutMs: 20000,
      bindingType: DeviceProfileRpcBindingType.MQTT_CUSTOM,
      mqttRequestTopic: 'api/jammer',
      mqttResponseTopic: 'api/jammer/response',
      mqttPayloadTemplate:
        '{"device_id":${params.device_id},"sector_id":${params.sector_id},"emit":{"freq":${params.freq},"power":${params.power},"mode":${params.mode}}}',
      mqttQos: 1,
      paramsTemplateJson: '{"device_id":0,"sector_id":0,"freq":["gps","bds"],"power":1,"mode":1}'
    },
    {
      id: 'preciseJammer',
      displayName: '精准反制',
      oneWay: false,
      timeoutMs: 20000,
      bindingType: DeviceProfileRpcBindingType.MQTT_CUSTOM,
      mqttRequestTopic: 'api/jammer',
      mqttResponseTopic: 'api/jammer/response',
      mqttPayloadTemplate:
        '{"device_id":${params.device_id},"target_uuid":"${params.target_uuid}","emit":{"freq":${params.freq},"duration":${params.duration}}}',
      mqttQos: 1,
      paramsTemplateJson: '{"device_id":0,"target_uuid":"","freq":["2400","5800"],"duration":20}'
    }
  ];
}

export function createDgbMqttPlatformSubscribeRequests(): MqttPullSubscribeRequest[] {
  return [
    { name: 'status_report', enabled: true, topic: 'dgb/${device.name}/status/status_report', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'status' },
    { name: 'detect_report', enabled: true, topic: 'dgb/${device.name}/status/detect_report', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'detect' },
    { name: 'oc4_report', enabled: true, topic: 'dgb/${device.name}/status/oc4_report', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'oc4' },
    { name: 'direction_report', enabled: true, topic: 'dgb/${device.name}/status/direction_report', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'direction' },
    { name: 'aoa_location', enabled: true, topic: 'dgb/${device.name}/status/aoa_location', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'aoa' },
    { name: 'channel_report', enabled: true, topic: 'dgb/${device.name}/status/channel_report', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'channel' },
    { name: 'lna_report', enabled: true, topic: 'dgb/${device.name}/status/lna_report', qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY, telemetryPayloadKey: 'lna' }
  ];
}

export function createDgbMqttPlatformRpcMethods(): DeviceProfileRpcMethod[] {
  return [
    {
      id: 'detectOpen',
      displayName: '侦测开启',
      oneWay: false,
      timeoutMs: 15000,
      bindingType: DeviceProfileRpcBindingType.MQTT_CUSTOM,
      mqttRequestTopic: 'dgb/${device.name}/request/detect_open',
      mqttResponseTopic: 'dgb/${device.name}/response/detect_open',
      mqttPayloadTemplate: '{}',
      mqttQos: 1
    },
    {
      id: 'detectClose',
      displayName: '侦测关闭',
      oneWay: false,
      timeoutMs: 15000,
      bindingType: DeviceProfileRpcBindingType.MQTT_CUSTOM,
      mqttRequestTopic: 'dgb/${device.name}/request/detect_close',
      mqttResponseTopic: 'dgb/${device.name}/response/detect_close',
      mqttPayloadTemplate: '{}',
      mqttQos: 1
    },
    {
      id: 'channelOpen',
      displayName: '干扰通道开启',
      oneWay: false,
      timeoutMs: 20000,
      bindingType: DeviceProfileRpcBindingType.MQTT_CUSTOM,
      mqttRequestTopic: 'dgb/${device.name}/request/channel_open',
      mqttResponseTopic: 'dgb/${device.name}/response/channel_set',
      mqttPayloadTemplate:
        '{"suppressChannels":${params.suppressChannels},"sustainTime":${params.sustainTime}}',
      mqttQos: 1,
      paramsTemplateJson:
        '{"suppressChannels":[{"suppressDirection":"1","channel":["2400","5800"]}],"sustainTime":10}'
    },
    {
      id: 'channelClose',
      displayName: '干扰通道关闭',
      oneWay: false,
      timeoutMs: 15000,
      bindingType: DeviceProfileRpcBindingType.MQTT_CUSTOM,
      mqttRequestTopic: 'dgb/${device.name}/request/channel_close',
      mqttResponseTopic: 'dgb/${device.name}/response/channel_close',
      mqttPayloadTemplate: '{}',
      mqttQos: 1
    }
  ];
}

export function normalizeDeviceTransportConfigurationForSave(
  transportType: DeviceTransportType | TransportType | null | undefined,
  configuration: DeviceTransportConfiguration | Record<string, unknown> | null | undefined,
  pullProfileContext?: { httpPullActive?: boolean; mqttPullActive?: boolean }
): DeviceTransportConfiguration | null | undefined {
  if (!configuration) {
    return configuration as null | undefined;
  }
  const effectiveType = (configuration?.type ?? transportType) as DeviceTransportType;
  const uiType = toUiTransportType(effectiveType);
  if (uiType === BasicTransportType.HTTP || pullProfileContext?.httpPullActive) {
    return normalizeHttpDeviceTransportConfigurationForSave(
      transportType,
      configuration,
      pullProfileContext?.httpPullActive
    );
  }
  if (uiType === BasicTransportType.MQTT || pullProfileContext?.mqttPullActive) {
    return normalizeMqttDeviceTransportConfigurationForSave(
      transportType,
      configuration,
      pullProfileContext?.mqttPullActive
    );
  }
  return { ...configuration, type: effectiveType } as DeviceTransportConfiguration;
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
  NONE = 'NONE',
  LINE = 'LINE',
  LENGTH_PREFIX_4 = 'LENGTH_PREFIX_4',
  LENGTH_PREFIX_2 = 'LENGTH_PREFIX_2',
  FIXED_LENGTH = 'FIXED_LENGTH'
}
export enum TcpWireAuthenticationMode {
  TOKEN = 'TOKEN',
  NONE = 'NONE',
  DEFERRED_PAYLOAD_TOKEN = 'DEFERRED_PAYLOAD_TOKEN',
  DEFERRED_PAYLOAD_DEVICE_ID = 'DEFERRED_PAYLOAD_DEVICE_ID'
}
export enum TcpJsonWithoutMethodMode {
  TELEMETRY_FLAT = 'TELEMETRY_FLAT',
  OPAQUE_FOR_RULE_ENGINE = 'OPAQUE_FOR_RULE_ENGINE'
}
export enum TransportTcpDataType {
  /** 界面展示为 UTF-8；链路上为 UTF-8 文本行并按 JSON 解析 */
  UTF8 = 'UTF8',
  /** 界面展示为「原始字节」；链路上为原始字节帧，再包成 {\"hex\":\"...\"} 供解析 */
  RAW_BYTES = 'RAW_BYTES',
  /**
   * 与 RAW_BYTES 链路上一致；配置为「协议模板」（帧模板 + 上行/下行命令），后端展开为原始字节解析。
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

export function normalizeTransportUdpDataType(
  v: string | TransportUdpDataType | undefined | null
): TransportUdpDataType | undefined {
  if (v == null || v === '') {
    return undefined;
  }
  if (v === 'MONITORING_PROTOCOL') {
    return TransportUdpDataType.PROTOCOL_TEMPLATE;
  }
  const s = String(v);
  if ((Object.values(TransportUdpDataType) as string[]).includes(s)) {
    return s as TransportUdpDataType;
  }
  return undefined;
}

/** 协议模板命令方向（与后端 ProtocolTemplateCommandDirection 一致） */
export enum ProtocolTemplateCommandDirection {
  UPLINK = 'UPLINK',
  DOWNLINK = 'DOWNLINK',
  BOTH = 'BOTH'
}

export enum ProtocolTemplateUplinkDataDestination {
  TELEMETRY = 'TELEMETRY',
  ATTRIBUTES = 'ATTRIBUTES'
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
  /** 命令读取类型为 BYTES_AS_HEX / BYTES_AS_UTF8 时的定长线期望值（十六进制串，位数 = 帧模板命令匹配宽度×2） */
  commandMatchBytesHex?: string;
  matchValueType?: TcpHexValueType;
  /** 仅 UPLINK/BOTH 生效：命中后上行结果写入遥测或属性。 */
  uplinkDataDestination?: ProtocolTemplateUplinkDataDestination;
  /** 可选：第二匹配偏移（整帧 0 起）；类型可为整型或 BYTES_AS_HEX / BYTES_AS_UTF8（与主命令一致） */
  secondaryMatchByteOffset?: number;
  secondaryMatchValueType?: TcpHexValueType;
  secondaryMatchValue?: number;
  /** 第二匹配为 BYTES_AS_HEX / BYTES_AS_UTF8 时的定长线期望值（十六进制串） */
  secondaryMatchBytesHex?: string;
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
  /** 列表「说明」；对应库表 description，未填时列表显示 — */
  description?: string;
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
  /** BYTES_AS_HEX/UTF8：长度 = 整帧长 - byteOffset - byteLengthEndTrim（如 @JSON$ 取中间 JSON） */
  byteLengthEndTrim?: number;
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
  /** 整型或 BYTES_AS_HEX / BYTES_AS_UTF8（定长线字节与 matchBytesHex 比较） */
  matchValueType: TcpHexValueType;
  matchValue: number;
  commandMatchWidth?: number;
  matchBytesHex?: string;
  secondaryMatchByteOffset?: number;
  secondaryMatchValueType?: TcpHexValueType;
  secondaryMatchValue?: number;
  secondaryMatchBytesHex?: string;
  /** 与 ltvRepeating 至少配置其一（由后端校验） */
  fields?: TcpHexFieldDefinition[];
  /** 可选：参数字段内为 LTV/TLV 列表时使用 */
  ltvRepeating?: TcpHexLtvRepeatingConfig;
}

/** 命令匹配下拉：不允许 FLOAT/DOUBLE/BYTES（用于 LTV 长度来源等仍为整型处） */
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

/** 协议模板「命令读取类型」：整型 + 定长原始字节（与帧模板字段类型一致） */
export const TCP_HEX_PROTOCOL_TEMPLATE_COMMAND_MATCH_TYPES: TcpHexValueType[] = [
  ...TCP_HEX_MATCH_VALUE_TYPES,
  TcpHexValueType.BYTES_AS_HEX,
  TcpHexValueType.BYTES_AS_UTF8
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
  TcpHexValueType.UINT64_BE,
  TcpHexValueType.UINT64_LE,
  TcpHexValueType.INT64_BE,
  TcpHexValueType.INT64_LE,
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
  /** 仅当 transportTcpDataType 为 RAW_BYTES：按顺序匹配命令规则 */
  hexCommandProfiles?: TcpHexCommandProfile[];
  /** 未匹配任何命令时的回退字段解析 */
  hexProtocolFields?: TcpHexFieldDefinition[];
  /** 未匹配命令时，在固定字段之后解析的 LTV/TLV 重复段 */
  hexLtvRepeating?: TcpHexLtvRepeatingConfig;
  /** 手动 RAW_BYTES：可选整帧校验 */
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
  /** SERVER：档案级专用监听端口；与延迟链路上鉴权配套时设备侧勿再填 serverBindPort */
  tcpProfileServerBindPort?: number;
  /** DEFERRED：解析后 JSON 中身份字段名（TOKEN 模式为 ACCESS_TOKEN；DEVICE_ID 模式为协议设备 ID） */
  tcpDeferredWireAuthTokenJsonKey?: string;
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

/** UDP 与 TCP 共用 HEX/协议模板结构；类型别名便于表单与后端 JSON 字段名区分 */
export type UdpHexValueType = TcpHexValueType;
export type UdpHexFieldDefinition = TcpHexFieldDefinition;
export type UdpHexLtvChunkOrder = TcpHexLtvChunkOrder;
export type UdpHexUnknownTagMode = TcpHexUnknownTagMode;
export type UdpHexLtvTagMapping = TcpHexLtvTagMapping;
export type UdpHexLtvRepeatingConfig = TcpHexLtvRepeatingConfig;
export type UdpHexCommandProfile = TcpHexCommandProfile;
export type UdpHexChecksumDefinition = TcpHexChecksumDefinition;
export const UDP_HEX_MATCH_VALUE_TYPES = TCP_HEX_MATCH_VALUE_TYPES;
export const UDP_HEX_PROTOCOL_TEMPLATE_COMMAND_MATCH_TYPES = TCP_HEX_PROTOCOL_TEMPLATE_COMMAND_MATCH_TYPES;
export const UDP_HEX_FRAME_FIELD_VALUE_TYPES = TCP_HEX_FRAME_FIELD_VALUE_TYPES;
export const UDP_HEX_LTV_TAG_VALUE_OPTIONS = TCP_HEX_LTV_TAG_VALUE_OPTIONS;
export const UDP_HEX_LTV_TAG_VALUE_TYPES = TCP_HEX_LTV_TAG_VALUE_TYPES;
export const isUdpHexVariableByteSlice = isTcpHexVariableByteSlice;
export const migrateLegacyLtvTagValueTypeUdp = migrateLegacyLtvTagValueType;

/** @deprecated 历史 JSON；UDP 仅设备向平台监听端口发数据报，无 CLIENT/SERVER 建连 */
export enum UdpTransportConnectMode {
  SERVER = 'SERVER',
  CLIENT = 'CLIENT'
}

export enum UdpTransportFramingMode {
  NONE = 'NONE',
  LINE = 'LINE',
  LENGTH_PREFIX_4 = 'LENGTH_PREFIX_4',
  LENGTH_PREFIX_2 = 'LENGTH_PREFIX_2',
  FIXED_LENGTH = 'FIXED_LENGTH'
}

export enum UdpWireAuthenticationMode {
  NONE = 'NONE',
  TOKEN = 'TOKEN',
  DEFERRED_PAYLOAD_TOKEN = 'DEFERRED_PAYLOAD_TOKEN',
  DEFERRED_PAYLOAD_DEVICE_ID = 'DEFERRED_PAYLOAD_DEVICE_ID'
}

export enum UdpJsonWithoutMethodMode {
  TELEMETRY_FLAT = 'TELEMETRY_FLAT',
  ATTRIBUTES_FLAT = 'ATTRIBUTES_FLAT'
}

export enum TransportUdpDataType {
  UTF8 = 'UTF8',
  ASCII = 'ASCII',
  RAW_BYTES = 'RAW_BYTES',
  PROTOCOL_TEMPLATE = 'PROTOCOL_TEMPLATE'
}

export interface TransportUdpDataTypeConfiguration {
  transportUdpDataType?: TransportUdpDataType;
  hexCommandProfiles?: UdpHexCommandProfile[];
  hexProtocolFields?: UdpHexFieldDefinition[];
  hexLtvRepeating?: UdpHexLtvRepeatingConfig;
  checksum?: UdpHexChecksumDefinition;
  protocolTemplates?: ProtocolTemplateDefinition[];
  protocolCommands?: ProtocolTemplateCommandDefinition[];
  protocolTemplateBundleId?: string;
  /** 历史 JSON 字段名（仅读取兼容） */
  monitoringTemplates?: ProtocolTemplateDefinition[];
  monitoringCommands?: ProtocolTemplateCommandDefinition[];
  monitoringProtocolBundleId?: string;
}

export interface UdpDeviceProfileTransportConfiguration {
  type?: DeviceTransportType;
  /** @deprecated 历史字段，保存时固定为 NONE */
  udpTransportConnectMode?: UdpTransportConnectMode;
  /** @deprecated 历史字段，保存时固定为 NONE */
  udpTransportFramingMode?: UdpTransportFramingMode;
  udpFixedFrameLength?: number;
  udpWireAuthenticationMode?: UdpWireAuthenticationMode;
  udpProfileServerBindPort?: number;
  udpDeferredWireAuthTokenJsonKey?: string;
  udpOutboundReconnectIntervalSec?: number;
  udpOutboundReconnectMaxAttempts?: number;
  udpReadIdleTimeoutSec?: number;
  udpJsonWithoutMethodMode?: UdpJsonWithoutMethodMode;
  udpOpaqueRuleEngineKey?: string;
  transportUdpDataTypeConfiguration?: TransportUdpDataTypeConfiguration;
}

export interface UdpDeviceTransportConfiguration {
  type?: DeviceTransportType;
  /** @deprecated 历史 CLIENT 模式字段 */
  host?: string;
  /** @deprecated 历史 CLIENT 模式字段 */
  port?: number;
  sourceHost?: string;
  serverBindPort?: number;
  udpWireAuthPayloadDeviceId?: string;
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
                                                   HttpPullDeviceProfileTransportConfiguration &
                                                   TcpDeviceProfileTransportConfiguration &
                                                   UdpDeviceProfileTransportConfiguration;

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

export function createDisabledDeviceProvisionConfiguration(): DeviceProvisionConfiguration {
  return { type: DeviceProvisionType.DISABLED };
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

export const createDeviceProfileTransportConfiguration = (type: TransportType): DeviceProfileTransportConfiguration => {
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
      case BasicTransportType.HTTP:
      case DeviceTransportType.HTTP_PULL:
        const httpPullTransportConfiguration: HttpPullDeviceProfileTransportConfiguration = {
          timeoutMs: 10000,
          readTimeoutMs: 10000,
          queryingFrequencyMs: 30000,
          pollRequests: [{
            name: 'poll-1',
            enabled: true,
            pollUrl: 'https://api.example.com/data',
            pollMethod: 'GET',
            dataType: HttpPullPollDataType.TELEMETRY,
            requiresAuth: true,
            routing: {
              routingMode: HttpPullRoutingMode.MULTI_DEVICE,
              deviceIdJsonPath: 'deviceId',
              telemetryPayloadKey: 'httpPullPayload'
            }
          }],
          auth: { authType: HttpPullAuthType.NONE }
        };
        transportConfiguration = {...httpPullTransportConfiguration, type: DeviceTransportType.HTTP_PULL};
        break;
      case BasicTransportType.MQTT:
      case DeviceTransportType.MQTT_PULL:
        const mqttPullTransportConfiguration: MqttPullDeviceProfileTransportConfiguration = {
          connectTimeoutMs: 10000,
          keepAliveSec: 60,
          cleanSession: true,
          reconnectIntervalMs: 5000,
          subscribeRequests: createUavMqttPlatformSubscribeRequests()
        };
        transportConfiguration = {...mqttPullTransportConfiguration, type: DeviceTransportType.MQTT_PULL} as unknown as DeviceProfileTransportConfiguration;
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
            transportTcpDataType: TransportTcpDataType.UTF8
          }
        };
        transportConfiguration = {...tcpTransportConfiguration, type: DeviceTransportType.TCP};
        break;
      case DeviceTransportType.UDP:
        const udpTransportConfiguration: UdpDeviceProfileTransportConfiguration = {
          udpTransportFramingMode: UdpTransportFramingMode.NONE,
          udpFixedFrameLength: null,
          udpWireAuthenticationMode: UdpWireAuthenticationMode.TOKEN,
          udpJsonWithoutMethodMode: UdpJsonWithoutMethodMode.TELEMETRY_FLAT,
          udpOpaqueRuleEngineKey: 'udpOpaquePayload',
          transportUdpDataTypeConfiguration: {
            transportUdpDataType: TransportUdpDataType.UTF8
          }
        };
        transportConfiguration = {...udpTransportConfiguration, type: DeviceTransportType.UDP};
        break;
    }
  }
  return transportConfiguration;
};

export const createDeviceTransportConfiguration = (type: TransportType, deviceProfile?: DeviceProfile | null): DeviceTransportConfiguration => {
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
      case BasicTransportType.HTTP:
      case DeviceTransportType.HTTP_PULL:
        const httpPullDeviceTransportConfiguration: HttpPullDeviceTransportConfiguration = {
          collector: true,
          externalDeviceId: null
        };
        transportConfiguration = {...httpPullDeviceTransportConfiguration, type: DeviceTransportType.HTTP_PULL};
        break;
      case BasicTransportType.MQTT:
      case DeviceTransportType.MQTT_PULL:
        const mqttPullDeviceTransportConfiguration: MqttPullDeviceTransportConfiguration = {
          brokerUrl: '',
          clientId: '',
          topicPrefix: defaultMqttPullDeviceTopicPrefix(deviceProfile),
          auth: { authType: MqttPullAuthType.NONE }
        };
        transportConfiguration = {...mqttPullDeviceTransportConfiguration, type: DeviceTransportType.MQTT_PULL};
        break;
      case DeviceTransportType.TCP:
        const tcpDeviceTransportConfiguration: TcpDeviceTransportConfiguration = {
          host: '127.0.0.1',
          port: 5025
        };
        transportConfiguration = {...tcpDeviceTransportConfiguration, type: DeviceTransportType.TCP};
        break;
      case DeviceTransportType.UDP:
        const udpDeviceTransportConfiguration: UdpDeviceTransportConfiguration = {
          sourceHost: '127.0.0.1'
        };
        transportConfiguration = {...udpDeviceTransportConfiguration, type: DeviceTransportType.UDP};
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


export enum DeviceProfileRpcBindingType {
  /** TCP/UDP 共用同一协议模板包与下行组帧（params.hex） */
  TCP_TEMPLATE = 'TCP_TEMPLATE',
  /** 与 {@link TCP_TEMPLATE} 等价，保存时统一为 TCP_TEMPLATE */
  UDP_TEMPLATE = 'UDP_TEMPLATE',
  NATIVE = 'NATIVE',
  /** HTTP Pull：平台主动调用厂家 HTTP 接口（与拉取共用鉴权） */
  HTTP_OUTBOUND = 'HTTP_OUTBOUND',
  /** MQTT 自定义数据格式：自定义请求/响应主题与 payload 模板 */
  MQTT_CUSTOM = 'MQTT_CUSTOM'
}

/** 支持协议模板（原始字节 / PROTOCOL_TEMPLATE）的传输类型 */
export function isProtocolTemplateWireTransport(type: DeviceTransportType | null | undefined): boolean {
  return type === DeviceTransportType.TCP || type === DeviceTransportType.UDP;
}

/** TCP/UDP 档案 RPC：协议模板绑定（共用 TCP_TEMPLATE；UDP_TEMPLATE 与之等价） */
export function isProtocolTemplateRpcBinding(bindingType: DeviceProfileRpcBindingType | null | undefined): boolean {
  return bindingType === DeviceProfileRpcBindingType.TCP_TEMPLATE
    || bindingType === DeviceProfileRpcBindingType.UDP_TEMPLATE;
}

export function isHttpPullProfileTransport(
  type: DeviceTransportType | string | null | undefined,
  transportConfiguration?: { type?: DeviceTransportType; httpTransportMode?: HttpTransportMode } | null
): boolean {
  return resolveHttpProfileTransportTypeForDisplay(type, transportConfiguration) === DeviceTransportType.HTTP_PULL;
}

export function isHttpOutboundRpcBinding(bindingType: DeviceProfileRpcBindingType | null | undefined): boolean {
  return bindingType === DeviceProfileRpcBindingType.HTTP_OUTBOUND;
}

export function isMqttCustomRpcBinding(bindingType: DeviceProfileRpcBindingType | null | undefined): boolean {
  return bindingType === DeviceProfileRpcBindingType.MQTT_CUSTOM;
}

export function isMqttProfileRpcTransport(
  type: DeviceTransportType | string | null | undefined,
  transportConfiguration?: { type?: DeviceTransportType; mqttTransportMode?: MqttTransportMode } | null
): boolean {
  const resolved = resolveMqttProfileTransportTypeForDisplay(type, transportConfiguration);
  return resolved === DeviceTransportType.MQTT || resolved === DeviceTransportType.MQTT_PULL;
}

export function isMqttPullProfileTransport(
  type: DeviceTransportType | string | null | undefined,
  transportConfiguration?: { type?: DeviceTransportType; mqttTransportMode?: MqttTransportMode } | null
): boolean {
  return resolveMqttProfileTransportTypeForDisplay(type, transportConfiguration) === DeviceTransportType.MQTT_PULL;
}

export function wireProfileProtocolTemplateBundleId(
  cfg: TcpDeviceProfileTransportConfiguration | UdpDeviceProfileTransportConfiguration | null | undefined
): string | null {
  if (!cfg) {
    return null;
  }
  const tcpCfg = cfg as TcpDeviceProfileTransportConfiguration;
  const udpCfg = cfg as UdpDeviceProfileTransportConfiguration;
  const tcpData = tcpCfg.transportTcpDataTypeConfiguration;
  const udpData = udpCfg.transportUdpDataTypeConfiguration;
  const id = tcpData?.protocolTemplateBundleId ?? tcpData?.monitoringProtocolBundleId
    ?? udpData?.protocolTemplateBundleId ?? udpData?.monitoringProtocolBundleId;
  return id?.trim() || null;
}

export function wireProfileTransportPayloadType(
  cfg: TcpDeviceProfileTransportConfiguration | UdpDeviceProfileTransportConfiguration | null | undefined
): TransportTcpDataType | TransportUdpDataType | undefined {
  if (!cfg) {
    return undefined;
  }
  const tcpCfg = cfg as TcpDeviceProfileTransportConfiguration;
  const udpCfg = cfg as UdpDeviceProfileTransportConfiguration;
  return tcpCfg.transportTcpDataTypeConfiguration?.transportTcpDataType
    ?? udpCfg.transportUdpDataTypeConfiguration?.transportUdpDataType;
}

/** 设备档案：平台 RPC 方法目录（对外统一 id，按绑定映射到 TCP 组帧或原生 RPC） */
export interface DeviceProfileRpcMethod {
  id: string;
  displayName?: string;
  oneWay?: boolean;
  timeoutMs?: number;
  bindingType: DeviceProfileRpcBindingType;
  /** TCP_TEMPLATE */
  templateCommandName?: string;
  commandValue?: number;
  templateId?: string;
  paramMap?: Record<string, string>;
  /** NATIVE */
  deviceMethod?: string;
  paramsTemplateJson?: string;
  /** HTTP_OUTBOUND */
  httpUrl?: string;
  httpMethod?: string;
  httpBody?: string;
  httpHeaders?: Record<string, string>;
  requiresAuth?: boolean;
  /** MQTT NATIVE 可选主题 / MQTT_CUSTOM 必填 */
  mqttRequestTopic?: string;
  mqttResponseTopic?: string;
  mqttPayloadTemplate?: string;
  mqttQos?: number;
}

export interface DeviceProfileData {
  configuration: DeviceProfileConfiguration;
  transportConfiguration: DeviceProfileTransportConfiguration;
  alarms?: Array<DeviceProfileAlarm>;
  provisionConfiguration?: DeviceProvisionConfiguration;
  rpcMethods?: DeviceProfileRpcMethod[];
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
  gateway?: boolean;
  externalDeviceId?: string;
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
  /** DEFERRED_PAYLOAD_DEVICE_ID：与负载 JSON 中档案配置的字段值一致，用于同端口多设备区分（可与其它端口使用相同字符串） */
  tcpWireAuthPayloadDeviceId?: string;
}

export type DeviceTransportConfigurations = DefaultDeviceTransportConfiguration &
  MqttDeviceTransportConfiguration &
  CoapDeviceTransportConfiguration &
  Lwm2mDeviceTransportConfiguration &
  SnmpDeviceTransportConfiguration &
  HttpPullDeviceTransportConfiguration &
  MqttPullDeviceTransportConfiguration &
  TcpDeviceTransportConfiguration &
  UdpDeviceTransportConfiguration;

export interface DeviceTransportConfiguration extends DeviceTransportConfigurations {
  type: DeviceTransportType;
}

export interface DeviceData {
  configuration: DeviceConfiguration;
  transportConfiguration: DeviceTransportConfiguration;
  /** @deprecated 旧版全局固定参数；请用 rpcParamDefaultsByMethod */
  rpcParamDefaults?: Record<string, unknown>;
  /** 按 RPC 方法 id 分组的固定参数 */
  rpcParamDefaultsByMethod?: Record<string, Record<string, unknown>>;
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
    [DeviceTransportType.HTTP_PULL, [DeviceCredentialsType.ACCESS_TOKEN]],
    [DeviceTransportType.TCP, [DeviceCredentialsType.ACCESS_TOKEN]],
    [DeviceTransportType.UDP, [DeviceCredentialsType.ACCESS_TOKEN]]
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
