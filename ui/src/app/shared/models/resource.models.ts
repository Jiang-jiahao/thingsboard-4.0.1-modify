import { BaseData, ExportableEntity, HasId } from '@shared/models/base-data';
import { TenantId } from '@shared/models/id/tenant-id';
import { JnksIotResourceId } from '@shared/models/id/jnks-iot-resource-id';
import { NULL_UUID } from '@shared/models/id/has-uuid';
import { HasTenantId } from '@shared/models/entity.models';

export enum ResourceType {
  LWM2M_MODEL = 'LWM2M_MODEL',
  PKCS_12 = 'PKCS_12',
  JKS = 'JKS',
  JS_MODULE = 'JS_MODULE'
}

export enum ResourceSubType {
  IMAGE = 'IMAGE',
  SCADA_SYMBOL = 'SCADA_SYMBOL',
  EXTENSION = 'EXTENSION',
  MODULE = 'MODULE'
}

export const ResourceTypeMIMETypes = new Map<ResourceType, string>(
  [
    [ResourceType.LWM2M_MODEL, 'application/xml,text/xml'],
    [ResourceType.PKCS_12, 'application/x-pkcs12'],
    [ResourceType.JKS, 'application/x-java-keystore'],
    [ResourceType.JS_MODULE, 'text/javascript,application/javascript']
  ]
);

export const ResourceTypeExtension = new Map<ResourceType, string>(
  [
    [ResourceType.LWM2M_MODEL, 'xml'],
    [ResourceType.PKCS_12, 'p12,pfx'],
    [ResourceType.JKS, 'jks'],
    [ResourceType.JS_MODULE, 'js']
  ]
);

export const ResourceTypeTranslationMap = new Map<ResourceType, string>(
  [
    [ResourceType.LWM2M_MODEL, 'resource.type.lwm2m-model'],
    [ResourceType.PKCS_12, 'resource.type.pkcs-12'],
    [ResourceType.JKS, 'resource.type.jks'],
    [ResourceType.JS_MODULE, 'resource.type.js-module']
  ]
);

export const ResourceSubTypeTranslationMap = new Map<ResourceSubType, string>(
  [
    [ResourceSubType.IMAGE, 'resource.sub-type.image'],
    [ResourceSubType.SCADA_SYMBOL, 'resource.sub-type.scada-symbol'],
    [ResourceSubType.EXTENSION, 'resource.sub-type.extension'],
    [ResourceSubType.MODULE, 'resource.sub-type.module']
  ]
);

export interface JnksIotResourceInfo<D> extends Omit<BaseData<JnksIotResourceId>, 'name' | 'label'>, HasTenantId, ExportableEntity<JnksIotResourceId> {
  tenantId?: TenantId;
  resourceKey?: string;
  title?: string;
  resourceType: ResourceType;
  resourceSubType?: ResourceSubType;
  fileName: string;
  public: boolean;
  publicResourceKey?: string;
  readonly link?: string;
  readonly publicLink?: string;
  descriptor?: D;
}

export type ResourceInfo = JnksIotResourceInfo<any>;

export interface Resource extends ResourceInfo {
  data: string;
  name?: string;
}

export interface ImageDescriptor {
  mediaType: string;
  width: number;
  height: number;
  size: number;
  etag: string;
  previewDescriptor: ImageDescriptor;
}

export type ImageResourceInfo = JnksIotResourceInfo<ImageDescriptor>;

export interface ImageResource extends ImageResourceInfo {
  base64?: string;
}

export interface ImageExportData {
  mediaType: string;
  fileName: string;
  title: string;
  subType: string;
  resourceKey: string;
  public: boolean;
  publicResourceKey: string;
  data: string;
}

export type ImageResourceType = 'tenant' | 'system';
export type TBResourceScope = 'tenant' | 'system';

export type ResourceReferences = {[entityType: string]: Array<BaseData<HasId> & HasTenantId>};

export interface ResourceInfoWithReferences extends ResourceInfo {
  references: ResourceReferences;
}

export interface ResourceDeleteResult {
  resource: JnksIotResourceInfo<any>;
  success: boolean;
  resourceIsReferencedError?: boolean;
  error?: any;
  references?: ResourceReferences;
}

export const toResourceDeleteResult = (resource: ResourceInfo, e?: any): ResourceDeleteResult => {
  if (!e) {
    return {resource, success: true};
  } else {
    const result: ResourceDeleteResult = {resource, success: false, error: e};
    if (e?.status === 400 && e?.error?.success === false && e?.error?.references) {
      const references: ResourceReferences = e?.error?.references;
      result.resourceIsReferencedError = true;
      result.references = references;
    }
    return result;
  }
};

export const imageResourceType = (imageInfo: ImageResourceInfo): ImageResourceType =>
  (!imageInfo.tenantId || imageInfo.tenantId?.id === NULL_UUID) ? 'system' : 'tenant';

export const JNKS_IOT_IMAGE_PREFIX = 'jnks-iot-image;';
export const JNKS_IOT_RESOURCE_PREFIX = 'jnks-iot-resource;';

export const IMAGES_URL_REGEXP = /\/api\/images\/(tenant|system)\/(.*)/;
export const IMAGES_URL_PREFIX = '/api/images';

export const RESOURCES_URL_REGEXP = /\/api\/resource\/(js_module)\/(tenant|system)\/(.*)/;

export const PUBLIC_IMAGES_URL_PREFIX = '/api/images/public';

export const IMAGE_BASE64_URL_PREFIX = 'data:image/';

export const removeJnksIotImagePrefix = (url: string): string => url ? url.replace(JNKS_IOT_IMAGE_PREFIX, '') : url;
export const removeJnksIotResourcePrefix = (url: string): string => url ? url.replace(JNKS_IOT_RESOURCE_PREFIX, '') : url;

export const removeJnksIotImagePrefixFromUrls = (urls: string[]): string[] => urls ? urls.map(url => removeJnksIotImagePrefix(url)) : [];

export const prependJnksIotImagePrefix = (url: string): string => {
  if (url && !url.startsWith(JNKS_IOT_IMAGE_PREFIX)) {
    url = JNKS_IOT_IMAGE_PREFIX + url;
  }
  return url;
};

export const prependJnksIotImagePrefixToUrls = (urls: string[]): string[] => urls ? urls.map(url => prependJnksIotImagePrefix(url)) : [];

export const prependJnksIotResourcePrefix = (url: string): string => {
  if (url && !url.startsWith(JNKS_IOT_RESOURCE_PREFIX)) {
    url = JNKS_IOT_RESOURCE_PREFIX + url;
  }
  return url;
};

export const isImageResourceUrl = (url: string): boolean => url && IMAGES_URL_REGEXP.test(url);

export const isJSResourceUrl = (url: string): boolean => url && RESOURCES_URL_REGEXP.test(url);
export const isJSResource = (url: string): boolean => url?.startsWith(JNKS_IOT_RESOURCE_PREFIX);

export const extractParamsFromImageResourceUrl = (url: string): {type: ImageResourceType; key: string} => {
  const res = url.match(IMAGES_URL_REGEXP);
  if (res?.length > 2) {
    return {type: res[1] as ImageResourceType, key: res[2]};
  } else {
    return null;
  }
};

export const extractParamsFromJSResourceUrl = (url: string): {type: ResourceType; scope: TBResourceScope; key: string} => {
  const res = url.match(RESOURCES_URL_REGEXP);
  if (res?.length > 3) {
    return {type: (res[1]).toUpperCase() as ResourceType, scope: res[2] as TBResourceScope, key: res[3]};
  } else {
    return null;
  }
};

export const isBase64DataImageUrl = (url: string): boolean => url && url.startsWith(IMAGE_BASE64_URL_PREFIX);

export const NO_IMAGE_DATA_URI = 'data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==';
