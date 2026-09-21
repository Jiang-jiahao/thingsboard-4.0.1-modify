import { FunctionArg, FunctionArgType, JnksIotEditorCompletions } from '@shared/models/ace/completion.models';

export const entityIdHref = '<code>EntityId</code>';

export const baseDataHref = '<code>Base data</code>';

export const alarmDataHref = '<code>Alarm data</code>';

export const alarmDataQueryHref = '<code>Alarm data query</code>';

export const attributeScopeHref = '<code>Attribute scope</code>';

export const entityTypeHref = '<code>EntityType</code>';

export const pageDataHref = '<code>PageData</code>';

export const deviceInfoHref = '<code>DeviceInfo</code>';

export const assetInfoHref = '<code>AssetInfo</code>';

export const entityViewInfoHref = '<code>EntityViewInfo</code>';

export const entityRelationsQueryHref = '<code>EntityRelationsQuery</code>';

export const entityRelationInfoHref = '<code>EntityRelationInfo</code>';

export const dashboardInfoHref = '<code>DashboardInfo</code>';

export const deviceHref = '<code>Device</code>';

export const assetHref = '<code>Asset</code>';

export const entityViewHref = '<code>entityView</code>';

export const entityRelationHref = '<code>Entity relation</code>';

export const dashboardHref = '<code>Dashboard</code>';

export const customerHref = '<code>Customer</code>';

export const attributeDataHref = '<code>Attribute Data</code>';

export const timeseriesDataHref = '<code>Timeseries Data</code>';

export const aggregationTypeHref = '<code>Aggregation Type</code>';

export const dataSortOrderHref = '<code>Data Sort Order</code>';

export const userHref = '<code>User</code>';

export const entityDataHref = '<code>Entity data</code>';

export const entityDataQueryHref = '<code>Entity Data Query</code>';

export const deviceCredentialsHref = '<code>DeviceCredentials</code>';

export const entityFilterHref = '<code>Entity filter</code>';

export const entityInfoHref = '<code>Entity info</code>';

export const aliasEntityTypeHref = '<code>Alias Entity Type</code>';

export const aliasFilterTypeHref = '<code>Alias filter type</code>';

export const entityAliasHref = '<code>Entity alias</code>';

export const dataKeyTypeHref = '<code>Data key type</code>';

export const subscriptionInfoHref = '<code>Subscription info</code>';

export const dataSourceHref = '<code>Datasource</code>';

export const stateParamsHref = '<code>State params</code>';

export const aliasInfoHref = '<code>Alias info</code>';

export const entityAliasFilterHref = '<code>Entity alias filter</code>';

export const entityAliasFilterResultHref = '<code>Entity alias filter result</code>';

export const importEntityDataHref = '<code>Import entity data</code>';

export const importEntitiesResultInfoHref = '<code>Import entities result info</code>';

export const customDialogComponentHref = '<code>CustomDialogComponent</code>';

export const resourceInfoHref = '<code>Resource info</code>';

export const bulkImportResultHref = '<code>Bulk import result</code>';

export const bulkImportRequestHref = '<code>Bulk import request</code>';

export const pageLinkArg: FunctionArg = {
  name: 'pageLink',
  type: '<code>PageLink</code>',
  description: 'Page link object used to perform paginated request.'
};

export const requestConfigArg: FunctionArg = {
  name: 'config',
  type: '<code>RequestConfig</code>',
  description: 'HTTP request configuration.',
  optional: true
};

export function observableReturnType(objectType: string): FunctionArgType {
  return {
    type: `Observable&lt;${objectType}&gt;`,
    description: `An <code>Observable</code> of <code>${objectType}</code> object.`
  };
}

export function observableReturnTypeVariable(variableType: string): FunctionArgType {
  return {
    type: `Observable&lt;${variableType}&gt;`,
    description: `An <code>Observable</code> of <code>${variableType}</code> variable.`
  };
}

export function observableVoid(): FunctionArgType {
  return {
    type: `Observable&lt;void&gt;`,
    description: `An <code>Observable</code>.`
  };
}

export function observableArrayReturnType(objectType: string): FunctionArgType {
  return {
    type: `Observable&lt;Array&lt;${objectType}&gt;&gt;`,
    description: `An <code>Observable</code> of array of <code>${objectType}</code> objects.`
  };
}

export function observableBaseDataReturnType(): FunctionArgType {
  return {
    type: `Observable&lt;${baseDataHref}&lt;${entityIdHref}&gt;&gt;`,
    description: `An <code>Observable</code> of <code>${baseDataHref}</code> object.`
  };
}

export function observableArrayBaseDataReturnType(): FunctionArgType {
  return {
    type: `Observable&lt;Array&lt;${baseDataHref}&lt;${entityIdHref}&gt;&gt;&gt;`,
    description: `An <code>Observable</code> of array of <code>${baseDataHref}</code> objects.`
  };
}

export function observablePageDataReturnType(objectType: string): FunctionArgType {
  return {
    type: `Observable&lt;${pageDataHref}&lt;${objectType}&gt;&gt;`,
    description: `An <code>Observable</code> of page result as a <code>${pageDataHref}</code> holding array of <code>${objectType}</code> objects.`
  };
}

export const serviceCompletions: JnksIotEditorCompletions = {
  deviceService: {
    description: 'Device Service API<br>' +
      'See <code>DeviceService</code> for API reference.',
    meta: 'service',
    type: '<code>DeviceService</code>',
    children: {
      getTenantDeviceInfos: {
        description: 'Get tenant devices',
        meta: 'function',
        args: [
          pageLinkArg,
          { name: 'type', type: 'string', optional: true, description: 'Device type'},
          requestConfigArg
        ],
        return: observablePageDataReturnType(deviceInfoHref)
      },
      getCustomerDeviceInfos: {
        description: 'Get customer devices',
        meta: 'function',
        args: [
          { name: 'customerId', type: 'string', description: 'Id of the customer'},
          pageLinkArg,
          { name: 'type', type: 'string', optional: true, description: 'Device type'},
          requestConfigArg
        ],
        return: observablePageDataReturnType(deviceInfoHref)
      },
      getDevice: {
        description: 'Get device by id',
        meta: 'function',
        args: [
          { name: 'deviceId', type: 'string', description: 'Id of the device'},
          requestConfigArg
        ],
        return: observableReturnType(deviceHref)
      },
      getDevices: {
        description: 'Get devices by ids',
        meta: 'function',
        args: [
          { name: 'deviceIds', type: `Array&lt;string&gt;`, description: 'List of device ids'},
          requestConfigArg
        ],
        return: observableArrayReturnType(deviceHref)
      },
      getDeviceInfo: {
        description: 'Get device info by id',
        meta: 'function',
        args: [
          { name: 'deviceId', type: 'string', description: 'Id of the device'},
          requestConfigArg
        ],
        return: observableReturnType(deviceInfoHref)
      },
      saveDevice: {
        description: 'Save device',
        meta: 'function',
        args: [
          { name: 'device', type: deviceHref, description: 'Device object to save'},
          requestConfigArg
        ],
        return: observableReturnType(deviceHref)
      },
      deleteDevice: {
        description: 'Delete device by id',
        meta: 'function',
        args: [
          { name: 'deviceId', type: 'string', description: 'Id of the device'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      getDeviceTypes: {
        description: 'Get all available devices types',
        meta: 'function',
        args: [
          requestConfigArg
        ],
        return: observableArrayReturnType('<code>EntitySubtype</code>')
      },
      getDeviceCredentials: {
        description: 'Get device credentials by device id',
        meta: 'function',
        args: [
          { name: 'deviceId', type: 'string', description: 'Id of the device'},
          { name: 'sync', type: 'boolean', description: 'Whether to execute HTTP request synchronously (false by default)', optional: true},
          requestConfigArg
        ],
        return: observableReturnType(deviceCredentialsHref)
      },
      saveDeviceCredentials: {
        description: 'Save device credentials',
        meta: 'function',
        args: [
          { name: 'deviceCredentials', type: deviceCredentialsHref, description: 'Device credentials object to save'},
          requestConfigArg
        ],
        return: observableReturnType(deviceCredentialsHref)
      },
      makeDevicePublic: {
        description: 'Make device public (available from public dashboard)',
        meta: 'function',
        args: [
          { name: 'deviceId', type: 'string', description: 'Id of the device'},
          requestConfigArg
        ],
        return: observableReturnType(deviceHref)
      },
      assignDeviceToCustomer: {
        description: 'Assign device to specific customer',
        meta: 'function',
        args: [
          { name: 'customerId', type: 'string', description: 'Id of the customer'},
          { name: 'deviceId', type: 'string', description: 'Id of the device'},
          requestConfigArg
        ],
        return: observableReturnType(deviceHref)
      },
      unassignDeviceFromCustomer: {
        description: 'Unassign device from any customer',
        meta: 'function',
        args: [
          { name: 'deviceId', type: 'string', description: 'Id of the device'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      sendOneWayRpcCommand: {
        description: 'Send one way (without response) RPC command to the device.',
        meta: 'function',
        args: [
          { name: 'deviceId', type: 'string', description: 'Id of the device'},
          { name: 'requestBody', type: 'object', description: 'Request body to be sent to device'},
          requestConfigArg
        ],
        return: {
          type: `Observable&lt;any&gt;`,
          description: `A command execution <code>Observable</code>.`
        }
      },
      sendTwoWayRpcCommand: {
        description: 'Sends two way (with response) RPC command to the device.',
        meta: 'function',
        args: [
          { name: 'deviceId', type: 'string', description: 'Id of the device'},
          { name: 'requestBody', type: 'object', description: 'Request body to be sent to device'},
          requestConfigArg
        ],
        return: {
          type: `Observable&lt;any&gt;`,
          description: `A command execution <code>Observable</code> of response body.`
        }
      },
      findByQuery: {
        description: 'Find devices by search query',
        meta: 'function',
        args: [
          { name: 'query', type: '<code>DeviceSearchQuery</code>',
            description: 'Device search query object'},
          requestConfigArg
        ],
        return: observableArrayReturnType(deviceHref)
      },
      findByName: {
        description: 'Find device by name',
        meta: 'function',
        args: [
          { name: 'deviceName', type: 'string',
            description: 'Search device name'},
          requestConfigArg
        ],
        return: observableReturnType(deviceHref)
      },
      claimDevice: {
        description: 'Send claim device request',
        meta: 'function',
        args: [
          { name: 'deviceName', type: 'string',
            description: 'Claiming device name'},
          requestConfigArg
        ],
        return: observableReturnType('<code>ClaimResult</code>')
      },
      unclaimDevice: {
        description: 'Send un-claim device request',
        meta: 'function',
        args: [
          { name: 'deviceName', type: 'string',
            description: 'Device name to un-claim'},
          requestConfigArg
        ],
        return: observableVoid()
      }
    }
  },
  assetService: {
    description: 'Asset Service API<br>' +
      'See <code>AssetService</code> for API reference.',
    meta: 'service',
    type: '<code>AssetService</code>',
    children: {
      getTenantAssetInfos: {
        description: 'Get tenant assets',
        meta: 'function',
        args: [
          pageLinkArg,
          {name: 'type', type: 'string', optional: true, description: 'Asset type'},
          requestConfigArg
        ],
        return: observablePageDataReturnType(assetInfoHref)
      },
      getCustomerAssetInfos: {
        description: 'Get customer assets',
        meta: 'function',
        args: [
          {name: 'customerId', type: 'string', description: 'Id of the customer'},
          pageLinkArg,
          {name: 'type', type: 'string', optional: true, description: 'Asset type'},
          requestConfigArg
        ],
        return: observablePageDataReturnType(assetInfoHref)
      },
      getTenantAssetInfosByAssetProfileId: {
        description: 'Get tenant asset infos by asset profile ID',
        meta: 'function',
        args: [
          pageLinkArg,
          { name: 'assetProfileId', type: 'string', optional: true, description: 'ID of the asset profile' },
          requestConfigArg
        ],
        return: observablePageDataReturnType(assetInfoHref)
      },
      getCustomerAssetInfosByAssetProfileId: {
        description: 'Get customer asset infos by asset profile ID',
        meta: 'function',
        args: [
          { name: 'customerId', type: 'string', description: 'ID of the customer' },
          pageLinkArg,
          { name: 'assetProfileId', type: 'string', optional: true, description: 'ID of the asset profile' },
          requestConfigArg
        ],
        return: observablePageDataReturnType(assetInfoHref)
      },
      bulkImportAssets: {
        description: 'Bulk import assets with provided entities data',
        meta: 'function',
        args: [
          { name: 'entitiesData', type: bulkImportRequestHref, description: 'Data for bulk importing assets' },
          requestConfigArg
        ],
        return: observableReturnType(bulkImportResultHref)
      },
      getAsset: {
        description: 'Get asset by id',
        meta: 'function',
        args: [
          {name: 'assetId', type: 'string', description: 'Id of the asset'},
          requestConfigArg
        ],
        return: observableReturnType(assetHref)
      },
      getAssets: {
        description: 'Get assets by ids',
        meta: 'function',
        args: [
          {name: 'assetIds', type: `Array&lt;string&gt;`, description: 'Ids of the assets'},
          requestConfigArg
        ],
        return: observableArrayReturnType(assetHref)
      },
      getAssetInfo: {
        description: 'Get asset info by id',
        meta: 'function',
        args: [
          {name: 'assetId', type: 'string', description: 'Id of the assets'},
          requestConfigArg
        ],
        return: observableReturnType(assetInfoHref)
      },
      saveAsset: {
        description: 'Save asset',
        meta: 'function',
        args: [
          {name: 'asset', type: assetHref, description: 'Asset object to save'},
          requestConfigArg
        ],
        return: observableReturnType(assetHref)
      },
      deleteAsset: {
        description: 'Delete asset by id',
        meta: 'function',
        args: [
          {name: 'assetId', type: 'string', description: 'Id of the asset'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      getAssetTypes: {
        description: 'Get all available assets types',
        meta: 'function',
        args: [
          requestConfigArg
        ],
        return: observableArrayReturnType('<code>EntitySubtype</code>')
      },
      makeAssetPublic: {
        description: 'Make asset public (available from public dashboard)',
        meta: 'function',
        args: [
          {name: 'assetId', type: 'string', description: 'Id of the asset'},
          requestConfigArg
        ],
        return: observableReturnType(assetHref)
      },
      assignAssetToCustomer: {
        description: 'Assign asset to specific customer',
        meta: 'function',
        args: [
          {name: 'customerId', type: 'string', description: 'Id of the customer'},
          {name: 'assetId', type: 'string', description: 'Id of the asset'},
          requestConfigArg
        ],
        return: observableReturnType(assetHref)
      },
      unassignAssetFromCustomer: {
        description: 'Unassign asset from any customer',
        meta: 'function',
        args: [
          {name: 'assetId', type: 'string', description: 'Id of the asset'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      findByQuery: {
        description: 'Find assets by search query',
        meta: 'function',
        args: [
          {
            name: 'query',
            type: '<code>AssetSearchQuery</code>',
            description: 'Asset search query object'
          },
          requestConfigArg
        ],
        return: observableArrayReturnType(assetHref)
      },
      findByName: {
        description: 'Find asset by name',
        meta: 'function',
        args: [
          {
            name: 'assetName', type: 'string',
            description: 'Search asset name'
          },
          requestConfigArg
        ],
        return: observableReturnType(assetHref)
      },
    },
  },
  entityViewService: {
    description: 'EntityView Service API<br>' +
      'See <code>EntityViewService</code> for API reference.',
    meta: 'service',
    type: '<code>EntityViewService</code>',
    children: {
      getTenantEntityViewInfos: {
        description: 'Get tenant entity view infos',
        meta: 'function',
        args: [
          pageLinkArg,
          {name: 'type', type: 'string', optional: true, description: 'Entity view type'},
          requestConfigArg
        ],
        return: observablePageDataReturnType(entityViewInfoHref)
      },
      getCustomerEntityViewInfos: {
        description: 'Get customer entities view infos by id',
        meta: 'function',
        args: [
          { name: 'customerId', type: 'string', description: 'Id of the customer'},
          pageLinkArg,
          { name: 'type', type: 'string', optional: true, description: 'Entity view type'},
          requestConfigArg
        ],
        return: observablePageDataReturnType(entityViewInfoHref)
      },
      getEntityView: {
        description: 'Get entity view by id',
        meta: 'function',
        args: [
          { name: 'entityViewId', type: 'string', description: 'Id of the entity view'},
          requestConfigArg
        ],
        return: observableReturnType(entityViewHref)
      },
      getEntityViewInfo: {
        description: 'Get entity view info by id',
        meta: 'function',
        args: [
          {name: 'entityViewId', type: 'string', description: 'Id of the entity view'},
          requestConfigArg
        ],
        return: observableReturnType(entityViewInfoHref)
      },
      saveEntityView: {
        description: 'Save entity view',
        meta: 'function',
        args: [
          {name: 'entityView', type: entityViewHref, description: 'Entity view object to save'},
          requestConfigArg
        ],
        return: observableReturnType(entityViewHref)
      },
      deleteEntityView: {
        description: 'Delete entity view by id',
        meta: 'function',
        args: [
          {name: 'entityViewId', type: 'string', description: 'Id of the entity view'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      getEntityViewTypes: {
        description: 'Get all available entity view types',
        meta: 'function',
        args: [
          requestConfigArg
        ],
        return: observableArrayReturnType('<code>EntitySubtype</code>')
      },
      makeEntityViewPublic: {
        description: 'Make entity view public (available from public dashboard)',
        meta: 'function',
        args: [
          {name: 'entityViewId', type: 'string', description: 'Id of the entity view'},
          requestConfigArg
        ],
        return: observableReturnType(entityViewHref)
      },
      assignEntityViewToCustomer: {
        description: 'Assign entity view to specific customer',
        meta: 'function',
        args: [
          {name: 'customerId', type: 'string', description: 'Id of the customer'},
          {name: 'entityViewId', type: 'string', description: 'Id of the entity view'},
          requestConfigArg
        ],
        return: observableReturnType(entityViewHref)
      },
      unassignEntityViewFromCustomer: {
        description: 'Unassign entity view from any customer',
        meta: 'function',
        args: [
          {name: 'entityViewId', type: 'string', description: 'Id of the entity view'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      findByQuery: {
        description: 'Find entities view by search query',
        meta: 'function',
        args: [
          {
            name: 'query',
            type: '<code>AssetSearchQuery</code>',
            description: 'Entity view search query object'
          },
          requestConfigArg
        ],
        return: observableArrayReturnType(entityViewHref)
      },
    }
  },
  customerService: {
    description: 'Customer Service API<br>' +
      'See <code>CustomerService</code> for API reference.',
    meta: 'service',
    type: '<code>CustomerService</code>',
    children: {
      getCustomer: {
        description: 'Get customer by id',
        meta: 'function',
        args: [
          {name: 'customerId', type: 'string', description: 'Id of the customer'},
          requestConfigArg
        ],
        return: observableReturnType(customerHref)
      },
      getCustomers: {
        description: 'Get customers by ids',
        meta: 'function',
        args: [
          pageLinkArg,
          requestConfigArg
        ],
        return: observablePageDataReturnType(customerHref)
      },
      saveCustomer: {
        description: 'Save customer',
        meta: 'function',
        args: [
          {name: 'customer', type: customerHref, description: 'Customer object to save'},
          requestConfigArg
        ],
        return: observableReturnType(customerHref)
      },
      deleteCustomer: {
        description: 'Delete customer by id',
        meta: 'function',
        args: [
          {name: 'customerId', type: 'string', description: 'Id of the customer'},
          requestConfigArg
        ],
        return: observableVoid()
      },
    }
  },
  dashboardService: {
    description: 'Dashboard Service API<br>' +
      'See <code>DashboardService</code> for API reference.',
    meta: 'service',
    type: '<code>DashboardService</code>',
    children: {
      getTenantDashboards: {
        description: 'Get tenant dashboards',
        meta: 'function',
        args: [
          pageLinkArg,
          requestConfigArg
        ],
        return: observablePageDataReturnType(dashboardInfoHref)
      },
      getTenantDashboardsByTenantId: {
        description: 'Get dashboards by tenant id',
        meta: 'function',
        args: [
          {name: 'tenantId', type: 'string', description: 'Id of the tenant'},
          pageLinkArg,
          requestConfigArg
        ],
        return: observablePageDataReturnType(dashboardInfoHref)
      },
      getCustomerDashboards: {
        description: 'Get dashboards by customer id',
        meta: 'function',
        args: [
          {name: 'customerId', type: 'string', description: 'Id of the customer'},
          pageLinkArg,
          requestConfigArg
        ],
        return: observablePageDataReturnType(dashboardInfoHref)
      },
      getDashboard: {
        description: 'Get dashboard by id',
        meta: 'function',
        args: [
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          requestConfigArg
        ],
        return: observableReturnType(dashboardHref)
      },
      getDashboardInfo: {
        description: 'Get dashboard info by id',
        meta: 'function',
        args: [
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          requestConfigArg
        ],
        return: observableReturnType(dashboardInfoHref)
      },
      saveDashboard: {
        description: 'Save dashboard',
        meta: 'function',
        args: [
          {name: 'dashboard', type: dashboardHref, description: 'Dashboard object to save'},
          requestConfigArg
        ],
        return: observableReturnType(dashboardHref)
      },
      deleteDashboard: {
        description: 'Delete dashboard by id',
        meta: 'function',
        args: [
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      assignDashboardToCustomer: {
        description: 'Assign dashboard to specific customer',
        meta: 'function',
        args: [
          {name: 'customerId', type: 'string', description: 'Id of the customer'},
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          requestConfigArg
        ],
        return: observableReturnType(dashboardHref)
      },
      unassignDashboardFromCustomer: {
        description: 'Unassign dashboard from specific customer',
        meta: 'function',
        args: [
          {name: 'customerId', type: 'string', description: 'Id of the customer'},
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      makeDashboardPublic: {
        description: 'Make dashboard public by id',
        meta: 'function',
        args: [
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          requestConfigArg
        ],
        return: observableReturnType(dashboardHref)
      },
      makeDashboardPrivate: {
        description: 'Make dashboard private by id',
        meta: 'function',
        args: [
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          requestConfigArg
        ],
        return: observableReturnType(dashboardHref)
      },
      updateDashboardCustomers: {
        description: 'Update customers assigned to dashboard by ids',
        meta: 'function',
        args: [
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          {name: 'customerIds', type: `Array&lt;string&gt;`, description: 'Ids of the customers'},
          requestConfigArg
        ],
        return: observableReturnType(dashboardHref)
      },
      addDashboardCustomers: {
        description: 'Assign (Add) customers to dashboard by ids',
        meta: 'function',
        args: [
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          {name: 'customerIds', type: `Array&lt;string&gt;`, description: 'Ids of the customers'},
          requestConfigArg
        ],
        return: observableReturnType(dashboardHref)
      },
      removeDashboardCustomers: {
        description: 'Unassign (Remove) customers from dashboard by ids',
        meta: 'function',
        args: [
          {name: 'dashboardId', type: 'string', description: 'Id of the dashboard'},
          {name: 'customerIds', type: `Array&lt;string&gt;`, description: 'Id of the customers'},
          requestConfigArg
        ],
        return: observableReturnType(dashboardHref)
      },
      getPublicDashboardLink: {
        description: 'Get public dashboard link',
        meta: 'function',
        args: [
          {name: 'dashboard', type: dashboardInfoHref, description: 'dashboard info'},
        ],
        return: {
          type: `string|null`,
          description: `Returns dashboard url`
        }
      },
      getServerTimeDiff: {
        description: 'Get time difference',
        meta: 'function',
        args: [
        ],
        return: observableReturnTypeVariable('number')
      },
    }
  },
  userService: {
    description: 'User Service API<br>' +
      'See <code>UserService</code> for API reference.',
    meta: 'service',
    type: '<code>UserService</code>',
    children: {
      getUsers: {
        description: 'Get  users',
        meta: 'function',
        args: [
          pageLinkArg,
          requestConfigArg
        ],
        return: observablePageDataReturnType(userHref)
      },
      getTenantAdmins: {
        description: 'Get tenant admins by id',
        meta: 'function',
        args: [
          {name: 'tenantId', type: 'string', description: 'Id of the tenant'},
          pageLinkArg,
          requestConfigArg
        ],
        return: observablePageDataReturnType(userHref)
      },
      getCustomerUsers: {
        description: 'Get customer users by id',
        meta: 'function',
        args: [
          {name: 'customerId', type: 'string', description: 'Id of the customer'},
          pageLinkArg,
          requestConfigArg
        ],
        return: observablePageDataReturnType(userHref)
      },
      getUser: {
        description: 'Get user by id',
        meta: 'function',
        args: [
          {name: 'userId', type: 'string', description: 'Id of the user'},
          requestConfigArg
        ],
        return: observableReturnType(userHref)
      },
      saveUser: {
        description: 'Save user',
        meta: 'function',
        args: [
          {name: 'user', type: userHref, description: 'User object to save'},
          {name: 'sendActivationMail', type: 'boolean', description: 'Send activation email', optional: true},
          requestConfigArg
        ],
        return: observableReturnType(userHref)
      },
      deleteUser: {
        description: 'Delete user by id',
        meta: 'function',
        args: [
          {name: 'userId', type: 'string', description: 'Id of the user'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      setUserCredentialsEnabled: {
        description: 'Set user credentials enabled by id',
        meta: 'function',
        args: [
          {name: 'userId', type: 'string', description: 'Id of the user'},
          {name: 'userCredentialsEnabled', type: 'boolean', description: 'User credentials enabled'},
          requestConfigArg
        ],
        return: observableReturnTypeVariable('any')
      },
      getActivationLink: {
        description: 'Get activation link by id',
        meta: 'function',
        args: [
          {name: 'userId', type: 'string', description: 'Id of the user'},
          requestConfigArg
        ],
        return: observableReturnTypeVariable('string')
      },
      sendActivationEmail: {
        description: 'Send activation email',
        meta: 'function',
        args: [
          {name: 'email', type: 'string', description: 'Email of the user'},
          requestConfigArg
        ],
        return: observableVoid()
      },
    }
  },
  entityRelationService: {
    description: 'Entity Relation Service API<br>' +
      'See <code>EntityRelationService</code> for API reference.',
    meta: 'service',
    type: '<code>EntityRelationService</code>',
    children: {
      saveRelation: {
        description: 'Save relation',
        meta: 'function',
        args: [
          {name: 'relation', type: entityRelationHref, description: 'Relation object to save'},
          requestConfigArg
        ],
        return: observableReturnType(entityRelationHref)
      },
      deleteRelation: {
        description: 'Delete relation by ids',
        meta: 'function',
        args: [
          {name: 'fromId', type: entityIdHref, description: 'From-id'},
          {name: 'relationType', type: 'string', description: 'Relation type'},
          {name: 'toId', type: entityIdHref, description: 'To-id'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      deleteRelations: {
        description: 'Delete relations by entity id',
        meta: 'function',
        args: [
          {name: 'entityId', type: entityIdHref, description: 'Entity Id'},
          requestConfigArg
        ],
        return: observableVoid()
      },
      getRelation: {
        description: 'Get relation by ids',
        meta: 'function',
        args: [
          {name: 'fromId', type: entityIdHref, description: 'From-id'},
          {name: 'relationType', type: 'string', description: 'Relation type'},
          {name: 'toId', type: entityIdHref, description: 'To-id'},
          requestConfigArg
        ],
        return: observableReturnType(entityRelationHref)
      },
      findByFrom: {
        description: 'Find by from-id',
        meta: 'function',
        args: [
          {name: 'fromId', type: entityIdHref, description: 'From-id'},
          requestConfigArg
        ],
        return: observableArrayReturnType(entityRelationHref)
      },
      findInfoByFrom: {
        description: 'Find info by from-id',
        meta: 'function',
        args: [
          {name: 'fromId', type: entityIdHref, description: 'From-id'},
          requestConfigArg
        ],
        return: observableArrayReturnType(entityRelationInfoHref)
      },
      findByFromAndType: {
        description: 'Find by from-id and relation type',
        meta: 'function',
        args: [
          {name: 'fromId', type: entityIdHref, description: 'From-id'},
          {name: 'relationType', type: 'string', description: 'Relation type'},
          requestConfigArg
        ],
        return: observableArrayReturnType(entityRelationHref)
      },
      findByTo: {
        description: 'Find by to-id',
        meta: 'function',
        args: [
          {name: 'toId', type: entityIdHref, description: 'To-id'},
          requestConfigArg
        ],
        return: observableArrayReturnType(entityRelationHref)
      },
      findInfoByTo: {
        description: 'Find info by to-id',
        meta: 'function',
        args: [
          {name: 'toId', type: entityIdHref, description: 'To-id'},
          requestConfigArg
        ],
        return: observableArrayReturnType(entityRelationInfoHref)
      },
      findByToAndType: {
        description: 'Find by to-id and relation type',
        meta: 'function',
        args: [
          {name: 'toId', type: entityIdHref, description: 'To-id'},
          {name: 'relationType', type: 'string', description: 'Relation type'},
          requestConfigArg
        ],
        return: observableArrayReturnType(entityRelationHref)
      },
      findByQuery: {
        description: 'Find by query',
        meta: 'function',
        args: [
          {name: 'query', type: entityRelationsQueryHref, description: 'Entity relations query'},
          requestConfigArg
        ],
        return: observableArrayReturnType(entityRelationHref)
      },
      findInfoByQuery: {
        description: 'Find info by query',
        meta: 'function',
        args: [
          {name: 'query', type: entityRelationsQueryHref, description: 'Entity relations query'},
          requestConfigArg
        ],
        return: observableArrayReturnType(entityRelationInfoHref)
      },
    }
  },
  attributeService: {
    description: 'Attribute Service API<br>' +
      'See <code>AttributeService</code> for API reference.',
    meta: 'service',
    type: '<code>AttributeService</code>',
    children: {
      getEntityAttributes: {
        description: 'Get entity attributes by id',
        meta: 'function',
        args: [
          {name: 'entityId', type: entityIdHref, description: 'Id of the entity'},
          {name: 'attributeScope', type: attributeScopeHref, description: 'Attribute scope'},
          {name: 'keys', type: `Array&lt;string&gt;`, description: 'Array of the keys'},
          requestConfigArg
        ],
        return: observableArrayReturnType(attributeDataHref)
      },
      deleteEntityAttributes: {
        description: 'Delete entity attributes by id',
        meta: 'function',
        args: [
          {name: 'entityId', type: entityIdHref, description: 'Id of the entity'},
          {name: 'attributeScope', type: attributeScopeHref, description: 'Attribute scope'},
          {name: 'attributes', type: `array&lt;${attributeDataHref}&gt;`, description: 'Array of the attributes data'},
          requestConfigArg
        ],
        return: observableReturnTypeVariable('any')
      },
      deleteEntityTimeseries: {
        description: 'Delete entity timeseries by id',
        meta: 'function',
        args: [
          {name: 'entityId', type: entityIdHref, description: 'Id of the entity'},
          {name: 'timeseries', type: `Array&lt;${attributeDataHref}&gt;>`, description: 'Array of the timeseries data'},
          {name: 'deleteAllDataForKeys', type: 'boolean', optional: true, description: 'Delete all data for keys'},
          requestConfigArg
        ],
        return: observableReturnTypeVariable('any')
      },
      saveEntityAttributes: {
        description: 'Save entity attributes',
        meta: 'function',
        args: [
          {name: 'entityId', type: entityIdHref, description: 'Id of the entity'},
          {name: 'attributeScope', type: attributeScopeHref, description: 'Attribute scope'},
          {name: 'attributes', type: 'Array&lt;${attributeDataHref}&gt;>', description: 'Array of the attributes data'},
          requestConfigArg
        ],
        return: observableReturnTypeVariable('any')
      },
      saveEntityTimeseries: {
        description: 'Save entity timeseries',
        meta: 'function',
        args: [
          {name: 'entityId', type: entityIdHref, description: 'Id of the entity'},
          {name: 'timeseriesScope', type: 'string', description: 'Timeseries scope'},
          {name: 'timeseries', type: `Array&lt;attributeDataHref&gt;`, description: 'Array of the timeseries data'},
          requestConfigArg
        ],
        return: observableReturnTypeVariable('any')
      },
      getEntityTimeseries: {
        description: 'Get entity timeseries',
        meta: 'function',
        args: [
          {name: 'entityId', type: entityIdHref, description: 'Id of the entity'},
          {name: 'keys', type: `Array&lt;string&gt;`, description: 'Array of the keys'},
          {name: 'startTs', type: 'number', description: 'Start time in milliseconds'},
          {name: 'endTs', type: 'number', description: 'End time in milliseconds'},
          {name: 'limit', type: 'number', description: 'Limit of values to receive for each key'},
          {name: 'agg', type: aggregationTypeHref, description: 'Aggregation type'},
          {name: 'interval', type: 'number', description: 'Aggregation interval'},
          {name: 'orderBy', type: dataSortOrderHref, description: 'Data order by time'},
          {name: 'useStrictDataTypes', type: 'boolean', description: 'If "false" all values will be returned as strings'},
          requestConfigArg
        ],
        return: observableReturnTypeVariable(timeseriesDataHref)
      },
    }
  },
  entityService: {
    description: 'Entity Service API<br>' +
      'See <code>EntityService</code> for API reference.',
    meta: 'service',
    type: '<code>EntityService</code>',
    children: {
      getEntity: {
        description: 'Get entity by id',
        meta: 'function',
        args: [
          {name: 'entityType', type: entityTypeHref, description: 'Entity type'},
          {name: 'entityId', type: 'string', description: 'Id of the entity'},
          requestConfigArg
        ],
        return: observableBaseDataReturnType()
      },
      getEntities: {
        description: 'Get entities by ids',
        meta: 'function',
        args: [
          {name: 'entityType', type: entityTypeHref, description: 'Entity type'},
          {name: 'entityIds', type: `Array&lt;string&gt;`, description: 'Ids of the entities'},
          requestConfigArg
        ],
        return: observableArrayBaseDataReturnType()
      },
      getEntitiesByNameFilter: {
        description: 'Get entities by name filter',
        meta: 'function',
        args: [
          {name: 'entityType', type: entityTypeHref, description: 'Entity type'},
          {name: 'entityNameFilter', type: 'string', description: 'Name filter for the entity'},
          {name: 'pageSize', type: 'number', description: 'Size of the page'},
          {name: 'subType', type: 'string', optional: true, description: 'Subtype'},
          requestConfigArg
        ],
        return: observableArrayBaseDataReturnType()
      },
      findEntityDataByQuery: {
        description: 'Find entity data by query',
        meta: 'function',
        args: [
          {name: 'query', type: entityDataQueryHref, description: 'Entity data query'},
          requestConfigArg
        ],
        return: observablePageDataReturnType(entityDataHref)
      },
      findAlarmDataByQuery: {
        description: 'Find alarm data by query',
        meta: 'function',
        args: [
          {name: 'query', type: alarmDataQueryHref, description: 'Alarm data query'},
          requestConfigArg
        ],
        return: observablePageDataReturnType(alarmDataHref)
      },
      findEntityInfosByFilterAndName: {
        description: 'Find entity infos by filter and name',
        meta: 'function',
        args: [
          {name: 'filter', type: entityFilterHref, description: 'Filter for the entities'},
          {name: 'searchText', type: 'string', description: 'Search text'},
          requestConfigArg
        ],
        return: observablePageDataReturnType(entityInfoHref)
      },
      findSingleEntityInfoByEntityFilter: {
        description: 'Find single entity infos by filter',
        meta: 'function',
        args: [
          {name: 'filter', type: entityFilterHref, description: 'Filter for the entity'},
          requestConfigArg
        ],
        return: observableReturnType(entityInfoHref)
      },
      getAliasFilterTypesByEntityTypes: {
        description: 'Get alias filter types by entity types',
        meta: 'function',
        args: [
          {name: 'entityTypes', type: `Array&lt;${entityTypeHref}|${aliasEntityTypeHref}&gt;`, description: 'Entity types'}
        ],
        return: {
          type: `Array&lt;${aliasFilterTypeHref}$gt;`,
          description: `Array of ${aliasFilterTypeHref} objects`
        }
      },
      filterAliasByEntityTypes: {
        description: 'Filter alias by entity types',
        meta: 'function',
        args: [
          {name: 'entityAlias', type: entityAliasHref, description: 'Alias of the entity'},
          {name: 'entityTypes', type: `Array&lt;${entityTypeHref}|${aliasEntityTypeHref}&gt;`, description: 'Entity types'}
        ],
        return: {
          type: 'boolean',
          description: `Returns <code>boolean</code> variable based on the filter`
        }
      },
      prepareAllowedEntityTypesList: {
        description: 'Prepare allowed entity types list',
        meta: 'function',
        args: [
          {name: 'allowedEntityTypes', type: `Array&lt;${entityTypeHref}|${aliasEntityTypeHref}&gt;`, description: 'Entity types'},
          {name: 'useAliasEntityTypes', type: 'boolean', description: 'Use alias entity types'},
        ],
        return: {
          type: `Array&lt;${entityTypeHref}|${aliasEntityTypeHref}&gt;`,
          description: `Returns entity types array`
        }
      },
      getEntityKeys: {
        description: 'Get entity keys by id',
        meta: 'function',
        args: [
          {name: 'entityId', type: entityIdHref, description: 'Id of the entity'},
          {name: 'query', type: 'string', description: 'Key name starts with'},
          {name: 'type', type: dataKeyTypeHref, description: 'Datakey type'},
          requestConfigArg
        ],
        return: {
          type: `Observable&lt;Array&lt;string&gt;&gt;`,
          description: `An <code>Observable</code> of array of <code>string</code> variables.`
        }
      },
      createDatasourcesFromSubscriptionsInfo: {
        description: 'Create datasources from subscriptions info',
        meta: 'function',
        args: [
          {name: 'subscriptionsInfo', type: 'array<subscriptionInfoHref>', description: 'Subscriptions info'}
        ],
        return: {
          type: `Array&lt;${dataSourceHref}&gt;`,
          description: `Array of <code>${dataSourceHref}</code> objects`
        }
      },
      createAlarmSourceFromSubscriptionInfo: {
        description: 'Create alarm source from subscriptions info',
        meta: 'function',
        args: [
          {name: 'subscriptionInfo', type: subscriptionInfoHref, description: 'Subscription info'}
        ],
        return: {
          type: `${dataSourceHref}`,
          description: `<code>${dataSourceHref}</code> object`
        }
      },
      resolveAlias: {
        description: 'Resolve alias',
        meta: 'function',
        args: [
          {name: 'entityAlias', type: entityAliasHref, description: 'Entity alias'},
          {name: 'stateParams', type: stateParamsHref, description: 'State params'},
        ],
        return: observableReturnType(aliasInfoHref)
      },
      resolveAliasFilter: {
        description: 'Resolve alias filter',
        meta: 'function',
        args: [
          {name: 'filter', type: entityAliasFilterHref, description: 'Entity alias filter'},
          {name: 'stateParams', type: stateParamsHref, description: 'State params'},
        ],
        return: observableReturnType(entityAliasFilterResultHref)
      },
      checkEntityAlias: {
        description: 'Check entity alias',
        meta: 'function',
        args: [
          {name: 'entityAlias', type: entityAliasHref, description: 'Entity alias'},
        ],
        return: observableReturnTypeVariable('boolean')
      },
      saveEntityParameters: {
        description: 'Save entity parameters',
        meta: 'function',
        args: [
          {name: 'entityType', type: entityTypeHref, description: 'Entity type'},
          {name: 'entityData', type: importEntityDataHref, description: 'Entity data'},
          {name: 'update', type: 'boolean', description: 'Update'},
          requestConfigArg
        ],
        return: observableReturnType(importEntitiesResultInfoHref)
      },
      saveEntityData: {
        description: 'Save entity data',
        meta: 'function',
        args: [
          {name: 'entityId', type: entityIdHref, description: 'Id of the entity'},
          {name: 'entityData', type: importEntityDataHref, description: 'Entity data'},
          requestConfigArg
        ],
        return: observableReturnTypeVariable('any')
      },
    }
  },
  resourceService: {
    description: 'Resource Service API<br>' +
      'See <code>ResourceService</code> for API reference.',
    meta: 'service',
    type: '<code>ResourceService</code>',
    children: {
      getResources: {
        description: 'Find resources by search text',
        meta: 'function',
        args: [
          pageLinkArg,
          requestConfigArg
        ],
        return: observablePageDataReturnType(resourceInfoHref)
      },
    }
  },
  dialogs: {
    description: 'Dialogs Service API<br>' +
      'See <code>DialogService</code> for API reference.',
    meta: 'service',
    type: '<code>DialogService</code>',
    children: {
      confirm: {
        description: 'Confirm',
        meta: 'function',
        args: [
          {name: 'title', type: 'string', description: 'Title'},
          {name: 'message', type: 'string', description: 'Message'},
          {name: 'cancel', type: 'string', optional: true, description: 'Cancel'},
          {name: 'ok', type: 'string', optional: true, description: 'Ok'},
          {name: 'fullscreen', type: 'boolean', optional: true, description: 'Fullscreen'},
        ],
        return: observableReturnTypeVariable('boolean')
      },
      alert: {
        description: 'Alert',
        meta: 'function',
        args: [
          {name: 'title', type: 'string', description: 'Title'},
          {name: 'message', type: 'string', description: 'Message'},
          {name: 'ok', type: 'string', optional: true, description: 'Ok'},
          {name: 'fullscreen', type: 'boolean', optional: true, description: 'Fullscreen'},
        ],
        return: observableReturnTypeVariable('boolean')
      },
      colorPicker: {
        description: 'Color picker',
        meta: 'function',
        args: [
          {name: 'color', type: 'string', description: 'Сolor'},
        ],
        return: observableReturnTypeVariable('string')
      },
      materialIconPicker: {
        description: 'Material icon picker',
        meta: 'function',
        args: [
          {name: 'icon', type: 'string', description: 'Icon'},
        ],
        return: observableReturnTypeVariable('string')
      },
      forbidden: {
        description: 'Forbidden',
        meta: 'function',
        args: [
        ],
        return: observableReturnTypeVariable('boolean')
      },
      todo: {
        description: 'To do',
        meta: 'function',
        args: [
        ],
        return: observableReturnTypeVariable('any')
      },
    }
  },
  customDialog: {
    description: 'Custom Dialog Service API<br>' +
      'See <code>CustomDialogService</code> for API reference.',
    meta: 'service',
    type: '<code>CustomDialogService</code>',
    children: {
      customDialog: {
        description: 'Custom Dialog',
        meta: 'function',
        args: [
          {name: 'template', type: 'string', description: 'Template'},
          {name: 'controller', type: customDialogComponentHref, description: 'Controller'},
          {name: 'data', type: 'any', description: 'Data', optional: true},
        ],
        return: observableReturnTypeVariable('any')
      },
    }
  },
  date: {
    description: 'Date Pipe<br>Formats a date value according to locale rules.<br>' +
      'See <a href="https://angular.io/api/common/DatePipe" target="_blank">DatePipe</a> for API reference.',
    meta: 'service',
    type: '<a href="https://angular.io/api/common/DatePipe" target="_blank">DatePipe</a>'
  },
  translate: {
    description: 'Translate Service API<br>' +
      'See <a href="https://github.com/ngx-translate/core#translateservice" target="_blank">TranslateService</a> for API reference.',
    meta: 'service',
    type: '<a href="https://github.com/ngx-translate/core#translateservice" target="_blank">TranslateService</a>'
  },
  http: {
    description: 'HTTP Client Service<br>' +
      'See <a href="https://angular.io/api/common/http/HttpClient" target="_blank">HttpClient</a> for API reference.',
    meta: 'service',
    type: '<a href="https://angular.io/api/common/http/HttpClient" target="_blank">HttpClient</a>'
  },
  sanitizer: {
    description: 'DomSanitizer Service<br>' +
      'See <a href="https://angular.io/api/platform-browser/DomSanitizer" target="_blank">DomSanitizer</a> for API reference.',
    meta: 'service',
    type: '<a href="https://angular.io/api/platform-browser/DomSanitizer" target="_blank">DomSanitizer</a>'
  },
  router: {
    description: 'Router Service<br>' +
      'See <a href="https://angular.io/api/router/Router" target="_blank">Router</a> for API reference.',
    meta: 'service',
    type: '<a href="https://angular.io/api/router/Router" target="_blank">Router</a>'
  }
};
