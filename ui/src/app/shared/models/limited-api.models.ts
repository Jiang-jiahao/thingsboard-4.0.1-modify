export enum LimitedApi {
  ENTITY_EXPORT = 'ENTITY_EXPORT',
  ENTITY_IMPORT = 'ENTITY_IMPORT',
  NOTIFICATION_REQUESTS = 'NOTIFICATION_REQUESTS',
  NOTIFICATION_REQUESTS_PER_RULE = 'NOTIFICATION_REQUESTS_PER_RULE',
  REST_REQUESTS_PER_TENANT = 'REST_REQUESTS_PER_TENANT',
  REST_REQUESTS_PER_CUSTOMER = 'REST_REQUESTS_PER_CUSTOMER',
  WS_UPDATES_PER_SESSION = 'WS_UPDATES_PER_SESSION',
  CASSANDRA_QUERIES = 'CASSANDRA_QUERIES',
  TRANSPORT_MESSAGES_PER_TENANT = 'TRANSPORT_MESSAGES_PER_TENANT',
  TRANSPORT_MESSAGES_PER_DEVICE = 'TRANSPORT_MESSAGES_PER_DEVICE',
  TRANSPORT_MESSAGES_PER_GATEWAY = 'TRANSPORT_MESSAGES_PER_GATEWAY',
  TRANSPORT_MESSAGES_PER_GATEWAY_DEVICE = 'TRANSPORT_MESSAGES_PER_GATEWAY_DEVICE'
}

export const LimitedApiTranslationMap = new Map<LimitedApi, string>(
  [
    [LimitedApi.ENTITY_EXPORT, 'api-limit.entity-version-creation'],
    [LimitedApi.ENTITY_IMPORT, 'api-limit.entity-version-load'],
    [LimitedApi.NOTIFICATION_REQUESTS, 'api-limit.notification-requests'],
    [LimitedApi.NOTIFICATION_REQUESTS_PER_RULE, 'api-limit.notification-requests-per-rule'],
    [LimitedApi.REST_REQUESTS_PER_TENANT, 'api-limit.rest-api-requests'],
    [LimitedApi.REST_REQUESTS_PER_CUSTOMER, 'api-limit.rest-api-requests-per-customer'],
    [LimitedApi.WS_UPDATES_PER_SESSION, 'api-limit.ws-updates-per-session'],
    [LimitedApi.CASSANDRA_QUERIES, 'api-limit.cassandra-queries'],
    [LimitedApi.TRANSPORT_MESSAGES_PER_TENANT, 'api-limit.transport-messages'],
    [LimitedApi.TRANSPORT_MESSAGES_PER_DEVICE, 'api-limit.transport-messages-per-device'],
    [LimitedApi.TRANSPORT_MESSAGES_PER_GATEWAY, 'api-limit.transport-messages-per-gateway'],
    [LimitedApi.TRANSPORT_MESSAGES_PER_GATEWAY_DEVICE, 'api-limit.transport-messages-per-gateway-device']
  ]
);
