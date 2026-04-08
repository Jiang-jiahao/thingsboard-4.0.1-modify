///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
///

/**
 * 与 TbHexProtocolParserNodeConfiguration.defaultConfiguration() 一致（监控类 UDP 报文：总长/设备ID/类别/命令 UINT32 LE，
 * 子命令体内 STRUCT+UNIT_LIST）。协议行 commandValue 为空表示匹配任意命令编号；validateTotalLengthU32Le 校验首 4 字节总长。
 */
export const HEX_PARSER_DEFAULT_PROTOCOL_EXAMPLE = {
  frameTemplates: [
    {
      id: 'device_datagram_frame',
      syncOffset: 0,
      minBytes: 16,
      paramStartOffset: 16,
      paramLenFieldOffset: 0,
      commandMatchOffset: 12,
      headerFields: [
        { name: 'packetLen', offset: 0, type: 'UINT32_LE' },
        { name: 'deviceId', offset: 4, type: 'UINT32_LE' },
        { name: 'category', offset: 8, type: 'UINT32_LE' },
        { name: 'commandNumber', offset: 12, type: 'UINT32_LE' }
      ],
      payloadFields: [
        { name: 'subCommandPayloadLen', offset: 0, type: 'UINT32_LE' },
        {
          name: 'numberBlock',
          offset: 4,
          type: 'STRUCT',
          length: 4,
          nestedFields: [
            { name: 'moduleField', offset: 0, type: 'UINT16_LE' },
            { name: 'currentModuleNo', offset: 2, type: 'UINT8' },
            { name: 'moduleType', offset: 3, type: 'UINT8' }
          ]
        },
        {
          name: 'dataItems',
          offset: 8,
          type: 'UNIT_LIST',
          unitLengthFieldOffset: 0,
          unitIdByteLength: 4,
          unitDataOutputName: 'dataContentHex',
          unitNumberOutputName: 'itemNumber',
          listItemFields: [
            { name: 'moduleField', offset: 0, type: 'UINT16_LE' },
            { name: 'currentModuleNo', offset: 2, type: 'UINT8' },
            { name: 'moduleType', offset: 3, type: 'UINT8' }
          ]
        }
      ]
    }
  ],
  protocols: [
    {
      id: 'monitor_udp_datagram',
      templateId: 'device_datagram_frame',
      minBytes: 16,
      commandByteOffset: 12,
      commandMatchWidth: 4,
      validateTotalLengthU32Le: true,
      checksum: { type: 'NONE' },
      fields: []
    }
  ]
} as const;
