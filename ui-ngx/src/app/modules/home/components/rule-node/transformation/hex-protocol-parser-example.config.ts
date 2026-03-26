///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
///

/**
 * Example frameTemplates + protocols matching TbHexProtocolParserNodeConfiguration.defaultConfiguration().
 * Shown read-only in the rule node UI for copy / reference.
 */
export const HEX_PARSER_THREE_PROTOCOL_EXAMPLE = {
  frameTemplates: [
    {
      id: 'device_response_frame',
      syncOffset: 0,
      minBytes: 6,
      paramStartOffset: 4,
      paramLenFieldOffset: 2,
      commandMatchOffset: 1,
      headerFields: [
        { name: 'deviceType', offset: 0, type: 'UINT8' },
        { name: 'functionCode', offset: 1, type: 'UINT8' },
        { name: 'dataLen', offset: 2, type: 'UINT16_LE' }
      ]
    },
    {
      id: 'jammer_a55a_frame',
      syncHex: 'A55A',
      syncOffset: 0,
      minBytes: 8,
      paramLenFieldOffset: 5,
      paramStartOffset: 7,
      commandMatchOffset: 4,
      headerFields: [
        { name: 'srcAddr', offset: 2, type: 'UINT8' },
        { name: 'dstAddr', offset: 3, type: 'UINT8' },
        { name: 'command', offset: 4, type: 'UINT8' },
        { name: 'paramLen', offset: 5, type: 'UINT16_LE' }
      ]
    },
    {
      id: 'monitor_udp_frame',
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
      ]
    }
  ],
  protocols: [
    {
      id: 'table63_cmdA2',
      templateId: 'jammer_a55a_frame',
      minBytes: 29,
      commandByteOffset: 4,
      commandValue: 0xa2,
      checksum: {
        type: 'SUM8',
        fromByte: 2,
        toExclusive: -1,
        checksumByteIndex: -1
      },
      fields: [
        { name: 'subType', offset: 0, type: 'UINT8' },
        { name: 'param1Hex', offset: 1, type: 'HEX_SLICE', length: 20 }
      ]
    },
    {
      id: 'jammer_cmd_0x21',
      templateId: 'jammer_a55a_frame',
      minBytes: 9,
      commandByteOffset: 4,
      commandValue: 0x21,
      checksum: {
        type: 'SUM8',
        fromByte: 2,
        toExclusive: -1,
        checksumByteIndex: -1
      },
      fields: [{ name: 'paramByte0', offset: 0, type: 'UINT8' }]
    },
    {
      id: 'jammer_cmd_0x06',
      templateId: 'jammer_a55a_frame',
      minBytes: 8,
      commandByteOffset: 4,
      commandValue: 0x06,
      checksum: {
        type: 'SUM8',
        fromByte: 2,
        toExclusive: -1,
        checksumByteIndex: -1
      },
      fields: []
    },
    {
      id: 'monitor_udp_cmd_1',
      templateId: 'monitor_udp_frame',
      minBytes: 16,
      commandByteOffset: 12,
      commandValue: 1,
      commandMatchWidth: 4,
      checksum: { type: 'NONE' },
      fields: [{ name: 'subCommandBodyHex', offset: 0, type: 'HEX_SLICE' }]
    },
    {
      id: 'monitor_udp_cmd_2',
      templateId: 'monitor_udp_frame',
      minBytes: 16,
      commandByteOffset: 12,
      commandValue: 2,
      commandMatchWidth: 4,
      checksum: { type: 'NONE' },
      fields: [{ name: 'subCommandBodyHex', offset: 0, type: 'HEX_SLICE' }]
    },
    {
      id: 'monitor_udp_cmd_3',
      templateId: 'monitor_udp_frame',
      minBytes: 16,
      commandByteOffset: 12,
      commandValue: 3,
      commandMatchWidth: 4,
      checksum: { type: 'NONE' },
      fields: [{ name: 'subCommandBodyHex', offset: 0, type: 'HEX_SLICE' }]
    },
    {
      id: 'monitor_udp_cmd_5',
      templateId: 'monitor_udp_frame',
      minBytes: 16,
      commandByteOffset: 12,
      commandValue: 5,
      commandMatchWidth: 4,
      checksum: { type: 'NONE' },
      fields: [{ name: 'subCommandBodyHex', offset: 0, type: 'HEX_SLICE' }]
    },
    {
      id: 'resp_write_cmd_0x10',
      templateId: 'device_response_frame',
      minBytes: 6,
      commandByteOffset: 1,
      commandValue: 0x10,
      checksum: {
        type: 'CRC16_CCITT',
        fromByte: 0,
        toExclusive: -2,
        checksumByteIndex: -2
      },
      fields: [{ name: 'paramSectionHex', offset: 0, type: 'HEX_SLICE_LEN_U16LE' }]
    },
    {
      id: 'resp_read_cmd_0x03',
      templateId: 'device_response_frame',
      minBytes: 6,
      commandByteOffset: 1,
      commandValue: 0x03,
      checksum: {
        type: 'CRC16_CCITT',
        fromByte: 0,
        toExclusive: -2,
        checksumByteIndex: -2
      },
      fields: [
        {
          name: 'params',
          offset: 0,
          type: 'TLV_LIST',
          toOffsetExclusive: -2,
          tlvIdSize: 2,
          tlvIdEndian: 'BE'
        }
      ]
    }
  ]
} as const;
