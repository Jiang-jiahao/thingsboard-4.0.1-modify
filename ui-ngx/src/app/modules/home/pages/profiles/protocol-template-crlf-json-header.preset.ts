/**
 * 二进制帧：`0d0a` + 报文长度(4, BE) + opcode(4, BE) + 随机数(4, BE) + `0d0a` + JSON 字符串(UTF-8)。
 * 报文长度语义：length = messageLength(4) + opcode(4) + random(4) + jsonPayload 字节数 = 12 + JSON 字节数。
 *
 * 上行：示例命令「设备心跳」按偏移 6、UINT32_BE、opcode=201 匹配。
 * 下行：示例命令「设备反制」UINT32_LE=105；仅配置 `downlinkPayloadLengthAuto` + `downlinkPayloadLengthFieldKey`；
 * 参与参长的键在**帧模板字段**上勾选 `includeInDownlinkPayloadLength`（无需在命令里重复写字段）。
 * 下行组帧时 `jsonPayload` 无固定 byteLength：按 RPC `values.jsonPayload` 的 **UTF-8 实际字节数** 写正文并计入 messageLength。
 */
import {
  ProtocolTemplateBundle,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection,
  ProtocolTemplateDefinition,
  TcpHexFieldDefinition,
  TcpHexValueType
} from '@shared/models/device.models';

const TEMPLATE_ID = 'crlf_json_header_tcp';

const FRAME_FIELDS: TcpHexFieldDefinition[] = [
  {
    key: 'frameStart',
    byteOffset: 0,
    valueType: TcpHexValueType.BYTES_AS_HEX,
    byteLength: 2,
    fixedBytesHex: '0d0a'
  },
  {
    key: 'messageLength',
    byteOffset: 2,
    valueType: TcpHexValueType.UINT32_BE,
    includeInDownlinkPayloadLength: true
  },
  {
    key: 'opcode',
    byteOffset: 6,
    valueType: TcpHexValueType.UINT32_BE,
    includeInDownlinkPayloadLength: true
  },
  {
    key: 'random',
    byteOffset: 10,
    valueType: TcpHexValueType.UINT32_BE,
    includeInDownlinkPayloadLength: true
  },
  {
    key: 'separator',
    byteOffset: 14,
    valueType: TcpHexValueType.BYTES_AS_HEX,
    byteLength: 2,
    fixedBytesHex: '0d0a'
  },
  {
    key: 'jsonPayload',
    byteOffset: 16,
    valueType: TcpHexValueType.BYTES_AS_UTF8,
    byteLengthFromByteOffset: 2,
    byteLengthFromValueType: TcpHexValueType.UINT32_BE,
    byteLengthFromIntegralSubtract: 12,
    includeInDownlinkPayloadLength: true
  }
];

export function buildCrlfJsonHeaderPresetBundle(displayName: string): ProtocolTemplateBundle {
  const name = String(displayName ?? '').trim() || 'CRLF+JSON 头';
  const template: ProtocolTemplateDefinition = {
    id: TEMPLATE_ID,
    name: 'CRLF 帧头 + BE 头四字段 + JSON 正文',
    commandByteOffset: 6,
    commandMatchWidth: 4,
    hexProtocolFields: FRAME_FIELDS
  };

  const uplinkHeartbeat: ProtocolTemplateCommandDefinition = {
    templateId: TEMPLATE_ID,
    name: '设备心跳（示例：opcode UINT32_BE = 201）',
    commandValue: 201,
    matchValueType: TcpHexValueType.UINT32_BE,
    direction: ProtocolTemplateCommandDirection.UPLINK
  };

  const downlinkCountermeasure: ProtocolTemplateCommandDefinition = {
    templateId: TEMPLATE_ID,
    name: '设备反制（示例：下行自动写 messageLength；UINT32_LE=105；参长在帧模板勾选）',
    commandValue: 105,
    matchValueType: TcpHexValueType.UINT32_LE,
    direction: ProtocolTemplateCommandDirection.DOWNLINK,
    downlinkPayloadLengthAuto: true,
    downlinkPayloadLengthFieldKey: 'messageLength'
  };

  return {
    id: '',
    name,
    protocolTemplates: [template],
    protocolCommands: [uplinkHeartbeat, downlinkCountermeasure]
  };
}
