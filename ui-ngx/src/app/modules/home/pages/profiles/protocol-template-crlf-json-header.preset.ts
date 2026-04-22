/**
 * 二进制帧：`\r\n` + 报文长度(4, BE) + opcode(4, BE) + 随机数(4, BE) + `\r\n` + JSON 字符串(UTF-8)。
 * 报文长度 = 12 + JSON 字节数（含 length、opcode、random 三字段共 12 字节 + 消息内容）。
 * 命令匹配：opcode 位于整帧字节偏移 6，宽度 4，UINT32_BE。
 *
 * JSON 正文以 `jsonPayload` 输出（BYTES_AS_UTF8：UTF-8 解码后的字符串），可直接 JSON.parse（建议在规则链 try/catch）。
 * 上行使用占位命令字 `0xDEADBEEF`（永不匹配），解析走帧模板默认字段，便于任意 opcode 共用同一套头字段。
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
    valueType: TcpHexValueType.UINT32_BE
  },
  {
    key: 'opcode',
    byteOffset: 6,
    valueType: TcpHexValueType.UINT32_BE
  },
  {
    key: 'random',
    byteOffset: 10,
    valueType: TcpHexValueType.UINT32_BE
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
    byteLengthFromIntegralSubtract: 12
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

  const placeholder: ProtocolTemplateCommandDefinition = {
    templateId: TEMPLATE_ID,
    name: '占位（0xDEADBEEF 不匹配，走默认帧模板）',
    commandValue: 0xdeadbeef,
    matchValueType: TcpHexValueType.UINT32_BE,
    direction: ProtocolTemplateCommandDirection.UPLINK
  };

  return {
    id: '',
    name,
    protocolTemplates: [template],
    protocolCommands: [placeholder]
  };
}
