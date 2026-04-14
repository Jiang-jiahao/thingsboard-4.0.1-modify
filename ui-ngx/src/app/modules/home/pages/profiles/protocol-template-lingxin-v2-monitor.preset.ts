/**
 * 灵信《定向 V2 亚冬版监控通信协议（含云台）》202410 摘要：
 * - 传输：UDP；帧头为总长(4) + 设备ID(4) + 设备类别(4) + 命令编号(4)，多字节小端（低字节在前）。
 * - 总长：含本字段在内的整包字节数。
 * - 设备类别示例：0x00000001 屏蔽设备。
 * - 命令编号示例：0x1 状态获取、0x2 设置、0x3 状态上报、0x12 设置回复、0x5/0x15 监控列表查询/回复 等。
 * - 子命令体：若干信息单元，每单元为 长度(4) + 编号(4) + 数据；长度 = 编号(4 字节) + 数据 的字节数。
 *   编号布局：模块字段(UINT16 LE) + 当前模块编号(UINT8) + 模块类型(UINT8)；功放模块类型 0x01，云台 0x05，整机参数 0x06。
 *
 * **本预设**：命令匹配偏移 12、宽度 4、类型 UINT32_LE；无校验。帧模板含头四字段。
 * **上行子命令体**：帧模板启用 LTV，起始 16，`lengthIncludesTag=true`（长度=编号+数据），UINT32_LE 长度与 4 字节编号（整格 Tag 映射）；
 * 未映射的单元在 `unknownTagMode=EMIT_HEX` 下输出 `lx_序号_unk_t{tag}`。示例 Tag：模块字段 0x0001、模块号 1、类型 0x01 → 线型值 16842753，映射键 `paSwitchU32`（UINT32 数据）。
 * **下行**：仍用覆盖字段单子单元 + `subUnitLen` 自动参长；JSON 须含 **`packetLen`**，例如
 * `{"packetLen":28,"deviceId":1,"category":1,"moduleField":1,"currentModuleNo":1,"moduleType":1,"dataU32":1}`（功放开关 1 开 0 关）。
 */
import {
  ProtocolTemplateBundle,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection,
  ProtocolTemplateDefinition,
  TcpHexFieldDefinition,
  TcpHexLtvChunkOrder,
  TcpHexLtvRepeatingConfig,
  TcpHexUnknownTagMode,
  TcpHexValueType
} from '@shared/models/device.models';

type CmdOpts = {
  direction?: ProtocolTemplateCommandDirection;
  fields?: TcpHexFieldDefinition[];
  downlinkPayloadLengthAuto?: boolean;
  downlinkPayloadLengthFieldKey?: string;
};

/** 子命令体内单个信息单元（与文档「长度+编号+数据」对齐；数据示例为 4 字节） */
const FIELDS_DOWNLINK_ONE_SUBUNIT: TcpHexFieldDefinition[] = [
  { key: 'subUnitLen', byteOffset: 16, valueType: TcpHexValueType.UINT32_LE },
  {
    key: 'moduleField',
    byteOffset: 20,
    valueType: TcpHexValueType.UINT16_LE,
    includeInDownlinkPayloadLength: true
  },
  {
    key: 'currentModuleNo',
    byteOffset: 22,
    valueType: TcpHexValueType.UINT8,
    includeInDownlinkPayloadLength: true
  },
  {
    key: 'moduleType',
    byteOffset: 23,
    valueType: TcpHexValueType.UINT8,
    includeInDownlinkPayloadLength: true
  },
  {
    key: 'dataU32',
    byteOffset: 24,
    valueType: TcpHexValueType.UINT32_LE,
    includeInDownlinkPayloadLength: true
  }
];

/** 灵信子命令体 LTV：长度含 4 字节编号 + 数据；编号整格 UINT32 LE 作 Tag */
const LINGXIN_SUB_HEX_LTV: TcpHexLtvRepeatingConfig = {
  startByteOffset: 16,
  lengthFieldType: TcpHexValueType.UINT32_LE,
  tagFieldType: TcpHexValueType.UINT32_LE,
  chunkOrder: TcpHexLtvChunkOrder.LTV,
  lengthIncludesTag: true,
  keyPrefix: 'lx',
  unknownTagMode: TcpHexUnknownTagMode.EMIT_HEX,
  maxItems: 32,
  tagMappings: [
    {
      tagValue: 16842753,
      telemetryKey: 'paSwitchU32',
      valueType: TcpHexValueType.UINT_AUTO_LE
    }
  ]
};

function cmd(name: string, commandValue: number, opts?: CmdOpts): ProtocolTemplateCommandDefinition {
  const direction = opts?.direction ?? ProtocolTemplateCommandDirection.UPLINK;
  const c: ProtocolTemplateCommandDefinition = {
    templateId: '',
    name,
    commandValue,
    matchValueType: TcpHexValueType.UINT32_LE,
    direction
  };
  if (opts?.fields?.length) {
    c.fields = opts.fields;
  }
  if (opts?.downlinkPayloadLengthAuto) {
    c.downlinkPayloadLengthAuto = true;
    const fk = String(opts.downlinkPayloadLengthFieldKey ?? '').trim();
    if (fk) {
      c.downlinkPayloadLengthFieldKey = fk;
    }
  }
  return c;
}

export function buildLingxinV2MonitorPresetBundle(displayName: string): ProtocolTemplateBundle {
  const id = String(displayName ?? '').trim() || 'Lingxin_V2_Monitor';
  const template: ProtocolTemplateDefinition = {
    id,
    commandByteOffset: 12,
    commandMatchWidth: 4,
    hexProtocolFields: [
      { key: 'packetLen', byteOffset: 0, valueType: TcpHexValueType.UINT32_LE },
      { key: 'deviceId', byteOffset: 4, valueType: TcpHexValueType.UINT32_LE },
      { key: 'category', byteOffset: 8, valueType: TcpHexValueType.UINT32_LE },
      { key: 'commandNumber', byteOffset: 12, valueType: TcpHexValueType.UINT32_LE }
    ],
    hexLtvRepeating: LINGXIN_SUB_HEX_LTV
  };

  const commands: ProtocolTemplateCommandDefinition[] = [
    cmd('下行 状态获取 0x1', 0x1, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),
    cmd('下行 设置命令 0x2（单子单元示例）', 0x2, {
      direction: ProtocolTemplateCommandDirection.DOWNLINK,
      downlinkPayloadLengthAuto: true,
      downlinkPayloadLengthFieldKey: 'subUnitLen',
      fields: FIELDS_DOWNLINK_ONE_SUBUNIT
    }),
    cmd('下行 监控列表查询 0x5', 0x5, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),
    cmd('下行 关闭 0x7', 0x7, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),
    cmd('下行 重启 0x8', 0x8, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),
    cmd('下行 返航开启 0xA', 0xa, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),
    cmd('下行 迫降开启 0xB', 0xb, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),

    cmd('上发 状态上报 0x3', 0x3),
    cmd('上发 设置回复 0x12', 0x12),
    cmd('上发 监控列表回复 0x15', 0x15),
    cmd('上发 重启回复 0x18', 0x18)
  ];
  for (const c of commands) {
    c.templateId = id;
  }

  return {
    id: '',
    name: displayName,
    protocolTemplates: [template],
    protocolCommands: commands
  };
}
