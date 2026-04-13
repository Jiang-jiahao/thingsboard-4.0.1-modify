/**
 * 湖南华诺《J3000+ 干扰器控制板网络通信协议》帧格式（与文档 5.1 干扰启停一致）：
 * - 公共：0-1 帧头 A5 5A；2-3 源/目的；4 命令；5-6 参数长度 UINT16 LE；7 起参数；末字节 SUM8（从源地址起至参数末）。
 *
 * **表 5-1 下行（PC→干扰器）0x21**：典型 Src=0x10(PC)、Dst=0xA0(干扰器)；参数长度 N=10（0x0A 0x00）；
 * 参数区字节 7–16 为 5 路功放各 2 字节（表 5-2：高字节保留、低字节 bit0~3 对应 C1/5.8G、S1/2.4G、L1/GPS L1、900M 等开关）。
 * 下行组帧字段为每路 `paN_hi` + `paN_lo`（UINT8），便于只配低字节位图；若 JSON 仍传 `paN_word`（UINT16 线值），服务端会按大端拆成 hi/lo（显式 hi/lo 优先）。
 * 校验和在字节 17（整帧 18 字节，索引 0–17）。
 *
 * **表 5-3 上行（干扰器→PC）应答**：命令字节为 0xA2；参数长度 N=11（0x0B 0x00）；
 * 字节 7（Param1）为回显原命令（对 0x21 应答则为 0x21）；字节 8–17 为 5 路功放当前状态（与下行同 2 字节/路结构）。
 * 校验和在字节 18。识别「应答哪条指令」用第二匹配：偏移 7 = 原命令码。
 *
 * **下行 0x21 示例（表 5-1）**：命令勾选自动参长、长度字段选 `paramLen`；10 行 `pa1_hi`/`pa1_lo`…`pa5_lo` 均勾选「参与参长」。
 * 组帧 JSON 推荐（高字节一般为 0 可省略键，由组帧填 0）：`{"pa1_lo":15,"pa2_lo":0,"pa3_lo":0,"pa4_lo":0,"pa5_lo":0}`。
 * 亦可写满 10 键；**仍兼容**旧键 `pa1_word`…`pa5_word`（UINT16 线值），由服务端拆成 hi/lo。
 * 其它下行命令（0x1F 等）若未覆盖 paramLen，仍用手填参长或后续在命令上增加字段/自动参长覆盖。
 * 命令「覆盖字段」可与帧模板合并：重叠区间以命令字段为准（见 mergeTemplateAndCommandFields）。
 * 帧模板默认只含公共头（至 paramLen），**paramFirst 仅配在上行命令**；下行不从模板继承 paramFirst。
 *
 * **字段语义**：上行命令下的覆盖字段 = 从设备上报帧中**解析**出的遥测项；下行命令下的覆盖字段 = 平台下发时需**按偏移与类型填入**的参数字典（组帧契约，TCP 层不据此自动编码）。
 *
 * **变长参区**：参区长度在 paramLen 时，可将整段参区配成 BYTES_AS_HEX +「从帧内偏移读长度」指向 5/UINT16_LE；参区内再细分字段若随 N 变化，需在规则链中处理 hex，或使用 LTV 段。
 */
import {
  ProtocolTemplateBundle,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection,
  ProtocolTemplateDefinition,
  TcpHexFieldDefinition,
  TcpHexValueType
} from '@shared/models/device.models';

type CmdOptions = {
  /** 应答帧 0xA2 时第 7 字节回显的原命令，用于第二匹配 */
  secondaryEcho?: number;
  /** 默认上行：设备→平台；下行：平台→设备（下发控制/反制等） */
  direction?: ProtocolTemplateCommandDirection;
  /** 与帧模板按字节合并；仅填本命令多出的字段即可（见后端 mergeTemplateAndCommandFields） */
  fields?: TcpHexFieldDefinition[];
  downlinkPayloadLengthAuto?: boolean;
  downlinkPayloadLengthFieldKey?: string;
};

/** 表 5-1：每路 2×UINT8，与 `paN_word` 线值兼容由服务端展开 */
const FIELDS_DOWNLINK_JAM_21: TcpHexFieldDefinition[] = [
  { key: 'pa1_hi', byteOffset: 7, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true },
  { key: 'pa1_lo', byteOffset: 8, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true },
  { key: 'pa2_hi', byteOffset: 9, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true },
  { key: 'pa2_lo', byteOffset: 10, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true },
  { key: 'pa3_hi', byteOffset: 11, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true },
  { key: 'pa3_lo', byteOffset: 12, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true },
  { key: 'pa4_hi', byteOffset: 13, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true },
  { key: 'pa4_lo', byteOffset: 14, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true },
  { key: 'pa5_hi', byteOffset: 15, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true },
  { key: 'pa5_lo', byteOffset: 16, valueType: TcpHexValueType.UINT8, includeInDownlinkPayloadLength: true }
];

/** 仅解析参区首字节（上行通用，下行勿用） */
const FIELDS_UPLINK_PARAM_FIRST: TcpHexFieldDefinition[] = [
  { key: 'paramFirst', byteOffset: 7, valueType: TcpHexValueType.UINT8 }
];

/** 表 5-3：Param1 回显 + 5 路状态（与公共头模板合并） */
const FIELDS_UPLINK_ACK_JAM_21: TcpHexFieldDefinition[] = [
  { key: 'originalCmdEcho', byteOffset: 7, valueType: TcpHexValueType.UINT8 },
  { key: 'pa1_statusWord', byteOffset: 8, valueType: TcpHexValueType.UINT16_BE },
  { key: 'pa2_statusWord', byteOffset: 10, valueType: TcpHexValueType.UINT16_BE },
  { key: 'pa3_statusWord', byteOffset: 12, valueType: TcpHexValueType.UINT16_BE },
  { key: 'pa4_statusWord', byteOffset: 14, valueType: TcpHexValueType.UINT16_BE },
  { key: 'pa5_statusWord', byteOffset: 16, valueType: TcpHexValueType.UINT16_BE }
];

function cmd(name: string, commandValue: number, secondaryEchoOrOpts?: number | CmdOptions): ProtocolTemplateCommandDefinition {
  let secondaryEcho: number | undefined;
  let direction = ProtocolTemplateCommandDirection.UPLINK;
  let fields: TcpHexFieldDefinition[] | undefined;
  let downlinkPayloadLengthAuto: boolean | undefined;
  let downlinkPayloadLengthFieldKey: string | undefined;
  if (secondaryEchoOrOpts !== undefined && typeof secondaryEchoOrOpts === 'object') {
    secondaryEcho = secondaryEchoOrOpts.secondaryEcho;
    if (secondaryEchoOrOpts.direction != null) {
      direction = secondaryEchoOrOpts.direction;
    }
    if (secondaryEchoOrOpts.fields?.length) {
      fields = secondaryEchoOrOpts.fields;
    }
    if (secondaryEchoOrOpts.downlinkPayloadLengthAuto) {
      downlinkPayloadLengthAuto = true;
      const fk = String(secondaryEchoOrOpts.downlinkPayloadLengthFieldKey ?? '').trim();
      if (fk) {
        downlinkPayloadLengthFieldKey = fk;
      }
    }
  } else if (typeof secondaryEchoOrOpts === 'number') {
    secondaryEcho = secondaryEchoOrOpts;
  }

  const c: ProtocolTemplateCommandDefinition = {
    templateId: '', // 由对话框在 patch 后同步为首帧模板 id
    name,
    commandValue,
    matchValueType: TcpHexValueType.UINT8,
    direction
  };
  if (secondaryEcho !== undefined) {
    c.secondaryMatchByteOffset = 7;
    c.secondaryMatchValueType = TcpHexValueType.UINT8;
    c.secondaryMatchValue = secondaryEcho;
  }
  if (fields?.length) {
    c.fields = fields;
  }
  if (downlinkPayloadLengthAuto) {
    c.downlinkPayloadLengthAuto = true;
    if (downlinkPayloadLengthFieldKey) {
      c.downlinkPayloadLengthFieldKey = downlinkPayloadLengthFieldKey;
    }
  }
  return c;
}

/** 生成可写入协议模板库的示例包（名称同时作为帧模板 id） */
export function buildHuanuoJ3000PresetBundle(displayName: string): ProtocolTemplateBundle {
  const id = String(displayName ?? '').trim() || 'J3000_Huanuo';
  const template: ProtocolTemplateDefinition = {
    id,
    commandByteOffset: 4,
    commandMatchWidth: 1,
    validateTotalLengthU32Le: false,
    checksum: {
      type: 'SUM8',
      fromByte: 2,
      toExclusive: -1,
      checksumByteIndex: -1
    },
    hexProtocolFields: [
      { key: 'srcAddr', byteOffset: 2, valueType: TcpHexValueType.UINT8 },
      { key: 'dstAddr', byteOffset: 3, valueType: TcpHexValueType.UINT8 },
      { key: 'cmd', byteOffset: 4, valueType: TcpHexValueType.UINT8 },
      { key: 'paramLen', byteOffset: 5, valueType: TcpHexValueType.UINT16_LE }
    ]
  };

  const commands: ProtocolTemplateCommandDefinition[] = [
    // 下行（平台→设备）：表 5-1；pa*_lo 按位填 bit0~3（表 5-2），或传 pa*_word 由服务端拆字节
    cmd('下发 干扰启停 0x21', 0x21, {
      direction: ProtocolTemplateCommandDirection.DOWNLINK,
      downlinkPayloadLengthAuto: true,
      downlinkPayloadLengthFieldKey: 'paramLen',
      fields: FIELDS_DOWNLINK_JAM_21
    }),
    cmd('下发 干扰参数配置 0x1F', 0x1f, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),
    cmd('下发 云台透传 0x60', 0x60, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),
    cmd('下发 云台查询 0x06', 0x06, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),
    cmd('下发 心跳轮询 0xA4', 0xa4, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),
    cmd('下发 反制通道/样式 0x22', 0x22, { direction: ProtocolTemplateCommandDirection.DOWNLINK }),

    // 上行（设备→平台）：paramFirst 仅在各行命令上合并，下行不继承
    cmd('上发温度报警 0x48', 0x48, { fields: FIELDS_UPLINK_PARAM_FIRST }),
    cmd('应答 回显心跳 0xA4', 0xa2, { secondaryEcho: 0xa4, fields: FIELDS_UPLINK_PARAM_FIRST }),
    cmd('应答 干扰启停 0x21（表5-3）', 0xa2, { secondaryEcho: 0x21, fields: FIELDS_UPLINK_ACK_JAM_21 }),
    cmd('应答 云台透传 0x60', 0xa2, { secondaryEcho: 0x60, fields: FIELDS_UPLINK_PARAM_FIRST }),
    cmd('应答 云台查询 0x06', 0xa2, { secondaryEcho: 0x06, fields: FIELDS_UPLINK_PARAM_FIRST }),
    cmd('应答 序列号查询 0x41', 0xa2, { secondaryEcho: 0x41, fields: FIELDS_UPLINK_PARAM_FIRST }),
    cmd('应答 网络查询 0x42', 0xa2, { secondaryEcho: 0x42, fields: FIELDS_UPLINK_PARAM_FIRST }),
    cmd('应答 版本查询 0x44', 0xa2, { secondaryEcho: 0x44, fields: FIELDS_UPLINK_PARAM_FIRST }),
    cmd('应答 干扰参数查询 0x1F', 0xa2, { secondaryEcho: 0x1f, fields: FIELDS_UPLINK_PARAM_FIRST }),
    cmd('应答 其它 0xA2（未细分）', 0xa2, { fields: FIELDS_UPLINK_PARAM_FIRST })
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
