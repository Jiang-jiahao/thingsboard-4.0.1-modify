package com.jnks.iot.rule.engine.geo;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "GPS 地理围栏过滤",
        configClazz = JnksIotGpsGeofencingFilterNodeConfiguration.class,
        relationTypes = {JnksIotNodeConnectionType.TRUE, JnksIotNodeConnectionType.FALSE},
        nodeDescription = "基于 GPS 地理围栏过滤传入消息",
        nodeDetails = "从传入消息中提取纬度和经度参数，并根据配置的围栏范围进行检查。 </br>" +
                "配置：</br></br>" +
                "<ul>" +
                "<li>纬度键名 - 包含位置纬度的消息字段名称；</li>" +
                "<li>经度键名 - 包含位置经度的消息字段名称；</li>" +
                "<li>围栏类型 - Polygon 或 Circle；</li>" +
                "<li>从消息元数据获取围栏范围 - 用于从消息元数据加载围栏范围的复选框； " +
                "   如果围栏范围专属于设备/资产，并且您将其存储为设备/资产属性，请启用此项；</li>" +
                "<li>围栏范围键名 - 存储围栏范围信息的元数据键名称；</li>" +
                "<li>对于 Polygon 围栏类型：<ul>" +
                "    <li>多边形定义 - 包含坐标数组的字符串，格式如下：[[lat1, lon1],[lat2, lon2],[lat3, lon3], ... , [latN, lonN]]</li>" +
                "</ul></li>" +
                "<li>对于 Circle 围栏类型：<ul>" +
                "   <li>中心纬度 - 圆形围栏中心的纬度；</li>" +
                "   <li>中心经度 - 圆形围栏中心的经度；</li>" +
                "   <li>范围 - 圆形围栏范围的值，双精度浮点数值；</li>" +
                "   <li>范围单位 - 以下之一：Meter、Kilometer、Foot、Mile、Nautical Mile；</li>" +
                "</ul></li></ul></br>" +
                "如果启用了「从消息元数据获取围栏范围」且未配置「围栏范围键名」，规则节点将使用默认的元数据键名称。 " +
                "Polygon 围栏类型的默认元数据键名为 \"perimeter\"。Circle 围栏的默认元数据键名为：\"centerLatitude\"、\"centerLongitude\"、\"range\"、\"rangeUnit\"。" +
                "</br></br>" +
                "圆形围栏定义的结构（例如，存储在服务端属性中）：" +
                "</br></br>" +
                "{\"latitude\":  48.198618758582384, \"longitude\": 24.65322245153503, \"radius\":  100.0, \"radiusUnit\": \"METER\" }" +
                "</br></br>" +
                "可用的半径单位：METER、KILOMETER、FOOT、MILE、NAUTICAL_MILE；<br><br>" +
                "输出连接：<code>True</code>、<code>False</code>、<code>Failure</code>",
        configDirective = "jnksIotFilterNodeGpsGeofencingConfig")
public class JnksIotGpsGeofencingFilterNode extends AbstractGeofencingNode<JnksIotGpsGeofencingFilterNodeConfiguration> {

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws JnksIotNodeException {
        ctx.tellNext(msg, checkMatches(msg) ? JnksIotNodeConnectionType.TRUE : JnksIotNodeConnectionType.FALSE);
    }

    @Override
    protected Class<JnksIotGpsGeofencingFilterNodeConfiguration> getConfigClazz() {
        return JnksIotGpsGeofencingFilterNodeConfiguration.class;
    }
}
