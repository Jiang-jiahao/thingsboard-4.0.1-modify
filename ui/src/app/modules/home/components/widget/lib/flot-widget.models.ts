// eslint-disable-next-line @typescript-eslint/triple-slash-reference
/// <reference path="../../../../../../../src/typings/jquery.flot.typings.d.ts" />

import {
  DataKey, DataKeySettingsWithComparison,
  Datasource,
  DatasourceData,
  FormattedData,
  LegendConfig
} from '@shared/models/widget.models';
import { DataKeyType } from '@shared/models/telemetry/telemetry.models';
import { ComparisonDuration } from '@shared/models/time/time.models';

export declare type ChartType = 'line' | 'pie' | 'bar' | 'state' | 'graph';

export declare type JnksIotFlotSettings = JnksIotFlotBaseSettings & JnksIotFlotLegendSettings &
  JnksIotFlotGraphSettings & JnksIotFlotBarSettings & JnksIotFlotPieSettings;

export declare type TooltipValueFormatFunction = (value: any, latestData: FormattedData) => string;

export declare type JnksIotFlotTicksFormatterFunction = (t: number, a?: JnksIotFlotPlotAxis) => string;

export interface JnksIotFlotSeries extends DatasourceData, JQueryPlotSeriesOptions {
  dataKey: JnksIotFlotDataKey;
  xaxisIndex?: number;
  yaxisIndex?: number;
  yaxis?: number;
}

export interface JnksIotFlotDataKey extends DataKey {
  settings?: JnksIotFlotKeySettings;
  tooltipValueFormatFunction?: TooltipValueFormatFunction;
}

export interface JnksIotFlotPlotAxis extends JQueryPlotAxis, JnksIotFlotAxisOptions {
  options: JnksIotFlotAxisOptions;
}

export interface JnksIotFlotAxisOptions extends JQueryPlotAxisOptions {
  tickUnits?: string;
  hidden?: boolean;
  keysInfo?: Array<{hidden: boolean}>;
  ticksFormatterFunction?: JnksIotFlotTicksFormatterFunction;
}

export interface JnksIotFlotPlotDataSeries extends JQueryPlotDataSeries {
  datasource?: Datasource;
  dataKey?: JnksIotFlotDataKey;
  percent?: number;
}

export interface JnksIotFlotPlotItem extends jquery.flot.item {
  series: JnksIotFlotPlotDataSeries;
}

export interface JnksIotFlotHoverInfo {
  seriesHover: Array<JnksIotFlotSeriesHoverInfo>;
  time?: any;
}

export interface JnksIotFlotSeriesHoverInfo {
  hoverIndex: number;
  units: string;
  decimals: number;
  label: string;
  color: string;
  index: number;
  tooltipValueFormatFunction: TooltipValueFormatFunction;
  value: any;
  time: any;
  distance: number;
}

export interface JnksIotFlotThresholdMarking {
  lineWidth?: number;
  color?: string;
  [key: string]: any;
}

export interface JnksIotFlotThresholdKeySettings {
  yaxis: number;
  lineWidth: number;
  color: string;
}

export interface JnksIotFlotGridSettings {
  color: string;
  backgroundColor: string;
  tickColor: string;
  outlineWidth: number;
  verticalLines: boolean;
  horizontalLines: boolean;
  minBorderMargin?: number;
  margin?: number;
}

export interface JnksIotFlotXAxisSettings {
  showLabels: boolean;
  title: string;
  color: boolean;
}

export interface JnksIotFlotSecondXAxisSettings {
  axisPosition: JnksIotFlotXAxisPosition;
  showLabels: boolean;
  title: string;
}

export interface JnksIotFlotYAxisSettings {
  min: number;
  max: number;
  showLabels: boolean;
  title: string;
  color: string;
  ticksFormatter: string;
  tickDecimals: number;
  tickSize: number;
  tickGenerator: string;
}

export interface JnksIotFlotBaseSettings {
  stack: boolean;
  enableSelection: boolean;
  shadowSize: number;
  fontColor: string;
  fontSize: number;
  tooltipIndividual: boolean;
  tooltipCumulative: boolean;
  tooltipValueFormatter: string;
  hideZeros: boolean;
  grid: JnksIotFlotGridSettings;
  xaxis: JnksIotFlotXAxisSettings;
  yaxis: JnksIotFlotYAxisSettings;
}

export interface JnksIotFlotLegendSettings {
  showLegend?: boolean;
  legendConfig?: LegendConfig;
}

export interface JnksIotFlotComparisonSettings {
  comparisonEnabled: boolean;
  timeForComparison: ComparisonDuration;
  xaxisSecond: JnksIotFlotSecondXAxisSettings;
  comparisonCustomIntervalValue?: number;
}

export interface JnksIotFlotThresholdsSettings {
  thresholdsLineWidth: number;
}

export interface JnksIotFlotCustomLegendSettings {
  customLegendEnabled: boolean;
  dataKeysListForLabels: Array<JnksIotFlotLabelPatternSettings>;
}

export interface JnksIotFlotLabelPatternSettings {
  name: string;
  type: DataKeyType;
  settings?: any;
}

export interface JnksIotFlotGraphSettings extends JnksIotFlotBaseSettings,
                                             JnksIotFlotThresholdsSettings, JnksIotFlotComparisonSettings, JnksIotFlotCustomLegendSettings {
  smoothLines: boolean;
}

export declare type BarAlignment = 'left' | 'right' | 'center';

export interface JnksIotFlotBarSettings extends JnksIotFlotBaseSettings,
                                           JnksIotFlotThresholdsSettings, JnksIotFlotComparisonSettings, JnksIotFlotCustomLegendSettings {
  defaultBarWidth: number;
  barAlignment: BarAlignment;
}

export interface JnksIotFlotPieSettings {
  radius: number;
  innerRadius: number;
  tilt: number;
  animatedPie: boolean;
  stroke: {
    color: string;
    width: number;
  };
  showTooltip: boolean;
  showLabels: boolean;
  fontColor: string;
  fontSize: number;
}

export declare type JnksIotFlotYAxisPosition = 'left' | 'right';
export declare type JnksIotFlotXAxisPosition = 'top' | 'bottom';

export declare type JnksIotFlotThresholdValueSource = 'predefinedValue' | 'entityAttribute';

export interface JnksIotFlotKeyThreshold {
  thresholdValueSource: JnksIotFlotThresholdValueSource;
  thresholdEntityAlias: string;
  thresholdAttribute: string;
  thresholdValue: number;
  lineWidth: number;
  color: string;
}

export interface JnksIotFlotKeySettings extends DataKeySettingsWithComparison {
  excludeFromStacking: boolean;
  hideDataByDefault: boolean;
  disableDataHiding: boolean;
  removeFromLegend: boolean;
  showLines: boolean;
  fillLines: boolean;
  fillLinesOpacity: number;
  showPoints: boolean;
  showPointShape: string;
  pointShapeFormatter: string;
  showPointsLineWidth: number;
  showPointsRadius: number;
  lineWidth: number;
  tooltipValueFormatter: string;
  showSeparateAxis: boolean;
  axisMin: number;
  axisMax: number;
  axisTitle: string;
  axisTickDecimals: number;
  axisTickSize: number;
  axisPosition: JnksIotFlotYAxisPosition;
  axisTicksFormatter: string;
  thresholds: JnksIotFlotKeyThreshold[];
}

export interface JnksIotFlotLatestKeySettings {
  useAsThreshold: boolean;
  thresholdLineWidth: number;
  thresholdColor: string;
}
