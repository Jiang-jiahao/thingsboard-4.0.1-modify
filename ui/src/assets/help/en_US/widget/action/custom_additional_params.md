#### Additional params object

<div class="divider"></div>
<br/>

<b>additionalParams:</b> <code>{[key: string]: any}</code>

An optional key/value object holding additional entity parameters depending on widget type and action source:

<ul>
  <li>Entities table widget (<i>On row click</i> or <i>Action cell button</i>) - <b>additionalParams:</b> <code>{ entity: <code>EntityData</code> }</code>:
    <ul>
      <li><b>entity:</b> <code><code>EntityData</code></code> - An 
            <code>EntityData</code> object
            presenting basic entity properties (ex. <code>id</code>, <code>entityName</code>) and <br> provides access to other entity attributes/timeseries declared in widget datasource configuration.
      </li>
    </ul>
  </li>        
  <li>Alarms table widget (<i>On row click</i> or <i>Action cell button</i>) - <b>additionalParams:</b> <code>{ alarm: <code>AlarmDataInfo</code> }</code>:
    <ul>
      <li><b>alarm:</b> <code><code>AlarmDataInfo</code></code> - An 
            <code>AlarmDataInfo</code> object
            presenting basic alarm properties (ex. <code>type</code>, <code>severity</code>, <code>originator</code>, etc.) and <br> provides access to other alarm or originator entity fields/attributes/timeseries declared in widget datasource configuration.
      </li>
    </ul>
  </li>        
  <li>Timeseries table widget (<i>On row click</i> or <i>Action cell button</i>) - <b>additionalParams:</b> <code><code>TimeseriesRow</code></code>:
    <ul>
      <li><b>additionalParams:</b> <code><code>TimeseriesRow</code></code> - A 
            <code>TimeseriesRow</code> object
            presenting <code>formattedTs</code> (a string value of formatted timestamp) and <br> timeseries values for each column declared in widget datasource configuration.
      </li>
    </ul>
  </li>
  <li>Map widgets (<i>On marker/polygon/circle click</i> or <i>Tag action</i>) - <b>additionalParams</b>: <code><code>FormattedData</code></code>:
    <ul>
      <li><b>additionalParams:</b> <code><code>FormattedData</code></code> - An object associated with a data layer (markers, polygons, circles) or with a specific data point of a route (for trips data layers).<br/>
          It contains basic entity properties (ex. <code>entityId</code>, <code>entityName</code>) and provides access to additional attributes and timeseries defined in datasource of the data layer configuration.
      </li>
    </ul>
  </li>
  <li>Entities hierarchy widget (<i>On node selected</i>) - <b>additionalParams:</b> <code>{ nodeCtx: <code>HierarchyNodeContext</code> }</code>:
    <ul>
      <li><b>nodeCtx:</b> <code><code>HierarchyNodeContext</code></code> - An 
            <code>HierarchyNodeContext</code> object
            containing <code>entity</code> field holding basic entity properties <br> (ex. <code>id</code>, <code>name</code>, <code>label</code>) and <code>data</code> field holding other entity attributes/timeseries declared in widget datasource configuration.
      </li>
    </ul>
  </li>
  <li>Pie (<i>On slice click</i>) - <b>additionalParams:</b> <code>LatestChartDataItem</code>:
    <ul>
      <li><b>additionalParams:</b> <code>LatestChartDataItem</code> - A 
            data object of clicked pie slice 
            containing <code>dataKey</code>, <code>datasource</code> and <code>value</code> fields.
      </li>
    </ul>
  </li>
  <li>Pie - Flot widget (deprecated) (<i>On slice click</i>) - <b>additionalParams:</b> <code><code>JnksIotFlotPlotItem</code></code>:
    <ul>
      <li><b>additionalParams:</b> <code><code>JnksIotFlotPlotItem</code></code> - A 
            <code>JnksIotFlotPlotItem</code> object
            containing <code>series</code> field with information about datasource and <br> data key of clicked pie slice.
      </li>
    </ul>
  </li>
  <li><i>All other widgets</i> - does not provide <b>additionalParams</b> value.
  </li>
</ul> 
