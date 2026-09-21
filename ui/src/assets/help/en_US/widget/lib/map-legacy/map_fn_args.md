  <li><b>data:</b> <code><code>FormattedData</code></code> - A <code>FormattedData</code> object associated with marker or data point of the route.<br/>
     Represents basic entity properties (ex. <code>entityId</code>, <code>entityName</code>)<br/>and provides access to other entity attributes/timeseries declared in widget datasource configuration.
  </li>
  <li><b>dsData:</b> <code><code>FormattedData[]</code></code> - All available data associated with markers or routes data points as array of <code>FormattedData</code> objects<br/>
     resolved from configured datasources. Each object represents basic entity properties (ex. <code>entityId</code>, <code>entityName</code>)<br/>
     and provides access to other entity attributes/timeseries declared in widget datasource configuration.
  </li>
  <li><b>dsIndex</b> <code>number</code> - index of the current marker data or route data point in <code>dsData</code> array.<br>
     <strong>Note: </strong> The <code>data</code> argument is equivalent to <code>dsData[dsIndex]</code> expression. 
  </li>
