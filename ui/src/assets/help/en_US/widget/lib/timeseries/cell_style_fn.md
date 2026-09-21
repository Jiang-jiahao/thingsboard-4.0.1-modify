#### Cell style function

<div class="divider"></div>
<br/>

*function (value, rowData, ctx): {[key: string]: string}*

A JavaScript function used to compute timeseries cell style depending on timeseries field value.

**Parameters:**

<ul>
  <li><b>value:</b> <code>any</code> - An timeseries field value displayed in the cell.
  </li>
  <li><b>rowData:</b> <code><code>TimeseriesRow</code></code> - A 
            <code>TimeseriesRow</code> object
            presenting <code>formattedTs</code> (a string value of formatted timestamp) and <br> timeseries values for each column declared in widget datasource configuration.
  </li>
  <li><b>ctx:</b> <code><code>WidgetContext</code></code> - A reference to <code>WidgetContext</code> that has all necessary API 
     and data used by widget instance.
  </li>
</ul>

**Returns:**

Should return key/value object presenting style attributes.

<div class="divider"></div>

##### Examples

* Set color depending on temperature value:

```javascript
var temperature = value;
var color = 'black';
if (temperature) {
    if (temperature > 25) {
      color = 'red';
    } else {
      color = 'green';
    }
}
return {
  fontWeight: 'bold',
  color: color
};
{:copy-code}
```

<br>
<br>
