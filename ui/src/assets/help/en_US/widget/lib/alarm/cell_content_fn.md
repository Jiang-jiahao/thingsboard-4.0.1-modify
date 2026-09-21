#### Cell content function

<div class="divider"></div>
<br/>

*function (value, alarm, ctx): string*

A JavaScript function used to compute alarm cell content HTML depending on alarm field value.

**Parameters:**

<ul>
  <li><b>value:</b> <code>any</code> - An alarm field value displayed in the cell.
  </li>
  <li><b>alarm:</b> <code><code>AlarmDataInfo</code></code> - An 
            <code>AlarmDataInfo</code> object
            presenting basic alarm properties (ex. <code>type</code>, <code>severity</code>, <code>originator</code>, etc.) and <br> provides access to other alarm or originator entity fields/attributes/timeseries declared in widget datasource configuration.
  </li>
  <li><b>ctx:</b> <code><code>WidgetContext</code></code> - A reference to <code>WidgetContext</code> that has all necessary API 
     and data used by widget instance.
  </li>
</ul>

**Returns:**

Should return string value presenting cell content HTML.

<div class="divider"></div>

##### Examples

* Format alarm start time using date/time pattern:

```javascript
var startTime = value;
return startTime ? ctx.date.transform(startTime, 'yyyy-MM-dd HH:mm:ss') : '';
{:copy-code}
```

* Styled cell content for originator alarm field:

```javascript
var originator = value;
return '<div style="border: 2px solid #0072ff; ' +
  'border-radius: 10px; padding: 5px; ' +
  'background-color: #e0e1ff; ' +
  'text-align: center;">' + originator + '</div>';
{:copy-code}
```

<br>
<br>
