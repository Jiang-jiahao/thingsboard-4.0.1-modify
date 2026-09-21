#### Node disabled function

<div class="divider"></div>
<br/>

*function (nodeCtx): boolean*

A JavaScript function evaluating whether current node should be disabled (not selectable).

**Parameters:**

<ul>
  <li><b>widgetCtx:</b> <code><code>WidgetContext</code></code> - A reference to <code>WidgetContext</code> that has all necessary API 
     and data used by widget instance.
  </li>
  <li><b>nodeCtx:</b> <code><code>HierarchyNodeContext</code></code> - An 
            <code>HierarchyNodeContext</code> object
            containing <code>entity</code> field holding basic entity properties <br> (ex. <code>id</code>, <code>name</code>, <code>label</code>) and <code>data</code> field holding other entity attributes/timeseries declared in widget datasource configuration.
   </li>
</ul>

**Returns:**

`true` if node should be disabled (not selectable), `false` otherwise.

<div class="divider"></div>

##### Examples

* Disable current node according to the value of example `nodeDisabled` attribute:

```javascript
var data = nodeCtx.data;
if (data.hasOwnProperty('nodeDisabled') && data['nodeDisabled'] !== null) {
  return data['nodeDisabled'] === 'true';
} else {
  return false;
}
{:copy-code}
```

<br>
<br>
