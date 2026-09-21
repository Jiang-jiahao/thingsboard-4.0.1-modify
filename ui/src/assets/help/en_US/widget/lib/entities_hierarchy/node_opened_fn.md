#### Node opened by default function

<div class="divider"></div>
<br/>

*function (nodeCtx): boolean*

A JavaScript function evaluating whether current node should be opened (expanded) when it first loaded.

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

`true` if node should be opened (expanded), `false` otherwise.

<div class="divider"></div>

##### Examples

* Open by default nodes up to third level:

```javascript
return nodeCtx.level <= 2;
{:copy-code}
```

<br>
<br>
