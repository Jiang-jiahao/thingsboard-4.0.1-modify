#### Nodes sort function

<div class="divider"></div>
<br/>

*function (nodeCtx1, nodeCtx2): number*

A JavaScript function used to compare nodes of the same level when sorting.

**Parameters:**

<ul>
  <li><b>widgetCtx:</b> <code><code>WidgetContext</code></code> - A reference to <code>WidgetContext</code> that has all necessary API 
     and data used by widget instance.
  </li>
  <li><b>nodeCtx1:</b> <code><code>HierarchyNodeContext</code></code> - First 
            node object to be compared.
   </li>
  <li><b>nodeCtx2:</b> <code><code>HierarchyNodeContext</code></code> - Second 
            node object to be compared.
   </li>
</ul>

**Returns:**

Should return integer value presenting nodes comparison result:
- **less than 0** - sort `nodeCtx1` to an index lower than `nodeCtx2`;
- **0** - leave `nodeCtx1` and `nodeCtx2` unchanged with respect to each other;
- **greater than 0** - sort `nodeCtx2` to an index lower than `nodeCtx1`;

<div class="divider"></div>

##### Examples

* Sort entities first by entity type in alphabetical order then by entity name in alphabetical order:

```javascript
var result = nodeCtx1.entity.id.entityType.localeCompare(nodeCtx2.entity.id.entityType);
if (result === 0) {
  result = nodeCtx1.entity.name.localeCompare(nodeCtx2.entity.name);
}
return result;
{:copy-code}
```

<br>
<br>
