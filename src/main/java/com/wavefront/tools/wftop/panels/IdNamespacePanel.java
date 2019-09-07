package com.wavefront.tools.wftop.panels;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.google.common.collect.Ordering;
import com.googlecode.lanterna.gui2.MultiWindowTextGUI;
import com.wavefront.tools.wftop.components.Node;
import com.wavefront.tools.wftop.panels.NamespacePanel;
import com.wavefront.tools.wftop.panels.SpyConfigurationPanel;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * The major panel of wftop that displays Id Creations.
 *
 * @author Joanna Ko (kjoanna@vmware.com).
 */
public class IdNamespacePanel extends NamespacePanel {

  @Override
  protected Comparator<Node> getComparator() {
    return (o1, o2) -> {
      if (sortIndex == 0) {
        return o1.getFlattened().compareTo(o2.getFlattened());
      } else if (sortIndex == 1) {
        // CPS
        return Double.compare(o1.getRate().getOneMinuteRate(), o2.getRate().getOneMinuteRate());
      } else if (sortIndex == 2) {
        // metric cardinality
        return Long.compare(o1.getEstimatedMetricCardinality(), o2.getEstimatedMetricCardinality());
      } else {
        return 0;
      }
    };
  }

  public IdNamespacePanel(SpyConfigurationPanel panel, MultiWindowTextGUI gui) {
    super(panel, gui);

    this.table.getTableModel().setColumnLabel(0, "Namespace");
    this.table.getTableModel().insertColumn(1, "CPS [↑]", null);
    this.table.getTableModel().insertColumn(2, "Num Metrics", null);
  }

  @Override
  protected void addFirstRow(Node root, double factor, Collection<Node> nodes, Snapshot snapshot) {
    this.table.getTableModel().addRow("..", // artificial ".."
        (Math.round(factor * root.getRate().getOneMinuteRate()) + "cps"),
        String.valueOf(root.getEstimatedMetricCardinality()));
  }

  @Override
  protected void addNodes(Node root, double factor, Collection<Node> nodes, Snapshot snapshot, String selectedLabel) {
    Ordering<Node> ordering = Ordering.from(getComparator());
    if (reverseSort) ordering = ordering.reverse();
    List<Node> sorted = ordering.sortedCopy(nodes);
    int num = 0;
    int newLocation = 0;
    for (Node node : sorted) {
      String flattened = StringUtils.abbreviate(node.getFlattened(), 50);
      if (flattened.equals(selectedLabel)) {
        newLocation = num + 1;
      }
      labelToNodeMap.put(flattened, node);
      table.getTableModel().addRow(flattened,
          (Math.round(factor * node.getRate().getOneMinuteRate()) + "cps"),
          String.valueOf(node.getEstimatedMetricCardinality()));
      num++;
      if (num > 1000) break;
    }
    table.setSelectedRow(newLocation);
  }

  @Override
  public void setGlobalPPS(double factor, Meter rate) {
    this.globalPPS.setText("Est. CPS: 1m " +
        Math.round(factor * rate.getOneMinuteRate()) + "cps | 5m " +
        Math.round(factor * rate.getFiveMinuteRate()) + "cps | 15m " +
        Math.round(factor * rate.getFifteenMinuteRate()) + "cps");
  }
}
