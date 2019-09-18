package com.wavefront.tools.wftop.panels;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.google.common.collect.Ordering;
import com.googlecode.lanterna.gui2.*;
import com.wavefront.tools.wftop.components.Node;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * The major panel of wftop that displays point.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class PointsNamespacePanel extends NamespacePanel {

  @Override
  protected Comparator<Node> getComparator() {
    return (o1, o2) -> {
      if (sortIndex == 0) {
        return o1.getFlattened().compareTo(o2.getFlattened());
      } else if (sortIndex == 1) {
        // PPS
        return Double.compare(o1.getRate().getOneMinuteRate(), o2.getRate().getOneMinuteRate());
      } else if (sortIndex == 2) {
        // % accessed
        return Double.compare((double) o1.getAccessed() / o1.getRate().getCount(),
            (double) o2.getAccessed() / o2.getRate().getCount());
      } else if (sortIndex == 3) {
        // median lag
        return Double.compare(o1.getLag().getSnapshot().getMedian(), o2.getLag().getSnapshot().getMedian());
      } else if (sortIndex == 4) {
        // p75 lag
        return Double.compare(o1.getLag().getSnapshot().get75thPercentile(),
            o2.getLag().getSnapshot().get75thPercentile());
      } else if (sortIndex == 5) {
        // p99 lag
        return Double.compare(o1.getLag().getSnapshot().get99thPercentile(),
            o2.getLag().getSnapshot().get99thPercentile());
      } else if (sortIndex == 6) {
        // metric cardinality
        return Long.compare(o1.getEstimatedMetricCardinality(), o2.getEstimatedMetricCardinality());
      } else if (sortIndex == 7) {
        // host cardinality
        return Long.compare(o1.getEstimatedHostCardinality(), o2.getEstimatedHostCardinality());
      } else if (sortIndex == 8) {
        return Double.compare(o1.getRange(), o2.getRange());
      } else {
        return 0;
      }
    };
  }

  public PointsNamespacePanel(SpyConfigurationPanel panel, MultiWindowTextGUI gui) {
    super(panel, gui);

    //add column to table here
    this.table.getTableModel().setColumnLabel(0, "Namespace");
    this.table.getTableModel().insertColumn(1, "PPS [↑]", null);
    this.table.getTableModel().insertColumn(2, "% Acc.", null);
    this.table.getTableModel().insertColumn(3, "P50 Lag", null);
    this.table.getTableModel().insertColumn(4, "P75 Lag", null);
    this.table.getTableModel().insertColumn(5, "P99 Lag", null);
    this.table.getTableModel().insertColumn(6, "Num Metrics", null);
    this.table.getTableModel().insertColumn(7, "Num Hosts", null);
    this.table.getTableModel().insertColumn(8, "Range", null);
  }

  @Override
  protected void addFirstRow(Node root, double factor, Collection<Node> nodes, Snapshot snapshot,
                             boolean takeSnapshot) {
    this.table.getTableModel().addRow("..", // artificial ".."
        (Math.round(factor * root.getRate().getOneMinuteRate()) + "pps"),
        (Math.round(100.0 * root.getAccessed() / root.getRate().getCount()) + "%"),
        Math.round(snapshot.getMedian()) + "ms",
        Math.round(snapshot.get75thPercentile()) + "ms",
        Math.round(snapshot.get99thPercentile()) + "ms",
        String.valueOf(root.getEstimatedMetricCardinality()),
        String.valueOf(root.getEstimatedHostCardinality()),
        String.valueOf(root.getRange()));
    if (takeSnapshot) exportData(rootPath, root, factor, snapshot);
  }

  @Override
  protected void addNodes(Node root, double factor, Collection<Node> nodes, Snapshot snapshot,
                          String selectedLabel, boolean takeSnapshot) {
    Ordering<Node> ordering = Ordering.from(getComparator());
    if (reverseSort) ordering = ordering.reverse();
    List<Node> sorted = ordering.sortedCopy(nodes);
    int num = 0;
    int newLocation = 0;
    for (Node node : sorted) {
      snapshot = node.getLag().getSnapshot();
      String flattened = StringUtils.abbreviate(node.getFlattened(), 50);
      if (flattened.equals(selectedLabel)) {
        newLocation = num + 1;
      }
      labelToNodeMap.put(flattened, node);
      table.getTableModel().addRow(flattened,
          (Math.round(factor * node.getRate().getOneMinuteRate()) + "pps"),
          (Math.round(100.0 * node.getAccessed() / node.getRate().getCount()) + "%"),
          Math.round(snapshot.getMedian()) + "ms",
          Math.round(snapshot.get75thPercentile()) + "ms",
          Math.round(snapshot.get99thPercentile()) + "ms",
          String.valueOf(node.getEstimatedMetricCardinality()),
          String.valueOf(node.getEstimatedHostCardinality()),
          String.valueOf(node.getRange()));
      if (takeSnapshot) {
        exportData(flattened, node, factor, snapshot);
      }
      num++;
      if (num > 1000) break;
    }
    table.setSelectedRow(newLocation);
  }

  @Override
  public void setGlobalPPS(double factor, Meter rate) {
    this.globalPPS.setText("Est. PPS: 1m " +
        Math.round(factor * rate.getOneMinuteRate()) + "pps | 5m " +
        Math.round(factor * rate.getFiveMinuteRate()) + "pps | 15m " +
        Math.round(factor * rate.getFifteenMinuteRate()) + "pps");
  }

  @Override
  public void setUpCSVWriter() {
    try {
      BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(exportFile));
      csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.DEFAULT.withHeader(
          "Namespace", "PPS", "% Acc.", "P50 Lag", "P75 Lag", "P99 Lag", "Num Metrics", "Num " +
              "Hosts", "Range"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void exportData(String namespace, Node node, double factor, Snapshot snapshot) {
    try {
      csvPrinter.printRecord(namespace,
          Math.round(factor * node.getRate().getOneMinuteRate()) + "pps",
          Math.round(100.0 * node.getAccessed() / node.getRate().getCount()) + "%",
          Math.round(snapshot.getMedian()) + "ms",
          Math.round(snapshot.get75thPercentile()) + "ms",
          Math.round(snapshot.get99thPercentile()) + "ms",
          String.valueOf(node.getEstimatedMetricCardinality()),
          String.valueOf(node.getEstimatedHostCardinality()),
          String.valueOf(node.getRange()));
      csvPrinter.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
