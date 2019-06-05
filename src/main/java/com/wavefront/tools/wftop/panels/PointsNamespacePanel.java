package com.wavefront.tools.wftop.panels;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.google.common.collect.Ordering;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.gui2.*;
import com.googlecode.lanterna.gui2.table.Table;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.input.KeyType;
import com.wavefront.tools.wftop.components.NamespaceBuilder;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.util.*;

/**
 * The major panel of wftop that displays point namespaces, pps, access %, etc.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class PointsNamespacePanel extends Panel {

  private final Label globalPPS = new Label("");
  private final Label connectivityStatus = new Label("");
  private final Label samplingRate = new Label("");
  private final Label path = new Label("> ");

  private Listener listener = null;
  private Table<String> table;
  private Map<String, NamespaceBuilder.Node> labelToNodeMap = new HashMap<>();
  private int sortIndex = 1;
  private boolean reverseSort = true;

  public PointsNamespacePanel(SpyConfigurationPanel panel, MultiWindowTextGUI gui) {
    this.setLayoutManager(new BorderLayout());

    // header.
    Panel header = new Panel(new BorderLayout());
    header.addComponent(globalPPS.withBorder(Borders.singleLine()).
        setLayoutData(BorderLayout.Location.CENTER));
    header.addComponent(connectivityStatus.withBorder(Borders.singleLine()).
        setLayoutData(BorderLayout.Location.LEFT));
    header.addComponent(samplingRate.withBorder(Borders.singleLine()).
        setLayoutData(BorderLayout.Location.RIGHT));
    header.addComponent(path.setLayoutData(BorderLayout.Location.BOTTOM));
    this.addComponent(header.setLayoutData(BorderLayout.Location.TOP));

    this.table = new Table<String>("Namespace", "Est. PPS [↑]", "% Accessed", "Median Lag", "P75 Lag", "P99 Lag",
        "Est. Metrics", "Est. Hosts") {
      @Override
      public Result handleKeyStroke(KeyStroke keyStroke) {
        Result result = super.handleKeyStroke(keyStroke);
        if (result != Result.UNHANDLED) {
          return result;
        }
        if (keyStroke.getKeyType() == KeyType.Home) {
          table.setSelectedRow(0);
          invalidate();
          return Result.HANDLED;
        } else if (keyStroke.getKeyType() == KeyType.End) {
          table.setSelectedRow(table.getTableModel().getRowCount() - 1);
          invalidate();
          return Result.HANDLED;
        }
        return Result.UNHANDLED;
      }
    };
    this.table.setCellSelection(false);
    this.table.setSelectAction(() -> {
      if (listener != null) {
        if (this.table.getSelectedRow() == 0) {
          listener.goUp();
        } else {
          NamespaceBuilder.Node node =
              this.labelToNodeMap.get(this.table.getTableModel().getRow(this.table.getSelectedRow()).get(0));
          if (node != null) {
            listener.selectElement(node);
          }
        }
      }
    });
    this.addComponent(table.setLayoutData(BorderLayout.Location.CENTER).withBorder(Borders.singleLine()));

    Panel footer = new Panel(new LinearLayout(Direction.HORIZONTAL));
    Button configBtn = new Button("Config");
    configBtn.addListener(button -> {
      gui.addWindowAndWait(panel);
    });
    footer.addComponent(configBtn);
    Button sortLeftBtn = new Button("<- Sort");
    sortLeftBtn.addListener(button -> {
      if (listener != null) {
        listener.sortLeft();
      }
    });
    footer.addComponent(sortLeftBtn);
    Button sortRightBtn = new Button("Sort ->");
    sortRightBtn.addListener(button -> {
      if (listener != null) {
        listener.sortRight();
      }
    });
    footer.addComponent(sortRightBtn);
    Button reverseSortBtn = new Button("Reverse");
    reverseSortBtn.addListener(button -> {
      if (listener != null) {
        listener.reverseSort();
      }
    });
    footer.addComponent(reverseSortBtn);
    Button stopStartBtn = new Button("Stop/Start");
    stopStartBtn.addListener(button -> {
      if (listener != null) {
        listener.onStopStart();
      }
    });
    footer.addComponent(stopStartBtn);
    Button exitBtn = new Button("Exit");
    exitBtn.addListener(button -> {
      if (listener != null) {
        listener.onExit();
      }
    });
    footer.addComponent(exitBtn);

    this.addComponent(footer.setLayoutData(BorderLayout.Location.BOTTOM));
  }

  public void setListener(Listener listener) {
    this.listener = listener;
  }

  public int getSortIndex() {
    return sortIndex;
  }

  public void setSortIndex(int sortIndex) {
    // remove arrow from existing column label.
    String columnLabel = this.table.getTableModel().getColumnLabel(this.sortIndex);
    this.table.getTableModel().setColumnLabel(this.sortIndex,
        columnLabel.substring(0, columnLabel.length() - 4));
    // set the sort index.
    this.sortIndex = sortIndex;
    // add arrow.
    columnLabel = this.table.getTableModel().getColumnLabel(sortIndex);
    this.table.getTableModel().setColumnLabel(sortIndex, columnLabel + (reverseSort ? " [↑]" : " [↓]"));
  }

  public void toggleSortOrder() {
    this.reverseSort = !this.reverseSort;
    String columnLabel = this.table.getTableModel().getColumnLabel(this.sortIndex);
    this.table.getTableModel().setColumnLabel(this.sortIndex,
        columnLabel.substring(0, columnLabel.length() - 4));
    columnLabel = this.table.getTableModel().getColumnLabel(sortIndex);
    this.table.getTableModel().setColumnLabel(sortIndex, columnLabel + (reverseSort ? " [↑]" : " [↓]"));
  }

  public void setVisibleRows(int count) {
    this.table.setVisibleRows(count);
  }

  private Comparator<NamespaceBuilder.Node> getComparator() {
    return (o1, o2) -> {
      if (sortIndex == 0) {
        return o1.getFlattened().compareTo(o2.getFlattened());
      } else if (sortIndex == 1) {
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
      } else {
        return 0;
      }
    };
  }

  public void renderNodes(NamespaceBuilder.Node root, double factor, Collection<NamespaceBuilder.Node> nodes) {
    synchronized (table) {
      @Nullable
      String selectedLabel = null;
      if (table.getSelectedRow() >= 0 && table.getTableModel().getRowCount() > 0) {
        List<String> selectedRow = table.getTableModel().getRow(table.getSelectedRow());
        selectedLabel = selectedRow.get(0);
      }
      int count = table.getTableModel().getRowCount();
      for (int i = 0; i < count; i++) {
        table.getTableModel().removeRow(0);
      }
      labelToNodeMap.clear();
      // add first row (to go up a folder).
      Snapshot snapshot = root.getLag().getSnapshot();
      table.getTableModel().addRow("..", // artificial ".."
          (Math.round(factor * root.getRate().getOneMinuteRate()) + "pps"),
          (Math.round(100.0 * root.getAccessed() / root.getRate().getCount()) + "%"),
          Math.round(snapshot.getMedian()) + "ms",
          Math.round(snapshot.get75thPercentile()) + "ms",
          Math.round(snapshot.get99thPercentile()) + "ms",
          String.valueOf(root.getEstimatedMetricCardinality()),
          String.valueOf(root.getEstimatedHostCardinality()));
      // now sort and add the nodes for this folder.
      Ordering<NamespaceBuilder.Node> ordering = Ordering.from(getComparator());
      if (reverseSort) ordering = ordering.reverse();
      List<NamespaceBuilder.Node> sorted = ordering.sortedCopy(nodes);
      int num = 0;
      int newLocation = 0;
      for (NamespaceBuilder.Node node : sorted) {
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
            String.valueOf(node.getEstimatedHostCardinality()));
        num++;
        if (num > 1000) break;
      }
      table.setSelectedRow(newLocation);
    }
  }

  public void setSamplingRate(double rate) {
    this.samplingRate.setText("Sampling: " + (rate * 100) + "%");
  }

  public void setGlobalPPS(double factor, Meter rate) {
    this.globalPPS.setText("Est. PPS: 1m " +
        Math.round(factor * rate.getOneMinuteRate()) + "pps | 5m " +
        Math.round(factor * rate.getFiveMinuteRate()) + "pps | 15m " +
        Math.round(factor * rate.getFifteenMinuteRate()) + "pps");
  }

  public void setConnecting() {
    connectivityStatus.setForegroundColor(TextColor.ANSI.BLUE);
    connectivityStatus.setText("CONNECTING...");
  }

  public void setConnectionError(String string) {
    connectivityStatus.setForegroundColor(TextColor.ANSI.RED);
    connectivityStatus.setText(string == null ? "DISCONNECTED" : string);
  }

  public void setPath(String path, boolean limited) {
    this.path.setText("> " + path);
    if (limited) {
      this.path.setForegroundColor(TextColor.ANSI.RED);
    } else {
      this.path.setForegroundColor(TextColor.ANSI.BLACK);
    }
  }

  public void setConnected() {
    connectivityStatus.setForegroundColor(TextColor.ANSI.GREEN);
    connectivityStatus.setText("CONNECTED");
  }

  public interface Listener {
    void onExit();

    void onStopStart();

    void sortLeft();

    void sortRight();

    void reverseSort();

    void selectElement(NamespaceBuilder.Node element);

    void goUp();
  }
}
