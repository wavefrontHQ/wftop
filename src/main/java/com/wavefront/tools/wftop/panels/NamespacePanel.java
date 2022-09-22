package com.wavefront.tools.wftop.panels;

import com.google.common.annotations.VisibleForTesting;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.gui2.*;
import com.googlecode.lanterna.gui2.table.Table;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.input.KeyType;
import com.wavefront.tools.wftop.components.Node;

import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.Snapshot;
import org.apache.commons.csv.CSVPrinter;

import javax.annotation.Nullable;
import java.util.*;

/**
 * The major panel of wftop that displays namespaces, pps, access %, etc.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public abstract class NamespacePanel extends Panel {

  protected final Label globalPPS = new Label("");
  protected final Label connectivityStatus = new Label("");
  protected final Label samplingRate = new Label("");
  protected final Label path = new Label("> ");
  protected final Label stopwatchTime = new Label("");
  protected final Panel header, footer;

  protected Listener listener = null;
  protected Table<String> table;
  protected Map<String, Node> labelToNodeMap = new HashMap<>();
  protected int sortIndex = 1;
  protected boolean reverseSort = true;
  protected Button configBtn, stopStartBtn;
  protected String exportFile = null;
  protected String rootPath = null;
  protected CSVPrinter csvPrinter;

  public NamespacePanel(SpyConfigurationPanel panel, MultiWindowTextGUI gui) {
    this.setLayoutManager(new BorderLayout());

    // header.
    header = new Panel(new BorderLayout());
    header.addComponent(globalPPS.withBorder(Borders.singleLine()).
        setLayoutData(BorderLayout.Location.CENTER));
    header.addComponent(connectivityStatus.withBorder(Borders.singleLine()).
        setLayoutData(BorderLayout.Location.LEFT));
    header.addComponent(samplingRate.withBorder(Borders.singleLine()).
        setLayoutData(BorderLayout.Location.RIGHT));
    header.addComponent(path.setLayoutData(BorderLayout.Location.BOTTOM));
    this.addComponent(header.setLayoutData(BorderLayout.Location.TOP));

    this.table = new Table<String>("Namespace") {
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
          Node node =
              this.labelToNodeMap.get(this.table.getTableModel().getRow(this.table.getSelectedRow()).get(0));
          if (node != null) {
            listener.selectElement(node);
          }
        }
      }
    });
    this.addComponent(table.setLayoutData(BorderLayout.Location.CENTER).withBorder(Borders.singleLine()));

    footer = new Panel(new LinearLayout(Direction.HORIZONTAL));
    configBtn = new Button("Config");
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
    stopStartBtn = new Button("Stop/Start");
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

  /**
   * Compares nodes based on columns.
   *
   * @return Comparison value returned.
   */
  protected abstract Comparator<Node> getComparator();

  /**
   * Add first row (to go up a folder).
   */
  protected abstract void addFirstRow(Node root, double factor, Collection<Node> nodes,
                                      Snapshot snapshot, boolean takeSnapshot);

  /**
   * Now sort and add the nodes for this folder.
   */
  protected abstract void addNodes(Node root, double factor, Collection<Node> nodes,
                                   Snapshot snapshot, String selectedLabel, boolean takeSnapshot);

  /**
   * Display global Points per Second (spy on Points) or Creations per Second (spy on Id Creation).
   *
   * @param factor Multiply by backend count to accurately display pps/cps.
   * @param rate   Used to get 1m, 5m, 15m intervals.
   */
  public abstract void setGlobalPPS(double factor, Meter rate);

  /**
   * Create CSV file for exporting data.
   */
  public abstract void setUpCSVWriter();

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

  public void renderNodes(Node root, double factor, Collection<Node> nodes, boolean takeSnapshot) {
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
      Snapshot snapshot = root.getLag().getSnapshot();
      addFirstRow(root, factor, nodes, snapshot, takeSnapshot);
      addNodes(root, factor, nodes, snapshot, selectedLabel, takeSnapshot);
    }
  }

  public void setExportData(boolean exportData, String exportFile) {
    if (exportData) {
      footer.removeComponent(configBtn);
      footer.removeComponent(stopStartBtn);
      header.addComponent(stopwatchTime.withBorder(Borders.singleLine()).
          setLayoutData(BorderLayout.Location.TOP));
      this.exportFile = exportFile;
      setUpCSVWriter();
    }
  }

  public void setStopwatchTime(long time) {
    this.stopwatchTime.setText("Time: " + time + " Seconds");
  }

  public void setSamplingRate(double rate) {
    this.samplingRate.setText("Sampling: " + (rate * 100) + "%");
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

  /**
   * @param path Displayed path for exporting data.
   */
  public void setRootPath(String path) {
    String[] temp = path.split("\\s+");
    if (temp.length > 0) this.rootPath = temp[temp.length - 1];
  }

  @VisibleForTesting
  public String getRootPath() {
    return this.rootPath;
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

    void selectElement(Node<?> element);

    void goUp();
  }

  public int getTableColumnCount() {
    return this.table.getTableModel().getColumnCount();
  }
}
