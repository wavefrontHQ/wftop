package com.wavefront.tools.wftop;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.gui2.*;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;
import com.wavefront.tools.wftop.components.*;
import com.wavefront.tools.wftop.panels.*;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class WavefrontTop {
  private static final Logger log = Logger.getLogger("wftop");

  private final Timer timer = new Timer(true);
  private final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private final ClusterConfigurationPanel clusterConfigurationPanel = new ClusterConfigurationPanel();
  private final PointsSpy pointsSpy = new PointsSpy();
  private final AtomicInteger backendCount = new AtomicInteger(0);
  private final List<Node> breadCrumbs = new ArrayList<>();
  /**
   * What are we analyzing (metric names? hosts? point tags?)
   */
  private Dimension analysisDimension = Dimension.METRIC;
  private Type IDType = Type.METRIC;
  private RootNode root = new RootNode("root");
  private boolean groupByIngestionSource = false;
  private boolean spyOnPoint = true;
  /**
   * Change Panel when spying on Point or Id.
   */
  private NamespacePanel namespacePanel;
  private AtomicBoolean exit;
  private BasicWindow pointsSpyWindow;
  private Screen screen;

  @Parameter(names = "--log", description = "Log to console")
  private boolean logToConsole = false;

  @Parameter(names = "--emulator", description = "Force console emulator")
  private boolean emulator = false;

  @Nullable
  @Parameter(names = "--token", description = "Wavefront Token")
  private String token = null;

  @Nullable
  @Parameter(names = "--cluster", description = "Wavefront Cluster (metrics.wavefront.com)")
  private String cluster = null;

  @Parameter(names = {"-h", "--help"}, description = "Display flag options", help = true)
  private boolean help = false;

  @Nullable
  @Parameter(names = "--export", description = "Export wftop data. " +
      "Specify with output (-f)ile and (-t)ime in seconds")
  private boolean exportData = false;

  /**
   * Export data flags to set Point Spy.
   */
  @Parameter(names = {"-spy", "-spy-on"}, description = "Spy on point or Id Creations")
  private String spyOnArg = "POINT";

  @Parameter(names = {"-dim", "-dimension"}, description = "Analysis dimension or Id type")
  private String dimenArg = "METRIC";

  @Parameter(names = {"-g", "-group"}, description = "Group By")
  private boolean groupByArg = false;

  @Parameter(names = {"-r", "-rate"}, description = "Sample rate")
  private double rateArg = 0.01;

  @Parameter(names = {"-sep", "-separators"}, description = "Separators")
  private String separatorsArg = ".-_=";

  @Parameter(names = "-days", description = "Usage lookback days")
  private int usageDaysArg = 7;

  @Parameter(names = {"-dep", "-depth"}, description = "Maximum depth")
  private int depthArg = 10;

  @Parameter(names = {"-top", "-top-level"}, description = "Top-level folder depth")
  private int topLevelArg = 1;

  @Parameter(names = {"-c", "-children"}, description = "Maximum child per node")
  private int maxChildrenArg = 1000;

  @Nullable
  @Parameter(names = {"-f", "-file"}, description = "File to save exported data. " +
      "Specify with --export and (-t)ime in seconds")
  private String exportFile = null;

  @Parameter(names = {"-t", "-time", "-time-in-seconds"}, description = "Export timer in seconds." +
      " Specify with --export and output (-f)ile")
  private long exportTime = 0;

  public static void main(String[] args) {
    WavefrontTop wavefrontTop = new WavefrontTop();
    JCommander jCommander = JCommander.newBuilder().addObject(wavefrontTop).build();
    try {
      jCommander.parse(args);
      wavefrontTop.validateArgs();
      if (wavefrontTop.help) {
        jCommander.usage();
        System.exit(0);
      }
      wavefrontTop.run();
    } catch (ParameterException pe) {
      System.out.println("ParameterException: " + pe.getMessage());
      System.out.println("Run ./target/wftop with -h or --help to view flag options");
      System.exit(1);
    }
  }

  private void run() {
    if (!logToConsole) {
      LogManager.getLogManager().reset();
      Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
      globalLogger.setLevel(java.util.logging.Level.OFF);
    }
    DefaultTerminalFactory defaultTerminalFactory = new DefaultTerminalFactory();
    screen = null;
    try {
      Terminal terminal = emulator ? defaultTerminalFactory.createTerminalEmulator() :
          defaultTerminalFactory.createTerminal();
      screen = new TerminalScreen(terminal);
      screen.startScreen();
      screen.setCursorPosition(null);
      // Create gui and start gui
      MultiWindowTextGUI gui =
          new MultiWindowTextGUI(screen, new DefaultWindowManager(),
              new EmptySpace(TextColor.ANSI.BLACK));
      SpyConfigurationPanel spyConfigurationPanel = new SpyConfigurationPanel(gui);
      PointsNamespacePanel pointsNamespacePanel = new PointsNamespacePanel(spyConfigurationPanel, gui);
      IdNamespacePanel idNamespacePanel = new IdNamespacePanel(spyConfigurationPanel, gui);
      this.spyOnPoint = spyOnArg.equals("POINT");
      namespacePanel = (spyOnPoint) ? pointsNamespacePanel : idNamespacePanel;
      namespacePanel.setExportData(exportData, exportFile);

      root.setSeparatorCharacters(separatorsArg);
      root.setMaxDepth(depthArg);
      root.setTopLevelDepth(topLevelArg);
      root.setMaxChildren(maxChildrenArg);
      groupByIngestionSource = spyOnPoint && groupByArg;
      if (spyOnPoint) analysisDimension = getPointDimension(dimenArg);
      setSpyConfigurationPanel(spyConfigurationPanel, pointsNamespacePanel, idNamespacePanel);

      if (token != null && cluster != null) {
        clusterConfigurationPanel.set(cluster, token);
      } else {
        if (collectClusterConfiguration(gui)) return;
      }

      setPointsSpy(clusterConfigurationPanel);
      pointsSpy.start();
      stopwatch.start();
      breadCrumbs.add(root);
      if (!groupByIngestionSource) breadCrumbs.add(root.getDefaultRoot());
      // begin spying.
      if (spyPoints(gui)) return;
    } catch (Throwable t) {
      t.printStackTrace();
      if (screen != null) {
        try {
          screen.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      System.exit(1);
    } finally {
      try {
        screen.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      System.exit(0);
    }
  }

  private void reset() {
    breadCrumbs.clear();
    root.reset();
    breadCrumbs.add(root);
    if (!groupByIngestionSource) breadCrumbs.add(root.getDefaultRoot());
    computePath();
  }

  private void setupNamespacePanelRefresh(MultiWindowTextGUI gui) {
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        double samplingRate = pointsSpy.getSamplingRate();
        namespacePanel.setGlobalPPS(Math.max(1, backendCount.get()) / samplingRate, root.getRate());
        namespacePanel.setStopwatchTime(exportTime - stopwatch.elapsed(TimeUnit.SECONDS));
        namespacePanel.setSamplingRate(samplingRate);
        namespacePanel.setVisibleRows(gui.getScreen().getTerminalSize().getRows() - 10);
        if (pointsSpy.isConnected()) {
          refreshNamespacePanel(samplingRate);
        }
      }
    }, 1000, 1000);
  }

  private void refreshNamespacePanel(double samplingRate) {
    if (breadCrumbs.size() >= 1) {
      Node node = breadCrumbs.get(breadCrumbs.size() - 1);
      if (exportData && stopwatch.elapsed(TimeUnit.SECONDS) == exportTime) {
        namespacePanel.renderNodes(node, Math.max(1, backendCount.get()) / samplingRate,
            node.getNodes().values(), true);
        try {
          screen.close();
        } catch (IOException e) {
          e.printStackTrace();
          System.exit(1);
        }
        System.exit(0);
      }
      namespacePanel.renderNodes(node, Math.max(1, backendCount.get()) / samplingRate,
          node.getNodes().values(), false);
      computePath();
    }
  }

  private boolean collectClusterConfiguration(MultiWindowTextGUI gui) {
    BasicWindow clusterConfigWindow = new BasicWindow();
    clusterConfigWindow.setHints(Collections.singletonList(Window.Hint.CENTERED));

    File lastKnownClusterConfig = new File(System.getProperty("user.home"), ".wftop_cluster");
    final Path path = new File(lastKnownClusterConfig.toURI()).toPath();
    if (lastKnownClusterConfig.exists() && lastKnownClusterConfig.canRead()) {
      try {
        List<String> strings = Files.readAllLines(path);
        if (strings.size() >= 2) {
          String clusterUrl = strings.get(0);
          String token = strings.get(1);
          clusterConfigurationPanel.set(clusterUrl, token);
        }
      } catch (IOException e) {
        log.log(Level.WARNING, "Cannot read .wftop_cluster", e);
      }
    }

    AtomicBoolean exit = new AtomicBoolean(false);
    clusterConfigurationPanel.setListener(new ClusterConfigurationPanel.Listener() {
      @Override
      public void onCancel(ClusterConfigurationPanel panel) {
        exit.set(true);
        clusterConfigWindow.close();
      }

      @Override
      public void onProceed(ClusterConfigurationPanel panel) {
        clusterConfigWindow.close();
      }
    });
    clusterConfigWindow.setComponent(clusterConfigurationPanel);
    gui.addWindowAndWait(clusterConfigWindow);
    if (exit.get()) {
      System.err.println("Cancel");
      return true;
    }
    if (!lastKnownClusterConfig.exists() || lastKnownClusterConfig.canWrite()) {
      try {
        Files.write(
            path,
            ImmutableList.of(clusterConfigurationPanel.getClusterUrl(), clusterConfigurationPanel.getToken()));
      } catch (IOException e) {
        log.log(Level.WARNING, "Cannot write .wftop_cluster", e);
      }
    }
    return false;
  }

  private void setPointsSpy(ClusterConfigurationPanel clusterConfigurationPanel) {
    pointsSpy.setSpyOn(spyOnPoint);
    pointsSpy.setSamplingRate(rateArg);
    pointsSpy.setUsageDaysThreshold(usageDaysArg);

    if (spyOnPoint) {
      pointsSpy.setParameters(clusterConfigurationPanel.getClusterUrl(),
          clusterConfigurationPanel.getToken(), null, null, null,
          rateArg, usageDaysArg);
    } else {
      pointsSpy.setParameters(clusterConfigurationPanel.getClusterUrl(),
          clusterConfigurationPanel.getToken(), null, null, rateArg);
      IDType = getIDType(dimenArg);
      pointsSpy.setTypePrefix(IDType);
    }
    pointsSpy.setListener(new PointsSpy.Listener() {
      @Override
      public void onBackendCountChanges(PointsSpy pointsSpy, int numBackends) {
        backendCount.set(numBackends);
        reset();
      }

      @Override
      public void onIdReceived(PointsSpy pointsSpy, Type type, String name) {
        root.accept(name);
      }

      @Override
      public void onMetricReceived(PointsSpy pointsSpy, boolean accessed, String metric, String host,
                                   Multimap<String, String> pointTags, long timestamp, double value) {
        root.accept(analysisDimension, groupByIngestionSource, accessed, metric, host,
            pointTags, timestamp, value);
      }

      @Override
      public void onConnectivityChanged(PointsSpy pointsSpy, boolean connected,
                                        @Nullable String message) {
        if (connected) {
          namespacePanel.setConnected();
        } else {
          namespacePanel.setConnectionError(message);
        }
      }

      @Override
      public void onConnecting(PointsSpy pointsSpy) {
        namespacePanel.setConnecting();
      }
    });
  }

  private void setSpyConfigurationPanel(SpyConfigurationPanel spyConfigurationPanel,
                                        PointsNamespacePanel pointsNamespacePanel,
                                        IdNamespacePanel idNamespacePanel) {
    spyConfigurationPanel.setSpyOn(spyOnPoint);
    spyConfigurationPanel.setSpyDimension(spyOnPoint, dimenArg);

    spyConfigurationPanel.setSamplingRate(rateArg);
    spyConfigurationPanel.setUsageDaysThreshold(usageDaysArg);
    spyConfigurationPanel.setGroupBy();

    spyConfigurationPanel.setSeparatorCharacters(root.getSeparatorCharacters());
    spyConfigurationPanel.setMaxDepth(root.getMaxDepth());
    spyConfigurationPanel.setTopLevelDepth(root.getTopLevelDepth());
    spyConfigurationPanel.setMaxChildren(root.getMaxChildren());

    spyConfigurationPanel.startParameters(this.spyOnPoint);
    spyConfigurationPanel.setListener(panel -> {
      this.spyOnPoint = panel.getSpyOnPoint();
      pointsSpy.setSpyOn(panel.getSpyOnPoint());
      pointsSpy.setSamplingRate(panel.getSamplingRate());
      pointsSpy.setUsageDaysThreshold(panel.getUsageThresholdDays());

      root.setSeparatorCharacters(panel.getSeparatorCharacters());
      root.setMaxDepth(panel.getMaxDepth());
      root.setTopLevelDepth(panel.getTopLevelDepth());
      root.setMaxChildren(panel.getMaxChildren());

      if (spyOnPoint) {
        analysisDimension = panel.getDimension();
        groupByIngestionSource = panel.getIngestionSource();
      } else {
        groupByIngestionSource = false;
        IDType = panel.getType();
        pointsSpy.setTypePrefix(IDType);
      }
      pointsSpy.start();
      reset();
      namespacePanel = (spyOnPoint) ? pointsNamespacePanel : idNamespacePanel;
      setNamespacePanel(exit, pointsSpyWindow);
    });
  }

  private void setNamespacePanel(AtomicBoolean exit, BasicWindow pointsSpyWindow) {
    pointsSpyWindow.setComponent(namespacePanel);
    namespacePanel.setListener(new NamespacePanel.Listener() {
      @Override
      public void onExit() {
        exit.set(true);
        pointsSpyWindow.close();
      }

      @Override
      public void onStopStart() {
        if (pointsSpy.isConnected()) {
          pointsSpy.stop();
        } else {
          pointsSpy.start();
        }
      }

      @Override
      public void sortLeft() {
        namespacePanel.setSortIndex(
            Math.max(0, namespacePanel.getSortIndex() - 1));
        refreshNamespacePanel(pointsSpy.getSamplingRate());
      }

      @Override
      public void sortRight() {
        namespacePanel.setSortIndex(
            Math.min(namespacePanel.getTableColumnCount() - 1, namespacePanel.getSortIndex() + 1));
        refreshNamespacePanel(pointsSpy.getSamplingRate());
      }

      @Override
      public void reverseSort() {
        namespacePanel.toggleSortOrder();
        refreshNamespacePanel(pointsSpy.getSamplingRate());
      }

      @Override
      public void selectElement(Node<?> element) {
        while (true) {
          if (element != null) {
            breadCrumbs.add(element);
            if (element.getNodes().size() != 1) {
              break;
            }
            element = Iterables.getOnlyElement(element.getNodes().values());
          } else {
            break;
          }
        }
        computePath();
        refreshNamespacePanel(pointsSpy.getSamplingRate());
      }

      @Override
      public void goUp() {
        while (breadCrumbs.size() > 1) {
          if (breadCrumbs.size() == 2 && !groupByIngestionSource) break;
          breadCrumbs.remove(breadCrumbs.size() - 1);
          if (breadCrumbs.get(breadCrumbs.size() - 1).getNodes().size() != 1) {
            break;
          }
        }
        computePath();
        refreshNamespacePanel(pointsSpy.getSamplingRate());
      }
    });
  }

  private boolean spyPoints(MultiWindowTextGUI gui) {
    exit = new AtomicBoolean(false);
    pointsSpyWindow = new BasicWindow("Wavefront Top");
    pointsSpyWindow.setHints(Collections.singletonList(Window.Hint.FULL_SCREEN));
    setNamespacePanel(exit, pointsSpyWindow);
    setupNamespacePanelRefresh(gui);
    gui.addWindowAndWait(pointsSpyWindow);
    return exit.get();
  }

  /**
   * Path built as: [rootNode, sourceNode, NamespaceNode root, NamespaceNode node, ..]
   */
  private void computePath() {
    StringBuilder path = new StringBuilder();
    if (groupByIngestionSource) {
      path.append("GROUP BY SOURCE: ");
      if (breadCrumbs.size() >= 2) {
        path.append(breadCrumbs.get(1).getValue()).append("\n> ").append(analysisDimension).append(": ");
      }
    } else path.append((spyOnPoint) ? analysisDimension : IDType).append(": ");
    boolean limited = false;
    for (int i = 0; i < breadCrumbs.size(); i++) {
      limited |= breadCrumbs.get(i).isLimited();
      if (i >= 2) path.append(breadCrumbs.get(i).getValue());
    }
    namespacePanel.setRootPath(path.toString());
    if (limited) {
      path.append(" [EXPANSION HALTED (PER CONFIG)]");
    }
    if (backendCount.get() == 0) {
      path.append(" [" + ((spyOnPoint) ? "PPS and %ACCESSED" : "CPS") + " IS NOT AVAILABLE/ACCURATE]");
    }
    namespacePanel.setPath(path.toString(), limited || backendCount.get() == 0);
  }

  /**
   * Validate flag values and combinations for spy and export.
   */
  private void validateArgs() {
    //check spy configuration args
    spyOnArg = spyOnArg.toUpperCase();
    if (!(spyOnArg.equals("POINT") || (spyOnArg.equals("ID")))) {
      throw new ParameterException("Spy On flag must be POINT or ID");
    }
    dimenArg = dimenArg.toUpperCase();
    StringBuilder spyError = new StringBuilder();
    if (spyOnArg.equals("POINT") && !(isPointDimension(dimenArg))) {
      spyError.append("Point dimensions: [METRIC, HOST, POINT_TAG_KEY, POINT_TAG]");
    } else if (spyOnArg.equals("ID") && !(isIDType(dimenArg))) {
      spyError.append("ID types: [METRIC, HOST, POINT_TAG, HISTOGRAM, SPAN]");
    }
    if (spyError.length() > 0) {
      throw new ParameterException("Cannot spy on given dimension, " + spyError);
    }
    if (spyOnArg.equals("POINT") && (rateArg < 0 || rateArg > 0.05)) {
      throw new ParameterException("Invalid sample rate, must be > 0 and <= 0.05 for POINT");
    } else if (spyOnArg.equals("ID") && (rateArg < 0 || rateArg > 1.0)) {
      throw new ParameterException("Invalid sample rate, must be > 0 and <= 1 for ID");
    }
    if (usageDaysArg < 1 || usageDaysArg > 60) {
      throw new ParameterException("Invalid usage days threshold, must be > 0 and <= 60");
    }
    if (depthArg < 1) {
      throw new ParameterException("Invalid max depth, must be > 0");
    }
    if (topLevelArg < 1 || topLevelArg > depthArg) {
      throw new ParameterException("Invalid max depth, must be > 0 and < max depth");
    }
    if (maxChildrenArg < 1) {
      throw new ParameterException("Invalid max children, must be > 0");
    }

    //check file and time given if exporting data
    if (exportData) {
      StringBuilder exportError = new StringBuilder();
      if (exportFile == null) {
        exportError.append("output file must be given");
      } else if (!exportFile.endsWith(".csv")) {
        exportFile = exportFile + ".csv";
      }
      if (exportTime == 0) {
        exportError.append(((exportFile == null) ? " AND " : "") +
            "length of timer in seconds must be given");
      }
      if (exportTime < 0) {
        exportError.append("length of timer must be greater than 0 seconds");
      } else {
        this.exportTime += 1;
      }
      if (exportError.length() > 0) {
        throw new ParameterException("To export data, " + exportError.toString());
      }
    } else {
      if (exportFile != null || exportTime != 0)
        throw new ParameterException("--export flag must be set");
    }
  }

  private boolean isPointDimension(String dimension) {
    switch (dimension) {
      case "METRIC":
      case "HOST":
      case "POINT_TAG_KEY":
      case "POINT_TAG":
        return true;
      default:
        return false;
    }
  }

  private boolean isIDType(String dimension) {
    switch (dimension) {
      case "HOST":
      case "METRIC":
      case "POINT_TAG":
      case "HISTOGRAM":
      case "SPAN":
        return true;
      default:
        return false;
    }
  }

  private Dimension getPointDimension(String dimension) {
    switch (dimension) {
      case "HOST":
        return Dimension.HOST;
      case "POINT_TAG_KEY":
        return Dimension.POINT_TAG_KEY;
      case "POINT_TAG":
        return Dimension.POINT_TAG;
      case "METRIC":
      default:
        return Dimension.METRIC;
    }
  }

  private Type getIDType(String type) {
    switch (type) {
      case "HOST":
        return Type.HOST;
      case "POINT_TAG":
        return Type.POINT_TAG;
      case "HISTOGRAM":
        return Type.HISTOGRAM;
      case "SPAN":
        return Type.SPAN;
      case "METRIC":
      default:
        return Type.METRIC;
    }
  }
}
