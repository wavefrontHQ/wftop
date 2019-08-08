package com.wavefront.tools.wftop;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.gui2.*;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;
import com.wavefront.tools.wftop.components.Dimension;
import com.wavefront.tools.wftop.components.NamespaceBuilder;
import com.wavefront.tools.wftop.components.PointsSpy;
import com.wavefront.tools.wftop.panels.ClusterConfigurationPanel;
import com.wavefront.tools.wftop.panels.PointsNamespacePanel;
import com.wavefront.tools.wftop.panels.SpyConfigurationPanel;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class WavefrontTop {

  private static final Logger log = Logger.getLogger("wftop");

  private final Timer timer = new Timer(true);

  private final ClusterConfigurationPanel clusterConfigurationPanel = new ClusterConfigurationPanel();
  private final PointsSpy pointsSpy = new PointsSpy();
  private final NamespaceBuilder namespaceBuilder = new NamespaceBuilder();
  private final AtomicInteger backendCount = new AtomicInteger(0);
  private final List<NamespaceBuilder.Node> breadCrumbs = new ArrayList<>();

  /**
   * What are we analyzing (metric names? hosts? point tags?)
   */
  private Dimension analysisDimension = Dimension.METRIC;
  private PointsNamespacePanel pointsNamespacePanel;

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

  public static void main(String[] args) {
    WavefrontTop wavefrontTop = new WavefrontTop();
    JCommander.newBuilder().addObject(wavefrontTop).build().parse(args);
    wavefrontTop.run();
  }

  private void run() {
    if (!logToConsole) {
      LogManager.getLogManager().reset();
      Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
      globalLogger.setLevel(java.util.logging.Level.OFF);
    }
    DefaultTerminalFactory defaultTerminalFactory = new DefaultTerminalFactory();
    Screen screen = null;
    try {
      Terminal terminal = emulator ? defaultTerminalFactory.createTerminalEmulator() :
          defaultTerminalFactory.createTerminal();
      screen = new TerminalScreen(terminal);
      screen.startScreen();
      screen.setCursorPosition(null);
      // Create gui and start gui
      MultiWindowTextGUI gui =
          new MultiWindowTextGUI(screen, new DefaultWindowManager(), new EmptySpace(TextColor.ANSI.BLACK));
      SpyConfigurationPanel spyConfigurationPanel = new SpyConfigurationPanel(gui);
      pointsNamespacePanel = new PointsNamespacePanel(spyConfigurationPanel, gui);

      spyConfigurationPanel.setSamplingRate(pointsSpy.getSamplingRate());
      spyConfigurationPanel.setUsageDaysThreshold(pointsSpy.getUsageDaysThreshold());

      spyConfigurationPanel.setSeparatorCharacters(namespaceBuilder.getSeparatorCharacters());
      spyConfigurationPanel.setMaxDepth(namespaceBuilder.getMaxDepth());
      spyConfigurationPanel.setMaxChildren(namespaceBuilder.getMaxChildren());

      spyConfigurationPanel.setListener(panel -> {
        pointsSpy.setSamplingRate(panel.getSamplingRate());
        pointsSpy.setUsageDaysThreshold(panel.getUsageThresholdDays());

        namespaceBuilder.setSeparatorCharacters(panel.getSeparatorCharacters());
        namespaceBuilder.setMaxDepth(panel.getMaxDepth());
        namespaceBuilder.setMaxChildren(panel.getMaxChildren());

        analysisDimension = panel.getDimension();
        pointsSpy.start();
        reset();
      });
      pointsSpy.setListener(new PointsSpy.Listener() {
        @Override
        public void onBackendCountChanges(PointsSpy pointsSpy, int numBackends) {
          backendCount.set(numBackends);
          reset();
        }

        @Override
        public void onMetricReceived(PointsSpy pointsSpy, boolean accessed, String metric, String host,
                                     Multimap<String, String> pointTags, long timestamp, double value) {
          if (analysisDimension == Dimension.METRIC) {
            namespaceBuilder.accept(metric, host, metric, timestamp, value, accessed);
          } else if (analysisDimension == Dimension.HOST) {
            namespaceBuilder.accept(host, host, metric, timestamp, value, accessed);
          } else if (analysisDimension == Dimension.POINT_TAG) {
            // here we are over-counting.
            for (Map.Entry<String, String> entry : pointTags.entries()) {
              namespaceBuilder.accept(entry.getKey() + "=" + entry.getValue(), host, metric, timestamp, value,
                  accessed);
            }
          } else if (analysisDimension == Dimension.POINT_TAG_KEY) {
            // here we are over-counting.
            for (String entry : pointTags.keySet()) {
              namespaceBuilder.accept(entry, host, metric, timestamp, value, accessed);
            }
          }
        }

        @Override
        public void onConnectivityChanged(PointsSpy pointsSpy, boolean connected, @Nullable String message) {
          if (connected) {
            pointsNamespacePanel.setConnected();
          } else {
            pointsNamespacePanel.setConnectionError(message);
          }
        }

        @Override
        public void onConnecting(PointsSpy pointsSpy) {
          pointsNamespacePanel.setConnecting();
        }
      });
      if (token != null && cluster != null) {
        clusterConfigurationPanel.set(cluster, token);
      } else {
        if (collectClusterConfiguration(gui)) return;
      }
      pointsSpy.setParameters(clusterConfigurationPanel.getClusterUrl(), clusterConfigurationPanel.getToken(),
          null, null, null, 0.01, 7);
      pointsSpy.start();
      breadCrumbs.add(namespaceBuilder.getRoot());
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
    namespaceBuilder.reset();
    breadCrumbs.clear();
    breadCrumbs.add(namespaceBuilder.getRoot());
    computePath();
  }

  private void setupPointsNamespacePanelRefresh(MultiWindowTextGUI gui) {
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        double samplingRate = pointsSpy.getSamplingRate();
        pointsNamespacePanel.setGlobalPPS(Math.max(1, backendCount.get()) / samplingRate,
            namespaceBuilder.getRoot().getRate());
        pointsNamespacePanel.setSamplingRate(samplingRate);
        pointsNamespacePanel.setVisibleRows(gui.getScreen().getTerminalSize().getRows() - 10);
        if (pointsSpy.isConnected()) {
          refreshPointsNamespacePanel(samplingRate);
        }
      }
    }, 1000, 1000);
  }

  private void refreshPointsNamespacePanel(double samplingRate) {
    NamespaceBuilder.Node node = breadCrumbs.get(breadCrumbs.size() - 1);
    pointsNamespacePanel.renderNodes(node, Math.max(1, backendCount.get()) / samplingRate,
        node.getNodes().values());
    computePath();
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

  private boolean spyPoints(MultiWindowTextGUI gui) {
    AtomicBoolean exit = new AtomicBoolean(false);
    BasicWindow pointsSpyWindow = new BasicWindow("Wavefront Top");
    pointsSpyWindow.setHints(Collections.singletonList(Window.Hint.FULL_SCREEN));
    pointsSpyWindow.setComponent(pointsNamespacePanel);
    pointsNamespacePanel.setListener(new PointsNamespacePanel.Listener() {
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
        pointsNamespacePanel.setSortIndex(
            Math.max(0, pointsNamespacePanel.getSortIndex() - 1));
        refreshPointsNamespacePanel(pointsSpy.getSamplingRate());
      }

      @Override
      public void sortRight() {
        pointsNamespacePanel.setSortIndex(
            Math.min(8, pointsNamespacePanel.getSortIndex() + 1));
        refreshPointsNamespacePanel(pointsSpy.getSamplingRate());
      }

      @Override
      public void reverseSort() {
        pointsNamespacePanel.toggleSortOrder();
        refreshPointsNamespacePanel(pointsSpy.getSamplingRate());
      }

      @Override
      public void selectElement(NamespaceBuilder.Node element) {
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
        refreshPointsNamespacePanel(pointsSpy.getSamplingRate());
      }

      @Override
      public void goUp() {
        while (breadCrumbs.size() > 1) {
          breadCrumbs.remove(breadCrumbs.size() - 1);
          if (breadCrumbs.get(breadCrumbs.size() - 1).getNodes().size() != 1) {
            break;
          }
        }
        computePath();
        refreshPointsNamespacePanel(pointsSpy.getSamplingRate());
      }
    });
    setupPointsNamespacePanelRefresh(gui);
    gui.addWindowAndWait(pointsSpyWindow);
    return exit.get();
  }

  private void computePath() {
    StringBuilder path = new StringBuilder();
    path.append(analysisDimension).append(": ");
    boolean limited = false;
    for (NamespaceBuilder.Node node : breadCrumbs) {
      limited |= node.isLimited();
      path.append(node.getValue());
    }
    if (limited) {
      path.append(" [EXPANSION HALTED (PER CONFIG)]");
    }
    if (backendCount.get() == 0) {
      path.append(" [PPS and %ACCESSED IS NOT AVAILABLE/ACCURATE]");
    }
    pointsNamespacePanel.setPath(path.toString(), limited || backendCount.get() == 0);
  }
}
