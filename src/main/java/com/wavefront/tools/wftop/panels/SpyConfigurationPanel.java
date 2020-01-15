package com.wavefront.tools.wftop.panels;

import com.google.common.collect.ImmutableList;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.gui2.*;
import com.googlecode.lanterna.gui2.dialogs.MessageDialogBuilder;
import com.wavefront.tools.wftop.components.Dimension;
import com.wavefront.tools.wftop.components.Type;

import java.util.regex.Pattern;

/**
 * Panel to configure sampling.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class SpyConfigurationPanel extends BasicWindow {

  private final TextBox samplingRateTB;
  private final TextBox separatorCharactersTB;
  private final TextBox usageDaysThresholdTB;
  private final TextBox maxDepthTB;
  private final TextBox topLevelDepthTB;
  private final TextBox maxChildrenTB;
  private final Label groupLabel = new Label("Group By: ");
  private final Label analysisLabel = new Label("Analysis Dimension:");
  private final Label typeLabel = new Label("Type: ");
  private final Label rateLabel = new Label("Sampling Rate (0 < r <= 0.05): ");

  private RadioBoxList<String> spyRadioBL = new RadioBoxList<>(new TerminalSize(20, 2));
  private RadioBoxList<String> dimensionRadioBL = new RadioBoxList<>(new TerminalSize(20, 4));
  private RadioBoxList<String> typeRadioBL = new RadioBoxList<>(new TerminalSize(20, 6));
  private RadioBoxList<String> ingestionRadioBL = new RadioBoxList<>(new TerminalSize(31, 2));

  private Panel form;
  private boolean startOnGroupBy = false;
  private int dimensionOrTypeIndex;
  private double startRate;
  private String startSeparators;
  private int startUsageDays;
  private int startMaxDepth;
  private int startTopLevelDepth;
  private int startMaxChildren;
  private boolean startOnPoint;
  private boolean spyOnPoint = true;

  private Listener listener;

  public SpyConfigurationPanel(MultiWindowTextGUI gui) {
    super("Configuration");
    this.setHints(ImmutableList.of(Hint.MODAL, Hint.CENTERED));
    Panel contents = new Panel();
    contents.setLayoutManager(new LinearLayout(Direction.VERTICAL));

    contents.addComponent(new EmptySpace(new TerminalSize(1, 1)));

    // input boxes.
    form = new Panel(new GridLayout(2));

    form.addComponent(new Label("Spy On:"));
    spyRadioBL.addItem("Points");
    spyRadioBL.addItem("Id Creations");
    spyRadioBL.setCheckedItemIndex(0);
    spyRadioBL.addListener((int selectedIndex, int previousSelection) -> {
      if (selectedIndex == 0) {
        setSpyOnPoint(form);
      } else if (selectedIndex == 1) {
        setSpyOnID(form);
      }
    });
    form.addComponent(spyRadioBL);

    form.addComponent(rateLabel);
    this.samplingRateTB = new TextBox(new TerminalSize(10, 1));
    this.samplingRateTB.setValidationPattern(Pattern.compile("[0-9.]+"));
    form.addComponent(samplingRateTB);

    form.addComponent(new Label("Separators: "));
    this.separatorCharactersTB = new TextBox(new TerminalSize(10, 1));
    form.addComponent(separatorCharactersTB);

    form.addComponent(new Label("Usage Lookback (Days): "));
    this.usageDaysThresholdTB = new TextBox(new TerminalSize(10, 1));
    this.usageDaysThresholdTB.setValidationPattern(Pattern.compile("[0-9]+"));
    form.addComponent(usageDaysThresholdTB);

    form.addComponent(new Label("Max Tree Depth:"));
    this.maxDepthTB = new TextBox(new TerminalSize(10, 1));
    this.maxDepthTB.setValidationPattern(Pattern.compile("[0-9]+"));
    form.addComponent(maxDepthTB);

    form.addComponent(new Label("Top Level Folder Depth:"));
    this.topLevelDepthTB = new TextBox(new TerminalSize(10, 1));
    this.topLevelDepthTB.setValidationPattern(Pattern.compile("[0-9]+"));
    form.addComponent(topLevelDepthTB);

    form.addComponent(new Label("Max Children Per Node:"));
    this.maxChildrenTB = new TextBox(new TerminalSize(10, 1));
    this.maxChildrenTB.setValidationPattern(Pattern.compile("[0-9]+"));
    form.addComponent(maxChildrenTB);

    form.addComponent(analysisLabel);
    dimensionRadioBL.addItem("Metric");
    dimensionRadioBL.addItem("Host");
    dimensionRadioBL.addItem("Point Tag");
    dimensionRadioBL.addItem("Point Tag Key");
    dimensionRadioBL.setCheckedItemIndex(0);
    form.addComponent(dimensionRadioBL);

    typeRadioBL.addItem("Metric");
    typeRadioBL.addItem("Host");
    typeRadioBL.addItem("Point Tag");
    typeRadioBL.addItem("Histogram");
    typeRadioBL.addItem("Span");
    typeRadioBL.setCheckedItemIndex(0);

    form.addComponent(groupLabel);
    ingestionRadioBL.addItem("None");
    ingestionRadioBL.addItem("Wavefront Ingestion Source");
    ingestionRadioBL.setCheckedItemIndex(0);
    form.addComponent(ingestionRadioBL);

    contents.addComponent(form);
    contents.addComponent(new EmptySpace(new TerminalSize(1, 1)));

    // buttons.
    Panel buttons = new Panel(new GridLayout(2));
    Button okBtn = new Button("OK");
    buttons.addComponent(okBtn);
    Button cancelBtn = new Button("Cancel");
    buttons.addComponent(cancelBtn);
    cancelBtn.addListener(button -> {
      resetConfigurations(form);
      gui.removeWindow(this);
    });
    okBtn.addListener(button -> {
      this.spyOnPoint = form.containsComponent(dimensionRadioBL);
      startParameters(spyOnPoint);
      SpyConfigurationPanel panel = SpyConfigurationPanel.this;
      if (panel.listener != null) {
        // validate configuration.
        double rate;
        try {
          rate = Double.valueOf(samplingRateTB.getText());
        } catch (NumberFormatException ex) {
          new MessageDialogBuilder().setText("Invalid sample rate, must be a number").setTitle("Invalid Input").build().
              showDialog(gui);
          return;
        }
        if (rate <= 0 || rate > 1) {
          new MessageDialogBuilder().setText("Invalid sample rate, must be > 0 and <= 1").setTitle("Invalid Input").
              build().showDialog(gui);
          return;
        } else if (spyOnPoint && rate > 0.05) {
          new MessageDialogBuilder().setText("Invalid sample rate, must be > 0 and <= 0.05").
              setTitle("Invalid Input").build().showDialog(gui);
          return;
        }
        int usageDaysThreshold;
        try {
          usageDaysThreshold = Integer.valueOf(usageDaysThresholdTB.getText());
        } catch (NumberFormatException ex) {
          new MessageDialogBuilder().setText("Invalid usage days threshold, must be an integer").setTitle("Invalid Input").
              build().showDialog(gui);
          return;
        }
        if ((usageDaysThreshold < 1 || usageDaysThreshold > 60) && spyOnPoint) {
          new MessageDialogBuilder().setText("Invalid usage days threshold, must be > 0 and <= 60").
              setTitle("Invalid Input").build().showDialog(gui);
          return;
        }
        int maxDepth;
        try {
          maxDepth = Integer.valueOf(maxDepthTB.getText());
        } catch (NumberFormatException ex) {
          new MessageDialogBuilder().setText("Invalid max depth, must be an integer").setTitle("Invalid Input").
              build().showDialog(gui);
          return;
        }
        if (maxDepth < 1) {
          new MessageDialogBuilder().setText("Invalid max depth, must be > 0").
              setTitle("Invalid Input").build().showDialog(gui);
          return;
        }
        int topLevelDepth;
        try {
          topLevelDepth = Integer.valueOf(topLevelDepthTB.getText());
        } catch (NumberFormatException ex) {
          new MessageDialogBuilder().setText("Invalid top-level folder depth, must be an integer").setTitle("Invalid Input").
              build().showDialog(gui);
          return;
        }
        if (topLevelDepth < 1 || topLevelDepth > maxDepth) {
          new MessageDialogBuilder().setText("Invalid top-level folder depth, must be > 0 and < Max Tree Depth").
              setTitle("Invalid Input").build().showDialog(gui);
          return;
        }
        int maxChildren;
        try {
          maxChildren = Integer.valueOf(maxChildrenTB.getText());
        } catch (NumberFormatException ex) {
          new MessageDialogBuilder().setText("Invalid max children, must be an integer").setTitle("Invalid Input").
              build().showDialog(gui);
          return;
        }
        if (maxChildren < 1) {
          new MessageDialogBuilder().setText("Invalid max children, must be > 0").
              setTitle("Invalid Input").build().showDialog(gui);
          return;
        }
        panel.listener.onProceed(panel);
      }
      gui.removeWindow(this);
    });
    contents.addComponent(buttons);
    this.setComponent(contents);
  }

  private void setSpyOnPoint(Panel form) {
    usageDaysThresholdTB.setReadOnly(false);
    form.removeComponent(typeLabel);
    form.removeComponent(typeRadioBL);
    form.addComponent(analysisLabel);
    form.addComponent(dimensionRadioBL);
    if (!form.containsComponent(ingestionRadioBL)) {
      form.addComponent(groupLabel);
      form.addComponent(ingestionRadioBL);
    }
    setSamplingRate(0.01);
    rateLabel.setText("Sampling Rate (0 < r <= 0.05): ");
  }

  private void setSpyOnID(Panel form) {
    usageDaysThresholdTB.setText("7").setReadOnly(true);
    form.removeComponent(groupLabel);
    form.removeComponent(ingestionRadioBL);
    form.removeComponent(analysisLabel);
    form.removeComponent(dimensionRadioBL);

    form.addComponent(typeLabel);
    form.addComponent(typeRadioBL);
    setSamplingRate(0.01);
    rateLabel.setText("Sampling Rate (0 < r <= 1): ");
  }

  public void setListener(Listener listener) {
    this.listener = listener;
  }

  public void startParameters(boolean startOnPoint) {
    if (startOnPoint) {
      dimensionOrTypeIndex = dimensionRadioBL.getCheckedItemIndex();
      startOnGroupBy = (ingestionRadioBL.getCheckedItemIndex() == 1);
    } else {
      dimensionOrTypeIndex = typeRadioBL.getCheckedItemIndex();
    }
    this.startOnPoint = startOnPoint;
    startRate = getSamplingRate();
    startSeparators = getSeparatorCharacters();
    startUsageDays = getUsageThresholdDays();
    startMaxDepth = getMaxDepth();
    startTopLevelDepth = getTopLevelDepth();
    startMaxChildren = getMaxChildren();
  }

  /**
   * Reverts configurations when user cancels.
   */
  private void resetConfigurations(Panel form) {
    setSamplingRate(startRate);
    setSeparatorCharacters(startSeparators);
    setUsageDaysThreshold(startUsageDays);
    setMaxDepth(startMaxDepth);
    setTopLevelDepth(startTopLevelDepth);
    setMaxChildren(startMaxChildren);
    spyRadioBL.setCheckedItemIndex((startOnPoint) ? 0 : 1);
    if (startOnPoint) {
      //Started on Point but currently was on ID screen
      if (!form.containsComponent(dimensionRadioBL)) {
        form.addComponent(analysisLabel);
        form.addComponent(dimensionRadioBL);
        dimensionRadioBL.setCheckedItemIndex(dimensionOrTypeIndex);

        form.addComponent(groupLabel);
        form.addComponent(ingestionRadioBL);
        ingestionRadioBL.setCheckedItemIndex((startOnGroupBy) ? 1 : 0);
      } else {
        if (dimensionRadioBL.getCheckedItemIndex() != dimensionOrTypeIndex) {
          dimensionRadioBL.setCheckedItemIndex(dimensionOrTypeIndex);
        }
        ingestionRadioBL.setCheckedItemIndex((startOnGroupBy) ? 1 : 0);
      }
    }
    //Started on ID but currently on Point Screen
    else if (!form.containsComponent(typeRadioBL)) {
      form.addComponent(typeLabel);
      form.addComponent(typeRadioBL);
      typeRadioBL.setCheckedItemIndex(dimensionOrTypeIndex);
    } else {
      typeRadioBL.setCheckedItemIndex(dimensionOrTypeIndex);
    }
  }

  public double getSamplingRate() {
    return Double.valueOf(samplingRateTB.getText());
  }

  public String getSeparatorCharacters() {
    return this.separatorCharactersTB.getText();
  }

  public int getUsageThresholdDays() {
    return Integer.valueOf(usageDaysThresholdTB.getText());
  }

  public int getMaxDepth() {
    return Integer.parseInt(this.maxDepthTB.getText());
  }

  public int getTopLevelDepth() { return Integer.parseInt(this.topLevelDepthTB.getText()); }

  public int getMaxChildren() {
    return Integer.parseInt(maxChildrenTB.getText());
  }

  public Dimension getDimension() {
    switch (dimensionRadioBL.getCheckedItem()) {
      case "Host":
        return Dimension.HOST;
      case "Point Tag Key":
        return Dimension.POINT_TAG_KEY;
      case "Point Tag":
        return Dimension.POINT_TAG;
      case "Metric":
      default:
        return Dimension.METRIC;
    }
  }

  public Type getType() {
    switch (typeRadioBL.getCheckedItem()) {
      case "Host":
        return Type.HOST;
      case "Point Tag":
        return Type.POINT_TAG;
      case "Histogram":
        return Type.HISTOGRAM;
      case "Span":
        return Type.SPAN;
      case "Metric":
      default:
        return Type.METRIC;
    }
  }

  public boolean getIngestionSource() {
    return ingestionRadioBL.getCheckedItem().equals("Wavefront Ingestion Source");
  }

  public void setSpyOn(boolean spyOnPoint) {
    this.spyOnPoint = spyOnPoint;
    this.spyRadioBL.setCheckedItemIndex((spyOnPoint) ? 0 : 1);
  }

  /**
   * Check corresponding item based on spyOn and dimension.
   */
  public void setSpyDimension(boolean spyOnPoint, String dimension) {
    switch (dimension) {
      case "METRIC":
        if (spyOnPoint) dimensionRadioBL.setCheckedItemIndex(0);
        else typeRadioBL.setCheckedItemIndex(0);
        break;
      case "HOST":
        if (spyOnPoint) dimensionRadioBL.setCheckedItemIndex(1);
        else typeRadioBL.setCheckedItemIndex(1);
        break;
      case "POINT_TAG":
        if (spyOnPoint) dimensionRadioBL.setCheckedItemIndex(2);
        else typeRadioBL.setCheckedItemIndex(2);
        break;
      case "POINT_TAG_KEY":
        dimensionRadioBL.setCheckedItemIndex(3);
        break;
      case "HISTOGRAM":
        typeRadioBL.setCheckedItemIndex(3);
        break;
      case "SPAN":
        typeRadioBL.setCheckedItemIndex(4);
      default:
        if (spyOnPoint) dimensionRadioBL.setCheckedItemIndex(0);
        else typeRadioBL.setCheckedItemIndex(0);
    }
  }

  public void setSamplingRate(double rate) {
    this.samplingRateTB.setText(String.valueOf(rate));
  }

  public void setSeparatorCharacters(String characters) {
    this.separatorCharactersTB.setText(characters);
  }

  public void setUsageDaysThreshold(int days) {
    this.usageDaysThresholdTB.setText(String.valueOf(days));
  }

  public void setGroupBy() {
    this.ingestionRadioBL.setCheckedItemIndex(1);
  }

  public void setMaxDepth(int depth) {
    this.maxDepthTB.setText(String.valueOf(depth));
  }

  public void setTopLevelDepth(int topLevelDepth) {
    this.topLevelDepthTB.setText(String.valueOf(topLevelDepth)); }

  public void setMaxChildren(int maxChildren) {
    this.maxChildrenTB.setText(String.valueOf(maxChildren));
  }

  public boolean getSpyOnPoint() {
    return this.spyOnPoint;
  }

  public interface Listener {
    void onProceed(SpyConfigurationPanel panel);
  }
}
