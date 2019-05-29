package com.wavefront.tools.wftop.panels;

import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.gui2.*;
import com.googlecode.lanterna.gui2.dialogs.MessageDialogBuilder;

import java.util.List;

/**
 * Panel to configure sampling.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class SpyConfigurationPanel extends BasicWindow {

  private final TextBox samplingRateTB;
  private final TextBox separatorCharactersTB;
  private final TextBox usageDaysThresholdTB;
  private final MultiWindowTextGUI gui;

  private Listener listener;

  public SpyConfigurationPanel(MultiWindowTextGUI gui) {
    super("Configuration");
    this.gui = gui;
    this.setHints(List.of(Hint.MODAL, Hint.CENTERED));
    Panel contents = new Panel();
    contents.setLayoutManager(new LinearLayout(Direction.VERTICAL));

    contents.addComponent(new Label("Spy Parameters:"));
    contents.addComponent(new EmptySpace(new TerminalSize(1, 1)));

    // input boxes.
    Panel form = new Panel(new GridLayout(2));
    form.addComponent(new Label("Sampling Rate (0 < r <= 1): "));
    this.samplingRateTB = new TextBox(new TerminalSize(10, 1));
    form.addComponent(samplingRateTB);
    form.addComponent(new Label("Separators: "));
    this.separatorCharactersTB = new TextBox(new TerminalSize(10, 1));
    form.addComponent(separatorCharactersTB);
    form.addComponent(new Label("Usage Lookback (Days): "));
    this.usageDaysThresholdTB = new TextBox(new TerminalSize(10, 1));
    form.addComponent(usageDaysThresholdTB);
    contents.addComponent(form);
    contents.addComponent(new EmptySpace(new TerminalSize(1, 1)));

    // buttons.
    Panel buttons = new Panel(new GridLayout(2));
    Button okBtn = new Button("OK");
    buttons.addComponent(okBtn);
    Button cancelBtn = new Button("Cancel");
    buttons.addComponent(cancelBtn);
    cancelBtn.addListener(button -> {
      gui.removeWindow(this);
    });
    okBtn.addListener(button -> {
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
        }
        int usageDaysThreshold;
        try {
          usageDaysThreshold = Integer.valueOf(usageDaysThresholdTB.getText());
        } catch (NumberFormatException ex) {
          new MessageDialogBuilder().setText("Invalid usage days threshold, must be an integer").setTitle("Invalid Input").
              build().showDialog(gui);
          return;
        }
        if (usageDaysThreshold < 1 || usageDaysThreshold > 60) {
          new MessageDialogBuilder().setText("Invalid usage days threshold, must be > 0 and <= 60").
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

  public void setListener(Listener listener) {
    this.listener = listener;
  }

  public double getSamplingRate() {
    return Double.valueOf(samplingRateTB.getText());
  }

  public String getSeparatorCharacters() {
    return separatorCharactersTB.getText();
  }

  public int getUsageThresholdDays() {
    return Integer.valueOf(usageDaysThresholdTB.getText());
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

  public interface Listener {
    void onProceed(SpyConfigurationPanel panel);
  }
}
