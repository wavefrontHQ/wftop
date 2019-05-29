package com.wavefront.tools.wftop.panels;

import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.gui2.*;

/**
 * Panel to input cluster configuration.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class ClusterConfigurationPanel extends Panel {

  private final TextBox clusterUrlTB;
  private final TextBox tokenTB;

  private Listener listener;

  public ClusterConfigurationPanel() {
    this.setLayoutManager(new LinearLayout(Direction.VERTICAL));

    this.addComponent(new Label(" __      __                     _____                      __   \n" +
        "/  \\    /  \\_____ ___  __ _____/ ____\\______  ____   _____/  |_ \n" +
        "\\   \\/\\/   /\\__  \\\\  \\/ // __ \\   __\\\\_  __ \\/  _ \\ /    \\   __\\\n" +
        " \\        /  / __ \\\\   /\\  ___/|  |   |  | \\(  <_> )   |  \\  |  \n" +
        "  \\__/\\  /  (____  /\\_/  \\___  >__|   |__|   \\____/|___|  /__|  \n" +
        "       \\/        \\/          \\/                         \\/      "));
    this.addComponent(new EmptySpace(new TerminalSize(1, 1)));
    this.addComponent(new Label("Please enter the access credentials for the target cluster to interrogate:"));
    this.addComponent(new EmptySpace(new TerminalSize(1, 1)));

    // input boxes.
    Panel form = new Panel(new GridLayout(2));
    form.addComponent(new Label("Cluster: "));
    this.clusterUrlTB = new TextBox(new TerminalSize(30, 1), "metrics.wavefront.com");
    form.addComponent(clusterUrlTB);
    form.addComponent(new Label("Token: "));
    this.tokenTB = new TextBox(new TerminalSize(50, 1), "(with Direct Ingestion permission)") {
      @Override
      protected void afterEnterFocus(FocusChangeDirection direction, Interactable previouslyInFocus) {
        super.afterEnterFocus(direction, previouslyInFocus);
        if (this.getText().equals("(with Direct Ingestion permission)")) {
          this.setText("");
        }
      }

      @Override
      protected void afterLeaveFocus(FocusChangeDirection direction, Interactable nextInFocus) {
        super.afterLeaveFocus(direction, nextInFocus);
        if (this.getText().isEmpty()) {
          this.setText("(with Direct Ingestion permission)");
        }
      }
    };
    form.addComponent(tokenTB);
    this.addComponent(form);
    this.addComponent(new EmptySpace(new TerminalSize(1, 1)));

    // buttons.
    Panel buttons = new Panel(new GridLayout(2));
    Button okBtn = new Button("Proceed");
    buttons.addComponent(okBtn);
    Button cancelBtn = new Button("Cancel");
    buttons.addComponent(cancelBtn);
    cancelBtn.addListener(button -> {
      ClusterConfigurationPanel panel = ClusterConfigurationPanel.this;
      if (panel.listener != null) {
        panel.listener.onCancel(panel);
      }
    });
    okBtn.addListener(button -> {
      ClusterConfigurationPanel panel = ClusterConfigurationPanel.this;
      if (panel.listener != null) {
        panel.listener.onProceed(panel);
      }
    });
    this.addComponent(buttons);
  }

  public void setListener(Listener listener) {
    this.listener = listener;
  }

  public String getClusterUrl() {
    return clusterUrlTB.getText();
  }

  public String getToken() {
    return tokenTB.getText();
  }

  public void set(String clusterUrl, String token) {
    this.clusterUrlTB.setText(clusterUrl);
    this.tokenTB.setText(token);
  }

  public interface Listener {
    void onCancel(ClusterConfigurationPanel panel);

    void onProceed(ClusterConfigurationPanel panel);
  }
}
