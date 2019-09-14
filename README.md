# Wavefront Top (wftop) [![Build Status](https://travis-ci.org/wavefrontHQ/wftop.svg?branch=master)](https://travis-ci.org/wavefrontHQ/wftop)

[Wavefront](https://docs.wavefront.com/) is a high-performance streaming analytics platform for monitoring and optimizing your environment and applications.

Wavefront Top (wftop) is an interactive tool for exploring the live metric ingestion data shape and to explore which metric namespaces were used in the last X days. Lag information (from wallclock of the machine running wftop compared to the ignested timestamp) is also available which can be used to discover data points that might be lagging behind (likely at the source).

## Features
  * Slice real-time metrics, host names, point tag keys, point tags into namespaces ("folders")
  * Discover new ID creations by specified type
  * Compute per namespace "pps" (for point tags, counting each occurrence)
  * Compute per namespace "% Accessed" (in the last X days, configurable)
  * Compute median, p75 and p99 lag for timestamps of each namespace (compared to wall-clock of the machine running wftop)
  * Group namespace by proxy or token ingestion source
  * Drill-down into each namespace via selection
  * Customizable separators (defaults to ".", "-", "_", "=")
  * Multiple sort dimensions
  * Start/Stop support
  * Automatic reconnection on server disconnects or network faults
  * Console emulation (--emulator) for high-resolution rendering
  * Credentials storage on "user.home" location for fast start-up (also supports --token and --cluster arguments)

## Screenshots

Setting up wftop (the tool will persist the cluster/token to ~/.wftop)
In the following screenshot: 
* Cluster: is the name of your Wavefront instance (`<instance>.wavefront.com`)
* Token is a [Wavefront API token](https://docs.wavefront.com/wavefront_api.html#generating-an-api-token) for your Wavefront instance. 

![Setup Screen](https://raw.githubusercontent.com/wavefronthq/wftop/master/screenshots/SetupScreen.png)

UI to browse top-level metric namespaces (delimiters are configurable and can be changed in the "Config" screen). Est. PPS estimates the pps sent by a particular namespace and % Accessed indicates how many of those points are found to be access in the last X days (configuration in "Config" screen between 1 and 60). Lag information is also shown which shows the median, p75 and p99 lag of metrics for each namespace. Range is the range of values the metric reports.

![Setup Screen](https://raw.githubusercontent.com/wavefronthq/wftop/master/screenshots/BrowseScreen.png)

Selecting a "folder" allows the user to drilldown into a single metric namespace.

![Setup Screen](https://raw.githubusercontent.com/wavefronthq/wftop/master/screenshots/DrillDown.png)

Configuration screen when spying on points can toggle group by metric ingestion source. Configure sample rate (typical Wavefront clusters allow up to 5% sampling, wftop will automatically scale all pps measurements based on this number and backend data distribution topologies). Separators (each) control how metrics are split up into "folders". Usage lookback (days) control when a metric is considered "used" (a value of 7 means the metric would be considered used if it was accessed in any of the last 7 days).

![Setup Screen](https://raw.githubusercontent.com/wavefronthq/wftop/master/screenshots/PointConfigurationScreen.png)

Group by ingestion source displays token user or proxy host name of ingested points. Points after 2019-30.x are sent into Wavefront with a non-persisted tag. Source displayed as “None” indicates no source tag was sent with the point and source was be determined.

Configuration screen when spying on ID creations can configure the type of id creations to display. Configurations (sampling rate, separators, tree depth and maximum children per node) are similar to spying on point. Usage lookback days does not exist when spying on ID creations.

![Setup Screen](https://raw.githubusercontent.com/wavefronthq/wftop/master/screenshots/IdConfigurationScreen.png)

## Requirements
  * Java >= 1.8
  * maven (to compile)
  * Any Wavefront cluster >= 2019-18.8 (33.8)
    * Older Wavefront clusters will still work with wftop but pps and % Accessed information will not be accurate/available.
  * Group by sources
    * Cluster >= 2019-30.x
    * Older Wavefront cluster points do not have ingestion tag and will be placed under source "None".
  * Spy on ID creations
    * Cluster >= 2019.38.x
    * Older Wavefront clusters will still display Id creations, but cps will not be accurate.

## Overview
  * Simply run ```mvn clean install -DskipTests```  to compile the tool
  * ./target/wftop is all you need to do afterwards
  
## To start developing

```
$ git clone https://github.com/wavefrontHQ/wftop.git ${directory}
$ cd ${directory}
$ mvn clean install
```

## Contributing
Public contributions are always welcome. Please feel free to report issues or submit pull requests.
