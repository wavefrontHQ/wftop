# Wavefront Top (wftop) [![Build Status](https://travis-ci.org/wavefrontHQ/wftop.svg?branch=master)](https://travis-ci.org/wavefrontHQ/wftop)

[Wavefront](https://docs.wavefront.com/) is a high-performance streaming analytics platform for monitoring and optimizing your environment and applications.

Wavefront Top (wftop) is an interactive tool for exploring the live metric ingestion data shape and to explore which metric namespaces were used in the last X days. Lag information (from wallclock of the machine running wftop compared to the ignested timestamp) is also available which can be used to discover data points that might be lagging behind (likely at the source).

## Requirements
  * Java >= 11
  * maven (to compile)
  * Any Wavefront cluster >= 2019-18.8

## Overview
  * Simply run mvn clean install to compile the tool
  * ./target/wftop is all you need to do afterwards
  
## To start developing

```
$ git clone github.com/wavefronthq/wftop ${directory}
$ cd ${directory}
$ mvn clean install
```

## Contributing
Public contributions are always welcome. Please feel free to report issues or submit pull requests.