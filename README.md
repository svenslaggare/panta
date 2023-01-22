# Panta
A tool for analyzing the performance of a Linux system (i.e. one server) by executing queries that correlates different performance metrics like CPU usage and network traffic with high sampling rate without a big cost.

The best way to think about Panta is it is like a time series database but without any data being stored.

## Usage
See `data/events.yaml` for a full example.

## Metrics collectors
* System
  * CPU
  * Memory
  * Network traffic
  * Disk usage & I/O stats
* Docker
  * Per container CPU & memory usage
* RabbitMQ
  * Per queue statistics
* Custom metrics
  * You send your own metrics over a unix domain socket to the panta program in statsd similar format.

## Events
Everything is built around _events_ that fires when an expression evaluates true. To be able to easily define many queries, an event consists of two type of metrics: independent metric (the metric we want to discover performance problem for) and the dependent metrics (the possible variables that the independent metric correlates with). 

In each event, the independent metric is matched with _all_ of the dependent metrics, but a specific instance of the query only refers to one. This makes the event definition generic, making it easy to define many underlying queries.

On every metric sampling, every query is evaluated. But _only_ events that are true generates output. This strategy is necessary in order to discover performance problems that are short-lived, but also don't create a lot of output that is costly to store.

## Aggregates
Rather than just looking at the latest metric values, it is more interesting to look at _aggregates_ of the metric over time. In panta, the following aggregates are supported:
* Average: `avg`
* Variance / standard deviation: `var/std`
* Covariance (two variables): `cov`
* Correlation coefficient (two variables): `corr`

The reasoning behind choosing these are that they can easily be implemented in an incremental fashion. The aggregates are also _over_ time, i.e. last the average value of the last 5 seconds.

## Metrics
A metric is a named floating point value that is collected by collectors at every sampling point. A metric can be divided into sub metrics.
A sub metric is defined as `metric:sub`.

If a metric is divided into sub metrics, then referring to the metric itself will just expand to all sub metrics in queries.

## Querying language
The queries are defined using "standard" infix notation.

The following variables exist:
* `ind`: for referring to the independent variable.
* `dep`: for referring to dependent variable.

The aggregates are used in the following way:
* `avg(ind, 5s)` is the average of `ind` in the last 5 seconds.
* `std(ind, 0.5m)` is the standard deviation of `ind` in the last 30 seconds.

Expressions are supported in aggregates, but aggregates of aggregates can't be created. Operations that uses both independent and dependent metrics are supported though.

The following functions exist:
* `abs`
* `sqrt`
* `square`
* `exp`
* `ln`

### Example
The query `avg(ind, 5s) > 0.1 && corr(ind, dep, 5s) > 0.5` means:

If the average value of independent variable in the last 5 seconds is greater than 0.1 and the correlation between the independent and dependent is greater than 0.5, then an event is created.

## Building
* Requires [cargo](https://rustup.rs/).
* `cargo build --release` - output in `target/release`.
* `cargo deb` - builds a debian package in `target/debian`.
