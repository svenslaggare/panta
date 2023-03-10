use std::collections::{VecDeque};
use std::time::{Duration};

use fnv::FnvHashMap;

use crate::model::{TimeInterval, TimePoint, ValueId};

pub struct AggregateOperations {
    next_aggregate_id: AggregateId,
    sum_aggregators: FnvHashMap<AggregateId, SumAggregator>,
    min_max_aggregators: FnvHashMap<AggregateId, MinMaxAggregator>,
    single_operations: FnvHashMap<SingleAggregateOperation, AggregateId>,
    dual_operations: FnvHashMap<DualAggregateOperation, AggregateId>
}

impl AggregateOperations {
    pub fn new() -> AggregateOperations {
        AggregateOperations {
            next_aggregate_id: AggregateId(1),
            sum_aggregators: FnvHashMap::default(),
            min_max_aggregators: FnvHashMap::default(),
            single_operations: FnvHashMap::default(),
            dual_operations: FnvHashMap::default()
        }
    }

    pub fn add_average(&mut self, value_id: ValueId, interval: TimeInterval) -> AverageAggregate {
        AverageAggregate(self.add_sum(value_id, interval))
    }

    pub fn add_variance(&mut self, value_id: ValueId, interval: TimeInterval) -> VarianceAggregate {
        let sum_op = self.add_sum(value_id, interval);
        let sum_square_op = self.add_sum_square(value_id, interval);
        VarianceAggregate(sum_op, sum_square_op)
    }

    pub fn add_covariance(&mut self, left: ValueId, right: ValueId, interval: TimeInterval) -> CovarianceAggregate {
        let sum_left_op = self.add_sum(left, interval);
        let sum_right_op = self.add_sum(right, interval);
        let sum_product_op = self.add_sum_product(left, right, interval);
        CovarianceAggregate(sum_left_op, sum_right_op, sum_product_op)
    }

    pub fn add_correlation(&mut self, left: ValueId, right: ValueId, interval: TimeInterval) -> CorrelationAggregate {
        let variance_left_op = self.add_variance(left, interval);
        let variance_right_op = self.add_variance(right, interval);
        let covariance_op = self.add_covariance(left, right, interval);
        CorrelationAggregate(variance_left_op, variance_right_op, covariance_op)
    }

    pub fn add_min(&mut self, value_id: ValueId, interval: TimeInterval) -> MinAggregate {
        MinAggregate(self.add_single(SingleAggregateOperation::Min { value_id, interval: interval.duration() }, interval))
    }

    pub fn add_max(&mut self, value_id: ValueId, interval: TimeInterval) -> MaxAggregate {
        MaxAggregate(self.add_single(SingleAggregateOperation::Max { value_id, interval: interval.duration() }, interval))
    }

    fn add_sum(&mut self, value_id: ValueId, interval: TimeInterval) -> AggregateId {
        self.add_single(SingleAggregateOperation::Sum { value_id, interval: interval.duration() }, interval)
    }

    fn add_sum_square(&mut self, value_id: ValueId, interval: TimeInterval) -> AggregateId {
        self.add_single(SingleAggregateOperation::SumSquare { value_id, interval: interval.duration() }, interval)
    }

    fn add_single(&mut self, op: SingleAggregateOperation, interval: TimeInterval) -> AggregateId {
        *self.single_operations
            .entry(op.clone())
            .or_insert_with(|| {
                let aggregate_id = self.next_aggregate_id;
                match op {
                    SingleAggregateOperation::Sum { .. } | SingleAggregateOperation::SumSquare { .. } => {
                        self.sum_aggregators.insert(aggregate_id, SumAggregator::new(interval));
                    }
                    SingleAggregateOperation::Min { .. } => {
                        self.min_max_aggregators.insert(aggregate_id, MinMaxAggregator::new(MinMaxOperation::Min, interval));
                    }
                    SingleAggregateOperation::Max { .. } => {
                        self.min_max_aggregators.insert(aggregate_id, MinMaxAggregator::new(MinMaxOperation::Max, interval));
                    }
                }

                self.next_aggregate_id.0 += 1;
                aggregate_id
            })
    }

    fn add_sum_product(&mut self, left: ValueId, right: ValueId, interval: TimeInterval) -> AggregateId {
        let op = DualAggregateOperation::SumProduct { left, right, interval: interval.duration() };
        *self.dual_operations
            .entry(op.clone())
            .or_insert_with(|| {
                let aggregate_id = self.next_aggregate_id;
                self.sum_aggregators.insert(aggregate_id, SumAggregator::new(interval));
                self.next_aggregate_id.0 += 1;
                aggregate_id
            })
    }

    pub fn handle_values(&mut self,
                         time: TimePoint,
                         values: &FnvHashMap<ValueId, f64>) {
        for (operation, aggregate) in &self.single_operations {
            match operation {
                SingleAggregateOperation::Sum { value_id, .. } => {
                    if let Some(&value) = values.get(value_id) {
                        self.sum_aggregators.get_mut(aggregate).unwrap().add(time, value);
                    }
                }
                SingleAggregateOperation::SumSquare { value_id, .. } => {
                    if let Some(&value) = values.get(value_id) {
                        self.sum_aggregators.get_mut(aggregate).unwrap().add(time, value * value);
                    }
                }
                SingleAggregateOperation::Min { value_id, .. } => {
                    if let Some(&value) = values.get(value_id) {
                        self.min_max_aggregators.get_mut(aggregate).unwrap().add(time, value);
                    }
                }
                SingleAggregateOperation::Max { value_id, .. } => {
                    if let Some(&value) = values.get(value_id) {
                        self.min_max_aggregators.get_mut(aggregate).unwrap().add(time, value);
                    }
                }
            }
        }

        for (operation, aggregate) in &self.dual_operations {
            match operation {
                DualAggregateOperation::SumProduct { left, right, .. } => {
                    match (values.get(left), values.get(right)) {
                        (Some(&left_value), Some(&right_value)) => {
                            self.sum_aggregators.get_mut(aggregate).unwrap().add(time, left_value * right_value);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    pub fn average(&self, aggregate: &AverageAggregate) -> Option<f64> {
        let sum_aggregator = self.sum_aggregators.get(&aggregate.0)?;
        Some(sum_aggregator.sum()? / sum_aggregator.len() as f64)
    }

    pub fn variance(&self, aggregate: &VarianceAggregate) -> Option<f64> {
        let sum_aggregator = self.sum_aggregators.get(&aggregate.0)?;
        let sum_square_aggregator = self.sum_aggregators.get(&aggregate.1)?;

        let sum = sum_aggregator.sum()?;
        let sum_squares = sum_square_aggregator.sum()?;
        let n = sum_aggregator.len() as f64;
        Some((sum_squares - (sum * sum) / n) / n)
    }

    pub fn standard_deviation(&self, aggregate: &VarianceAggregate) -> Option<f64> {
        Some(self.variance(aggregate)?.sqrt())
    }

    pub fn covariance(&self, aggregate: &CovarianceAggregate) -> Option<f64> {
        let sum_left_aggregator = self.sum_aggregators.get(&aggregate.0)?;
        let sum_right_aggregator = self.sum_aggregators.get(&aggregate.1)?;
        let sum_product_aggregator = self.sum_aggregators.get(&aggregate.2)?;

        let sum_left = sum_left_aggregator.sum()?;
        let sum_right = sum_right_aggregator.sum()?;
        let sum_product = sum_product_aggregator.sum()?;
        let n = sum_product_aggregator.len() as f64;
        Some((sum_product - (sum_left * sum_right) / n) / n)
    }

    pub fn correlation(&self, aggregate: &CorrelationAggregate) -> Option<f64> {
        let std_left = self.standard_deviation(&aggregate.0)?;
        let std_right = self.standard_deviation(&aggregate.1)?;
        let std_product = std_left * std_right;
        if std_product.abs() < 1E-9 {
            return None;
        }

        let covariance = self.covariance(&aggregate.2)?;
        Some(covariance / std_product)
    }

    pub fn min(&self, aggregate: &MinAggregate) -> Option<f64> {
        let aggregator = self.min_max_aggregators.get(&aggregate.0)?;
        aggregator.value()
    }

    pub fn max(&self, aggregate: &MaxAggregate) -> Option<f64> {
        let aggregator = self.min_max_aggregators.get(&aggregate.0)?;
        aggregator.value()
    }
}

#[derive(Debug)]
pub struct AverageAggregate(AggregateId);

#[derive(Debug)]
pub struct VarianceAggregate(AggregateId, AggregateId);

#[derive(Debug)]
pub struct CovarianceAggregate(AggregateId, AggregateId, AggregateId);

#[derive(Debug)]
pub struct CorrelationAggregate(VarianceAggregate, VarianceAggregate, CovarianceAggregate);

#[derive(Debug)]
pub struct MinAggregate(AggregateId);

#[derive(Debug)]
pub struct MaxAggregate(AggregateId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AggregateId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum SingleAggregateOperation {
    Sum { value_id: ValueId, interval: Duration },
    SumSquare { value_id: ValueId, interval: Duration },
    Min { value_id: ValueId, interval: Duration },
    Max { value_id: ValueId, interval: Duration }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum DualAggregateOperation {
    SumProduct { left: ValueId, right: ValueId, interval: Duration },
}

pub struct SumAggregator {
    interval: Duration,
    values: VecDeque<(TimePoint, f64)>,
    sum: f64
}

impl SumAggregator {
    pub fn new(interval: TimeInterval) -> SumAggregator {
        SumAggregator {
            interval: interval.duration(),
            values: VecDeque::new(),
            sum: 0.0
        }
    }

    pub fn add(&mut self, time: TimePoint, value: f64) {
        self.values.push_back((time, value));
        self.sum += value;

        loop {
            if let Some((first_time, first_value)) = self.values.front() {
                if time.duration_since(*first_time) > self.interval {
                    self.sum -= first_value;
                    self.values.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn sum(&self) -> Option<f64> {
        if !self.values.is_empty() {
            Some(self.sum)
        } else {
            None
        }
    }
}

pub enum MinMaxOperation {
    Min,
    Max
}

pub struct MinMaxAggregator {
    operation: MinMaxOperation,
    interval: Duration,
    interval_start: Option<TimePoint>,
    current_value: Option<f64>
}

impl MinMaxAggregator {
    pub fn new(operation: MinMaxOperation, interval: TimeInterval) -> MinMaxAggregator {
        MinMaxAggregator {
            operation,
            interval: interval.duration(),
            interval_start: None,
            current_value: None,
        }
    }

    pub fn add(&mut self, time: TimePoint, value: f64) {
        let change = if let (Some(interval_start), Some(current_value)) = (self.interval_start, self.current_value) {
            if (time - interval_start) <= self.interval {
                match self.operation {
                    MinMaxOperation::Min => value < current_value,
                    MinMaxOperation::Max => value > current_value
                }
            } else {
                true
            }
        } else {
            true
        };

        if change {
            self.interval_start = Some(time);
            self.current_value = Some(value);
        }
    }

    pub fn value(&self) -> Option<f64> {
        self.current_value
    }
}

#[test]
fn test_average1() {
    use std::ops::Add;

    let mut operations = AggregateOperations::new();
    let t0 = TimePoint::now();

    let x = ValueId(0);
    let average_x = operations.add_average(x, TimeInterval::Seconds(10.0));
    let mut values = FnvHashMap::default();

    values.insert(x, 1.0);
    operations.handle_values(t0, &values);

    values.insert(x, 2.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(2.0)), &values);

    values.insert(x, 4.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(4.0)), &values);

    assert_eq!(Some((1.0 + 2.0 + 4.0) / 3.0), operations.average(&average_x));

    values.insert(x, 8.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(11.5)), &values);

    assert_eq!(Some((2.0 + 4.0 + 8.0) / 3.0), operations.average(&average_x));
}

#[test]
fn test_average2() {
    use std::ops::Add;
    use assert_approx_eq::assert_approx_eq;

    let mut operations = AggregateOperations::new();
    let t0 = TimePoint::now();

    let x = ValueId(0);
    let average = operations.add_average(x, TimeInterval::Seconds(10.0));
    let mut values = FnvHashMap::default();

    values.insert(x, 1.0);
    operations.handle_values(t0, &values);

    values.insert(x, 2.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(2.0)), &values);

    values.insert(x, 4.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(4.0)), &values);

    values.insert(x, 8.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(20.0)), &values);

    values.insert(x, 16.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(21.0)), &values);

    assert_approx_eq!(12.0, operations.average(&average).unwrap());
}

#[test]
fn test_variance1() {
    use std::ops::Add;
    use assert_approx_eq::assert_approx_eq;

    let mut operations = AggregateOperations::new();
    let t0 = TimePoint::now();

    let x = ValueId(0);
    let variance = operations.add_variance(x, TimeInterval::Seconds(10.0));
    let mut values = FnvHashMap::default();

    values.insert(x, 1.0);
    operations.handle_values(t0, &values);

    values.insert(x, 2.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(2.0)), &values);

    values.insert(x, 4.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(4.0)), &values);

    assert_approx_eq!(1.5555555, operations.variance(&variance).unwrap());

    values.insert(x, 8.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(11.5)), &values);

    assert_approx_eq!(6.22222222, operations.variance(&variance).unwrap());
}

#[test]
fn test_covariance1() {
    use std::ops::Add;
    use assert_approx_eq::assert_approx_eq;

    let mut operations = AggregateOperations::new();
    let t0 = TimePoint::now();

    let x = ValueId(0);
    let y = ValueId(1);
    let covariance_xy = operations.add_covariance(x, y, TimeInterval::Seconds(10.0));
    let mut values = FnvHashMap::default();

    values.insert(x, 1.0); values.insert(y, 10.0);
    operations.handle_values(t0, &values);

    values.insert(x, 2.0); values.insert(y, 20.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(2.0)), &values);

    assert_approx_eq!(2.5, operations.covariance(&covariance_xy).unwrap());

    values.insert(x, 4.0); values.insert(y, 40.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(4.0)), &values);

    assert_approx_eq!(15.5555555, operations.covariance(&covariance_xy).unwrap());

    values.insert(x, 8.0); values.insert(y, 80.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(11.5)), &values);

    assert_approx_eq!(62.22222222, operations.covariance(&covariance_xy).unwrap());
}

#[test]
fn test_covariance2() {
    use std::ops::Add;
    use assert_approx_eq::assert_approx_eq;

    let mut operations = AggregateOperations::new();
    let t0 = TimePoint::now();

    let x = ValueId(0);
    let y = ValueId(1);
    let average_x = operations.add_average(x, TimeInterval::Seconds(10.0));
    let average_y = operations.add_average(y, TimeInterval::Seconds(10.0));
    let covariance_xy = operations.add_covariance(x, y, TimeInterval::Seconds(10.0));
    let mut values = FnvHashMap::default();

    values.insert(x, 1.0); values.insert(y, 10.0);
    operations.handle_values(t0, &values);

    values.insert(x, 2.0); values.insert(y, 20.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(2.0)), &values);

    values.insert(x, 4.0); values.insert(y, 40.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(4.0)), &values);

    values.insert(x, 8.0); values.insert(y, 80.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(20.0)), &values);

    values.insert(x, 16.0); values.insert(y, 160.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(21.0)), &values);

    assert_approx_eq!(12.0, operations.average(&average_x).unwrap());
    assert_approx_eq!(120.0, operations.average(&average_y).unwrap());
    assert_approx_eq!(160.0, operations.covariance(&covariance_xy).unwrap());
}

#[test]
fn test_correlation1() {
    use std::ops::Add;
    use assert_approx_eq::assert_approx_eq;

    let mut operations = AggregateOperations::new();
    let t0 = TimePoint::now();

    let x = ValueId(0);
    let y = ValueId(1);
    let correlation_xy = operations.add_correlation(x, y, TimeInterval::Seconds(10.0));
    let mut values = FnvHashMap::default();

    values.insert(x, 1.0); values.insert(y, 10.0);
    operations.handle_values(t0, &values);

    values.insert(x, 2.0); values.insert(y, 20.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(2.0)), &values);

    assert_approx_eq!(1.0, operations.correlation(&correlation_xy).unwrap());

    values.insert(x, 4.0); values.insert(y, -40.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(4.0)), &values);

    assert_approx_eq!(-0.8824975, operations.correlation(&correlation_xy).unwrap());
}

#[test]
fn test_max1() {
    use std::ops::Add;

    let mut aggregator = MinMaxAggregator::new(MinMaxOperation::Max, TimeInterval::Seconds(10.0));
    let t0 = TimePoint::now();

    aggregator.add(t0, 2.0);
    assert_eq!(Some(2.0), aggregator.value());

    aggregator.add(t0.add(Duration::from_secs_f64(2.0)), 4.0);
    assert_eq!(Some(4.0), aggregator.value());

    aggregator.add(t0.add(Duration::from_secs_f64(3.0)), 3.0);
    assert_eq!(Some(4.0), aggregator.value());

    aggregator.add(t0.add(Duration::from_secs_f64(13.0)), 3.0);
    assert_eq!(Some(3.0), aggregator.value());
}

#[test]
fn test_max2() {
    use std::ops::Add;

    let mut operations = AggregateOperations::new();
    let t0 = TimePoint::now();

    let x = ValueId(0);
    let max_x = operations.add_max(x, TimeInterval::Seconds(10.0));
    let mut values = FnvHashMap::default();

    values.insert(x, 2.0);
    operations.handle_values(t0, &values);
    assert_eq!(Some(2.0), operations.max(&max_x));

    values.insert(x, 4.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(2.0)), &values);
    assert_eq!(Some(4.0), operations.max(&max_x));

    values.insert(x, 3.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(3.0)), &values);
    assert_eq!(Some(4.0), operations.max(&max_x));

    values.insert(x, 3.0);
    operations.handle_values(t0.add(Duration::from_secs_f64(13.0)), &values);
    assert_eq!(Some(3.0), operations.max(&max_x));
}