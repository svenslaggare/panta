use std::collections::{VecDeque};
use std::ops::Add;
use std::time::{Duration, Instant};

use fnv::FnvHashMap;

use assert_approx_eq::assert_approx_eq;

use crate::model::{TimeInterval, ValueId};

pub struct AggregateOperations {
    next_aggregate_id: AggregateId,
    aggregators: FnvHashMap<AggregateId, SumAggregator>,
    single_operations: FnvHashMap<SingleAggregateOperation, AggregateId>,
    dual_operations: FnvHashMap<DualAggregateOperation, AggregateId>
}

impl AggregateOperations {
    pub fn new() -> AggregateOperations {
        AggregateOperations {
            next_aggregate_id: AggregateId(1),
            aggregators: FnvHashMap::default(),
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
                self.aggregators.insert(aggregate_id, SumAggregator::new(interval));
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
                self.aggregators.insert(aggregate_id, SumAggregator::new(interval));
                self.next_aggregate_id.0 += 1;
                aggregate_id
            })
    }

    pub fn handle_values(&mut self,
                         time: Instant,
                         values: &FnvHashMap<ValueId, f64>) {
        for (operation, aggregate) in &self.single_operations {
            match operation {
                SingleAggregateOperation::Sum { value_id: metric, .. } => {
                    if let Some(&value) = values.get(metric) {
                        self.aggregators.get_mut(aggregate).unwrap().add(time, value);
                    }
                }
                SingleAggregateOperation::SumSquare { value_id: metric, .. } => {
                    if let Some(&value) = values.get(metric) {
                        self.aggregators.get_mut(aggregate).unwrap().add(time, value * value);
                    }
                }
            }
        }

        for (operation, aggregate) in &self.dual_operations {
            match operation {
                DualAggregateOperation::SumProduct { left, right, .. } => {
                    match (values.get(left), values.get(right)) {
                        (Some(&left_value), Some(&right_value)) => {
                            self.aggregators.get_mut(aggregate).unwrap().add(time, left_value * right_value);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    pub fn average(&self, aggregate: &AverageAggregate) -> Option<f64> {
        let sum_aggregator = self.aggregators.get(&aggregate.0)?;
        Some(sum_aggregator.sum()? / sum_aggregator.len() as f64)
    }

    pub fn variance(&self, aggregate: &VarianceAggregate) -> Option<f64> {
        let sum_aggregator = self.aggregators.get(&aggregate.0)?;
        let sum_square_aggregator = self.aggregators.get(&aggregate.1)?;

        let sum = sum_aggregator.sum()?;
        let sum_squares = sum_square_aggregator.sum()?;
        let n = sum_aggregator.len() as f64;
        Some((sum_squares - (sum * sum) / n) / n)
    }

    pub fn covariance(&self, aggregate: &CovarianceAggregate) -> Option<f64> {
        let sum_left_aggregator = self.aggregators.get(&aggregate.0)?;
        let sum_right_aggregator = self.aggregators.get(&aggregate.1)?;
        let sum_product_aggregator = self.aggregators.get(&aggregate.2)?;

        let sum_left = sum_left_aggregator.sum()?;
        let sum_right = sum_right_aggregator.sum()?;
        let sum_product = sum_product_aggregator.sum()?;
        let n = sum_product_aggregator.len() as f64;
        Some((sum_product - (sum_left * sum_right) / n) / n)
    }
}

#[derive(Debug)]
pub struct AverageAggregate(AggregateId);

#[derive(Debug)]
pub struct VarianceAggregate(AggregateId, AggregateId);

#[derive(Debug)]
pub struct CovarianceAggregate(AggregateId, AggregateId, AggregateId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AggregateId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum SingleAggregateOperation {
    Sum { value_id: ValueId, interval: Duration },
    SumSquare { value_id: ValueId, interval: Duration }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum DualAggregateOperation {
    SumProduct { left: ValueId, right: ValueId, interval: Duration },
}

pub struct SumAggregator {
    interval: Duration,
    values: VecDeque<(Instant, f64)>,
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

    pub fn add(&mut self, time: Instant, value: f64) {
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

    fn sum_linear(&self) -> Option<f64> {
        if !self.values.is_empty() {
            Some(self.values.iter().map(|(_, value)| value).sum::<f64>())
        } else {
            None
        }
    }
}

#[test]
fn test_aggregate_operations_average1() {
    let mut operations = AggregateOperations::new();
    let t0 = Instant::now();

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
fn test_aggregate_operations_average2() {
    let mut operations = AggregateOperations::new();
    let t0 = Instant::now();

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
fn test_aggregate_operations_variance1() {
    let mut operations = AggregateOperations::new();
    let t0 = Instant::now();

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
fn test_aggregate_operations_covariance1() {
    let mut operations = AggregateOperations::new();
    let t0 = Instant::now();

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
fn test_aggregate_operations_covariance2() {
    let mut operations = AggregateOperations::new();
    let t0 = Instant::now();

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