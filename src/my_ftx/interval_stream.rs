use tokio::time::{Interval, Instant, Duration, interval};
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::Stream;

pub struct IntervalStream {
    interval: Interval,
}

impl IntervalStream {
    pub fn new(period: Duration) -> IntervalStream {
        IntervalStream { interval: interval(period) }
    }
}

impl Stream for IntervalStream {
    type Item = Instant;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Instant>> {
        self.interval.poll_tick(cx).map(|inner| Some(inner))
    }
}