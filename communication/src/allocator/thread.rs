//! Intra-thread communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::collections::VecDeque;

use crate::allocator::{Allocate, AllocateBuilder};
use crate::allocator::counters::Pusher as CountPusher;
use crate::allocator::counters::Puller as CountPuller;
use crate::{Push, Pull};

/// Builder for single-threaded allocator.
pub struct ThreadBuilder;

impl AllocateBuilder for ThreadBuilder {
    type Allocator = Thread;
    fn build(self) -> Self::Allocator { Thread::default() }
}


/// An allocator for intra-thread communication.
#[derive(Default)]
pub struct Thread {
    /// Shared counts of messages in channels.
    events: Rc<RefCell<Vec<usize>>>,
}

impl Allocate for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn allocate<T: 'static>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>) {
        let (pusher, puller) = Thread::new_from(identifier, Rc::clone(&self.events));
        (vec![Box::new(pusher)], Box::new(puller))
    }
    fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        &self.events
    }
    fn await_events(&self, duration: Option<Duration>) {
        if self.events.borrow().is_empty() {
            if let Some(duration) = duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }
}

/// Thread-local counting channel push endpoint.
pub type ThreadPusher<T> = CountPusher<T, Pusher<T>>;
/// Thread-local counting channel pull endpoint.
pub type ThreadPuller<T> = CountPuller<T, Puller<T>>;

impl Thread {
    /// Creates a new thread-local channel from an identifier and shared counts.
    pub fn new_from<T: 'static>(identifier: usize, events: Rc<RefCell<Vec<usize>>>)
        -> (ThreadPusher<T>, ThreadPuller<T>)
    {
        let shared = Rc::new(RefCell::new((VecDeque::<T>::new(), VecDeque::<T>::new())));
        let pusher = Pusher { target: Rc::clone(&shared) };
        let pusher = CountPusher::new(pusher, identifier, Rc::clone(&events));
        let puller = Puller { source: shared, current: None };
        let puller = CountPuller::new(puller, identifier, events);
        (pusher, puller)
    }
}


/// The push half of an intra-thread channel.
pub struct Pusher<T> {
    target: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Push<T> for Pusher<T> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        let mut borrow = self.target.borrow_mut();
        if let Some(element) = element.take() {
            borrow.0.push_back(element);
        }
        *element = borrow.1.pop_front();
    }
}

/// The pull half of an intra-thread channel.
pub struct Puller<T> {
    current: Option<T>,
    source: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        let mut borrow = self.source.borrow_mut();
        if let Some(element) = self.current.take() {
            // Retain a bounded number of values for the producer to reclaim.
            // Consumers that took the value with `recv()` leave `current` as
            // `None` and therefore correctly return nothing.
            if borrow.1.len() < 16 {
                borrow.1.push_back(element);
            }
        }
        self.current = borrow.0.pop_front();
        &mut self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pull_returns_unconsumed_resources_to_the_producer() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let (mut pusher, mut puller) = Thread::new_from(0, events);

        let mut first = Vec::with_capacity(1024);
        first.extend(0..16);
        let first_capacity = first.capacity();
        let mut first = Some(first);
        pusher.push(&mut first);
        assert!(first.is_none());

        puller.pull().as_mut().unwrap().clear();
        assert!(puller.pull().is_none());

        let mut second = Some(vec![99]);
        pusher.push(&mut second);
        let returned = second.expect("producer should reclaim the prior value");
        assert!(returned.is_empty());
        assert_eq!(returned.capacity(), first_capacity);
    }

    #[test]
    fn recv_transfers_ownership_without_returning_a_resource() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let (mut pusher, mut puller) = Thread::new_from(0, events);

        pusher.send(vec![1, 2, 3]);
        assert_eq!(puller.recv(), Some(vec![1, 2, 3]));
        assert!(puller.pull().is_none());

        let mut next = Some(vec![4]);
        pusher.push(&mut next);
        assert!(next.is_none(), "a taken value must not appear in the return queue");
    }

    #[test]
    fn resource_return_queue_is_bounded() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let (mut pusher, mut puller) = Thread::new_from(0, events);

        for value in 0..32 {
            pusher.send(vec![value]);
        }
        for _ in 0..32 {
            puller.pull().as_mut().unwrap().clear();
        }
        assert!(puller.pull().is_none());

        let mut returned = 0;
        for _ in 0..32 {
            let mut item = Some(Vec::new());
            pusher.push(&mut item);
            returned += usize::from(item.is_some());
        }
        assert_eq!(returned, 16);
    }
}
