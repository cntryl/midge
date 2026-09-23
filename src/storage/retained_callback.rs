//! Keep admitted buffers charged until an asynchronous backend finishes.

use crate::common::resource_budget::ResourceReservation;
use std::sync::{mpsc, Arc};

#[cfg(test)]
thread_local! {
    static RETAIN_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Number of `retain` calls made on the current thread (test hook).
#[cfg(test)]
pub(crate) fn retain_calls_on_this_thread() -> usize {
    RETAIN_CALLS.with(std::cell::Cell::get)
}

pub(crate) fn retain<T: Send + 'static>(
    callback: mpsc::Sender<T>,
    reservation: Arc<ResourceReservation>,
) -> crate::common::MidgeResult<mpsc::Sender<T>> {
    #[cfg(test)]
    RETAIN_CALLS.with(|calls| calls.set(calls.get() + 1));
    let stack = reservation.reserve_related(64 * 1024, "storage completion adapter stack")?;
    let (sender, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("midge-storage-completion".into())
        .stack_size(64 * 1024)
        .spawn(move || {
            let _reservation = reservation;
            let _stack = stack;
            if let Ok(event) = receiver.recv() {
                let _ = callback.send(event);
            }
        })?;
    Ok(sender)
}
