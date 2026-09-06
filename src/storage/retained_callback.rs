//! Keep admitted buffers charged until an asynchronous backend finishes.

use crate::common::resource_budget::ResourceReservation;
use std::sync::{mpsc, Arc};

pub(crate) fn retain<T: Send + 'static>(
    callback: mpsc::Sender<T>,
    reservation: Arc<ResourceReservation>,
) -> std::io::Result<mpsc::Sender<T>> {
    let stack = reservation
        .reserve_related(64 * 1024, "storage completion adapter stack")
        .map_err(std::io::Error::other)?;
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
