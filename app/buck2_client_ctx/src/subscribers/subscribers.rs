/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

use std::future::Future;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::subscribers::observer::ErrorObserver;
use crate::subscribers::subscriber::EventSubscriber;

#[derive(Default)]
pub struct EventSubscribers<'a> {
    subscribers: Vec<Box<dyn EventSubscriber + 'a>>,
}

impl<'a> EventSubscribers<'a> {
    pub fn new(subscribers: Vec<Box<dyn EventSubscriber + 'a>>) -> EventSubscribers<'a> {
        EventSubscribers { subscribers }
    }

    /// Helper method to abstract the process of applying an `EventSubscriber` method to all of the subscribers.
    /// Quits on the first error encountered.
    pub(crate) async fn for_each_subscriber<'b, Fut>(
        &'b mut self,
        f: impl FnMut(&'b mut Box<dyn EventSubscriber + 'a>) -> Fut,
    ) -> anyhow::Result<()>
    where
        Fut: Future<Output = anyhow::Result<()>> + 'b,
    {
        let mut futures: FuturesUnordered<_> = self.subscribers.iter_mut().map(f).collect();
        while let Some(res) = futures.next().await {
            res?;
        }
        Ok(())
    }

    pub(crate) async fn handle_exit(&mut self) -> anyhow::Result<()> {
        let mut r = Ok(());
        for subscriber in &mut self.subscribers {
            // Exit all subscribers, do not stop on first one.
            let subscriber_err = subscriber.exit().await;
            if r.is_ok() {
                // Keep first error.
                r = subscriber_err;
            }
        }
        r
    }

    pub(crate) fn handle_daemon_connection_failure(&mut self, error: &buck2_error::Error) {
        for subscriber in &mut self.subscribers {
            subscriber.handle_daemon_connection_failure(error);
        }
    }

    pub(crate) fn error_observers(&self) -> impl Iterator<Item = &dyn ErrorObserver> {
        self.subscribers
            .iter()
            .filter_map(|s| s.as_error_observer())
    }
}
