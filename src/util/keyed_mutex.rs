use std::collections::HashSet;
use std::hash::Hash;

use tokio::sync::watch::Sender as WatchSender;

pub struct KeyedMutex<K> {
    state: WatchSender<HashSet<K>>,
}

impl<K: Hash + Eq + Clone> KeyedMutex<K> {
    pub fn new() -> Self {
        KeyedMutex {
            state: WatchSender::new(HashSet::new()),
        }
    }

    pub async fn lock(&self, key: K) -> LockGuard<'_, K> {
        let mut receiver = self.state.subscribe();
        loop {
            if self.state.send_if_modified(|set| set.insert(key.clone())) {
                return LockGuard {
                    lock: self,
                    key,
                }
            }
            // this can't error because we still hold a sender
            let _ = receiver.wait_for(|set| !set.contains(&key)).await;
        }
    }
}

pub struct LockGuard<'a, K: Hash + Eq > {
    lock: &'a KeyedMutex<K>,
    key: K,
}

impl<'a, K: Hash + Eq> Drop for LockGuard<'a, K> {
    fn drop(&mut self) {
        self.lock.state.send_modify(|set| assert!(set.remove(&self.key), "unlocked mutex that wasn't locked"))
    }
}
