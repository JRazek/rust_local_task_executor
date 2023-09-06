#![feature(unboxed_closures)]
#![feature(fn_traits)]

use tokio::sync::mpsc as tokio_mpsc;

use rayon::ThreadPool;

use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::LocalSet;

#[derive(Debug)]
pub struct LocalTaskChannel<T: Send> {
    task_tx: tokio_mpsc::Sender<T>,
}

impl<T: Send> Clone for LocalTaskChannel<T> {
    fn clone(&self) -> Self {
        LocalTaskChannel {
            task_tx: self.task_tx.clone(),
        }
    }
}

fn runner<T: Send + 'static>(
    mut task_rx: tokio_mpsc::Receiver<T>,
    mut executor: impl FnMut(T) -> () + std::marker::Send + 'static,
) {
    let rt = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let local_set = LocalSet::new();

    local_set.spawn_local(async move {
        while let Some(task) = task_rx.recv().await {
            executor(task);
        }
    });

    rt.block_on(local_set);
}

pub fn channel<T: Send + 'static>(
    rayon: &ThreadPool,
    executor: impl FnMut(T) -> () + std::marker::Send + 'static,
    channel_size: usize,
) -> LocalTaskChannel<T> {
    let (task_tx, task_rx) = tokio_mpsc::channel(channel_size);

    let channel = LocalTaskChannel { task_tx };

    rayon.spawn(move || runner(task_rx, executor));

    channel
}

impl<T: Send + 'static> LocalTaskChannel<T> {
    pub async fn spawn(&self, task: T) -> Result<(), tokio_mpsc::error::SendError<T>> {
        let task_tx = self.task_tx.clone();

        task_tx.send(task).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {

    enum Task {
        A(u32),
        B,
    }

    struct Executor {
        callback_tx: tokio::sync::mpsc::Sender<Task>,
    }

    impl Executor {
        fn executor(&mut self, task: Task) {
            use std::thread::ThreadId;

            fn get_init_thread_id() -> ThreadId {
                static mut THREAD_ID: Option<ThreadId> = None;
                static INIT: std::sync::Once = std::sync::Once::new();

                unsafe {
                    INIT.call_once(|| {
                        let thread_id = std::thread::current().id();
                        THREAD_ID.replace(thread_id);
                    });

                    THREAD_ID.unwrap()
                }
            }

            assert_eq!(std::thread::current().id(), get_init_thread_id());

            self.callback_tx.try_send(task).unwrap();
        }
    }

    impl FnOnce<(Task,)> for Executor {
        type Output = ();

        extern "rust-call" fn call_once(mut self, task: (Task,)) -> Self::Output {
            let (task,) = task;
            self.executor(task)
        }
    }

    impl FnMut<(Task,)> for Executor {
        extern "rust-call" fn call_mut(&mut self, task: (Task,)) -> Self::Output {
            let (task,) = task;
            self.executor(task)
        }
    }

    #[tokio::test]
    async fn test() {
        let rayon = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();

        let (callback_tx, mut callback_rx) = tokio::sync::mpsc::channel(10);

        let executor = Executor {
            callback_tx: callback_tx.clone(),
        };

        let channel = super::channel(&rayon, executor, 10);

        let channel_cloned = channel.clone();

        channel_cloned.spawn(Task::A(1)).await.unwrap();
        channel_cloned.spawn(Task::B).await.unwrap();

        let recv_task = async move {
            let mut a_recv = false;
            let mut b_recv = false;

            for _ in 0..2 {
                match callback_rx.recv().await.unwrap() {
                    Task::A(1) => a_recv = true,
                    Task::B => b_recv = true,
                    _ => panic!("unexpected task"),
                }
            }

            assert!(a_recv);
            assert!(b_recv);
        };

        tokio::select! {
            _ = recv_task => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                panic!("timeout");
            }
        }
    }
}
