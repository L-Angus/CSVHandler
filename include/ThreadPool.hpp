#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "ThreadSafeQueue.hpp"

class ThreadPool {
  using Task = std::function<void()>;

public:
  explicit ThreadPool(
      size_t worker_threads = std::thread::hardware_concurrency())
      : m_worker_threads(worker_threads), m_stopped(false), m_paused(false) {
    for (size_t i = 0; i < m_worker_threads; ++i) {
      m_workers.emplace_back([this] {
        while (true) {
          Task task;
          {
            std::unique_lock<std::mutex> locker(m_worker_mutex);
            m_worker_condition.wait(locker, [this] {
              return m_stopped.load() || !m_tasks.empty() || m_paused.load();
            });
            if (m_stopped.load() && m_tasks.empty())
              return;
            if (m_paused.load()) {
              std::unique_lock<std::mutex> idle_locker(m_idle_mutex);
              ++m_idle_threads;
              m_resume_condition.wait(locker, [this] { return !m_paused.load(); });
              --m_idle_threads;
            }
            task = std::move(m_tasks.front());
            m_tasks.pop();
          }
          task();
        }
      });
    }
  }

  ~ThreadPool() { Stop(); }

//  template <class F, class... Args>
//  auto Enqueue(F &&f, Args &&...args)
//      -> std::future<std::invoke_result_t<F, Args...>> {
//    using return_type = std::invoke_result_t<F, Args...>;
//    auto task = std::make_unique<std::packaged_task<return_type()>>(
//        [f = std::forward<F>(f),
//         args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
//          std::apply(f, args);
//        });
//    std::future<return_type> res = task->get_future();
//    {
//      std::unique_lock<std::mutex> locker(m_worker_mutex);
//      if (m_stopped.load())
//        throw std::runtime_error("enqueue on stopped ThreadPool");
//      m_tasks.emplace([task = std::move(task)]() mutable { (*task)(); });
//    }
//    m_worker_condition.notify_one();
//    return res;
//  }

  template <class F>
  void Enqueue(F &&f) {
    {
      std::unique_lock<std::mutex> locker(m_worker_mutex);
      if (m_stopped.load())
        throw std::runtime_error("enqueue on stopped ThreadPool");
      m_tasks.emplace(std::forward<F>(f));
    }
    m_worker_condition.notify_one();
  }

  // 停止线程池
  void Stop() {
    m_stopped.store(true);
    m_worker_condition.notify_all(); // 通知所有线程，线程池已停止
    for (auto &worker : m_workers) {
      worker.join(); // 等待所有线程完成
    }
  }
  // 暂停线程池
  void Pasue() {
    m_paused.store(true);
    m_worker_condition.notify_all();
  }
  // 恢复线程池
  void Resume() {
    m_paused.store(false);
    m_resume_condition.notify_all();
  }
  // 获取总的线程数
  size_t GetCapacity() const {
    std::lock_guard<std::mutex> locker(m_worker_mutex);
    return m_worker_threads;
  }
  // 获取闲置线程数
  size_t GetIdleThreads() const {
    std::unique_lock<std::mutex> locker(m_idle_mutex);
    return m_idle_threads;
  }

private:
  std::vector<std::thread> m_workers;
  size_t m_idle_threads = 0;
  size_t m_worker_threads = 0;

  mutable std::mutex m_worker_mutex;
  mutable std::mutex m_idle_mutex;
  std::atomic<bool> m_stopped;
  std::atomic<bool> m_paused;
  std::condition_variable m_worker_condition;
  std::condition_variable m_resume_condition;

  std::queue<Task> m_tasks;
};
