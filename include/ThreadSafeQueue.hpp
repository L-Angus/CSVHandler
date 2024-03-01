#ifndef THREAD_SAFE_QUEUE_HPP
#define THREAD_SAFE_QUEUE_HPP

#include <condition_variable>
#include <mutex>
#include <memory>
#include <queue>

template <typename T> class ThreadSafeQueue {
public:
  ThreadSafeQueue() = default;
  ThreadSafeQueue(const ThreadSafeQueue &) = delete;
  ThreadSafeQueue &operator=(const ThreadSafeQueue &) = delete;

  void push(T value) {
    std::scoped_lock locker(m_mutex);
    m_queue.push(std::move(value));
    m_condition.notify_one();
  }

  bool try_pop(T &value) {
    std::scoped_lock locker(m_mutex);
    if (m_queue.empty())
      return false;
    value = std::move(m_queue.front());
    m_queue.pop();
    return true;
  }

  T try_pop() {
    std::scoped_lock locker(m_mutex);
    if (m_queue.empty()) {
      return std::nullopt;
    }
    T value = std::move(m_queue.front());
    m_queue.pop();
    return value;
  }

  void wait_and_pop(T &value) {
    std::unique_lock<std::mutex> locker(m_mutex);
    m_condition.wait(locker, [this]() { return !m_queue.empty(); });
    value = std::move(m_queue.front());
    m_queue.pop();
  }

  T wait_and_pop() {
    std::unique_lock<std::mutex> locker(m_mutex);
    m_condition.wait(locker, [this]() { return !m_queue.empty(); });
    T value = std::move(m_queue.front());
    m_queue.pop();
    return value;
  }

  bool empty() const {
    std::scoped_lock locker(m_mutex);
    return m_queue.empty();
  }

  size_t size() const {
    std::scoped_lock locker(m_mutex);
    return m_queue.size();
  }

  T front() const {
    std::scoped_lock locker(m_mutex);
    if (m_queue.empty()) {
      return std::nullopt;
    }
    return std::move(m_queue.front());
  }

  void pop() {
    std::scoped_lock locker(m_mutex);
    if (!m_queue.empty()) {
      m_queue.pop();
    }
  }

private:
  mutable std::mutex m_mutex;
  std::queue<std::unique_ptr<T>> m_queue;
  std::condition_variable m_condition;
};

#endif

