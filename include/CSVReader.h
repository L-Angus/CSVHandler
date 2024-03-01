//
// Created by 李顺东 on 2024/2/21.
//

#ifndef CSV_CSVREADER_H
#define CSV_CSVREADER_H

#include <any>
#include <atomic>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "ThreadSafeQueue.hpp"
#include "ThreadPool.hpp"

// #define CSV_NO_THREAD

using CSVRow = std::vector<std::string>;
using CSVRowData = std::vector<CSVRow>;
constexpr size_t ChunkDataSize = 4096*4;

class BaseIO {
public:
  virtual size_t Read(char *buffer, int size) = 0;
  virtual ~BaseIO() = default;
};

class IStreamIO : public BaseIO {
public:
  explicit IStreamIO(std::istream &in) : m_in(in) {}
  size_t Read(char *buffer, int size) override {
    m_in.read(buffer, size);
    return m_in.gcount();
  }

private:
  std::istream &m_in;
};

class DataWrapper {
public:
  CSVRow operator()(std::string_view line, char quote = ',') {
    CSVRow row;
    size_t start = 0;
    size_t end = line.find(quote);
    while (end != std::string::npos) {
      row.emplace_back(std::string_view(line.data() + start, end - start));
      start = end + 1;
      end = line.find(quote, start);
    }
    row.emplace_back(
        std::string_view(line.data() + start, line.size() - start));
    return row;
  }
};

#ifdef CSV_NO_THREAD

class SyncReader {
public:
  void Init(std::unique_ptr<BaseIO> io, size_t file_size);
  void Read();
  [[nodiscard]] CSVRowData GetCSVRow() const { return m_rows; }

private:
  bool ReadChunkData();
  [[nodiscard]] bool HasNextLine() const;
  CSVRowData m_rows;
  std::unique_ptr<BaseIO> m_source_io = nullptr;
  size_t m_total_byte_size = 0;
  size_t m_read_byte_size = 0;
  std::string m_remained_data;
};

#else

/****************** AsyncReader ******************/
class AsyncReader {
public:
  void Init(std::unique_ptr<BaseIO> io, size_t file_size);
  void Read();
  [[nodiscard]] CSVRowData GetCSVRow() const { return m_rows; }

private:
  void Producer(); // 生产者线程函数，负责从文件中读取数据并存入缓冲区
  void Consumer(); // 消费者线程函数，负责从缓冲区中读取数据并解析为CSV行
  [[nodiscard]] bool IsDone() const; // 判断是否读取完毕
  std::unique_ptr<BaseIO> m_source_io = nullptr;
  size_t m_total_byte_size = 0;
  size_t m_read_byte_size = 0;
  std::string m_incomplete_line; // 缓存不完整行
  std::mutex m_mutex;              // 互斥锁，用于保护缓冲区的访问
  std::queue<std::string> m_queue; // 缓冲区
  std::condition_variable m_not_full; // 条件变量，用于通知生产者缓冲区不满
  std::condition_variable m_not_empty; // 条件变量，用于通知消费者缓冲区不空
  std::thread m_producer_thread; // 生产者线程
  std::thread m_consumer_thread; // 消费者线程
  CSVRowData m_rows;             // 存储CSV行的数据
  std::promise<void> m_exception;
};

#endif

class CSVReader {
#ifndef CSV_NO_THREAD
  AsyncReader reader;
#else
  SyncReader reader;
#endif

public:
  CSVReader() = default;
  CSVReader(const CSVReader &) = delete;
  CSVReader &operator=(const CSVReader &) = delete;
  ~CSVReader();

  void Read(const std::string &csv);
  CSVRowData GetRows() const { return reader.GetCSVRow(); }

private:
  std::unique_ptr<BaseIO> OpenFile();
  void CheckFileSize(std::ifstream &in);
  size_t GetSourceFileSize() const { return m_file_size; }
  std::string m_csv;
  std::ifstream m_file;
  size_t m_file_size = 0;
};

#endif // CSV_CSVREADER_H
