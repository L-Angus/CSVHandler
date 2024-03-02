//
// Created by 李顺东 on 2024/2/21.
//

#ifndef CSV_CSVREADER_H
#define CSV_CSVREADER_H

#include <atomic>
#include <fstream>
#include <future>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "ThreadPool.hpp"
#include "ThreadSafeQueue.hpp"

// #define CSV_NO_THREAD

constexpr size_t ChunkDataSize = 4096 * 4;

namespace LineParser {
inline std::vector<std::string_view> SplitLine(std::string_view str,
                                               const char &ch) {
  std::vector<std::string_view> tmp;
  tmp.reserve(str.size() / 2); // 预分配内存
  size_t start = 0;
  for (size_t pos = 0; pos < str.size(); ++pos) {
    if (str[pos] == ch) {
      if (pos - start > 1)
        tmp.emplace_back(str.substr(start, pos - start)); // 插入子串
      start = pos + 1;
    }
  }
  if (start < str.size())
    tmp.emplace_back(str.substr(start, str.size() - start)); // 插入子串
  tmp.shrink_to_fit(); // 释放剩余内存
  return tmp;
}

inline std::string_view SplitFirstChunk(std::string_view str, const char &ch) {
  auto pos = str.find_first_of(ch);
  return str.substr(0, pos);
}
}; // namespace LineParser

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

using CSVRow = std::vector<std::string_view>;

#ifdef CSV_NO_THREAD

class SyncReader {
public:
  void Init(std::unique_ptr<BaseIO> io, size_t file_size);
  void ReadDataFromCSV();
  size_t GetDataSize() const noexcept { return m_csv_data.size(); }
  [[nodiscard]] std::vector<CSVRow> GetCSVData() const { return m_csv_data; }
  void SetColumnNames(const std::vector<std::string_view> &cols);

private:
  void ParseRows();
  void ParseColumns();
  bool CheckColumnNames();
  std::unique_ptr<BaseIO> m_source_io = nullptr;
  size_t m_total_byte_size = 0;
  std::string m_reading_buffer;
  std::string m_header_line;
  CSVRow m_rows;
  CSVRow m_column_names;
  std::vector<CSVRow> m_csv_data;
};

#else

/****************** AsyncReader ******************/
class AsyncReader {
public:
  void Init(std::unique_ptr<BaseIO> io, size_t file_size);
  void ReadDataFromCSV();
  size_t GetDataSize() const noexcept { return m_csv_data.size(); }
  [[nodiscard]] std::vector<CSVRow> GetCSVData() const { return m_csv_data; }
  void SetColumnNames(const std::vector<std::string_view>& cols);

private:
  void ParseRows();
  void ParseColumns();
  bool CheckColumnNames();
  std::unique_ptr<BaseIO> m_source_io = nullptr;
  size_t m_total_byte_size = 0;
  std::string m_reading_buffer;
  std::string m_header_line;
  CSVRow m_rows;
  CSVRow m_column_names;
  std::vector<CSVRow> m_csv_data; // 存储CSV数据
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

  template <typename... Args> explicit CSVReader(Args &&...args) {
    SetColumnNames(std::forward<Args>(args)...);
  }
  template <typename... Args> void SetColumnNames(Args &&...args) {
    std::initializer_list<std::string_view> columns{
        std::forward<Args>(args)...};
    if (columns.size() == 0)
      throw std::invalid_argument("No column names provided");
    // 检查column name是否重名,set会自动去重，检查size即可
    std::set<std::string_view> unique_column(columns.begin(), columns.end());
    if (unique_column.size() < columns.size())
      throw std::invalid_argument("Duplicate column names found");
    std::vector<std::string_view> column_names(columns.begin(), columns.end());
    reader.SetColumnNames(column_names);
  }
  void Read(std::string_view csv);
  std::vector<CSVRow> GetCSVData() const { return reader.GetCSVData(); }

private:
  std::unique_ptr<BaseIO> OpenFile(std::string_view csv);
  size_t GetSourceFileSize() const { return m_file_size; }
  bool CheckFileIsCSV(std::string_view csv) const;

  std::string m_csv;
  std::ifstream m_file;
  size_t m_file_size = 0;
};

#endif // CSV_CSVREADER_H
