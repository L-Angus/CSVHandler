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
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "ExceptionManager.hpp"

class BaseIO {
public:
  virtual size_t Read(char *buffer, size_t size) = 0;
  virtual ~BaseIO() {}
};

class IStreamIO : public BaseIO {
public:
  IStreamIO(std::istream &stream) : m_stream(stream) {}
  size_t Read(char *buffer, size_t size) override {
    m_stream.read(buffer, size);
    return m_stream.gcount();
  }

private:
  std::istream &m_stream;
};

namespace CSVUtils {

namespace FileOperations {
std::unique_ptr<IStreamIO> OpenFileHandle(std::ifstream &in) {
  if (!in.is_open())
    throw ExceptionManager::FileOpenException();
  return std::make_unique<IStreamIO>(in);
}
size_t CalcFileSize(std::ifstream &file) {
  file.seekg(0, std::ios::end);
  size_t size = file.tellg();
  file.seekg(0, std::ios::beg);
  return size;
}
bool CheckFileExtension(const std::string &filename,
                        const std::string &extension) {
  return filename.substr(filename.rfind('.') + 1) == extension;
}
}; // namespace FileOperations

namespace ParseOperations {
std::vector<std::string_view> SplitRow(std::string_view str, const char &ch) {
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
    tmp.emplace_back(str.substr(start, str.size() - start)); // 插入子串c
  return tmp;
}
std::string_view SplitFirstRow(std::string_view str, const char &ch) {
  auto pos = str.find_first_of(ch);
  return str.substr(0, pos);
}
} // namespace ParseOperations

}; // namespace CSVUtils

// 管理文件IO资源安全
class FileHandle {
public:
  explicit FileHandle(const std::string &filename)
      : m_filename(filename), m_file(filename, std::ios::binary) {
    if (!m_file.is_open()) {
      throw ExceptionManager::FileOpenException(filename);
    }
  }
  ~FileHandle() {
    if (m_file.is_open()) {
      m_file.close();
    }
  }
  std::ifstream &GetHandle() { return m_file; }
  const std::string &GetHandleContext() const { return m_filename; }

private:
  std::ifstream m_file;
  std::string m_filename;
};

class FileManager {
public:
  explicit FileManager(const std::string &filename)
      : m_fileHandle(std::make_unique<FileHandle>(filename)) {
    OpenSourceFile(filename);
  }
  ~FileManager() = default;

  std::unique_ptr<BaseIO> CreateFileHandler() {
    return CSVUtils::FileOperations::OpenFileHandle(m_fileHandle->GetHandle());
  }
  size_t GetFileSize() const { return m_file_size; }
  std::string GetFileName() const { return m_fileHandle->GetHandleContext(); }

private:
  void OpenSourceFile(const std::string &file) {
    auto ok = CSVUtils::FileOperations::CheckFileExtension(file, "csv");
    if (!ok)
      throw ExceptionManager::FileIsInvalid(file);
    if (!m_fileHandle)
      throw ExceptionManager::FileOpenException(file);
    m_file_size =
        CSVUtils::FileOperations::CalcFileSize(m_fileHandle->GetHandle());
  }

  size_t m_file_size = 0;
  std::unique_ptr<FileHandle> m_fileHandle = nullptr;
};

class ParserImpl {
public:
  void SetColumnNames(const std::vector<std::string_view> &column_names) {
    m_column_names = column_names;
  }

  void ParseRows(const std::unique_ptr<BaseIO> &io, const size_t &size) {
    Initialize();
    m_read_buffer.resize(size);
    io->Read(m_read_buffer.data(), size);
    m_rows = CSVUtils::ParseOperations::SplitRow(m_read_buffer, '\n');
  }

  void ParseColumns(const std::vector<std::string_view> &rows) {
    m_csv_data.reserve(rows.size());
    size_t current_line_number = 0;
    for (const auto &row : rows) {
      ++current_line_number;
      auto columns = CSVUtils::ParseOperations::SplitRow(row, ',');
      // column size一致性校验
      if (!m_column_names.empty() && !ValidateColumnCount(columns)) {
        throw ExceptionManager::InvalidDataLine(current_line_number,
                                                "Invalid columns");
      }
      m_csv_data.emplace_back(columns);
    }
  }

  void AsyncParseColumns(const std::vector<std::string_view> &rows,
                         size_t thread_nums) {
    std::vector<std::thread> threads; // 创建一个线程向量，存储多个线程
    std::atomic<size_t> counter(0); //创建一个原子计数器，用于记录当前处理的行数
    m_csv_data.resize(rows.size()); // 一维数组的size初始化二维数组的size

    auto CheckAndSplitRow2Columns = [this, &rows](size_t j) {
      auto columns = CSVUtils::ParseOperations::SplitRow(rows.at(j), ',');
      if (!m_column_names.empty() && !ValidateColumnCount(columns)) {
        throw ExceptionManager::InvalidDataLine(j, "Invalid columns");
      }
      return std::vector(columns);
    };

    for (size_t i = 0; i < thread_nums; ++i) {
      threads.emplace_back([this, &counter, &CheckAndSplitRow2Columns, &rows] {
        while (true) {
          size_t j = counter.fetch_add(1);
          if (j >= rows.size())
            break;
          m_csv_data[j] = CheckAndSplitRow2Columns(j);
        }
      });
    }
    // 监听线程的完成情况
    for (auto &t : threads) {
      t.join();
    }
  }

  std::vector<std::vector<std::string_view>> GetCSVData() const {
    return m_csv_data;
  }
  size_t GetDataSize() const { return m_csv_data.size(); }
  std::vector<std::string_view> GetRowData() const { return m_rows; }

private:
  bool ValidateColumnCount(const std::vector<std::string_view> &columns) {
    m_header_line =
        CSVUtils::ParseOperations::SplitFirstRow(m_read_buffer, '\n');
    m_column_names = CSVUtils::ParseOperations::SplitRow(m_header_line, ',');
    return columns.size() == m_column_names.size();
  }

  void Initialize() {
    m_header_line = "";
    m_read_buffer = "";
    m_column_names.clear();
    m_csv_data.clear();
    m_rows.clear();
  }

  std::string m_header_line;
  std::string m_read_buffer;
  std::vector<std::string_view> m_column_names;
  std::vector<std::string_view> m_rows;
  std::vector<std::vector<std::string_view>> m_csv_data;
};

class ParserStrategy {
public:
  ParserStrategy() : m_impl(std::make_unique<ParserImpl>()) {}
  virtual ~ParserStrategy() = default;

  virtual void ParseDataFromCSV(const std::unique_ptr<BaseIO> &io,
                                const size_t &size) = 0;

  void SetColumnNames(const std::vector<std::string_view> &columns) {
    m_impl->SetColumnNames(columns);
  }

  std::vector<std::vector<std::string_view>> GetCSVData() const {
    return m_impl->GetCSVData();
  }

  size_t GetCSVDataSize() const noexcept { return m_impl->GetDataSize(); }

protected:
  std::unique_ptr<ParserImpl> m_impl = nullptr;
};

class SynchronousParser : public ParserStrategy {
public:
  virtual void ParseDataFromCSV(const std::unique_ptr<BaseIO> &io,
                                const size_t &size) override {
    m_impl->ParseRows(std::move(io), size);
    m_impl->ParseColumns(m_impl->GetRowData());
  }
};

class AsynchronousParser : public ParserStrategy {
public:
  AsynchronousParser(size_t thread_num = std::thread::hardware_concurrency())
      : m_thread_num(thread_num) {}

  virtual void ParseDataFromCSV(const std::unique_ptr<BaseIO> &io,
                                const size_t &size) override {
    m_impl->ParseRows(std::move(io), size);
    m_impl->AsyncParseColumns(m_impl->GetRowData(), m_thread_num);
  }

private:
  size_t m_thread_num = 0;
};

enum class ParseMode { Synchronous, Asynchronous };

class CSVParser {
public:
  CSVParser() = default;
  CSVParser(const CSVParser &) = delete;
  CSVParser &operator=(const CSVParser &) = delete;
  ~CSVParser() = default;

  template <typename... Args>
  explicit CSVParser(ParseMode mode, Args &&...args) {
    SetParser(mode);
    if constexpr (sizeof...(Args) > 0) { // 检查Args是否有参数
      SetColumnNames(std::forward<Args>(args)...);
    }
  }

  template <typename... Args> void SetColumnNames(Args &&...args) {
    std::initializer_list<std::string_view> columns{
        std::forward<Args>(args)...};
    if (columns.size() == 0)
      throw ExceptionManager::InvalidHeaderLine("No column names provided");
    // 检查column name是否重名,set会自动去重，检查size即可
    std::set<std::string_view> unique_column(columns.begin(), columns.end());
    if (unique_column.size() < columns.size())
      throw ExceptionManager::InvalidHeaderLine("Duplicate column names found");
    std::vector<std::string_view> column_names(columns.begin(), columns.end());
    m_parser->SetColumnNames(column_names);
  }

  void SetParser(ParseMode mode) {
    switch (mode) {
    case ParseMode::Synchronous:
      m_parser = std::make_unique<SynchronousParser>();
      break;
    case ParseMode::Asynchronous:
      m_parser = std::make_unique<AsynchronousParser>();
      break;
    default:
      throw std::runtime_error("Unsupported parse mode.");
    }
  }

  void ParseDataFromCSV(const std::string &filename) {
    try {
      auto fileManager = std::make_unique<FileManager>(filename);
      auto fileHandler = fileManager->CreateFileHandler();
      m_parser->ParseDataFromCSV(fileHandler, fileManager->GetFileSize());
    } catch (const std::exception &e) {
      std::cerr << "Error parsing CSV file: " << e.what() << std::endl;
    }
  }

  std::vector<std::vector<std::string_view>> GetCSVData() const {
    return m_parser->GetCSVData();
  }

  size_t GetCSVDataSize() const noexcept { return m_parser->GetCSVDataSize(); }

private:
  std::unique_ptr<ParserStrategy> m_parser = nullptr;
};

#endif // CSV_CSVREADER_H
