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
bool CheckFileExtension(const std::string &filename, const std::string &extension) {
  return filename.substr(filename.rfind('.') + 1) == extension;
}
}; // namespace FileOperations

namespace ParseOperations {
std::vector<std::string_view> SplitRow(std::string_view str, const char &ch) {
  std::vector<std::string_view> tmp;
  tmp.reserve(str.size() / 2); // 预分配内存
  size_t start = 0;
  for (size_t pos = 0; pos < str.size(); ++pos) {
    if (str[pos] == ch || (str[pos] == '\r' && pos + 1 < str.size() && str[pos + 1] == '\n')) {
      if (pos - start > 0)
        tmp.emplace_back(str.substr(start, pos - start)); // 插入子串
      if (str[pos] == ch || (str[pos] == '\r' && str[pos + 1] == '\n')) {
        start = pos + (str[pos] == '\r' ? 2 : 1);
        ++pos; // 如果遇到'\r\n'，跳过下一个'\n'
      }
    }
  }
  if (start < str.size())
    tmp.emplace_back(str.substr(start, str.size() - start)); // 插入子串
  return tmp;
}

std::string_view SplitFirstRow(std::string_view str, const char &ch) {
  auto pos = str.find_first_of(ch);
  return str.substr(0, pos);
}

std::vector<std::string_view> SplitRowSkipHeader(std::string_view str, const char &ch) {
  auto pos = str.find_first_of(ch);
  auto new_str = str.substr(pos + 1);
  return SplitRow(new_str, ch);
}
} // namespace ParseOperations

}; // namespace CSVUtils

// 管理文件IO资源安全
class FileHandle {
public:
  explicit FileHandle(const std::string &filename) : m_filename(filename), m_file(filename, std::ios::binary) {
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

using namespace CSVUtils;

class FileManager {
public:
  explicit FileManager(const std::string &filename) : m_fileHandle(std::make_unique<FileHandle>(filename)) {
    OpenSourceFile(filename);
  }
  ~FileManager() = default;

  std::unique_ptr<BaseIO> CreateFileHandler() { return FileOperations::OpenFileHandle(m_fileHandle->GetHandle()); }
  size_t GetFileSize() const { return m_file_size; }
  std::string GetFileName() const { return m_fileHandle->GetHandleContext(); }

private:
  void OpenSourceFile(const std::string &file) {
    auto ok = FileOperations::CheckFileExtension(file, "csv");
    if (!ok)
      throw ExceptionManager::FileIsInvalid(file);
    if (!m_fileHandle)
      throw ExceptionManager::FileOpenException(file);
    m_file_size = FileOperations::CalcFileSize(m_fileHandle->GetHandle());
  }

  size_t m_file_size = 0;
  std::unique_ptr<FileHandle> m_fileHandle = nullptr;
};

using OperateStrategyCallback = std::function<void(std::vector<std::vector<std::string_view>> &)>;
using QueryStrategyCallback =
    std::function<std::vector<std::string_view>(const std::vector<std::vector<std::string_view>> &)>;

class ParserImpl {
public:
  ParserImpl() { Initialize(); }
  void SetColumnNames(const std::vector<std::string_view> &column_names) { m_column_names = column_names; }
  void ParseRows(const std::unique_ptr<BaseIO> &io, const size_t &size) {
    m_read_buffer.resize(size);
    io->Read(m_read_buffer.data(), size);
    m_rows = ParseOperations::SplitRowSkipHeader(m_read_buffer, '\n');
  }
  void ParseColumns(const std::vector<std::string_view> &rows) {
    // m_csv_data.resize(rows.size());
    m_csv_data.reserve(rows.size());
    size_t current_line_number = 1;
    for (size_t i = 0; i < rows.size(); ++i) {
      current_line_number += i;
      auto columns = ParseOperations::SplitRow(rows[i], ',');
      // column size一致性校验
      if (!m_column_names.empty() && !ValidateColumnCount(columns)) {
        throw ExceptionManager::InvalidDataLine(current_line_number, "Invalid columns");
      }
      m_csv_data.emplace_back(columns);
      // m_csv_data[i] = columns;
    }
  }
  void AsyncParseColumns(const std::vector<std::string_view> &rows, size_t thread_nums) {
    std::vector<std::thread> threads; // 创建一个线程向量，存储多个线程
    std::atomic<size_t> counter(0);   //创建一个原子计数器，用于记录当前处理的行数
    m_csv_data.resize(rows.size());   // 一维数组的size初始化二维数组的size

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
  void WriteToFile(const std::string &des_file_path) {
    std::ofstream ofs(des_file_path, std::ios::out);
    if (!ofs.is_open()) {
      throw ExceptionManager::FileOpenException(des_file_path);
    }
    // 先写首行
    for (const auto &column_name : m_column_names) {
      ofs << column_name;
      if (&column_name != &m_column_names.back()) {
        ofs << ",";
      }
    }
    ofs << std::endl;
    // 写数据
    for (const auto &row : m_csv_data) {
      for (size_t i = 0; i < row.size(); ++i) {
        ofs << row[i];
        if (i != row.size() - 1) {
          ofs << ",";
        }
      }
      ofs << std::endl;
    }
    ofs.close();
  }
  void OnOperationCallback(OperateStrategyCallback onOperateStrategy) {
    if (onOperateStrategy)
      onOperateStrategy(m_csv_data);
  }
  std::vector<std::string_view> OnQueryCallback(QueryStrategyCallback onQueryStrategy) {
    if (onQueryStrategy)
      return onQueryStrategy(m_csv_data);
    return std::vector<std::string_view>();
  }
  std::vector<std::vector<std::string_view>> GetCSVData() const { return m_csv_data; }
  size_t GetDataSize() const { return m_csv_data.size(); }
  std::vector<std::string_view> GetRowData() const { return m_rows; }

private:
  bool ValidateColumnCount(const std::vector<std::string_view> &columns) {
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

  virtual void ParseDataFromCSV(const std::unique_ptr<BaseIO> &io, const size_t &size) = 0;

  virtual void WriteDataToCSV(const std::string &destination_path) = 0;

  void SetColumnNames(const std::vector<std::string_view> &columns) { m_impl->SetColumnNames(columns); }
  std::vector<std::vector<std::string_view>> GetCSVData() const { return m_impl->GetCSVData(); }
  size_t GetCSVDataSize() const noexcept { return m_impl->GetDataSize(); }

  void OnOperation(OperateStrategyCallback doOperation) { m_impl->OnOperationCallback(doOperation); }

  std::vector<std::string_view> OnQuery(QueryStrategyCallback doQuery) { return m_impl->OnQueryCallback(doQuery); }

protected:
  std::unique_ptr<ParserImpl> m_impl = nullptr;
};

class SynchronousParser : public ParserStrategy {
public:
  virtual void ParseDataFromCSV(const std::unique_ptr<BaseIO> &io, const size_t &size) override {
    m_impl->ParseRows(std::move(io), size);
    m_impl->ParseColumns(m_impl->GetRowData());
  }

  virtual void WriteDataToCSV(const std::string &destination_path) override { m_impl->WriteToFile(destination_path); }
};

class AsynchronousParser : public ParserStrategy {
public:
  AsynchronousParser(size_t thread_num = std::thread::hardware_concurrency()) : m_thread_num(thread_num) {}

  virtual void ParseDataFromCSV(const std::unique_ptr<BaseIO> &io, const size_t &size) override {
    m_impl->ParseRows(std::move(io), size);
    m_impl->AsyncParseColumns(m_impl->GetRowData(), m_thread_num);
  }

  virtual void WriteDataToCSV(const std::string &destination_path) override { m_impl->WriteToFile(destination_path); }

private:
  size_t m_thread_num = 0;
};

enum class ParseMode { Synchronous, Asynchronous };

class CSVParser {
  using DataContainer = std::vector<std::vector<std::string_view>>;

public:
  CSVParser() = default;
  CSVParser(const CSVParser &) = delete;
  CSVParser &operator=(const CSVParser &) = delete;
  ~CSVParser() = default;

  template <typename... Args> explicit CSVParser(ParseMode mode, Args &&...args) {
    SetParser(mode);
    if constexpr (sizeof...(Args) > 0) { // 检查Args是否有参数
      SetColumnNames(std::forward<Args>(args)...);
    }
  }
  template <typename... Args> void SetColumnNames(Args &&...args) {
    std::initializer_list<std::string_view> columns{std::forward<Args>(args)...};
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
    auto fileManager = std::make_unique<FileManager>(filename);
    auto fileHandler = fileManager->CreateFileHandler();
    m_parser->ParseDataFromCSV(fileHandler, fileManager->GetFileSize());
  }
  DataContainer GetCSVData() const { return m_parser->GetCSVData(); }
  size_t GetCSVDataSize() const noexcept { return m_parser->GetCSVDataSize(); }
  void WriteCSVDataToFile(const std::string &filename) { m_parser->WriteDataToCSV(filename); }
  void OnAdd(OperateStrategyCallback Add) { m_parser->OnOperation(Add); }
  void OnDelete(OperateStrategyCallback Del) { m_parser->OnOperation(Del); }
  void OnModify(OperateStrategyCallback Modify) { m_parser->OnOperation(Modify); }
  std::vector<std::string_view> OnQuery(QueryStrategyCallback Query) { return m_parser->OnQuery(Query); }

private:
  std::unique_ptr<ParserStrategy> m_parser = nullptr;
};

#endif // CSV_CSVREADER_H
