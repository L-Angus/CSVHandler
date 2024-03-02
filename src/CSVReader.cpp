#include "./CSVReader.h"
#include <charconv>
#include <iostream>

#ifdef CSV_NO_THREAD
/****************** SyncReader ******************/
void SyncReader::Init(std::unique_ptr<BaseIO> io, size_t file_size) {
  if (!io)
    throw std::runtime_error("Init Failed.");
  m_source_io = std::move(io);
  m_total_byte_size = file_size;
  m_reading_buffer.resize(m_total_byte_size);
}
void SyncReader::ReadDataFromCSV() {
  m_source_io->Read(m_reading_buffer.data(), m_total_byte_size);
  CheckColumnNames(); // 校验CSV的Header Line
  ParseRows();        // 获取一维CSV数据, 以换行符分割
  ParseColumns();     // 获取二维CSV数据，以逗号分割
}
void SyncReader::ParseRows() {
  m_rows = LineParser::SplitLine(m_reading_buffer, '\n');
}
void SyncReader::ParseColumns() {
  m_csv_data.reserve(m_rows.size());
  for (const auto &row : m_rows) {
    m_csv_data.emplace_back(LineParser::SplitLine(std::move(row), ','));
  }
}
void SyncReader::SetColumnNames(const std::vector<std::string_view> &cols) {
  m_column_names = std::move(cols);
}
bool SyncReader::CheckColumnNames() {
  m_header_line = LineParser::SplitFirstChunk(m_reading_buffer, '\n');
  auto header_columns = LineParser::SplitLine(m_header_line, ',');
  if (m_column_names != header_columns) {
    throw std::runtime_error("Incorrect Header Line.");
  }
  return true;
}

#else

/****************** AsyncReader ******************/
void AsyncReader::Init(std::unique_ptr<BaseIO> io, size_t file_size) {
  if (!io)
    throw std::runtime_error("Init Failed.");
  m_source_io = std::move(io);
  m_total_byte_size = file_size; // 获取源文件总的字节数
  m_reading_buffer.resize(m_total_byte_size);
}

void AsyncReader::ReadDataFromCSV() {
  std::future<void> async_reader = std::async(std::launch::async, [&] {
    m_source_io->Read(m_reading_buffer.data(), m_total_byte_size);
    CheckColumnNames();
    ParseRows();
    ParseColumns();
  });
  async_reader.get();
}

void AsyncReader::ParseRows() {
  m_rows = std::move(LineParser::SplitLine(m_reading_buffer, '\n'));
}

void AsyncReader::ParseColumns() {
  // 获取可用的线程数
  size_t n = std::thread::hardware_concurrency();
  // 创建一个线程向量，存储多个线程
  std::vector<std::thread> threads;
  // 创建一个原子计数器，用于记录当前处理的行数
  std::atomic<size_t> counter(0);
  // 一维数组的size初始化二维数组的size
  m_csv_data.resize(m_rows.size());
  // 创建多个线程，每个线程循环地从计数器中获取一个行数，直到处理完所有的行数据
  for (size_t i = 0; i < n; ++i) {
    threads.emplace_back([this, &counter] {
      while (true) {
        // 从计数器中获取一个行数，原子地递增计数器
        size_t j = counter.fetch_add(1);
        // 如果行数超过了总行数，退出循环
        if (j >= m_rows.size()) break;
        // 分割这一行的数据，并存储到二维vector中
        // 使用at函数来访问或修改元素，避免数组越界的错误
        m_csv_data.at(j) = LineParser::SplitLine(m_rows.at(j), ',');
      }
    });
  }
  // 等待所有的线程结束
  for (auto& t : threads) {
    t.join();
  }
}

void AsyncReader::SetColumnNames(const std::vector<std::string_view> &cols) {
  m_column_names = std::move(cols);
}

bool AsyncReader::CheckColumnNames() {
  m_header_line = LineParser::SplitFirstChunk(m_reading_buffer, '\n');
  std::cout << "m_header_line: " << m_header_line << std::endl;
  auto header_columns = LineParser::SplitLine(m_header_line, ',');
  if (m_column_names != header_columns) {
    throw std::runtime_error("Incorrect Header Line.");
  }
  return true;
}

#endif

inline std::string_view extractExtension(std::string_view filename) {
  return filename.substr(filename.rfind('.'));
}

inline size_t CalcFileByteSize(std::istream &in) {
  size_t file_byte_size = 0;
  in.seekg(0, std::ios::end);
  file_byte_size = in.tellg();
  in.seekg(0, std::ios::beg);
  return file_byte_size;
}

CSVReader::~CSVReader() {
  if (m_file)
    m_file.close();
}

void CSVReader::Read(std::string_view csv) {
  reader.Init(OpenFile(csv), GetSourceFileSize());
  reader.ReadDataFromCSV();
  m_file.close();
}

std::unique_ptr<BaseIO> CSVReader::OpenFile(std::string_view csv) {
  if (!CheckFileIsCSV(csv))
    throw std::runtime_error("Not a csv file.");
  m_file.open(csv, std::ios::binary);
  if (!m_file.is_open())
    throw std::runtime_error("Failed Open CSV.");
  m_file_size = CalcFileByteSize(m_file);
  return std::make_unique<IStreamIO>(m_file);
}

bool CSVReader::CheckFileIsCSV(std::string_view csv) const {
  return extractExtension(csv) == ".csv";
}
