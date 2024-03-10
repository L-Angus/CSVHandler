// #include "./CSVReader.h"
// #include <future>
// #include <iostream>

// #ifdef CSV_NO_THREAD
/****************** SyncParser ******************/
// void SyncParser::Init(std::unique_ptr<BaseIO> io, size_t file_size) {
//   if (!io)
//     throw std::runtime_error("Init Failed.");
//   m_source_io = std::move(io);
//   m_total_byte_size = file_size;
//   m_reading_buffer.resize(m_total_byte_size);
// }

// void SyncParser::ReadDataFromCSV() {
//   m_source_io->Read(m_reading_buffer.data(), m_total_byte_size);
//   CheckColumnNames(); // 校验CSV的Header Line
//   ParseRows();        // 获取一维CSV数据, 以换行符分割
//   ParseColumns();     // 获取二维CSV数据，以逗号分割
// }

// void SyncParser::ParseRows() {
//   m_rows = ParseHelper::SplitLine(m_reading_buffer, '\n');
// }

// void SyncParser::ParseColumns() {
//   m_csv_data.reserve(m_rows.size());
//   for (const auto &row : m_rows) {
//     auto columns = ParseHelper::SplitLine(row, ',');
//     // column size一致性校验
//     if (!m_column_names.empty() && columns.size() != m_column_names.size()) {
//       throw ExceptionManager::InvalidDataLine(j, "Inconsistant column
//       numbers");
//     }
//     m_csv_data.emplace_back(std::move(columns));
//   }
// }

// void SyncParser::SetColumnNames(const std::vector<std::string_view> &cols) {
//   m_column_names = std::move(cols);
// }

// bool SyncParser::CheckColumnNames() {
//   m_header_line = ParseHelper::SplitFirstChunk(m_reading_buffer, '\n');
//   auto header_columns = ParseHelper::SplitLine(m_header_line, ',');
//   if (m_column_names != header_columns) {
//     throw ExceptionManager::InvalidHeaderLine();
//   }
//   return true;
// }

// #else

// /****************** AsyncParser ******************/
// void AsyncParser::Init(std::unique_ptr<BaseIO> io, size_t file_size) {
//   if (!io)
//     throw std::runtime_error("Init Failed.");
//   m_source_io = std::move(io);
//   m_total_byte_size = file_size; // 获取源文件总的字节数
//   m_reading_buffer.resize(m_total_byte_size);
// }

// void AsyncParser::ReadDataFromCSV() {
//   std::future<void> async_reader = std::async(std::launch::async, [&] {
//     m_source_io->Read(m_reading_buffer.data(), m_total_byte_size);
//     CheckColumnNames();
//     ParseRows();
//     ParseColumns();
//   });
//   async_reader.get();
// }

// void AsyncParser::ParseRows() {
//   m_rows = std::move(ParseHelper::SplitLine(m_reading_buffer, '\n'));
// }

// void AsyncParser::ParseColumns() {
//   size_t n = std::thread::hardware_concurrency(); // 获取可用的线程数
//   std::vector<std::thread> threads; // 创建一个线程向量，存储多个线程
//   std::atomic<size_t> counter(0); //
//   创建一个原子计数器，用于记录当前处理的行数
//   m_csv_data.resize(m_rows.size()); // 一维数组的size初始化二维数组的size

//   auto CheckAndSplitRow2Columns = [this](size_t j) -> CSVRow {
//     auto columns = ParseHelper::SplitLine(m_rows.at(j), ',');
//     if (!m_column_names.empty() && columns.size() != m_column_names.size()) {
//       throw ExceptionManager::InvalidDataLine(j, "Inconsistant column
//       numbers");
//     }
//     return CSVRow{std::move(columns)};
//   };

//   for (size_t i = 0; i < n; ++i) {
//     threads.emplace_back([this, &counter, &CheckAndSplitRow2Columns] {
//       while (true) {
//         size_t j = counter.fetch_add(1);
//         if (j >= m_rows.size())
//           break;
//         m_csv_data[j] = CheckAndSplitRow2Columns(j);
//       }
//     });
//   }
//   // 监听线程的完成情况
//   for (auto &t : threads) {
//     t.join();
//   }
// }

// bool AsyncParser::CheckColumnNames() {
//   m_header_line = ParseHelper::SplitFirstChunk(m_reading_buffer, '\n');
//   auto header_columns = ParseHelper::SplitLine(m_header_line, ',');
//   if (m_column_names != header_columns) {
//     throw ExceptionManager::InvalidHeaderLine();
//   }
//   return true;
// }

// void AsyncParser::SetColumnNames(const std::vector<std::string_view> &cols) {
//   m_column_names = std::move(cols);
// }

// #endif

// CSVReader::~CSVReader() {
//   if (m_file)
//     m_file.close();
// }

// void CSVReader::Read(std::string_view csv) {
//   reader.Init(OpenFile(csv), GetSourceFileSize());
//   reader.ReadDataFromCSV();
//   m_file.close();
// }

// std::unique_ptr<BaseIO> CSVReader::OpenFile(std::string_view csv) {
//   if (!CheckFileIsCSV(csv))
//     throw ExceptionManager::FileIsInvalid(csv.data());
//   m_file.open(csv, std::ios::binary);
//   if (!m_file.is_open())
//     throw ExceptionManager::FileOpenFailure(csv.data());
//   m_file_size = CalcFileByteSize(m_file);
//   return std::make_unique<IStreamIO>(m_file);
// }

// bool CSVReader::CheckFileIsCSV(std::string_view csv) const {
//   return extractExtension(csv) == ".csv";
// }
