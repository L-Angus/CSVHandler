#include "./CSVReader.h"
#include <charconv>
#include <iostream>

inline CSVRow ParseLine(std::string_view line, char quote = ',') {
  CSVRow row;
  size_t start = 0;
  size_t end = line.find(quote);
  while (end != std::string::npos) {
    row.emplace_back(std::string_view(line.data() + start, end - start));
    start = end + 1;
    end = line.find(quote, start);
  }
  row.emplace_back(std::string_view(line.data() + start, line.size() - start));
  return row;
}

#ifdef CSV_NO_THREAD
/****************** SyncReader ******************/

void SyncReader::Init(std::unique_ptr<BaseIO> io, size_t file_size) {
  if (!io)
    throw std::runtime_error("Init Failed.");
  m_source_io = std::move(io);
  m_total_byte_size = file_size;
  m_read_byte_size = 0;
}

bool SyncReader::HasNextLine() const {
  return m_read_byte_size < m_total_byte_size;
}

void SyncReader::Read() {
  while (HasNextLine()) {
    if (!ReadChunkData())
      break;
  }
}

bool SyncReader::ReadChunkData() {
  std::array<char, ChunkDataSize> chunk_data{};
  auto buffer_size = m_source_io->Read(chunk_data.data(), ChunkDataSize);
  if (buffer_size <= 0)
    return false;

  m_read_byte_size += buffer_size;
  std::string buffer(std::move(m_remained_data));
  buffer.append(chunk_data.data(), buffer_size);

  size_t start = 0;
  size_t end = buffer.find('\n');
  while (end != std::string::npos) {
    std::string line = buffer.substr(start, end - start);
    m_rows.emplace_back(ParseLine(line));
    start = end + 1;
    end = buffer.find('\n', start);
  }
  m_remained_data = buffer.substr(start);
  return true;
}
#else

/****************** AsyncReader ******************/
void AsyncReader::Init(std::unique_ptr<BaseIO> io, size_t file_size) {
  m_source_io = std::move(io);
  m_total_byte_size = file_size; // 获取源文件总的字节数
  m_read_byte_size = 0;          // 记录当前已读取的字节数
  m_rows.clear();                // 缓存CSV数据的容器
  m_producer_thread = std::thread(&AsyncReader::Producer, this); // 数据读取线程
  m_consumer_thread = std::thread(&AsyncReader::Consumer, this); // 数据处理线程
}

void AsyncReader::Read() {
  if (m_producer_thread.joinable())
    m_producer_thread.join();
  if (m_consumer_thread.joinable())
    m_consumer_thread.join();
}

bool AsyncReader::IsDone() const {
  return m_read_byte_size == m_total_byte_size; // 已读取所有字节的数据
}

void AsyncReader::Producer() {
  try {
    while (!IsDone()) {
      {
        std::unique_lock<std::mutex> locker(m_mutex);
        m_not_full.wait(locker, [&] {
          return m_queue.size() < ChunkDataSize;
        }); // 如果队列未满
        std::vector<char> buffer(ChunkDataSize, '\0');
        size_t read_bytes =
            m_source_io->Read(buffer.data(), ChunkDataSize); // 读取分块数据
        m_read_byte_size += read_bytes; // update已更新的字节数
        m_queue.emplace(buffer.data(),
                        buffer.size()); // 将读取的字节数据块压入队列
        locker.unlock();
      }
      m_not_empty.notify_one(); // 通知处理线程来Fetch缓冲区中数据
    }
  } catch (...) {
    m_exception.set_exception(std::current_exception());
  }
}

void AsyncReader::Consumer() {
  try {
    std::string chunk_data{};
    // 如果没有读取完、或者缓冲区不空，循环执行
    while (!IsDone() || !m_queue.empty()) {
      // 先将上次未处理的不完整行数据块进行缓存
      chunk_data = std::move(m_incomplete_line);
      {
        std::unique_lock<std::mutex> locker(m_mutex); // 控制缓冲区的访问权
        m_not_empty.wait(locker, [&] {
          return !m_queue.empty();
        });                            // 如果缓冲区空了，阻塞等待
        chunk_data += m_queue.front(); // 取出缓冲区中的数据
        m_queue.pop();                 // 将取出的数据弹栈
        locker.unlock();
      }
      m_not_full.notify_one(); // 通知生产者，已经取了数据，缓冲区不满
      // 消费者继续做处理的工作
      size_t start = 0;
      size_t end = chunk_data.find('\n', start);
      while (end != std::string::npos) {
        std::string line = chunk_data.substr(start, end - start);
        m_rows.emplace_back(ParseLine(line));
        start = end + 1;
        end = chunk_data.find('\n', start);
      }
      m_incomplete_line = chunk_data.substr(start);
      chunk_data.clear();
    }
  } catch (...) {
    m_exception.set_exception(std::current_exception());
  }
}

#endif

CSVReader::~CSVReader() {
  if (m_file)
    m_file.close();
}

void CSVReader::Read(const std::string &csv) {
  m_file.open(csv, std::ios::binary);
  if (!m_file.is_open())
    throw std::runtime_error("Failed Open CSV.");
  reader.Init(OpenFile(), GetSourceFileSize());
  reader.Read();
  m_file.close();
}

std::unique_ptr<BaseIO> CSVReader::OpenFile() {
  CheckFileSize(m_file);
  return std::make_unique<IStreamIO>(m_file);
}

void CSVReader::CheckFileSize(std::ifstream &in) {
  in.seekg(0, std::ios::end);
  m_file_size = in.tellg();
  in.seekg(0, std::ios::beg);
}
