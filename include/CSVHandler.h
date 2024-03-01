//
// Created by 李顺东 on 2024/2/21.
//

#ifndef CSV_CSVHANDLER_H
#define CSV_CSVHANDLER_H

#include <string>
#include <unordered_map>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <set>
#include <iostream>
#include <thread>
#include <sstream>
#include <fstream>
#include <atomic>

using CSVRowData = std::vector<std::string>;
using DataContainer = std::vector<CSVRowData>;
using DataFetcher = std::vector<std::string>;

//class TaskScheduler
//{
//    struct DataChannel
//    {
//        explicit DataChannel(const std::string &data) { ParseRowData(data); }
//        CSVRowData GetRowData() { return m_row_data; }
//
//    private:
//        void ParseRowData(const std::string &row)
//        {
//            std::stringstream ss(row);
//            std::string cell;
//            while (std::getline(ss, cell, ','))
//            {
//                m_row_data.push_back(cell);
//            }
//        }
//        CSVRowData m_row_data;
//    };
//
//public:
//    TaskScheduler() = default;
//    TaskScheduler(const TaskScheduler &) = delete;
//    TaskScheduler &operator=(const TaskScheduler &) = delete;
//    ~TaskScheduler() = default;
//
//    void read_with_header(const std::string &filename)
//    {
//        std::ifstream infile(filename);
//        if (!infile.is_open())
//        {
//            std::cerr << "Error: Failed to open file: " << filename << std::endl;
//            return;
//        }
//        std::string header;
//        std::getline(infile, header); // 读取并忽略首行
//        std::string line;
//        while (std::getline(infile, line))
//        {
//            TaskFetcher(line);
//        }
//        infile.close(); // 关闭文件流
//    }
//
//    void start()
//    {
//        isStoped.store(false);
//        std::thread producer_thread(&TaskScheduler::TaskProducer, this);
//        std::thread consumer_thread(&TaskScheduler::TaskConsumer, this);
//        if (producer_thread.joinable())
//            producer_thread.join();
//        if (consumer_thread.joinable())
//            consumer_thread.join();
//        releaseFetcher();
//        if (!exception_handler.empty())
//        {
//            auto exception = exception_handler.front();
//            exception_handler.pop();
//            std::rethrow_exception(exception);
//        }
//    }
//
//    void stop()
//    {
//        isStoped.store(true);
//        condProducer.notify_all();
//        condComsumer.notify_all();
//    }
//
//    DataContainer DataGenerator() const { return m_generator; }
//
//protected:
//    void TaskProducer()
//    {
//        for (auto &fetch : m_fetched)
//        {
//            {
//                std::unique_lock<std::mutex> lock(m_mutex);
//                condProducer.wait(lock, [this]()
//                                  { return DataQueue.size() < m_fetched.size() / 10.0 || isStoped; });
//                if (IsStoped())
//                    return;
//                DataQueue.push(DataChannel(fetch));
//            }
//            condComsumer.notify_one();
//        }
//    }
//    void TaskConsumer()
//    {
//        std::cout << "TaskConsumer start" << std::endl;
//        m_generator.clear();
//        while (!IsStoped() && !isMaxSize())
//        {
//            try
//            {
//                TakeTask();
//            }
//            catch (...)
//            {
//                std::exception_ptr exception = std::current_exception();
//                WorkerExceptionCapture(exception);
//                isStoped.store(true);
//                condProducer.notify_one();
//                break;
//            }
//        }
//    }
//    void TakeTask()
//    {
//        CSVRowData temp;
//        {
//            std::unique_lock<std::mutex> lock(m_mutex);
//            condProducer.wait(lock, [this]()
//                              { return !m_fetched.empty(); });
//            temp = std::move(DataQueue.front().GetRowData());
//            DataQueue.pop();
//        }
//        m_generator.push_back(temp);
//        condProducer.notify_one();
//    }
//
//    void WorkerExceptionCapture(const std::exception_ptr &exception)
//    {
//        std::unique_lock<std::mutex> lock(m_mutex);
//        if (!exception)
//            return;
//        exception_handler.emplace(exception);
//    }
//
//    void TaskFetcher(const std::string &line) { m_fetched.emplace_back(std::move(line)); }
//    bool isMaxSize() { return m_generator.size() == m_fetched.size(); }
//    bool IsStoped() { return isStoped.load(); }
//    void releaseFetcher() { DataFetcher().swap(m_fetched); }
//
//private:
//    std::queue<DataChannel> DataQueue;
//    std::mutex m_mutex;
//    std::condition_variable condComsumer;
//    std::condition_variable condProducer;
//    DataFetcher m_fetched;
//    DataContainer m_generator;
//    std::queue<std::exception_ptr> exception_handler;
//    std::atomic<bool> isStoped{false};
//};


#endif // CSV_CSVHANDLER_H
