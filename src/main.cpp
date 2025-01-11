#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "CSVReader.h"

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <filename.csv>" << std::endl;
    return 1;
  }

  std::string filename = argv[1];
  // std::vector<std::string_view> files{filename, argv[2]};
  CSVParser parser(ParseMode::Synchronous);
  parser.SetColumnNames("ID", "Name", "Score");
  // parser.SetColumnNames("StimName", "StimType", "PinName", "Frequency",
  // "Power",
  //                       "WaveFile");
  std::cout << "Set success" << std::endl;
  auto start = std::chrono::high_resolution_clock::now();
  try {
    parser.ParseDataFromCSV(filename);
    std::vector<std::string_view> new_row = {"000011", "NewAdd", "88"};
    // 实现添加操作
    auto add_data = [&new_row](std::vector<std::vector<std::string_view>> &data) {
      std::vector<std::string_view> row = {};
      data.push_back(row);
      std::cout << "Data added!" << std::endl;
    };
    parser.OnAdd(add_data);
  } catch (const std::exception &e) {
    std::cout << e.what() << std::endl;
  }
  // parser.WriteCSVDataToFile(argv[2]);

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "Cost time: " << duration << "ms" << std::endl;
  std::cout << "Total: " << parser.GetCSVDataSize() << std::endl;
  // std::cout << "Column: " << parser.GetCSVData().back()[0] << std::endl;
  for (const auto &row : parser.GetCSVData()) {
    for (const auto &col : row) {
      std::cout << col << " ";
    }
    std::cout << std::endl;
  }

  return 0;
}
