#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "CSVReader.h"

int main(int argc, char *argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <filename.csv>" << std::endl;
    return 1;
  }

  std::string filename = argv[1];
  std::vector<std::string> files{filename, argv[2]};
  CSVParser parser(ParseMode::Asynchronous);
  parser.SetColumnNames("ID", "Name", "Score");
  auto start = std::chrono::high_resolution_clock::now();
  try {
    for (const auto &file : files) {
      parser.ParseDataFromCSV(file);
    }
  } catch (const std::exception &e) {
    std::cout << e.what() << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  std::cout << "Cost time: " << duration << "ms" << std::endl;
  std::cout << "Total: " << parser.GetCSVDataSize() << std::endl;
  // std::cout << "Column: " << parser.GetCSVData().back().back() << std::endl;

  return 0;
}
