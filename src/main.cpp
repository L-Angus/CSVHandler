#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

 #include "CSVReader.h"


int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <filename.csv>" << std::endl;
    return 1;
  }

  std::string filename = argv[1];
  CSVReader reader;
  auto start = std::chrono::high_resolution_clock::now();
  try {
    reader.Read(filename);
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "Cost time: " << duration << "ms" << std::endl;
  std::cout << "Total: " << reader.GetRows().size() << std::endl;

  return 0;
}
