#ifndef EXCEPTION_MANAGER_HPP
#define EXCEPTION_MANAGER_HPP

#include <exception>
#include <string>

namespace ExceptionManager {

class CSVException : public std::exception {
public:
  explicit CSVException(const char *message) : m_message(message) {}
  explicit CSVException(const std::string &message) : m_message(message) {}
  const char *what() const noexcept override { return m_message.c_str(); }

private:
  std::string m_message;
};

class FileOpenException : public CSVException {
public:
  FileOpenException() : CSVException("Failed to open file.") {}
  explicit FileOpenException(const std::string &file)
      : CSVException("Failed to open file: " + file) {}
};

class InputStreamReadException : public CSVException {
public:
  explicit InputStreamReadException(const std::string &details = "")
      : CSVException("Input stream read exception. " + details) {}
};

class FileIsInvalid : public CSVException {
public:
  explicit FileIsInvalid(const std::string &file)
      : CSVException("Not a csv file: " + file) {}
};

class InvalidHeaderLine : public CSVException {
public:
  explicit InvalidHeaderLine(const std::string &details = "")
      : CSVException("Invalid or missing header line in CSV. " + details) {}
};

class InvalidDataLine : public CSVException {
public:
  explicit InvalidDataLine(size_t line_number, const std::string &details = "")
      : CSVException("Invalid data at line: " + std::to_string(line_number) +
                     "," + details) {}
};

}; // namespace ExceptionManager

// class ExceptionCapture {
// public:
//   ExceptionCapture() = default;

//   template <typename T>
//   explicit ExceptionCapture(T &&exception)
//       : m_exceptions(std::make_exception_ptr(std::forward<T>(exception))) {}

//   void rethrow() const {
//     if (m_exceptions) {
//       std::rethrow_exception(m_exceptions);
//     }
//   }

// private:
//   std::exception_ptr m_exceptions;
// };

#endif //! EXCEPTION_MANAGER_HPP
