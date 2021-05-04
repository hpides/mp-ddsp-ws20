#include "TCPReceiver.h"

#include "fmt/core.h"

using namespace Operator;

namespace
{
    std::string parseIntegral() {
        std::stringstream stream;
        stream << "{variableType} {variableName} = 0;" << std::endl
               << "{{{{" << std::endl
               << "while ({whileCondition})" << std::endl
               << "    {variableName} = {variableName} * 10 + (*{posName}++ - '0');" << std::endl
               << "{posName}++;" << std::endl
               << "}}}}" << std::endl;
        return stream.str();
    }

    std::string parseFloatingPoint() {
        std::stringstream stream;
        stream << "{variableType} {variableName} = 0.0;" << std::endl
               << "{{{{" << std::endl
               << "while (*{posName} >= '0' && *{posName} <= '9')" << std::endl
               << "    {variableName} = {variableName} * 10 + (*{posName}++ - '0');" << std::endl
               << "{posName}++;uint8_t numDecimals = 1; {variableType} tmp = 0.0;" << std::endl
               << "while ({whileCondition}) {{{{" << std::endl
               << "    tmp = tmp * 10 + (*{posName}++ - '0');numDecimals++;" << std::endl
               << "}}}}" << std::endl
               << "{posName}++;" << std::endl
               << "{variableName} += tmp / std::pow(10.0, numDecimals);"
               << "}}}}" << std::endl;
        return stream.str();
    }

    std::string parse(std::string whileCondition, std::string posName, std::string variableType, std::string variableName) {
        return fmt::format(
            (variableType == "double" || variableType == "float") ? parseFloatingPoint() : parseIntegral(),
            fmt::arg("whileCondition", whileCondition),
            fmt::arg("posName", posName),
            fmt::arg("variableName", variableName),
            fmt::arg("variableType", variableType));
    }
}

std::string _TCPReceiver::getGlobalDeclarations() {
    return "auto " + _inputSocketFuture->getVariableIdentifier()
        + " = std::async(std::launch::async, [] {"
          "    sockaddr_in serverAddr{{}};"
          "    memset(&serverAddr, 0, sizeof(sockaddr_in));"
          "    int socketDescriptor = socket(AF_INET, SOCK_STREAM, 0);"
          "    if (socketDescriptor == -1) {"
          "        std::cerr << \"Error. Could not create socket: \" << strerror(errno) << \"\\n\";"
          "        exit(EXIT_FAILURE);"
          "    }"
          "int bufferSize = "
        + std::to_string(_bufferSize)
        + ";"
          "if (setsockopt(socketDescriptor, SOL_SOCKET, SO_RCVBUF, &bufferSize, sizeof(bufferSize)) == -1) {"
          "std::cerr << \"ERROR: Setting send buffer size failed.\";"
          "}"
          "    int option = 1;\n"
          "    setsockopt(socketDescriptor, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(int));"
          "    serverAddr.sin_family = AF_INET;"
          "    serverAddr.sin_port = htons("
        + std::to_string(_port)
        + ");"
          "    inet_pton(AF_INET, \"127.0.0.1\", &serverAddr.sin_addr);"
          "    int success = connect(socketDescriptor, (sockaddr*)&serverAddr, sizeof(serverAddr));"
          "    if (success == -1) {"
          "        std::cerr << \"Error. Could not connect to server socket: \" << strerror(errno) << \"\\n\";"
          "        exit(EXIT_FAILURE);"
          "    }"
          "    return socketDescriptor;"
          "});"
        + _socketDescriptor->getDataType() + " " + _socketDescriptor->getVariableIdentifier() + "="
        + _inputSocketFuture->getVariableIdentifier() + ".get();";
}

std::string _TCPReceiver::getCode() {
    std::stringstream stream;
    stream << "const size_t bufferSize = " << _bufferSize
           << ";"
              "char buffer[bufferSize];"
              "std::string surplus;"
              "while (true) {{{{"
              "    strncpy(buffer, surplus.c_str(), surplus.size());"
              "    int bytesRead = recv(";
    stream << _socketDescriptor->getVariableIdentifier();
    stream << ", buffer + surplus.size(), bufferSize - surplus.size(), 0);"
              "    bytesRead += surplus.size();"
              "    if (bytesRead == -1) {{{{"
              "        std::cerr << \"ERROR: Receiving bytes from server failed: \" << strerror(errno) << \"\\n\";"
              "        exit(6);\n"
              "    }}}} else if (bytesRead == 0) {{{{"
              "        throw std::runtime_error(\"end of pipeline\");"
              "    }}}}"
              "    int nextStart = 0;\n"
              "    for (int i = 0; i < bytesRead; i++) {{{{"
              "        if (buffer[i] == '\\n') {{{{"
              "        int tupleLength = i - nextStart;"
              "         char* pos = buffer + nextStart;\n"
              "         char* end = pos + tupleLength;"
              "         nextStart = i + 1;"
              "         {{{{{yield}}}}}"
              "        }}}}"
              "    }}}}"
              "surplus.assign(buffer + nextStart, bytesRead - nextStart);"
              "}}}}";
    return stream.str();
}

std::set<std::string> _TCPReceiver::getHeaders() {
    return {"<arpa/inet.h>",
            "<netinet/in.h>",
            "<sys/socket.h>",
            "<cerrno>",
            "<cmath>",
            "<cstdint>",
            "<cstdlib>",
            "<cstring>",
            "<future>",
            "<iostream>",
            "<queue>",
            "<string>",
            "<thread>",
            "<unistd.h>"};
}

std::shared_ptr<_Schema> _TCPReceiver::getOutputSchema() {
    return Schema(POD("std::string", "input"));
}
