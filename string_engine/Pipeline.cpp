#include "Pipeline.h"

#include <array>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace
{
    std::pair<int, std::string> runCommand(std::string command) {
        auto c_command = command.c_str();

        std::array<char, 128> buffer{};
        std::string processOutput;
        FILE* pipe = popen(c_command, "r");
        if (!pipe)
            throw std::runtime_error("popen() failed!");

        while (fgets(buffer.data(), buffer.size(), pipe) != nullptr)
            processOutput += buffer.data();

        auto exitCode = pclose(pipe);

        return {exitCode, processOutput};
    }
}

std::string _Pipeline::getGlobalDeclarations() {
    std::string globalDeclarations;
    for (auto& _operator : _operators)
        globalDeclarations += _operator->getGlobalDeclarations();

    return globalDeclarations;
}

std::set<std::string> _Pipeline::getHeaders() {
    std::set<std::string> headers;
    for (auto& _operator : _operators) {
        auto currentHeaders = _operator->getHeaders();
        headers.insert(currentHeaders.begin(), currentHeaders.end());
    }

    return headers;
}

std::string _Pipeline::getQueryCode() {
    std::string queryCode;

    for (auto it = _operators.rbegin(); it != _operators.rend(); ++it)
        queryCode = fmt::format((*it)->getCode(), fmt::arg("yield", queryCode));

    return queryCode;
}

std::string _Pipeline::generateSourcefile() {
    std::stringstream file;
    for (auto& header : getHeaders())
        file << "#include " + header << std::endl;
    file << std::endl
         << "int main() {" << std::endl
         << getGlobalDeclarations() << std::endl
         << getQueryCode() << std::endl
         << "return 0;" << std::endl
         << "}" << std::endl;

    return file.str();
}

void _Pipeline::compile() {
    {
        std::cout << "Pipeline generated!" << std::endl << "Writing to file...";
        std::filesystem::create_directories("compiled_queries");
        std::ofstream queryFile("compiled_queries/query.cpp");
        queryFile << generateSourcefile();
        queryFile.close();
        std::cout << "done!" << std::endl;
    }

    {
        std::cout << "Attempting Clang Format..." << std::flush;
        auto [exitCode, processOutput] = runCommand("clang-format -i compiled_queries/query.cpp");
        if (exitCode == 0) {
            std::cout << "done!" << std::endl;
        } else {
            std::cout << "failed!" << std::endl;
        }
    }

    {
        std::cout << "Attempting compilation..." << std::flush;
        auto [exitCode, processOutput] = runCommand("g++ -std=c++2a -pthread -O3 -o compiled_queries/query -I ../generated_code_libraries/ compiled_queries/query.cpp");
        if (exitCode == 0) {
            std::cout << "done!" << std::endl;
        } else {
            std::cout << "failed!" << std::endl;
        }
    }
}
