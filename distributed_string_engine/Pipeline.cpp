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

std::pair<std::vector<std::shared_ptr<_Datatype>>, std::string> _Pipeline::getQueryCode() {
    std::vector<std::shared_ptr<_Datatype>> threadIdentifiers{POD("std::thread", "thread")};
    std::string queryCode = threadIdentifiers.back()->datatype() + " " + threadIdentifiers.back()->id() + "([&] {{ {yield} }});";

    for (int i = 0; i < _operators.size(); i++) {
        queryCode = fmt::format(queryCode, fmt::arg("yield", _operators[i]->getCode()));

        if (_operators[i]->isPipelineBreaker()) {
            threadIdentifiers.push_back(POD("std::thread", "thread"));
            queryCode += "\n" + threadIdentifiers.back()->datatype() + " " + threadIdentifiers.back()->id() + "([&] { "
                + dynamic_pointer_cast<Operator::_PipelineBreaker>(_operators[i])->getNewThreadCode() + " });";
        }

        if (i < _operators.size() - 1) {
            queryCode = preprocessForFmt(queryCode, {"yield"});
        }
    }

    return {threadIdentifiers, queryCode};
}

std::string _Pipeline::generateSourcefile() {
    std::stringstream file;
    for (auto& header : getHeaders())
        file << "#include " + header << std::endl;

    auto [threadIdentifier, queryCode] = getQueryCode();
    file << std::endl
         << "int main(int argc, char* argv[]) {" << std::endl
         << "struct MPIConfig"
            "{"
            "    int MPIRank, MPISize, providedThreadSupportLevel;"
            "};"
            "MPIConfig mpiConfig{};"
            "MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpiConfig.providedThreadSupportLevel);"
            "MPI_Comm_rank(MPI_COMM_WORLD, &mpiConfig.MPIRank);"
            "MPI_Comm_size(MPI_COMM_WORLD, &mpiConfig.MPISize);"
         << getGlobalDeclarations() << std::endl
         << queryCode << std::endl;
    for (const auto& thread : threadIdentifier)
        file << thread->id() << ".join();" << std::endl;
    file << "return 0;" << std::endl << "}" << std::endl;

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
        auto [exitCode, processOutput] = runCommand("../openmpi/bin/mpicxx -L../openmpi/lib/ -std=c++2a -pthread -O3 -o compiled_queries/query -I "
                                                    "../generated_code_libraries/ compiled_queries/query.cpp");
        if (exitCode == 0) {
            std::cout << "done!" << std::endl;
        } else {
            std::cout << "failed!" << std::endl;
        }
    }
}
