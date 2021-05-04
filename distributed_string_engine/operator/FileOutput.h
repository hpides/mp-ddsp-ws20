#pragma once

#include <iostream>

#include "../Definitions.h"
#include "Operator.h"
#include "fmt/core.h"

namespace Operator
{
    template<typename... Args>
    class _FileOutput : public _ConsumingOperator
    {
    public:
        _FileOutput(std::string formatString, Args... variables)
            : _variables(std::make_tuple(variables...)) {
            _formatString = std::regex_replace(formatString, std::regex(","), R"( << "," << )");
        }

        using BaseType = _ConsumingOperator;

        std::string getGlobalDeclarations() override {
            return _outputFile->declaration(R"("output" + std::to_string(mpiConfig.MPIRank) + ".csv", std::ios::out | std::ios::trunc)");
        }

        std::string getCode() override {
            auto getAccessor = [this](std::shared_ptr<_Datatype> variable) -> std::string { return _inputSchema->getAccessor(variable); };
            auto tupleStrings = std::apply([&](auto... x) { return std::make_tuple(getAccessor(x)...); }, _variables);
            std::stringstream stream;
            stream << _outputFile->id() << " << std::fixed << "
                   << std::apply([&](auto... tupleStrings) { return fmt::format(_formatString, tupleStrings...); }, tupleStrings)
                   << " << std::endl;";
            return stream.str();
        }

        std::set<std::string> getHeaders() override { return {"<iostream>", "<fstream>"}; };

    private:
        std::string _formatString;
        std::tuple<Args...> _variables;
        std::shared_ptr<_Datatype> _outputFile = POD("std::ofstream", "output");
    };

    template<typename... Args>
    auto FileOutput(std::string formatString, Args... args) {
        return std::shared_ptr<_ConsumingOperator>(new _FileOutput<Args...>{formatString, args...});
    }
}
