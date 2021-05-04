#pragma once

#include <iostream>

#include "../Definitions.h"
#include "Operator.h"
#include "fmt/core.h"

namespace Operator
{
    template<typename... Args>
    class _ConsoleOutput : public _ConsumingOperator
    {
    public:
        _ConsoleOutput(std::string formatString, Args... variables)
            : _variables(std::make_tuple(variables...)) {
            _formatString = std::regex_replace(formatString, std::regex(","), R"( << "," << )");
        }

        using BaseType = _ConsumingOperator;

        std::string getCode() override {
            auto getAccessor = [this](std::shared_ptr<_Datatype> variable) -> std::string { return _inputSchema->getAccessor(variable); };
            auto tupleStrings = std::apply([&](auto... x) { return std::make_tuple(getAccessor(x)...); }, _variables);
            std::stringstream stream;
            stream << "std::cout << std::fixed << "
                   << std::apply([&](auto... tupleStrings) { return fmt::format(_formatString, tupleStrings...); }, tupleStrings)
                   << " << std::endl;";
            return stream.str();
        }

        std::set<std::string> getHeaders() override { return {"<iostream>"}; };

    private:
        std::string _formatString;
        std::tuple<Args...> _variables;
    };

    template<typename... Args>
    auto ConsoleOutput(std::string formatString, Args... args) {
        return std::shared_ptr<_ConsumingOperator>(new _ConsoleOutput<Args...>{formatString, args...});
    }
}
