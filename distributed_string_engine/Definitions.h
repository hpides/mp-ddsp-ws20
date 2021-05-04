#pragma once

#include <memory>
#include <regex>
#include <sstream>
#include <vector>

#define DECLARE_SHARED_PTR(PTR_NAME) \
    template<typename... ConstructorArgs> \
    auto PTR_NAME(ConstructorArgs... constructorArgs) { \
        return std::shared_ptr<typename _##PTR_NAME::BaseType>(new _##PTR_NAME{constructorArgs...}); \
    };


#define DECLARE_SHARED_PTR_TEMPLATE(PTR_NAME) \
    template<typename... Args, typename... ConstructorArgs> \
    auto PTR_NAME(ConstructorArgs... constructorArgs) { \
        return std::shared_ptr<typename _##PTR_NAME<Args...>::BaseType>(new _##PTR_NAME<Args...>{constructorArgs...}); \
    };

static std::string preprocessForFmt(std::string formatString, std::vector<std::string> wildcards) {
    formatString = std::regex_replace(formatString, std::regex("\\{|\\}"), "$&$&");
    for (auto& wildcard : wildcards)
        formatString = std::regex_replace(formatString, std::regex("\\{\\{" + wildcard + "\\}\\}"), "{" + wildcard + "}");

    return formatString;
}
