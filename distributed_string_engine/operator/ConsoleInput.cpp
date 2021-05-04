#include "ConsoleInput.h"

using namespace Operator;

std::set<std::string> _ConsoleInput::getHeaders() {
    return {"<iostream>"};
}

std::string _ConsoleInput::getCode() {
    return "std::string input;"
           "while (std::cin >> input) {"
           "    char* str = const_cast<char*>(input.c_str());"
           "    char* pos = str;"
           "    char* end = str + input.size();"
           "    {yield}"
           "}";
}

std::shared_ptr<_Schema> _ConsoleInput::getOutputSchema() {
    return Schema(POD("std::string", "input"));
}
