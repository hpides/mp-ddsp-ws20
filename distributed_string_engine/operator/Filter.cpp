#include "Filter.h"

#include <iostream>

using namespace Operator;

std::string _Filter::getCode()
{
    std::stringstream stream;
    stream << "if ({filterDatatype} {filterExpression} ) continue;" << std::endl;
    stream << "{{yield}}" << std::endl;
    std::string result = fmt::format(
        stream.str(), fmt::arg("filterDatatype", _filterDatatype->id()), fmt::arg("filterExpression", _filterExpression));
    return result;
}
