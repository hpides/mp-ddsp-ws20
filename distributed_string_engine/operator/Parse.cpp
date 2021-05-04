#include "Parse.h"

using namespace Operator;

std::set<std::string> _Parse::getHeaders()
{
    return {};
}

std::string _Parse::getCode()
{
    std::stringstream stream;
    for (int i = 0; i < _schema->getVariables().size() - 1; i++) {
        auto datatype = _schema->getVariables()[i];
        stream << parse("*pos != ','", "pos", datatype->datatype(), datatype->id());
    }
    auto lastDatatype = _schema->getVariables().back();
    stream << parse("pos != end", "pos", lastDatatype->datatype(), lastDatatype->id()) << std::endl;
    stream << "{yield}";
    return stream.str();
};

std::string _Parse::parseIntegral()
{
    std::stringstream stream;
    stream << "{variableType} {variableName} = 0;" << std::endl
           << "{" << std::endl
           << "while ({whileCondition})" << std::endl
           << "    {variableName} = {variableName} * 10 + (*{posName}++ - '0');" << std::endl
           << "{posName}++;" << std::endl
           << "}" << std::endl;

    return preprocessForFmt(stream.str(), {"variableName", "variableType", "posName", "whileCondition"});
}

std::string _Parse::parseFloatingPoint()
{
    std::stringstream stream;
    stream << "{variableType} {variableName} = 0.0;" << std::endl
           << "{" << std::endl
           << "while (*{posName} >= '0' && *{posName} <= '9')" << std::endl
           << "    {variableName} = {variableName} * 10 + (*{posName}++ - '0');" << std::endl
           << "{posName}++;uint8_t numDecimals = 0; {variableType} tmp = 0.0;" << std::endl
           << "while ({whileCondition}) {" << std::endl
           << "    tmp = tmp * 10 + (*{posName}++ - '0');numDecimals++;" << std::endl
           << "}" << std::endl
           << "{posName}++;" << std::endl
           << "{variableName} += tmp / std::pow(10.0, numDecimals);"
           << "}" << std::endl;

    return preprocessForFmt(stream.str(), {"variableName", "variableType", "posName", "whileCondition"});
}

std::string _Parse::parse(const std::string& whileCondition, const std::string& posName, const std::string& variableType, const std::string& variableName)
{
    return fmt::format(
        (variableType == "double" || variableType == "float") ? parseFloatingPoint() : parseIntegral(),
        fmt::arg("whileCondition", whileCondition),
        fmt::arg("posName", posName),
        fmt::arg("variableName", variableName),
        fmt::arg("variableType", variableType));
}
