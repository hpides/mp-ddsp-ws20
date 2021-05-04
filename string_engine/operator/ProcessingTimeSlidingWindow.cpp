#include "ProcessingTimeSlidingWindow.h"

#include <iostream>
#include <sstream>

using namespace Operator;

void _ProcessingTimeSlidingWindow::setInputSchema(std::shared_ptr<_Schema> inputSchema) {
    _inputSchema = inputSchema;

    _outputSchema = Schema(QueueType(ContainerType(TupleType(_inputSchema->getVariables()))));
}

std::shared_ptr<_Schema> _ProcessingTimeSlidingWindow::getOutputSchema() {
    return _outputSchema;
}

std::string _ProcessingTimeSlidingWindow::getCode() {
    auto queueIdentifier = _outputSchema->getOnly()->getVariableIdentifier();
    std::stringstream stream;

    // build tuple
    stream << "auto newTuple = std::make_tuple(";
    auto& variables = _inputSchema->getVariables();
    for (int i = 0; i < variables.size() - 1; i++)
        stream << variables[i]->getVariableIdentifier() << ",";
    stream << variables.back()->getVariableIdentifier() << ");" << std::endl;

    //evaluate if window closes
    stream << "std::chrono::time_point<std::chrono::high_resolution_clock> currentTime = std::chrono::high_resolution_clock::now();"
           << std::endl;

    stream << "while (std::chrono::time_point(" << _nextWindowClosesAtIdentifier << ") <= currentTime) {{"
           << std::endl
           //update next window closing
           << _nextWindowClosesAtIdentifier << " += std::chrono::milliseconds(" << _slideSizeMilliseconds << ");" << std::endl
           << "while (" << queueIdentifier << ".size() > " << _slidesPerWindow << ") {{" << std::endl
           << queueIdentifier << ".pop_front(); " << std::endl
           << "}}" << std::endl
           << "{yield}"
           << queueIdentifier << ".emplace_back();"
           << "}}" << std::endl;

    //push to queue
    stream << queueIdentifier << ".back().push_back(newTuple);" << std::endl;

    return stream.str();
}

std::set<std::string> _ProcessingTimeSlidingWindow::getHeaders() {
    return {{"<queue>"}, {"<tuple>"}, {"<chrono>"}};
}

std::string _ProcessingTimeSlidingWindow::getGlobalDeclarations() {
    std::stringstream stream;
    stream << _outputSchema->getOnly()->getDeclaration() << std::endl;
    stream << "std::chrono::time_point<std::chrono::high_resolution_clock> " << _nextWindowClosesAtIdentifier
           << " = std::chrono::high_resolution_clock::now() + "
           << "std::chrono::milliseconds(" << _slideSizeMilliseconds << ");" << std::endl;

    return stream.str();
}
