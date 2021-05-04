#include "HashJoin.h"

#include <iostream>

using namespace Operator;

std::string _HashJoinInputAdapter::getGlobalDeclarations() {
    std::stringstream stream;
    stream << _outputSchema->getOnly()->declaration() << std::endl;
    stream << _closedSemaphore->declaration() << std::endl;
    stream << _consumedSemaphore->declaration("1") << std::endl;
    return stream.str();
}

std::string _HashJoinInputAdapter::getCode() {
    std::stringstream stream;

    stream << _consumedSemaphore->id() << ".wait();"
           << _outputSchema->getOnly()->id() << " = std::move("
           << _inputSchema->getOnly()->id() + ");" << std::endl
           << _closedSemaphore->id() << ".notify();" << std::endl;
    return stream.str();
}

std::set<std::string> _HashJoin::getHeaders() {
    std::set<std::string> headers{"<thread>", "\"../../generated_code_libraries/Semaphore.h\""};
    auto leftSideHeaders = _leftPipeline->getHeaders();
    headers.insert(leftSideHeaders.begin(), leftSideHeaders.end());

    auto rightSideHeader = _rightPipeline->getHeaders();
    headers.insert(rightSideHeader.begin(), rightSideHeader.end());

    return headers;
};

std::string _HashJoin::getCode() {
    auto [leftThreadIdentifier, leftPipelineQueryCode] = _leftPipeline->getQueryCode();
    auto [rightThreadIdentifier, rightPipelineQueryCode] = _rightPipeline->getQueryCode();

    std::stringstream stream;
    stream << leftPipelineQueryCode << std::endl
           << rightPipelineQueryCode
           << std::endl
           << std::endl
           << "while (true) {" << std::endl
           << _leftWindowClosedSemaphore->id() << ".wait();" << std::endl
           << _rightWindowClosedSemaphore->id() << ".wait();" << std::endl
           << "for (auto& [" << _keyType->id() << "," << _leftElementType->id()
           << "] : " << _leftPipeline->getPipelineOutputSchema()->getOnly()->id() << ") {" << std::endl
           << "if (auto it = " << _rightPipeline->getPipelineOutputSchema()->getOnly()->id() << ".find("
           << _keyType->id()
           << "); it != " << _rightPipeline->getPipelineOutputSchema()->getOnly()->id() << ".end()) {" << std::endl
           << "auto& " << _rightElementType->id() << " = it->second;" << std::endl
           << "{yield}" << std::endl
           << "}" << std::endl
           << "}" << std::endl
           << _leftWindowConsumedSemaphore->id() << ".notify();"
           << _rightWindowConsumedSemaphore->id() << ".notify();"
           << "}";

    return stream.str();
}

std::string _HashJoin::getGlobalDeclarations() {
    std::string globalDeclarations;
    globalDeclarations += _leftPipeline->getGlobalDeclarations();
    globalDeclarations += _rightPipeline->getGlobalDeclarations();

    return globalDeclarations;
}

std::shared_ptr<_Schema> _HashJoin::getOutputSchema() {
    return Schema(std::vector{_keyType, _leftElementType, _rightElementType});
}
