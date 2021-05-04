#include "HashJoin.h"

#include <iostream>

using namespace Operator;

std::string _HashJoinInputAdapter::getGlobalDeclarations() {
    std::stringstream stream;
    stream << _outputSchema->getOnly()->getDeclaration() << std::endl;
    stream << _semaphore->getDeclaration() << std::endl;
    return stream.str();
}

std::string _HashJoinInputAdapter::getCode() {
    std::stringstream stream;
    stream << _outputSchema->getOnly()->getVariableIdentifier() << " = std::move("
           << _inputSchema->getOnly()->getVariableIdentifier() + ");" << std::endl;
    stream << _semaphore->getVariableIdentifier() << ".notify();" << std::endl;
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
    auto leftPipelineQueryCode = _leftPipeline->getQueryCode();
    auto rightPipelineQueryCode = _rightPipeline->getQueryCode();

    std::stringstream stream;
    stream << "std::thread leftThread_" << _Datatype::generateUniqueVariableSuffix() << "([&] {" << leftPipelineQueryCode << "});"
           << std::endl
           << "std::thread rightThread_" << _Datatype::generateUniqueVariableSuffix() << "([&] {" << rightPipelineQueryCode << "});"
           << std::endl
           << std::endl
           << "while (true) {" << std::endl
           << _leftSemaphore->getVariableIdentifier() << ".wait();" << std::endl
           << _rightSemaphore->getVariableIdentifier() << ".wait();" << std::endl
           << "for (auto& [" << _keyType->getVariableIdentifier() << "," << _leftElementType->getVariableIdentifier()
           << "] : " << _leftPipeline->getPipelineOutputSchema()->getOnly()->getVariableIdentifier() << ") {" << std::endl
           << "if (auto it = " << _rightPipeline->getPipelineOutputSchema()->getOnly()->getVariableIdentifier() << ".find("
           << _keyType->getVariableIdentifier()
           << "); it != " << _rightPipeline->getPipelineOutputSchema()->getOnly()->getVariableIdentifier() << ".end()) {" << std::endl
           << "auto& " << _rightElementType->getVariableIdentifier() << " = it->second;" << std::endl
           << "{yield}" << std::endl
           << "}" << std::endl
           << "}" << std::endl
           << "}";

    return preprocessForFmt(stream.str(), {"yield"});
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
