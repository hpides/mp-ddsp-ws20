#pragma once

#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "Definitions.h"
#include "fmt/format.h"
#include "operator/Operator.h"

class _Pipeline
{
public:
    using BaseType = _Pipeline;

    void add(std::shared_ptr<Operator::_ProducingOperator> op) { _operators.push_back(op); }
    void add(std::shared_ptr<Operator::_TransformingOperator> op) {
        if (_operators.empty())
            throw std::runtime_error("TransformingOperator can not be at the beginning of a pipeline!");
        op->setInputSchema(std::dynamic_pointer_cast<Operator::_ProducingOperator>(_operators.back())->getOutputSchema());
        _operators.push_back(op);
    }
    void add(std::shared_ptr<Operator::_ConsumingOperator> op) {
        if (_operators.empty())
            throw std::runtime_error("ConsumingOperator can not be at the beginning of a pipeline!");
        op->setInputSchema(std::dynamic_pointer_cast<Operator::_ProducingOperator>(_operators.back())->getOutputSchema());
        _operators.push_back(op);
    }

    std::set<std::string> getHeaders();
    std::string getGlobalDeclarations();
    std::string getQueryCode();
    std::string generateSourcefile();

    std::shared_ptr<_Schema> getPipelineOutputSchema() {
        if (_operators.empty()) {
            return Schema(std::vector<std::shared_ptr<_Datatype>>{});
        } else {
            return std::dynamic_pointer_cast<Operator::_ProducingOperator>(_operators.back())->getOutputSchema();
        }
    }

    void compile();


private:
    std::vector<std::shared_ptr<Operator::_Operator>> _operators;
};

DECLARE_SHARED_PTR(Pipeline)
