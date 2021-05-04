#pragma once

#include <iostream>

#include "ConsumingOperator.h"
#include "Definitions.h"

template<typename InputType>
class _ConsoleOutputOperator : public _ConsumingOperator<InputType>
{
public:
    void executePipeline()
    {
        while (true)
            if (auto res = this->executePreviousOperator(); !res.empty())
                std::cout << res << std::endl;
            else
                break;
    }
};

template<typename ElementType>
class _ConsoleOutputOperator<std::vector<ElementType>> : public _ConsumingOperator<std::vector<ElementType>>
{
public:
    void executePipeline() override
    {
        while (true) {
            if (auto res = this->executePreviousOperator(); !res.empty())
                for (auto el : res)
                    std::cout << el << ' ';
            else
                break;
        }
    }
};

template<typename ElementType>
class _ConsoleOutputOperator<std::vector<std::tuple<ElementType, ElementType>>>
    : public _ConsumingOperator<std::vector<std::tuple<ElementType, ElementType>>>
{
public:
    void executePipeline() override
    {
        while (true) {
            if (auto res = this->_previousOperator->execute(); !res.empty())
                for (auto el : res)
                    std::cout << std::get<0>(el) << std::endl;
            else
                break;
        }
    }
};

template<template<typename... TupleArgs> typename TupleType, typename... TupleArgs>
class _ConsoleOutputOperator<std::vector<TupleType<TupleArgs...>>> : public _ConsumingOperator<std::vector<TupleType<TupleArgs...>>>
{
public:
    void executePipeline() override
    {
        while (true) {
            if (auto res = this->_previousOperator->execute(); !res.empty())
                for (auto el : res)
                    std::apply(
                        [](TupleArgs const&... tupleArgs) {
                            std::size_t n{0};
                            ((std::cout << std::fixed << tupleArgs << (++n != sizeof...(tupleArgs) ? "," : "")), ...);
                            std::cout << '\n';
                        },
                        el);
        }
    }
};
DECLARE_SHARED_PTR(ConsoleOutputOperator);
