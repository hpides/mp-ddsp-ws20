#pragma once

#include <memory>

#define DECLARE_SHARED_PTR(PTR_NAME) \
    template<typename... Args, typename... ConstructorArgs> \
    auto PTR_NAME(ConstructorArgs... constructorArgs) \
    { \
        return std::shared_ptr<typename _##PTR_NAME<Args...>::BaseType>(new _##PTR_NAME<Args...>{constructorArgs...}); \
    };

#define DECLARE_SHARED_PTR_NON_TEMPLATE(PTR_NAME) \
    template<typename... ConstructorArgs> \
    auto PTR_NAME(ConstructorArgs... constructorArgs) \
    { \
        return std::shared_ptr<typename _##PTR_NAME::BaseType>(new _##PTR_NAME{constructorArgs...}); \
    };
