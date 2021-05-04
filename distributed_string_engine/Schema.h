#pragma once

#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "Definitions.h"
#include "fmt/core.h"

class _Schema;

class _Datatype
{
public:
    _Datatype(std::string variableName)
        : _variableName(variableName)
        , _variableIdentifier(variableName + "_" + generateUniqueVariableSuffix()) {}
    virtual std::string datatype() = 0;
    std::string id() { return _variableIdentifier; };
    std::string declaration(std::string parameter = "") { return datatype() + " " + id() + ((parameter != "") ? "(" + parameter + ");" : ";"); }

    virtual bool contains(std::shared_ptr<_Datatype> variable) { return false; };

    virtual std::string getAccessor(std::shared_ptr<_Datatype> variable) { throw std::runtime_error("Cannot access requested variable"); };

    using BaseType = _Datatype;

    static std::string generateUniqueVariableSuffix() {
        static int identifierCounter = 0;
        return std::to_string(identifierCounter++);
    }

    virtual std::shared_ptr<_Datatype> clone() = 0;

    friend class _Schema;

protected:
    std::string _variableName;
    std::string _variableIdentifier;
};


class _ContainerType : public _Datatype
{
public:
    _ContainerType(std::shared_ptr<_Datatype> valueType)
        : _Datatype("container")
        , _valueType(valueType) {}

    std::string datatype() override { return "std::vector<" + _valueType->datatype() + ">"; }

    std::shared_ptr<_Datatype> getValueType() { return _valueType; }

    std::shared_ptr<_Datatype> clone() override { return std::shared_ptr<_Datatype>(new _ContainerType(*this)); }

    using BaseType = _Datatype;

private:
    _ContainerType(_ContainerType& containerType)
        : _Datatype("containerCopy")
        , _valueType(containerType.getValueType()) {}

    std::shared_ptr<_Datatype> _valueType;
};

DECLARE_SHARED_PTR(ContainerType);

class _QueueType : public _Datatype
{
public:
    _QueueType(std::shared_ptr<_Datatype> valueType)
        : _Datatype("queue")
        , _valueType(valueType) {}

    std::string datatype() override { return "std::deque<" + _valueType->datatype() + ">"; }

    std::shared_ptr<_Datatype> getValueType() { return _valueType; }

    std::shared_ptr<_Datatype> clone() override { return std::shared_ptr<_Datatype>(new _QueueType(*this)); }

    using BaseType = _Datatype;

private:
    _QueueType(_QueueType& queueType)
        : _Datatype("queueCopy")
        , _valueType(queueType.getValueType()) {}
    std::shared_ptr<_Datatype> _valueType;
};

DECLARE_SHARED_PTR(QueueType);


class _MapType : public _Datatype
{
public:
    _MapType(std::shared_ptr<_Datatype> keyType, std::shared_ptr<_Datatype> valueType)
        : _Datatype("map")
        , _keyType(keyType)
        , _valueType(valueType) {}

    std::string datatype() override { return "std::unordered_map<" + _keyType->datatype() + "," + _valueType->datatype() + ">"; }

    std::shared_ptr<_Datatype> getKeyType() { return _keyType; }

    std::shared_ptr<_Datatype> getValueType() { return _valueType; }

    std::shared_ptr<_Datatype> clone() override { return std::shared_ptr<_Datatype>(new _MapType(*this)); }

    using BaseType = _Datatype;

private:
    _MapType(_MapType& mapType)
        : _Datatype("mapCopy")
        , _keyType(mapType._keyType)
        , _valueType(mapType._valueType) {}

    std::shared_ptr<_Datatype> _keyType;
    std::shared_ptr<_Datatype> _valueType;
};

DECLARE_SHARED_PTR(MapType);

class _TupleType : public _Datatype
{
public:
    _TupleType(std::vector<std::shared_ptr<_Datatype>> tupleElements)
        : _Datatype("tuple")
        , _tupleElements(tupleElements) {}

    virtual bool contains(std::shared_ptr<_Datatype> variable) override {
        for (auto tupleElement : _tupleElements)
            if (tupleElement == variable || tupleElement->contains(variable))
                return true;
        return false;
    };

    virtual std::string getAccessor(std::shared_ptr<_Datatype> variable) override {
        int index = 0;
        for (auto tupleElement : _tupleElements) {
            if (tupleElement == variable)
                return "std::get<" + std::to_string(index) + ">({})";

            if (tupleElement->contains(variable))
                return fmt::format(tupleElement->getAccessor(variable), "std::get<" + std::to_string(index) + ">({})");

            index++;
        }

        throw std::runtime_error("Cannot access requested variable");
    };

    std::string datatype() override {
        std::string typeString = "std::tuple<";
        for (auto& tupleElement : _tupleElements)
            typeString += tupleElement->datatype() + ",";
        typeString.pop_back();
        typeString += ">";
        return typeString;
    }

    std::shared_ptr<_Datatype> clone() override { return std::shared_ptr<_Datatype>(new _TupleType(*this)); }

    using BaseType = _Datatype;

private:
    _TupleType(_TupleType& tupleType)
        : _Datatype("tupleCopy")
        , _tupleElements(tupleType._tupleElements) {}

    std::vector<std::shared_ptr<_Datatype>> _tupleElements;
};

DECLARE_SHARED_PTR(TupleType);


class _POD : public _Datatype
{
public:
    _POD(std::string dataType, std::string variableName)
        : _Datatype(variableName)
        , _dataType(dataType) {}

    std::string datatype() override { return _dataType; }

    std::shared_ptr<_Datatype> clone() override { return std::shared_ptr<_Datatype>(new _POD(*this)); }

    using BaseType = _Datatype;

private:
    _POD(_POD& _pod)
        : _Datatype(_pod._variableName + "Copy")
        , _dataType(_pod.datatype()) {}

    std::string _dataType;
};

DECLARE_SHARED_PTR(POD);

class _Schema
{
public:
    _Schema(std::shared_ptr<_Datatype> variable)
        : _variables({variable}) {}
    _Schema(std::vector<std::shared_ptr<_Datatype>> variables)
        : _variables(variables){};

    std::vector<std::shared_ptr<_Datatype>>& getVariables() { return _variables; }

    std::shared_ptr<_Datatype> getOnly() {
        if (_variables.size() != 1)
            throw std::runtime_error("Requesting only element of schema, but is not the only element.");

        return _variables.front();
    }

    std::string getAccessor(std::shared_ptr<_Datatype> variableToFind) {
        auto result = std::find(_variables.begin(), _variables.end(), variableToFind);
        if (result != _variables.end())
            return (*result)->id();

        for (auto variable : _variables) {
            if (variable->contains(variableToFind))
                return fmt::format(variable->getAccessor(variableToFind), variable->id());
        }

        throw std::runtime_error("Cannot access requested variable in current schema!");
    }

    /*std::shared_ptr<_Datatype> get(std::string variableName) const
    {
        auto result =
            std::find_if(_variables.begin(), _variables.end(), [&](auto variable) { return variable->_variableName == variableName; });
        if (result != _variables.end())
            return *result;
        else
            return nullptr;
    }*/

    using BaseType = _Schema;

private:
    std::vector<std::shared_ptr<_Datatype>> _variables;
};

DECLARE_SHARED_PTR(Schema);

class _AggregationSchema : public _Schema
{
public:
    enum AggregationType
    {
        key,
        min,
        max,
        sum,
        noop
    };

    static std::vector<std::shared_ptr<_Datatype>> convertToSchemaVariables(
        const std::vector<std::pair<std::shared_ptr<_Datatype>, AggregationType>>& variables) {
        std::vector<std::shared_ptr<_Datatype>> result;
        result.reserve(variables.size());
        for (const auto& [datatype, aggregationType] : variables)
            result.emplace_back(datatype);
        return result;
    }

    std::vector<std::pair<std::shared_ptr<_Datatype>, AggregationType>>& getAggregationVariables() { return _variables; };

    explicit _AggregationSchema(const std::vector<std::pair<std::shared_ptr<_Datatype>, AggregationType>>& variables)
        : _Schema(convertToSchemaVariables(variables))
        , _variables(variables){};

    using BaseType = _AggregationSchema;

private:
    std::vector<std::pair<std::shared_ptr<_Datatype>, AggregationType>> _variables;
};

DECLARE_SHARED_PTR(AggregationSchema);

namespace AggregationStrategy
{
    struct _AggregationStrategy
    {
        virtual std::string run(std::string leftOperand, std::string rightOperand) = 0;
        virtual std::set<std::string> getHeaders() { return {}; };
    };

    struct _Diff : public _AggregationStrategy
    {
        std::string run(std::string leftOperand, std::string rightOperand) override { return leftOperand + " - " + rightOperand; }
        using BaseType = _AggregationStrategy;
    };
    DECLARE_SHARED_PTR(Diff);

    struct _Max : public _AggregationStrategy
    {
        std::string run(std::string leftOperand, std::string rightOperand) override {
            return "std::max(" + leftOperand + ", " + rightOperand + ")";
        }

        //std::set<std::string> getHeaders() override { return {"<algorithm>"}; }

        using BaseType = _AggregationStrategy;
    };
    DECLARE_SHARED_PTR(Max);
}
