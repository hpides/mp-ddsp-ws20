#pragma once

#include "Definitions.h"

// As flink, this only works with equal joins
template<typename LeftInputType, typename RightInputType, typename JoinType>
class _HashJoinOperator : public _GeneratingOperator<std::vector<std::pair<LeftInputType, RightInputType>>>
{
public:
    _HashJoinOperator(
        std::shared_ptr<_GeneratingOperator<std::vector<LeftInputType>>> leftInputStream,
        std::shared_ptr<_GeneratingOperator<std::vector<RightInputType>>> rightInputStream,
        std::function<JoinType(LeftInputType)> leftKeyGetter,
        std::function<JoinType(RightInputType)> rightKeyGetter)
        : _leftInputStream(leftInputStream)
        , _rightInputStream(rightInputStream)
        , _leftKeyGetter(leftKeyGetter)
        , _rightKeyGetter(rightKeyGetter)
    {}

    std::vector<std::pair<LeftInputType, RightInputType>> execute() override
    {
        map.clear();
        std::vector<std::pair<LeftInputType, RightInputType>> result;
        auto leftInputStreamFuture = std::async(std::launch::async, [&]() {
            return _leftInputStream->execute();
        });
        auto rightInputStreamFuture = std::async(std::launch::async, [&]() {
            return _rightInputStream->execute();
        });
        for (const auto& element : leftInputStreamFuture.get()) {
            map.emplace(_leftKeyGetter(element), element);
        }

        //auto rightInputStream = _rightInputStream->execute();
        for (const auto& rightElement : rightInputStreamFuture.get()) {
            auto key = _rightKeyGetter(rightElement);
            auto range = map.equal_range(key);
            for (auto it = range.first; it != range.second; it++) {
                result.emplace_back(it->second, rightElement);
            }
        }
        return result;
    }

private:
    std::shared_ptr<_GeneratingOperator<std::vector<LeftInputType>>> _leftInputStream;
    std::shared_ptr<_GeneratingOperator<std::vector<RightInputType>>> _rightInputStream;
    std::function<JoinType(LeftInputType)> _leftKeyGetter;
    std::function<JoinType(RightInputType)> _rightKeyGetter;
    std::unordered_multimap<JoinType, LeftInputType> map;
};
DECLARE_SHARED_PTR(HashJoinOperator);
