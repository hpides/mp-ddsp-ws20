#include "googletest/googletest/include/gtest/gtest.h"
#include "../OperatorPipe.h"
#include "../operators/ConstantInputOperator.h"
#include "../operators/FilterOperator.h"
#include "../operators/LambdaOutputOperator.h"

#include <queue>

namespace
{
    TEST(FilterSimpleData, nothing_is_filtered) {
        std::queue<int> inputData;
        inputData.push(1);
        inputData.push(2);
        inputData.push(3);

        std::queue<int> outputData;

        ConstantInputOperator<int>(inputData) |
            FilterOperator<int>([](int i) { return i < 10; }) |
            LambdaOutputOperator<int>(3, [](int iteration_number, int value) {
                ASSERT_EQ(value ,iteration_number);
        });
    }

    TEST(FilterSimpleData, something_is_filtered) {
        std::queue<int> inputData;
        inputData.push(1);
        inputData.push(2);
        inputData.push(3);

        std::queue<int> outputData;

        ConstantInputOperator<int>(inputData) |
        FilterOperator<int>([](int i) { return i > 2; }) |
        LambdaOutputOperator<int>(1, [](int iteration_number, int value) {
          ASSERT_EQ(value,3);
        });
    }
}
