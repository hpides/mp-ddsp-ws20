# Tests

## Writing a new test
1. Create test class in this dir. 
2. Include `#include "googletest/googletest/include/gtest/gtest.h`
3. Add source file to CMakeLists.txt
4. Write a test like 
```cpp
namespace{
    TEST(suite_name, test_name) {
        // Test code here
    }
}
```


## Running tests
Execute main() in main.cpp

## Setup
Do not forget to init or update git submodules to have gtest provided
