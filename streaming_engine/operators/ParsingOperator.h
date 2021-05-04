#pragma once

#include <cmath>
#include <cstdint>

#include "Definitions.h"
#include "TransformingOperator.h"

namespace Parsing
{
    template<typename T>
    inline T parse(char*& pos)
    {
        T ret = 0;
        while (*pos != ',')
            ret = ret * 10 + (*pos++ - '0');
        pos++;
        return ret;
    }

    template<>
    inline double parse<double>(char*& pos)
    {
        double ret = 0;
        while (*pos >= '0' && *pos <= '9')
            ret = ret * 10 + (*pos++ - '0');
        pos++;
        uint8_t numDecimals = 0;
        double tmp = 0.0;
        while (*pos != ',') {
            tmp = tmp * 10 + (*pos++ - '0');
            numDecimals++;
        }
        pos++;

        ret += tmp / std::pow(10.0, numDecimals);
        return ret;
    }

    template<typename T>
    inline T parse(char*& pos, const char* endPos)
    {
        T ret = 0;
        while (pos != endPos)
            ret = ret * 10 + (*pos++ - '0');
        pos++;
        return ret;
    }

}
