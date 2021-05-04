#include <cmath>

#include "OperatorPipe.h"
#include "operators/ConsoleOutputOperator.h"
#include "operators/FilterOperator.h"
#include "operators/HashJoinOperator.h"
#include "operators/MapOperator.h"
#include "operators/ParsingOperator.h"
#include "operators/TCPReceiveOperator.h"
#include "operators/WindowOperator.h"

inline uint64_t getCurrentUnixTimestampInMilliseconds()
{
    const auto t = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

struct AdTuple
{
    AdTuple() = default;

    AdTuple(int adId, int userId, double cost, uint64_t eventTime)
        : adId(adId)
        , userId(userId)
        , cost(cost)
        , eventTime(eventTime)
        , processingTime(getCurrentUnixTimestampInMilliseconds())
        , numTuple(1){};

    int adId;
    int userId;
    double cost;
    uint64_t eventTime;
    uint64_t processingTime;
    uint64_t numTuple;
};

struct CheckoutTuple
{
    CheckoutTuple() = default;
    CheckoutTuple(int purchaseId, int userId, int adId, double value, uint64_t eventTime)
        : purchaseId(purchaseId)
        , userId(userId)
        , adId(adId)
        , value(value)
        , eventTime(eventTime)
        , processingTime(getCurrentUnixTimestampInMilliseconds()){};

    int purchaseId;
    int userId;
    int adId;
    double value;
    uint64_t eventTime;
    uint64_t processingTime;
};

int main()
{
    auto adStream = TCPReceiveOperator(12345) | MapOperator<std::string, AdTuple>([](auto string) {
                        using namespace Parsing;
                        const char* cstr_end = string.c_str() + string.size();
                        char* c_str = const_cast<char*>(string.c_str());
                        int adId = parse<int>(c_str);
                        int userId = parse<int>(c_str);
                        double cost = parse<double>(c_str);
                        auto eventTime = parse<uint64_t>(c_str, cstr_end);
                        return AdTuple(adId, userId, cost, eventTime);
                    })
        | WindowOperator<std::vector<AdTuple>>((uint64_t)5000, (uint64_t)1000)
        | MapOperator<std::vector<AdTuple>, std::vector<AdTuple>>([](auto inputVector) {
                        std::unordered_map<int, AdTuple> aggregateMap;
                        for (auto& adTuple : inputVector) {
                            if (auto it = aggregateMap.find(adTuple.adId); it == aggregateMap.end()) {
                                aggregateMap[adTuple.adId] = adTuple;
                            } else {
                                auto& result = it->second;
                                result.cost += adTuple.cost;
                                result.eventTime = std::max(result.eventTime, adTuple.eventTime);
                                result.processingTime = std::max(result.processingTime, adTuple.processingTime);
                                result.numTuple += adTuple.numTuple;
                            }
                        }
                        std::vector<AdTuple> resultVector;
                        resultVector.reserve(aggregateMap.size());
                        for (auto& [key, value] : aggregateMap) {
                            resultVector.push_back(value);
                        }
                        return resultVector;
                    });

    auto checkoutStream = TCPReceiveOperator(12346) | MapOperator<std::string, CheckoutTuple>([](auto string) {
                              using namespace Parsing;
                              const char* cstr_end = string.c_str() + string.size();
                              char* c_str = const_cast<char*>(string.c_str());
                              int purchaseId = parse<int>(c_str);
                              int userId = parse<int>(c_str);
                              int adId = parse<int>(c_str);
                              double cost = parse<double>(c_str);
                              auto eventTime = parse<uint64_t>(c_str, cstr_end);
                              return CheckoutTuple(purchaseId, userId, adId, cost, eventTime);
                          })
        | FilterOperator<CheckoutTuple>([](auto input) -> bool { return input.adId != 0; })
        | WindowOperator<std::vector<CheckoutTuple>>((uint64_t)5000, (uint64_t)1000)
        | MapOperator<std::vector<CheckoutTuple>, std::vector<CheckoutTuple>>([](auto inputVector) {
                              std::unordered_map<int, CheckoutTuple> aggregateMap;
                              for (auto& checkoutTuple : inputVector) {
                                  if (auto it = aggregateMap.find(checkoutTuple.adId); it == aggregateMap.end()) {
                                      aggregateMap[checkoutTuple.adId] = checkoutTuple;
                                  } else {
                                      auto& result = it->second;
                                      result.value += checkoutTuple.value;
                                      result.eventTime = std::max(result.eventTime, checkoutTuple.eventTime);
                                      result.processingTime = std::max(result.processingTime, checkoutTuple.processingTime);
                                  }
                              }
                              std::vector<CheckoutTuple> resultVector;
                              resultVector.reserve(aggregateMap.size());
                              for (auto& [key, value] : aggregateMap) {
                                  resultVector.push_back(value);
                              }
                              return resultVector;
                          });

    HashJoinOperator<AdTuple, CheckoutTuple, int>(
        adStream, checkoutStream, [](auto input) -> int { return input.adId; }, [](auto input) -> int { return input.adId; })
        | MapOperator<
            std::vector<std::pair<AdTuple, CheckoutTuple>>,
            std::vector<std::tuple<int, double, uint64_t, uint64_t, uint64_t, uint64_t>>>([](auto inputVector) {
              std::vector<std::tuple<int, double, uint64_t, uint64_t, uint64_t, uint64_t>> result;
              result.reserve(inputVector.size());
              for (auto& [ad, checkout] : inputVector) {
                  result.emplace_back(
                      ad.adId,
                      checkout.value - ad.cost,
                      std::max(checkout.eventTime, ad.eventTime),
                      std::max(checkout.processingTime, ad.processingTime),
                      getCurrentUnixTimestampInMilliseconds(),
                      ad.numTuple);
              }
              return result;
          })
        | ConsoleOutputOperator<std::vector<std::tuple<int, double, uint64_t, uint64_t, uint64_t, uint64_t>>>();
    return 0;
}
