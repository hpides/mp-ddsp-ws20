#include "OperatorPipe.h"
#include "Schema.h"
#include "operator/Aggregation.h"
#include "operator/Append.h"
#include "operator/Combine.h"
#include "operator/ConsoleOutput.h"
#include "operator/Filter.h"
#include "operator/HashJoin.h"
#include "operator/Parse.h"
#include "operator/ProcessingTimeSlidingWindow.h"
#include "operator/TCPReceiver.h"

int main() {
    // clang-format off
    using namespace Operator;

    struct AdTuple { std::shared_ptr<_Datatype> adId, userId, cost, eventTime, processingTime, numTuple; };

    AdTuple adTuple{
        POD("int", "adId"),
        POD("int", "userId"),
        POD("double", "cost"),
        POD("uint64_t", "eventTime"),
        POD("uint64_t", "processingTime"),
        POD("uint64_t", "numTuple")};

    auto aggregatedAdTuple = AggregationSchema(std::vector{
        std::pair{adTuple.adId, _AggregationSchema::key},
        {adTuple.userId, _AggregationSchema::noop},
        {adTuple.cost, _AggregationSchema::sum},
        {adTuple.eventTime, _AggregationSchema::max},
        {adTuple.processingTime, _AggregationSchema::max},
        {adTuple.numTuple, _AggregationSchema::sum}});

    struct CheckoutTuple { std::shared_ptr<_Datatype> purchaseId, userId, adId, value, eventTime, processingTime; };

    CheckoutTuple checkoutTuple{
        POD("int", "purchaseId"),
        POD("int", "userId"),
        POD("int", "adId"),
        POD("double", "value"),
        POD("uint64_t", "eventTime"),
        POD("uint64_t", "processingTime")};

    auto aggregatedCheckoutTuple = AggregationSchema(std::vector{
        std::pair{checkoutTuple.purchaseId, _AggregationSchema::noop},
        {checkoutTuple.userId, _AggregationSchema::noop},
        {checkoutTuple.adId, _AggregationSchema::key},
        {checkoutTuple.value, _AggregationSchema::sum},
        {checkoutTuple.eventTime, _AggregationSchema::max},
        {checkoutTuple.processingTime, _AggregationSchema::max}});

    auto adPipeline = TCPReceiver(12345)
        | Parse(Schema(std::vector{adTuple.adId, adTuple.userId, adTuple.cost, adTuple.eventTime}))
        | AppendTimestamp(adTuple.processingTime)
        | Append(adTuple.numTuple, "1")
        | ProcessingTimeSlidingWindow(1000, 5)
        | Aggregation(aggregatedAdTuple);

    auto checkoutPipeline = TCPReceiver(12346) | Parse(Schema(std::vector{
        checkoutTuple.purchaseId,
        checkoutTuple.userId,
        checkoutTuple.adId,
        checkoutTuple.value,
        checkoutTuple.eventTime}))
        | Filter(checkoutTuple.adId, " == 0")
        | AppendTimestamp(checkoutTuple.processingTime)
        | ProcessingTimeSlidingWindow(1000, 5)
        | Aggregation(aggregatedCheckoutTuple);

    auto valueCostDiff = POD("double", "valueCostDiff");
    auto maxEventTime = POD("uint64_t", "maxEventTime");
    auto maxProcessingTime = POD("uint64_t", "maxProcessingTime");
    auto finishedTime = POD("uint64_t", "finishedTime");

    HashJoin(adPipeline, checkoutPipeline)
        | Combine(valueCostDiff, checkoutTuple.value, adTuple.cost, AggregationStrategy::Diff())
        | Combine(maxEventTime, adTuple.eventTime, checkoutTuple.eventTime, AggregationStrategy::Max())
        | Combine(maxProcessingTime, adTuple.processingTime, checkoutTuple.processingTime, AggregationStrategy::Max())
        | AppendTimestamp(finishedTime)
        | ConsoleOutput("{},{},{},{},{},{}", adTuple.adId,
                        valueCostDiff,
                        maxEventTime,
                        maxProcessingTime,
                        finishedTime,
                        adTuple.numTuple);

    // clang-format on
    return 0;
}
