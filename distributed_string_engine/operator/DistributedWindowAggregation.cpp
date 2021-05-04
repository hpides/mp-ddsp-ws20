#include "DistributedWindowAggregation.h"

using namespace Operator;

void _DistributedWindowAggregation::setInputSchema(std::shared_ptr<_Schema> inputSchema) {

    bool keyFound = false;
    for (size_t i = 0; i < _aggregationSchema->getAggregationVariables().size(); i++) {
        const auto& [dataType, aggregationType] = _aggregationSchema->getAggregationVariables()[i];
        if (aggregationType == _AggregationSchema::key) {
            keyFound = true;
            _keyType = dataType;
            _keyIndex = i;
            break;
        }
    }
    if (!keyFound) {
        throw std::runtime_error("No key provided for aggregation!");
    }

    _inputSchema = inputSchema;
    auto tupleType = TupleType(_inputSchema->getVariables());
    _preSendAggregateMap = MapType(_keyType, tupleType);
    _rankSpecificWindowedAggregateMap = MapType(_keyType, tupleType);
    _reShuffledSlideContainers = QueueType(MapType(_keyType, tupleType));
    _tuplesPerRank = ContainerType(ContainerType(tupleType));
    _sendBuffer = POD(tupleType->datatype() + "*", "sendBuffer");
    _outputSchema = Schema(_rankSpecificWindowedAggregateMap);
}

std::shared_ptr<_Schema> _DistributedWindowAggregation::getOutputSchema() {
    return _outputSchema;
};

std::string _DistributedWindowAggregation::getCode() {
    std::stringstream stream;
    auto preSendAggregateMapName = _preSendAggregateMap->id();
    std::string tupleDatatype = TupleType(_inputSchema->getVariables())->datatype();

    // Build new tuple from input variables
    stream << "auto newTuple = std::make_tuple(";
    auto& variables = _inputSchema->getVariables();
    for (int i = 0; i < variables.size() - 1; i++)
        stream << variables[i]->id() << ",";
    stream << variables.back()->id() << ");";

    // Evaluate if window slide closes
    stream << "std::chrono::time_point<std::chrono::high_resolution_clock> currentTime = std::chrono::high_resolution_clock::now();";

    stream << "while (currentTime"
           << " > " << _nextWindowClosesAt->id()
           << ") {"
           // Update the timestamp when next window slide closes
           << _nextWindowClosesAt->id() << " += std::chrono::milliseconds(" << _slideSizeMilliseconds << ");"
           << _windowConsumedSemaphore->id() << ".wait();"
           << "for (auto& [key, tuple] : " << preSendAggregateMapName << ")"
           << _tuplesPerRank->id() + "[key % mpiConfig.MPISize].push_back(tuple);" << _sendBuffer->id() << "= (" << _sendBuffer->datatype()
           << ")malloc(" << preSendAggregateMapName << ".size() * sizeof(" << tupleDatatype << "));";
    stream << preSendAggregateMapName << ".clear();" << _windowClosedSemaphore->id() << ".notify();"
           << "}" << std::endl;

    // Pre-aggregate each new tuple in the default case (window slide not closed)
    stream << "     if (auto it = " << preSendAggregateMapName << ".find(" << getValueByIndex(_keyIndex, "newTuple")
           << "); it == " << preSendAggregateMapName << ".end()) {" << std::endl;
    stream << "         " << preSendAggregateMapName << "[" << getValueByIndex(_keyIndex, "newTuple") << "] = newTuple;" << std::endl;
    stream << "     } else {" << std::endl;
    stream << "         auto& result = it->second;" << std::endl;
    for (uint i = 0; i < _aggregationSchema->getAggregationVariables().size(); i++) {
        auto const& [_, aggregationType] = _aggregationSchema->getAggregationVariables()[i];
        stream << getElementAggregation(i, aggregationType, "newTuple");
    }
    stream << "}" << std::endl;

    return stream.str();
}

std::string _DistributedWindowAggregation::getNewThreadCode() {
    std::stringstream stream;
    std::string tupleDatatype = TupleType(_inputSchema->getVariables())->datatype();
    stream << "while (true) {" << std::endl
           << _windowClosedSemaphore->id() << ".wait();"
           << "std::vector<int> numberOfSendTuplesPerRank, sendTupleStartIndices;"
           << "int startIndex = 0;"
           << "for (auto& tuples : " << _tuplesPerRank->id() << ") {"
           << "std::copy(tuples.begin(), tuples.end()," << _sendBuffer->id() << " + startIndex);"
           << "numberOfSendTuplesPerRank.push_back(tuples.size() * sizeof(" << tupleDatatype << "));"
           << "sendTupleStartIndices.push_back(startIndex * sizeof(" << tupleDatatype << "));"
           << "startIndex += tuples.size();"
           << "tuples.clear();"
           << "}";

    stream << "std::vector<int> numberOfRecvTuplesPerRank(mpiConfig.MPISize);"
           << "MPI_Alltoall(numberOfSendTuplesPerRank.data(), 1, MPI_INT32_T, numberOfRecvTuplesPerRank.data(), 1, MPI_INT32_T, "
           << _mpiComm->id() << ");";

    stream << "int totalTuplesToReceive = std::accumulate(numberOfRecvTuplesPerRank.begin(), numberOfRecvTuplesPerRank.end(), 0);"
           << "auto recvBuffer = (" << _sendBuffer->datatype() << ")malloc(totalTuplesToReceive * sizeof(" << tupleDatatype << "));"
           << "std::vector<int> recvTupleStartIndices;"
           << "startIndex = 0;"
           << "for (auto& numberTuples : numberOfRecvTuplesPerRank) {"
           << "     recvTupleStartIndices.push_back(startIndex);"
           << "     startIndex += numberTuples;"
           << "}" << std::endl
           << "MPI_Alltoallv(" + _sendBuffer->id() + ", numberOfSendTuplesPerRank.data(), sendTupleStartIndices.data(),"
           << "MPI_BYTE, recvBuffer, numberOfRecvTuplesPerRank.data(), recvTupleStartIndices.data(),"
           << "MPI_BYTE," << _mpiComm->id() << ");"
           << "free(" << _sendBuffer->id() << ");" << _windowConsumedSemaphore->id() << ".notify();";

    // Go through the received tuples and aggregate them into current window slide
    stream << "for (int i = 0; i < totalTuplesToReceive / sizeof(" << tupleDatatype << "); i++) {"
           << "auto& currentSlideMap = " << _reShuffledSlideContainers->id() << ".back();"
           << "auto* tuplePtr = &recvBuffer[i]; " << tupleDatatype << " tuple = *tuplePtr;"
           << "int key = " << getValueByIndex(_keyIndex, "tuple") << ";"
           << "if (auto it = currentSlideMap.find(key); it == currentSlideMap.end())"
           << "     currentSlideMap[key] = tuple;"
           << "else {"
           << "     auto& result = it->second;";
    for (uint i = 0; i < _aggregationSchema->getAggregationVariables().size(); i++) {
        auto const& [_, aggregationType] = _aggregationSchema->getAggregationVariables()[i];
        stream << getElementAggregation(i, aggregationType);
    }
    stream << "}}";

    // Create aggregated output map for full window
    stream << _rankSpecificWindowedAggregateMap->declaration() << "for (auto &slideContainer : " << _reShuffledSlideContainers->id()
           << ") {"
           << "for (auto& [key, tuple] : slideContainer) {"
           << "if (auto it = " << _rankSpecificWindowedAggregateMap->id() << ".find(key); it == " << _rankSpecificWindowedAggregateMap->id()
           << ".end()) " << _rankSpecificWindowedAggregateMap->id() << "[key] = tuple;"
           << "else {"
           << "auto& result = it->second;";
    for (uint i = 0; i < _aggregationSchema->getAggregationVariables().size(); i++) {
        auto const& [_, aggregationType] = _aggregationSchema->getAggregationVariables()[i];
        stream << getElementAggregation(i, aggregationType);
    }
    stream << "}}}" << _reShuffledSlideContainers->id() << ".emplace_back();";

    // Remove old slide if not belonging to next window anymore
    stream << "while (" << _reShuffledSlideContainers->id() << ".size() > " << _slidesPerWindow << ") {" << _reShuffledSlideContainers->id()
           << ".pop_front(); " << std::endl
           << "}"
           << "{yield}";

    stream << "}";
    return stream.str();
}

std::string _DistributedWindowAggregation::getValueByIndex(uint index, const std::string& tupleName) {
    return "std::get<" + std::to_string(index) + ">(" + tupleName + ")";
}

std::string
_DistributedWindowAggregation::getElementAggregation(uint index, _AggregationSchema::AggregationType type, const std::string& tupleName) {
    std::stringstream stream;
    switch (type) {
    case _AggregationSchema::max:
        stream << getValueByIndex(index, "result") << " = std::max(" << getValueByIndex(index, tupleName) << ", "
               << getValueByIndex(index, "result") << ");\n";
        break;
    case _AggregationSchema::min:
        stream << getValueByIndex(index, "result") << " = std::min(" << getValueByIndex(index, tupleName) << ", "
               << getValueByIndex(index, "result") << ");\n";
    case _AggregationSchema::sum:
        stream << getValueByIndex(index, "result") << "+= " << getValueByIndex(index, tupleName) << ";\n";
        break;
    case _AggregationSchema::key:
    case _AggregationSchema::noop:
        break;
    }

    return stream.str();
}

std::string _DistributedWindowAggregation::getGlobalDeclarations() {
    std::stringstream stream;
    stream << _reShuffledSlideContainers->declaration();
    stream << _preSendAggregateMap->declaration();
    stream << "MPI_Barrier(MPI_COMM_WORLD);";
    stream << _tuplesPerRank->declaration("mpiConfig.MPISize");
    stream << _sendBuffer->declaration();
    stream << _nextWindowClosesAt->declaration();
    stream << _nextWindowClosesAt->id() << " = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(1000);";
    stream << _nextWindowClosesAt->id() << "-= " << _nextWindowClosesAt->id() << ".time_since_epoch() % std::chrono::milliseconds("
           << _slideSizeMilliseconds << ");";
    stream << _windowClosedSemaphore->declaration();
    stream << _windowConsumedSemaphore->declaration("1");
    stream << _mpiComm->declaration();
    stream << "MPI_Comm_dup(MPI_COMM_WORLD, &" << _mpiComm->id() << ");";

    return stream.str();
}

std::set<std::string> _DistributedWindowAggregation::getHeaders() {
    return {"<unordered_map>", "<numeric>", "<mpi.h>", "\"../../generated_code_libraries/Semaphore.h\""};
}
