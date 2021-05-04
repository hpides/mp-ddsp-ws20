# Streaming Engine

This is our first approach for designing a streaming engine. It is build iterator style, i.e. the operators are chained and call each other. There is no query compilation involved.

We used this as a lower bound baseline for benchmarking. Right now, there is no reason to use it anymore.
