# Testing the functionality off our engines

To test an engine, run ./run_tests.sh and pass the directory of the desired engine as first parameter.
Everything else should work automatically.

What is tested?
 - correct number of output rows
 - correct output values for ad_id, revenue, event_timestamp, count
 - correct assignment to windows

See whatIsTested.txt to learn, what lines off the test input validate which feature off our engine.
See expectedAggregationResult-Step1-3 to learn about how the values are aggregated and therefore result in results.csv as the gold standard.

If the result is not correct at first try, re-run the test script. Maybe its just bad timing. This may be especially the case, if there is a deviation only in one row. If the same reoccurs, there might be a problem.

This test does not work for the distributed queries and is generally kind of buggy.
