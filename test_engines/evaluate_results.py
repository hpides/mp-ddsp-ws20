import pandas as pd
import sys

result_file = sys.argv[1]
passed = []

#preprocess
should = pd.read_csv("results.csv")
should['event_timestamp'] = ("16103456" + should['event_timestamp'].astype(str) + "9000").astype(int)
actual = pd.read_csv(result_file, header = None)
actual.columns = ['ad_id', 'revenue', 'event_timestamp',
                  'ingest_timestamp', 'export_timestamp', 'count']

# Test 1
print("Check #1: Is the number of records identical?")
passed.append(len(should) == len(actual))
print("\tNumber of rows in gold file: " + str(len(should)))
print("\tNumber of rows in actual file: " + str(len(actual)))

# Test 2
print("Check #2: Is the content of files indentical?")
columns_to_check = ['ad_id', 'revenue', 'event_timestamp', 'count']
passed.append(True)

should_index=should.index
actual_index = actual.sort_values(['export_timestamp', 'ad_id']).index


for i, zipped in enumerate(zip(should_index, actual_index)):
    # extraxt rows
    print("\tReading row number: " + str(i))
    row_should = should.loc[zipped[0]]
    row_actual = actual.loc[zipped[1]]

    # check content
    identical = True
    for col in columns_to_check:
        if row_actual[col] != row_should[col]:
            identical = False

    # evaluate
    if identical:
        print("\t\tidentical")
    else:
        print("\t\tnot identical")
        print("\t\tdiverging row is:")
        print(row_actual)
        print("\t\tshould have been:")
        print(row_should)
        passed[1] = False
        #break

print("Check #3: Test if windows are outputed correctly")
passed.append(True)
for i in range(min(len(should), len(actual))):
    if (i==0):
        print("\tSkip first row.")
        continue

    print("\tChecking row number: " + str(i))
    delta = actual.loc[i]["export_timestamp"] - actual.loc[i-1]["export_timestamp"]
    if (should.loc[i]["window"] == should.loc[i-1]["window"]):
        print("\tValue should be in the same window as before.")
        if(delta > -100 and delta < 100):
            print("\t\tcorrect")
        else:
            print("\t\tincorrect")
            passed[2] = False
    else:
        print("\tValue should NOT be in the same window as before.")
        if (delta > 900 and delta < 1100):                        
            print("\t\tcorrect")
        else:
            print("\t\tincorrect")
            passed[2] = False

#Print results
allPassed = True
print("\n\nFINAL RESULTS")
for i, value in enumerate(passed):
    if value:
        print("\tTest number " + str(i) + " passed")
    else:
        allPassed = False
        print("\tTest number " + str(i) + " failed") 

if (allPassed):
    sys.exit(0)
else:
    sys.exit(1)