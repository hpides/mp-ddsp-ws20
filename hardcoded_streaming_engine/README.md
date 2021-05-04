# Hardcoded stream processing

This is a straight forward implementation of our use case in a hardcoded way. The main function spawns a thread for each stream, that puts all the received tuples into a map. The aggregation of the tuples is done ''on the fly'' while inserting. For every slide there is an individual map. If the window closes, the pre-aggregations for the relevant slides are combined.

A special remark can be made about the parsing of the raw byte input to a tuple: Instead of using build in parsing or casting it turned out to be tremendously faster doing this by ''hand'', i.e. reading the input character wise and then applying mathematical operations based on the input type.

For example, the following code snippet reads from the buffer until the next field seperator (',') is found and interprets the input as a whole number (integer, long, ...) number:
```c++
template<typename T>
inline T parse(char*& pos)
{
    T ret = 0;
    while (*pos != ',')
        ret = ret * 10 + (*pos++ - '0');
    pos++;
    return ret;
}
```