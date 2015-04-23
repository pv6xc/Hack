# Hack

Efficient map reduce program to calculate count,mean,min,max,sd,percentile of integer data

A cascade of two map reduce jobs are used to calculate the required statistics from the data.

First Map reduce Job
----------------------
map() -> emit (count ,min, max, sum, sumSqare)   - uses combiner to optimize internally
reduce() -> emit (global (count, min, max, sum, mean, sd)) 

Second Map reduce Job
---------------------------
map() -> emit (integer,1) 
shuffle and sort: internal to map reduce, will sort all the data
reduce() -> emit (25th,50th,75th percentile)

--------------------------
Sample Input file format : input.txt
--------------------------
15
20
35
50
45
60

--------------------------
Sample output 
--------------------------
output 1 from MR1:

count   6.0
sum     225.0
min     15.0
max     60.0
mean    37.0
sd      17.535679

output 2 from MR2:

25th percentile 17.5
50th percentile 40.0
75th percentile 55.0


