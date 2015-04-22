# Hack

Efficient map reduce program to calculate count,mean,min,max,sd,percentile of integer data

A single map and reduce job to find the above statistics

Map task : During map phase, we calculate the total number of integer entries in the input data. We use counters to keep track of the total numbers across multiple mappers.
We also calculate the value of sum of all the integers and sum of integer squares. These values are used to calculate standard deviation and mean. Minimum and maximum values are also calculated using counters. 			
In the clean up stage of the mapper each of the above calculated values is written to context, such that reducer can read the data. Here we could use a data-cache to store these values.
We also emit each integer as key to the reduce function.

Reduce task: Since each key to the reduce function is the actual integer, the keys coming to the reduce function will be sorted during shuffle and sort phase. This makes it easy for use to calculate the percentiles.
With the help of total number of integers we calculate the percentiles and emit all the required details to the context, which is written to a file. In this scenario I have used  a single reduce task.

Multiple mappers are used and a single reducer to complete the task.

-------------------------
How to run the program
--------------------------
1. create a jar file from the project.
2. run the HackDriver.class. hadoop jar hack.jar HackDriver <input> <output>

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
count   6.0
sum     225.0
min     15.0
max     60.0
mean    37.0
sd      17.535679
25th percentile 17.5
50th percentile 40.0
75th percentile 55.0


