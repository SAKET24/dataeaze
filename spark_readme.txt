
This readme contains prerequisites and steps requried for compilation,execution of spark.sh file.

Prerequisites(Software Requirements):

1. Linux OS (Ubuntu 18.04 or higher preferred)
2. Install java (1.8 or higher, preferred 1.8)
3. Install scala (2.13 or higher, preffered 2.13.3)
4. Install spark (2.6 or higher, preferred 3.0)

Steps for Exection of spark.sh:

1. Goto the location where file resides, change directory command.
   cd [Directory]
2. Use the following command on your linux machine.
   ./spark.sh 

Steps performed in spark.sh

1. spark-shell << EOF , spark-shell opens scala commandline and << passes below commands till EOF(End Of File).
2. Read both the files using spark and convert it to DataFrame.
3. Combine both DataFrames using union().

*1st Query Solution = 2746*

4. Filter the combined dataframe, where "State" = "Virginia".
5. write filtered df to csv.
6. Count the df using count().

OUTPUT :-

+---------------------+
|vacount: Long = 2746 |
|                     |
+---------------------+


*2nd Query Solution = *

7. Create a df using groupBy, by grouping OwnerTypes and count them.
8. print the df.
9. write df to csv.

OUTPUT :-

+--------------------+-----+                                                    
|          OwnerTypes|count|
+--------------------+-----+
|Government Use: F...|   79|
|Non-Profit Org., ...|    1|
|Educational: K-12...|    3|
|Local Government,...|    1|
|  Federal Government|  137|
|        Confidential|  519|
|Educational: Univ...|   48|
|         Profit Org.|  347|
|Investor: REIT,No...|    8|
|Investor: Pension...|    6|
|Investor: Insuran...|    4|
|Educational: K-12...|   23|
|                null|  547|
|          Individual|   82|
|Corporate: Privat...|  197|
|Local Government,...|    2|
|Investor: Investm...|   16|
|Government Use: L...|   48|
|Community Develop...|    3|
|    State Government|   91|
+--------------------+-----+
only showing top 20 rows


*3rd Query Solution*

10. Use previously combined DF and print it schema.
11. Type cast the "GrossSqFoot" from String to Integer.
12. Create a df, select only those records where "Iscertified" = "Yes".
13. Group by "State" and calulate its sum.
14. Write the DF to csv.

OUTPUT :-

+-----+----------------+                                                        
|State|sum(GrossSqFoot)|
+-----+----------------+	
|   VA|       388348170|
+-----+----------------+

+-----+----------------+                                                        
|State|sum(GrossSqFoot)|
+-----+----------------+	IsCertified = Yes
|   VA|       149144380|
+-----+----------------+


*4th Query Solution*

15. Create a DF, groupBy "Zipcode" and count records for every zipcode.
16. Apply max() aggregation function on counted records.
17. Select Zipcode and Count where count is greater than 519(519 is the max_value, fetched from previous query).
18. Print output.
19. Write output to csv

 +------------+-----+                                                            
|     Zipcode|count|
+------------+-----+
|Confidential|  519|
+------------+-----+

