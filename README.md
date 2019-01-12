# Generic_Parallel_Sort_Join
The code builds a generic parallel sort and parallel join algorithm.

# GENERIC PARALLEL SORT AND PARALLEL JOIN

The required task is to build a generic parallel sort and parallel join algorithm.

1.	A Python function ParallelSort() that takes as input:  
(1) InputTable stored in a PostgreSQL database  
(2) SortingColumnName the name of the column used to order the tuples by  
(3) OutputTable  
(4) openconnection  

ParallelSort() then sorts all tuples (using five parallelized threads) and stores the sorted tuples for in a table named OutputTable (the output table name is passed to the function). The OutputTable contains all the tuple present in InputTable sorted in ascending order.

Function Interface:-

ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection)  
InputTable – Name of the table on which sorting needs to be done.  
SortingColumnName – Name of the column on which sorting needs to be done, would be either of type integer or real or float. Basically Numeric format. Will be Sorted in Ascending order.  
OutputTable – Name of the table where the output needs to be stored.  
openconnection – connection to the database.  

2.	A Python function ParallelJoin() that takes as input:   
(1) InputTable1 and InputTable2 table stored in a PostgreSQL database  
(2) Table1JoinColumn and Table2JoinColumn that represent the join key in each input table respectively.  
(3) OutputTable  
(4) openconnection  

ParallelJoin() then joins both InputTable1 and InputTable2 (using five parallelized threads) and stored the resulting joined tuples in a table named OutputTable (the output table name is passed to the function). The schema of OutputTable should be
InputTable1.Column1, InputTable.Column2, …, IputTable2.Column1, InputTable2.Column2….

Function Interface:-

ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,OutputTable, openconnection)  
InputTable1 – Name of the first table on which you need to perform join.  
InputTable2 – Name of the second table on which you need to perform join.  
Table1JoinColumn – Name of the column from first table i.e. join key for first table.  
Table2JoinColumn – Name of the column from second table i.e. join key for second table.  
OutputTable - Name of the table where the output needs to be stored.  
openconnection – connection to the database.  

Let’s use an example to understand these fields and their usage.


Example: -
You will have to create two tables in database manually. Let us name them MovieRating and MovieBoxOfficeCollection. Suppose, you want to sort MovieRating by column Rating and MovieBoxOfficeCollection by column Collection. You also want to join MovieRating and MovieBoxOfficeCollection by column MovieID. Then, you would define the variables mentioned above as:


FIRST_TABLE_NAME = ‘MovieRating’

SECOND_TABLE_NAME = ‘MovieBoxOfficeCollection’

SORT_COLUMN_NAME_FIRST_TABLE = ‘Rating’

SORT_COLUMN_NAME_SECOND_TABLE = ‘Collection’

JOIN_COLUMN_NAME_FIRST_TABLE = ‘MovieID’

JOIN_COLUMN_NAME_SECOND_TABLE = ‘MovieID’

