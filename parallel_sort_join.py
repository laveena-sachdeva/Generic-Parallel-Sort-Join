import psycopg2
import os
import sys
import threading

#######Parallel Sorting Starts##################

#creates table structures for intermediate tables as identical to the input table
def createPartitionTablesSort(InputTable, OutputTable, openconnection):
	cur = openconnection.cursor()
	
	#fetch table schema of the input table
	cur.execute("select column_name, data_type from information_schema.columns where table_name = '" + InputTable + "'")
	
	table_schema = cur.fetchall()
	
	#create five intermediate tables for five threads
	for i in range(5):
		table_name = "parallel_sort_" + str(i)
		cur.execute("drop table if exists " + table_name + "")
		cur.execute("create table " + table_name + " (" + table_schema[0][0] + "  " + table_schema[0][1] + ")")
		
		for j in range(1,len(table_schema)):
			cur.execute("alter table " + table_name + " add column " + table_schema[j][0] + "  " + table_schema[j][1] + "")
	
	#create output table identical in structure to the input table
	table_name = OutputTable
	cur.execute("drop table if exists " + table_name + "")
	cur.execute("create table " + table_name + " (" + table_schema[0][0] + "  " + table_schema[0][1] + ")")
	
	for j in range(1,len(table_schema)):
		cur.execute("alter table " + table_name + " add column " + table_schema[j][0] + "  " + table_schema[j][1] + "")

	openconnection.commit()
	cur.close()

		
#called in parallel by five threads to sort five partitions of the data in parallel
def parallel_sort_range(InputTable, SortingColumnName, OutputTable, part, low, high, openconnection):
	cur = openconnection.cursor()
	
	if part==0:
		cur.execute("insert into parallel_sort_" + str(part) + " select * from " + InputTable + " where " + SortingColumnName + " >= " + str(low)  + " and " + SortingColumnName + " <= " + str(high) + " order by " + SortingColumnName + " asc ")
	else:
		cur.execute("insert into parallel_sort_" + str(part) + " select * from " + InputTable + " where " + SortingColumnName + " > " + str(low)  + " and " + SortingColumnName + " <= " + str(high) + " order by " + SortingColumnName + " asc ")


	openconnection.commit()
	cur.close()

#merge the tables sorted by five threads into one table and insert into the output table
def mergeTables(OutputTable, openconnection):

	cur = openconnection.cursor()
	
	i=0
	query_select = "select * from parallel_sort_" + str(i) + ""	
	output_query = "copy ({0}) to STDOUT delimiter as '\t'".format(query_select)
	with open("parallel_sort.txt",'w') as f:
		cur.copy_expert(output_query,f)
	
	for i in range(1,5):
		query_select = "select * from parallel_sort_" + str(i) + ""	
		output_query = "copy ({0}) to STDOUT delimiter as '\t'".format(query_select)
		with open("parallel_sort.txt",'a') as f:
			cur.copy_expert(output_query,f)

	loadout = open("parallel_sort.txt",'r')
    
	cur.copy_from(loadout,OutputTable,sep = '\t')
	openconnection.commit()
	cur.close()


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
	#Implement ParallelSort Here.
	cur = openconnection.cursor()
	
	#create intermediate tables
	createPartitionTablesSort(InputTable, OutputTable, openconnection)

	#identify the range of sorting column to divide data into five threads
	cur.execute("select min(" + str(SortingColumnName) + ") as min, max(" + str(SortingColumnName) + ") as max from " + InputTable + "");

	min_max = cur.fetchone()
	min_val = float(min_max[0])
	max_val = float(min_max[1])
	interval = (max_val - min_val)/5
	
	low = min_val
	high = min_val + interval
	
	#start five parallel threads to sort the data
	t = [0 for i in range(5)]
	for i in range(5):
		t[i] = threading.Thread(target = parallel_sort_range, args=(InputTable, SortingColumnName, OutputTable, i, low, high, openconnection))
		low = high
		high = low + interval
		t[i].start()
		
	for i in range(5):
		t[i].join()

	#merge data sorted by threads and insert into output table	
	mergeTables(OutputTable, openconnection)

	openconnection.commit()
	cur.close()

	
#######Parallel Join Starts##################	
	
#create tables indentical to input tables for intermediate processing
def createPartitionTablesJoin(InputTable1, InputTable2, OutputTable, openconnection):
	cur = openconnection.cursor()
	
	#select schema of the input tables
	cur.execute("select column_name, data_type from information_schema.columns where table_name = '" + InputTable1 + "'")
	table1_schema = cur.fetchall()
	
	cur.execute("select column_name, data_type from information_schema.columns where table_name = '" + InputTable2 + "'")
	table2_schema = cur.fetchall()
	
	#create tables for intermediate processing	
	for i in range(5):
		table_name = "parallel_join1_" + str(i)
		cur.execute("drop table if exists " + table_name + "")
		cur.execute("create table " + table_name + " (" + table1_schema[0][0] + "  " + table1_schema[0][1] + ")")
		
		for j in range(1,len(table1_schema)):
			cur.execute("alter table " + table_name + " add column " + table1_schema[j][0] + "  " + table1_schema[j][1] + "")
			
	for i in range(5):
		table_name = "parallel_join2_" + str(i)
		cur.execute("drop table if exists " + table_name + "")
		cur.execute("create table " + table_name + " (" + table2_schema[0][0] + "  " + table2_schema[0][1] + ")")
		
		for j in range(1,len(table2_schema)):
			cur.execute("alter table " + table_name + " add column " + table2_schema[j][0] + "  " + table2_schema[j][1] + "")
	
	#create output table with columns from both the input tables
	table_name = OutputTable
	cur.execute("drop table if exists " + table_name + "")
	cur.execute("create table " + table_name + " (" + table1_schema[0][0] + "  " + table1_schema[0][1] + ")")
	
	for j in range(1,len(table1_schema)):
		cur.execute("alter table " + table_name + " add column " + table1_schema[j][0] + "  " + table1_schema[j][1] + "")
	
	for j in range(0,len(table2_schema)):
		cur.execute("alter table " + table_name + " add column " + table2_schema[j][0] + "  " + table2_schema[j][1] + "")
	
	
	openconnection.commit()
	cur.close()


#called by five parallel threads to join specific range of data and insert into output table	
def parallel_join_range(InputTable1, InputTable2, OutputTable, Table1JoinColumn, Table2JoinColumn, part, low, high, openconnection):
	cur = openconnection.cursor()
	
	if part==0:
		cur.execute("insert into  parallel_join1_" + str(part) + " select * from " + InputTable1 + " where " + Table1JoinColumn + " >= " + str(low)  + " and " + Table1JoinColumn + " <= " + str(high) + " order by " + Table1JoinColumn + " asc ")
	else:
		cur.execute("insert into  parallel_join1_" + str(part) + " select * from " + InputTable1 + " where " + Table1JoinColumn + " > " + str(low)  + " and " + Table1JoinColumn + " <= " + str(high) + " order by " + Table1JoinColumn + " asc ")

	if part==0:
		cur.execute("insert into  parallel_join2_" + str(part) + " select * from " + InputTable2 + " where " + Table2JoinColumn + " >= " + str(low)  + " and " + Table2JoinColumn + " <= " + str(high) + " order by " + Table2JoinColumn + " asc ")
	else:
		cur.execute("insert into parallel_join2_" + str(part) + " select * from " + InputTable2 + " where " + Table2JoinColumn + " > " + str(low)  + " and " + Table2JoinColumn + " <= " + str(high) + " order by " + Table2JoinColumn + " asc ")

	cur.execute("insert into  "  + OutputTable + " select * from parallel_join1_" + str(part) + " inner join parallel_join2_" + str(part) + " on parallel_join1_" + str(part) + "." + Table1JoinColumn + " = parallel_join2_" + str(part) + "." + Table2JoinColumn + "")

	openconnection.commit()
	cur.close()

#Create five parallel threads to join two tables	
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    
	cur = openconnection.cursor()
	
	createPartitionTablesJoin(InputTable1, InputTable2, OutputTable, openconnection)

	#select the range of join column to partition data into five sets
	cur.execute("select min(" + str(Table1JoinColumn) + ") as min, max(" + str(Table1JoinColumn) + ") as max from " + InputTable1 + "");
	min_max_table1 = cur.fetchone()
	min_table1 = float(min_max_table1[0])
	max_table1 = float(min_max_table1[1])
	
	
	cur.execute("select min(" + str(Table2JoinColumn) + ") as min, max(" + str(Table2JoinColumn) + ") as max from " + InputTable2 + "");
	min_max_table2 = cur.fetchone()
	min_table2 = float(min_max_table2[0])
	max_table2 = float(min_max_table2[1])
	
	min_val = min(min_table1,min_table2)
	max_val = max(max_table1,max_table2)
	
	interval = (max_val - min_val)/5
	
	low = min_val
	high = min_val + interval
	
	#start five parallel threads to join the given tables in parallel
	t = [0 for i in range(5)]
	for i in range(5):
		t[i] = threading.Thread(target = parallel_join_range, args=(InputTable1, InputTable2, OutputTable, Table1JoinColumn, Table2JoinColumn , i, low, high, openconnection))
		low = high
		high = low + interval
		t[i].start()
		
	for i in range(5):
		t[i].join()
	
	openconnection.commit()
	cur.close()
	

	

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
