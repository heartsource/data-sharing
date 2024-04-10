package com.virtualpairprogrammers.sql;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class DataFramesWithSql {
	// SparkSQL
	public static void main(String[] args) throws Exception {
		logFile();
	}

	public static void logFile() {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();

		StructField[] fields = {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
		};
		StructType schema = new StructType(fields);
		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

		dataSet.createOrReplaceTempView("logging_table");

		// NOTE: the format code [date_format(datetime,'M')] is based on SimpleDateFormat
		Dataset<Row> resultSet = spark.sql("select level, date_format(datetime,'MMMM') as month, " +
				"cast(date_format(datetime,'M') as int) as monthnum, count(1) as my_total " +
				"from logging_table " +
				"group by 1, 2 , 3 " +
				"order by 3");

		resultSet = resultSet.drop("monthnum");
		resultSet.show(100);

//		resultSet.createOrReplaceTempView("results_table");
//		Dataset<Row> totals = spark.sql("select sum(my_total) from  results_table");
//		totals.show();
	}

	public static void dateFormatting() {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();

		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		StructField[] fields = {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
		};
		StructType schema = new StructType(fields);
		Dataset<Row> dataSet = spark.createDataFrame(inMemory, schema);
//		dataSet.show();
		dataSet.createOrReplaceTempView("logging_table");

		// NOTE: the format code [date_format(datetime,'M')] is based on SimpleDateFormat
//		Dataset<Row> resultSet = spark.sql("select level, date_format(datetime,'MMMM') as month, count(1) " +
//				"from logging_table " +
//				"group by level, date_format(datetime,'MMMM')");
//		resultSet.show();


		// NOTE: the format code [date_format(datetime,'M')] is based on SimpleDateFormat
		Dataset<Row> resultSet = spark.sql("select level, date_format(datetime,'MMMM') as my_month from logging_table");
		resultSet.show();

		resultSet.createOrReplaceTempView("logging_table");
		resultSet = spark.sql("select level, my_month, count(1) from logging_table group by level, my_month");
		resultSet.show();

	}
	public static void grouping() {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();

		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		StructField[] fields = {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
		};
		StructType schema = new StructType(fields);
		Dataset<Row> dataSet = spark.createDataFrame(inMemory, schema);
//		dataSet.show();
		dataSet.createOrReplaceTempView("logging_table");

		Dataset<Row> resultSet = spark.sql("select level, collect_list(datetime) from logging_table group by level");
		resultSet.show();

	}

	public static void inMemoryExampleSetupUnitTests() {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();

		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		StructField[] fields = {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
		};
		StructType schema = new StructType(fields);
		Dataset<Row> dataSet = spark.createDataFrame(inMemory, schema);
		dataSet.show();


	}
	public static void sqlStatement() {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		// students --> student_id,exam_center_id,subject,year,quarter,score,grade
		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
//		dataSet.show();

		dataSet.createOrReplaceTempView("student_view");

		Dataset<Row> resultSet = spark.sql("select subject, grade, max(score) from student_view where subject='Modern Art' and year >= 2007 group by subject, grade");
		resultSet.show();
		resultSet = spark.sql("select distinct year from student_view where subject='Modern Art' order by year desc");
		resultSet.show();

	}
	public static void lambdaFunction() {  // getting column using functions.col(<col_name>) method
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		// students --> student_id,exam_center_id,subject,year,quarter,score,grade
		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataSet.show();

		long num = dataSet.count();
		System.out.println(num);

		// col("subject") = functions.col("subject") by import static org.apache.spark.sql.functions.*;
//		Column subjectColumn = col("subject");
//		Column yearColumn = col("year");

		Dataset<Row> modArt = dataSet.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
		modArt.show();

	}

	public static void lambdaColumn() {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		// students --> student_id,exam_center_id,subject,year,quarter,score,grade
		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataSet.show();

		long num = dataSet.count();
		System.out.println(num);

		//Modern Art
//		num = dataSet.filter(myRow -> myRow.getAs("subject").equals("Geography")).count();
//		System.out.println(num);

		Column subjectColumn = dataSet.col("subject");
		Dataset<Row> modArt = dataSet.filter(subjectColumn.equalTo("Modern Art"));
		modArt.show();

		Column yearColumn = dataSet.col("year");
		Dataset<Row> modArt2007 = dataSet.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));
		modArt2007.show();
	}

	public static void lambdaFilters() {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		// students --> student_id,exam_center_id,subject,year,quarter,score,grade
		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataSet.show();

		long num = dataSet.count();
		System.out.println(num);

		//Modern Art
//		num = dataSet.filter("subject='Modern Art'").count();
//		System.out.println(num);

		num = dataSet.filter(myRow -> myRow.getAs("subject").equals("Geography")).count();
		System.out.println(num);
		num = dataSet.filter(myRow ->
				myRow.getAs("subject").equals("Modern Art") &&
						Integer.parseInt(myRow.getAs("year"))>2007).count();
		System.out.println(num);
	}
	public static void stringFilters() {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		// students --> student_id,exam_center_id,subject,year,quarter,score,grade
		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataSet.show();

		long num = dataSet.count();
		System.out.println(num);

//		Row row = dataSet.first();
//		System.out.println(row.getAs("subject").toString());
//
//		int year = Integer.parseInt( row.getAs("year"));
//		System.out.println(year);

		//Modern Art
		num = dataSet.filter("subject='Modern Art'").count();
		System.out.println(num);
		num = dataSet.filter("subject='Modern Art' and grade='E'").count();
		System.out.println(num);
		num = dataSet.filter("subject='Modern Art' or grade='E'").count();
		System.out.println(num);
	}
}
