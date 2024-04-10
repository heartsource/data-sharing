package com.virtualpairprogrammers;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Main1 {
	// SparkSQL
	public static void main(String[] args) throws Exception {
		explainingHashAggregationVsSortAggregation();
	}

	//*****Note defined the number of partitions for the shuffle when in SQL
	public static void testing() {
		SparkSession spark = getSparkSession();
		//Note defined the number of partitions for the shuffle when in SQL
		spark.conf().set("spark.sql.shuffle.partitions", "12");

		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		dataSet.show();

		long t = System.currentTimeMillis();
		dataSet.createOrReplaceTempView("logging_table");
		spark.udf().register("monNum", (String  dateString) -> {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-M-d k:m:s");
			Date dt = df.parse(dateString);
			return dt.getMonth();
		} , DataTypes.IntegerType);
		spark.udf().register("monName", (String  dateString) -> {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-M-d k:m:s");
			Date dt = df.parse(dateString);
			return (new SimpleDateFormat("MMMM")).format(dt);
		} , DataTypes.StringType);


		Dataset<Row> sqlResults =  spark.sql(
				"select level, date_format(datetime,'MMMM') as month,  count(1) as total " +
						"from logging_table " +
						"group by level, date_format(datetime,'MMMM') " +
						"order by cast(first(date_format(datetime,'M')) as int)"
		).drop("monthnum");

		sqlResults.show();
		sqlResults.explain();
		System.out.println("-->" + (System.currentTimeMillis() - t));

	}

	public static void explainingHashAggregationVsSortAggregation() {
		SparkSession spark = getSparkSession();
		//Note defined the number of partitions for the shuffle when in SQL
		spark.conf().set("spark.sql.shuffle.partitions", "10");

		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		dataSet.show();

//		long t = System.currentTimeMillis();
//		dataSet.createOrReplaceTempView("logging_table");
//		spark.udf().register("monNum", (String  dateString) -> {
//			SimpleDateFormat df = new SimpleDateFormat("yyyy-M-d k:m:s");
//			Date dt = df.parse(dateString);
//			return dt.getMonth();
//		} , DataTypes.IntegerType);
//		spark.udf().register("monName", (String  dateString) -> {
//			SimpleDateFormat df = new SimpleDateFormat("yyyy-M-d k:m:s");
//			Date dt = df.parse(dateString);
//			return (new SimpleDateFormat("MMMM")).format(dt);
//		} , DataTypes.StringType);
//
//
//		Dataset<Row> sqlResults =  spark.sql(
//				"select level, date_format(datetime,'MMMM') as month,  count(1) as total " +
//						"from logging_table " +
//						"group by level, date_format(datetime,'MMMM') " +
//						"order by cast(first(date_format(datetime,'M')) as int)"
//		).drop("monthnum");
//
//		sqlResults.show();
//		sqlResults.explain();
//		System.out.println("-->" + (System.currentTimeMillis() - t));
//
//		dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
//		dataSet = dataSet.select(col("level")
//				, date_format(col("datetime"),"MMMM").alias("month")
//				, date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType)
//		);
////		dataSet.show();
//
//		dataSet = dataSet.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum");
//		dataSet = dataSet.drop("monthnum");
////		dataSet.show();
//		dataSet.explain();
	}

	public static void dsPerformanceSqlVsDF() {
		SparkSession spark = getSparkSession();

		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		dataSet.show();

		long t = System.currentTimeMillis();
		dataSet = dataSet.select(col("level")
				, date_format(col("datetime"),"MMMM").alias("month")
				, date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType)
		);
//		dataSet.show();

		dataSet = dataSet.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum");
		dataSet = dataSet.drop("monthnum");
		dataSet.show();


		Scanner scn = new Scanner(System.in); System.out.println("-->" + (System.currentTimeMillis() - t));
		scn.nextLine();

		dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		t = System.currentTimeMillis();
		dataSet.createOrReplaceTempView("logging_table");
		spark.udf().register("monNum", (String  dateString) -> {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-M-d k:m:s");
			Date dt = df.parse(dateString);
			return dt.getMonth();
		} , DataTypes.IntegerType);
		spark.udf().register("monName", (String  dateString) -> {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-M-d k:m:s");
			Date dt = df.parse(dateString);
			return (new SimpleDateFormat("MMMM")).format(dt);
		} , DataTypes.StringType);


		Dataset<Row> sqlResults =  spark.sql(
				"select level, monName(datetime) as my_mon, monNum(datetime) as monthnum, count(1) as total_cnt " +
						"from logging_table " +
						"group by level, 2, 3 " +
						"order by  monthnum"
		).drop("monthnum");

		sqlResults.show();
		System.out.println("-->" + (System.currentTimeMillis() - t));

	}

	private static SparkSession getSparkSession() {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		return spark;
	}

}

