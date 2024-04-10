package com.virtualpairprogrammers.dataframes;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class PivotTableExamples {
	// SparkSQL
	public static void main(String[] args) throws Exception {
		pivotTables();
	}
	public static void pivotTables() {
		SparkSession spark = getSparkSession();

		Dataset<Row> dataSet = spark.read().option("header", true)
//				.option("inferSchema", true)  // Extra loop of data
				.csv("src/main/resources/exams/students.csv");
//		dataSet.show();

		dataSet = dataSet.select(col("subject"),col("year"),col("score").cast(DataTypes.IntegerType));
//		dataSet.show();


		dataSet = dataSet.groupBy("subject" /* row */).pivot("year" /* column */)
				.agg(
						round(avg(col("score")), 2).alias("average"),
						round(stddev(col("score")), 2).alias("deviation")
				);
		dataSet.show();


//		ArrayList<Object> columns = new ArrayList<Object>();
//		columns.add("January");
//		columns.add("February");
//		columns.add("March");
//		columns.add("April");
//		columns.add("Blank"); // Added only to show how to remove the null cell and replace with 0
//		dataSet = dataSet.groupBy("level" /* row */).pivot("mon_name" /* column */, columns).count().na().fill(0);
//
//
//		dataSet.show(100);

	}

	public static void pivotTablesOrig() {
		SparkSession spark = getSparkSession();

		Dataset<Row> dataSet = spark.read().option("header", true)
//				.option("inferSchema", true)  // Extra loop of data
				.csv("src/main/resources/exams/students.csv");
		dataSet.show();

		dataSet = dataSet.groupBy("subject", "grade").agg(
				max(col("score").cast(DataTypes.IntegerType)).alias("max score"),
				min(col("score").cast(DataTypes.IntegerType)).alias("min score")
		);

		dataSet.show();

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
