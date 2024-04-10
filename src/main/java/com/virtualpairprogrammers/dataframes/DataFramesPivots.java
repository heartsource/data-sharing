package com.virtualpairprogrammers.dataframes;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class DataFramesPivots {
	// SparkSQL
	public static void main(String[] args) throws Exception {
		pivotTables();
	}

	public static void pivotTables() {
		SparkSession spark = getSparkSession();

		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

		dataSet = dataSet.select(col("level"), date_format(col("datetime"), "MMMM").alias("mon_name")
				, date_format(col("datetime"), "MM").cast(DataTypes.IntegerType).alias("mon_num") );

		ArrayList<Object> columns = new ArrayList<Object>();
		columns.add("January");
		columns.add("February");
		columns.add("March");
		columns.add("April");
		columns.add("Blank"); // Added only to show how to remove the null cell and replace with 0
		dataSet = dataSet.groupBy("level" /* row */).pivot("mon_name" /* column */, columns).count().na().fill(0);


		dataSet.show(100);

	}

	public static void groupByWithDataFramesAPI() {
		SparkSession spark = getSparkSession();

		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

		dataSet = dataSet.select(col("level"), date_format(col("datetime"), "MMMM").alias("mon_name")
				, date_format(col("datetime"), "MM").cast(DataTypes.IntegerType).alias("mon_num") );
		RelationalGroupedDataset grp = dataSet.groupBy(col("level"),col("mon_name"), col("mon_num"));
		dataSet = grp.count();
		dataSet = dataSet.orderBy(col("mon_num"));
		dataSet = dataSet.drop(col("mon_num"));

		dataSet.show(100);

	}
	public static void usingSelectWithDataFramesToIncludeColumns() {
		SparkSession spark = getSparkSession();

		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

//		dataSet = dataSet.select(functions.col("level"), functions.date_format(functions.col("datetime"), "MMMM"));
		dataSet = dataSet.select(col("level"), date_format(col("datetime"), "MMMM").alias("mon_name"));
		dataSet.show();

	}
	public static void example1() {
		SparkSession spark = getSparkSession();

		StructField[] fields = {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
		};
		StructType schema = new StructType(fields);
		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

		dataSet = dataSet.selectExpr("level", "datetime", "date_format(datetime,'MMMM') as month_name");
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
