package com.virtualpairprogrammers.sql;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class UserDefinedFunctions {
	// SparkSQL
	public static void main(String[] args) throws Exception {
		userDefinedFunctionWithSQL();
	}

	private static Set<String> passingGrades = new HashSet<>();
	static {
		passingGrades.add("A+");
		passingGrades.add("A");
		passingGrades.add("B");
		passingGrades.add("C");
	}
	public static void userDefinedFunctionWithSQL() {
		SparkSession spark = getSparkSession();

		Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
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
						"order by  monthnum, level"
		).drop("monthnum");

		sqlResults.show(100);


//		spark.udf().register("isPassingMore", (String grd) ->  passingGrades.contains(grd) , DataTypes.BooleanType);

//		spark.udf().register("biologyGrade", (String grd, String subject) ->  {
//			if (subject.equals("Biology")) {
//				if (grd.startsWith("A")) return true;
//				else return false;
//			}
//			return passingGrades.contains(grd);
//		}, DataTypes.BooleanType);

	}

	public static void userDefinedFunction() {
		SparkSession spark = getSparkSession();

		Dataset<Row> dataSet = spark.read().option("header", true)
				.csv("src/main/resources/exams/students.csv");

		spark.udf().register("isPassingMore", (String grd) ->  passingGrades.contains(grd) , DataTypes.BooleanType);
		spark.udf().register("biologyGrade", (String grd, String subject) ->  {
			if (subject.equals("Biology")) {
				if (grd.startsWith("A")) return true;
				else return false;
			}
			return passingGrades.contains(grd);
		}, DataTypes.BooleanType);
		dataSet = dataSet.withColumn("passing", callUDF("isPassingMore", col("grade"))); // using a field in the data
		dataSet = dataSet.withColumn("passingBio", callUDF("biologyGrade", col("grade"), col("subject"))); // using a field in the data

//		spark.udf().register("isPassing", (String grd) ->  grd.equals("A+") , DataTypes.BooleanType);
//		dataSet = dataSet.withColumn("passing", callUDF("isPassing", col("grade"))); // using a field in the data

//		dataSet = dataSet.withColumn("passing", lit("Yes")); // lit = literal
//		dataSet = dataSet.withColumn("passing", lit(col("grade").equalTo("A+"))); // using a field in the data

//		dataSet = dataSet.filter("subject in ('Biology','Math')");
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
