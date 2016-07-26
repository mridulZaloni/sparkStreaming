package com.zaloni.mgohain.sparkHbaseIntegration.services

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mgohain on 7/26/2016.
  * This class will read employee records from a csv file and store it to hbase
  */

case class Employee(eId : String,name : String, designation : String, doj : String, address : String, mobile : String, dob : String)
object Employee {
  def main(args : Array[String]) = {
    val sparkConf = new SparkConf().setAppName("Spark Hbase Integration").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    val records = sparkContext.textFile(args(0))
    val hBaseConfiguration = HBaseConfiguration.create()
    val table = "Employee"
    val jobConf = new JobConf(hBaseConfiguration, this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val employees = records.map(Employee.parseToEmployee)
    val puts = employees.map(employee => Employee.convertToPut(employee))
    puts.saveAsHadoopDataset(jobConf)
  }
  def parseToEmployee(record : String) : Employee = {
    val tokens = record.split(",")
    Employee(tokens(0), tokens(1),tokens(2),tokens(3),tokens(4),tokens(5),tokens(6))
  }
  def convertToPut(employee : Employee) : (ImmutableBytesWritable, Put) = {
    val cfProfessionalData = Bytes.toBytes("professional_data")
    val cfPersonalData = Bytes.toBytes("personal_data")
    val rowKey = "e_" + employee.eId
    val put = new Put(Bytes.toBytes(rowKey))
    put.add(cfProfessionalData, Bytes.toBytes("Emp_Id"),Bytes.toBytes(employee.eId))
    put.add(cfProfessionalData, Bytes.toBytes("Name"),Bytes.toBytes(employee.name))
    put.add(cfProfessionalData, Bytes.toBytes("Designation"),Bytes.toBytes(employee.designation))
    put.add(cfProfessionalData, Bytes.toBytes("DOJ"),Bytes.toBytes(employee.doj))
    put.add(cfPersonalData, Bytes.toBytes("Address"),Bytes.toBytes(employee.address))
    put.add(cfPersonalData, Bytes.toBytes("Phone"),Bytes.toBytes(employee.mobile))
    put.add(cfPersonalData, Bytes.toBytes("DOB"),Bytes.toBytes(employee.dob))
    (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
  }
}