package com.zaloni.mgohain.sparkHbaseIntegration.services

import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}

object Employee {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("In correct number of arguments " + args.length)
      System.out.println("Please provide correct arguments.")
      System.exit(1)
    }
    val hbaseConf = HBaseConfiguration.create()
    val tableName = "employee"
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hbaseConf.set("hbase.zookeeper.quorum","quickstart.cloudera")
    hbaseConf.set("hbase.zookeeper.property.client.port","2181")
    val admin = new HBaseAdmin(hbaseConf)
    val cfProfessionalData = Bytes.toBytes("professional_data")
    val cfPersonalData = Bytes.toBytes("personal_data")
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(cfProfessionalData))
      tableDesc.addFamily(new HColumnDescriptor(cfPersonalData))
    }
    val hTable = new HTable(hbaseConf,tableName)
    //val records = sc.textFile(args(0))
    val put = new Put(Bytes.toBytes("e_1"))
    val eId = Bytes.toBytes("Emp_id")
    val name = Bytes.toBytes("Name")
    val dsgtn = Bytes.toBytes("Designation")
    val doj = Bytes.toBytes("DOJ")
    val addr = Bytes.toBytes("Address")
    val phn = Bytes.toBytes("Phone")
    val dob = Bytes.toBytes("DOB")
    put.add(cfProfessionalData, eId, Bytes.toBytes(1))
    put.add(cfProfessionalData, name, Bytes.toBytes("Mridul Gohain"))
    put.add(cfProfessionalData, dsgtn, Bytes.toBytes("SE"))
    put.add(cfProfessionalData, doj, Bytes.toBytes("15-07-2015"))
    put.add(cfPersonalData, addr, Bytes.toBytes("Chabua"))
    put.add(cfPersonalData, phn, Bytes.toBytes("9859559606"))
    put.add(cfPersonalData, dob, Bytes.toBytes("04-10-1991"))
    hTable.put(put)
    hTable.close()
  }
}