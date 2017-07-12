package org.apache.spark.scheduler
import scala.collection.mutable
import org.apache.spark.internal.Logging
class SparkJob(key:Int) extends Logging {
  var stages = new mutable.HashMap[Int,SparkStage]()
  val jobId = key
  var jobResult: JobResult = null
  private var startTime:Long = 0
  private var endTime:Long = 0
  
  
  def addStage(stageId:Int,sparkStage:SparkStage) = {
    stages.+=(stageId -> sparkStage)
  }
  def updateResult(result: JobResult) = this.jobResult = result
  
  def setStartTime(time:Long) = this.startTime = time
  def setEndTime(time:Long) = this.endTime = time
  def getRunningTime:Long = endTime - startTime
  
  def numsStage = this.stages.size
  def getStageWithId(key:Int):SparkStage = this.stages.apply(key)  
}

object SparkJob {
  	
}