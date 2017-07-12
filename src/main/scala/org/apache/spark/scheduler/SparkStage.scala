package org.apache.spark.scheduler
import scala.collection.mutable
import org.apache.spark.internal.Logging
class SparkStage(jId:Int,info:StageInfo){
  import SparkStage._
  var stageInfo = info
  val stageId = info.stageId
  val jobId =jId
  var taskSet = new mutable.HashMap[Long,SparkTask]()
    
  def numTasks = this.taskSet.size
  def addTask(taskId:Long,sparkTask:SparkTask) = {
    this.taskSet.+=(taskId -> sparkTask)
  }
  def updateStageInfo(info:StageInfo) = this.stageInfo = info
  def getDetailOfStage:String = getStageStatusDetail(stageInfo)  
}

object SparkStage {
  def getStageStatusDetail(info: StageInfo): String = {
    val failureReason = info.failureReason.map("(" + _ + ")").getOrElse("")
    val timeTaken = info.submissionTime.map(
      x => info.completionTime.getOrElse(System.currentTimeMillis()) - x
    ).getOrElse("-")

    s"Stage(${info.stageId}, ${info.attemptId}); Name: '${info.name}'; " +
      s"Status: ${info.getStatusString}$failureReason; numTasks: ${info.numTasks}; " +
      s"Took: $timeTaken msec"
  }
}