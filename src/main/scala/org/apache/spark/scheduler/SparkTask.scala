package org.apache.spark.scheduler
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging

class SparkTask(jid:Int,tid:Int,info:TaskInfo) extends Logging{
  val stageId = tid
  val taskId = info.taskId
  val jobId = jid
  
  
  val taskInfo = info
  var taskMetrics:TaskMetrics = null

  
  def setTaskMetrics(data:TaskMetrics) = 
    if(this.taskMetrics == null)
      this.taskMetrics = data
    else
      logInfo("init failed\n")
  
}


object SparkTask {
	def getTaskSetStatusDetails(info:TaskInfo,stageId:Int):String = {
	  val taskId = info.id
	  val taskStatus = info.status
	  val executorId = info.executorId+":"+info.host
	  val duringTime = info.duration
	  
	  s"TaskInfo($taskId,$stageId);Executor($executorId);"+
	   s"taskStatus($taskStatus);Took($duringTime)"
	}
}