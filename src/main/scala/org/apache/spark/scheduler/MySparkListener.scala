package org.apache.spark.scheduler
import org.apache.spark.internal.Logging
import scala.collection.mutable
import org.apache.spark.executor.TaskMetrics


class MySparkListener extends SparkListener with Logging{
  
  
  private val jobList = new mutable.HashMap[Int,SparkJob]()
  
  private val stageToJobID = new mutable.HashMap[Int,Int]()
  
  
   /**
   * Called when a task starts
   */
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {

  }

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
   override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
      
   }

  /**
   * Called when a task ends
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit ={
    val stageId = taskEnd.stageId
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    val jobId = stageToJobID.apply(stageId)
    val sparkJob = jobList.apply(jobId)
    val sparkStage = sparkJob.getStageWithId(stageId)
    val sparkTask = new SparkTask(jobId,stageId,taskInfo)
    sparkTask.setTaskMetrics(taskMetrics)
    sparkStage.addTask(taskInfo.taskId, sparkTask)
  }
  
  
    /**
   * Called when a stage is submitted
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    
  } 
  
  
  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val jobId = stageToJobID.apply(stageInfo.stageId)
    val sparkJob = jobList.apply(jobId)
    sparkJob.getStageWithId(stageInfo.stageId).updateStageInfo(stageInfo)
  }



 

  /**
   * Called when a job starts
   */
   override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
     val jobId = jobStart.jobId
     val sparkJob = new SparkJob(jobId)
     sparkJob.setStartTime(jobStart.time)
     jobList.+=(jobId -> sparkJob)
     jobStart.stageInfos.foreach { x => {
       sparkJob.addStage(x.stageId, new SparkStage(jobId,x)) 
       stageToJobID.+=(x.stageId -> jobId)
       } 
     }
   }

  /**
   * Called when a job ends
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val sparkJob = jobList.apply(jobEnd.jobId)
    sparkJob.setEndTime(jobEnd.time)
    sparkJob.updateResult(jobEnd.jobResult)
  }

  
  
   /**
   * Called when the application starts
   */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit ={

  }
  
  
  
  /**
   * Called when the application ends
   */
  override  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit ={
      jobList.foreach{case (id,job) => {
        logInfo(s"Job-${job.jobId},numsofstage:${job.numsStage},during:${job.getRunningTime}\n")
        job.stages.foreach{case (id,stage) => {
            logInfo("Stage-"+stage.getDetailOfStage)
            val taskMetrics: TaskMetrics = stage.stageInfo.taskMetrics
            if(taskMetrics != null){
              logInfo("Stage-"+s"executorRunTime:${taskMetrics.executorRunTime}\n")
              logInfo("Stage-"+s"jvmGCTime:${taskMetrics.jvmGCTime}\n")
              logInfo("Stage-"+s"inputMetrics:${taskMetrics.inputMetrics}\n")
              logInfo("Stage-"+s"outputMetrics:${taskMetrics.outputMetrics}\n")
              logInfo("Stage-"+s"shuffleReadMetrics:${taskMetrics.shuffleReadMetrics}\n")
              logInfo("Stage-"+s"shuffleWriteMetrics:${taskMetrics.shuffleWriteMetrics}\n")
            }else{
              logInfo("Stage-"+"taskMetrics is null\n")
            }
            val taskSet = stage.taskSet
            taskSet.foreach{ case (taskId,sparkTask) => {
              logInfo("Task-"+s"jobId(${sparkTask.jobId}),stageId(${sparkTask.stageId}),taskId(${sparkTask.taskId})")
              logInfo("Task-"+s"task status${sparkTask.taskInfo.status}")
              val taskMetrics: TaskMetrics = sparkTask.taskMetrics
              if(taskMetrics != null){
              logInfo("Task-"+s"executorRunTime:${taskMetrics.executorRunTime}\n")
              logInfo("Task-"+s"jvmGCTime:${taskMetrics.jvmGCTime}\n")
              logInfo("Task-"+s"inputMetrics:${taskMetrics.inputMetrics}\n")
              logInfo("Task-"+s"outputMetrics:${taskMetrics.outputMetrics}\n")
              logInfo("Task-"+s"shuffleReadMetrics:${taskMetrics.shuffleReadMetrics}\n")
              logInfo("Task-"+s"shuffleWriteMetrics:${taskMetrics.shuffleWriteMetrics}\n")
            }else{
              logInfo("Task-"+"taskMetrics is null\n")
            }
            }}
            
            
            
            
            
          }
        }
        }
      }
    
    
  }
  
  
  
  
  
  /**
   * Called when environment properties have been updated
   */
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {

  }

  /**
   * Called when a new block manager has joined
   */
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {

  }

  /**
   * Called when an existing block manager has been removed
   */
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit ={

  }

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit ={

  }

 
  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  override  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit ={

  }

  /**
   * Called when the driver registers a new executor.
   */
  override  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit ={

  }

  /**
   * Called when the driver removes an executor.
   */
  override  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit ={
    
  }

  /**
   * Called when the driver receives a block update info.
   */
  override  def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit ={

  }

  /**
   * Called when other events like SQL-specific events are posted.
   */
  override  def onOtherEvent(event: SparkListenerEvent): Unit ={

  }
}