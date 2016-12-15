package org.apache.spark.deploy.worker

import org.apache.spark.internal.Logging
import scala.sys.process._

import java.io.File

class Cgroup(name: String, cpuShares: Option[Int] = None)
  extends Logging {
  val rootDir = "/sys/fs/cgroup/"
  val parent = "/spark-worker/"
  val cpuDir = new File(rootDir+"cpu,cpuacct"+parent+name)

  if (!cpuDir.exists) {
    if (!cpuDir.mkdirs()) {
      throw new Exception(s"Failed to create cgroup for application $name")
    }

    cpuShares match {
      case Some(i) =>
        logInfo(s"Detected $name cpuShares value as $i. Adjusting our cgroup.")
        (s"echo $i" #> new File(cpuDir.getPath+"/cpu.shares")).!
      case _ =>
        logInfo(s"App $name cpuShares value not set. Using default of parent 'spark-worker'")
      }
  }

  def addTask(process: java.lang.Process) = {
    getProcessPid(process) match {
      case Some(i) =>
        logInfo(s"Detected executor pid for application $name as $i. Moving it to the correct cgroup.")
        (s"echo $i" #> new File(cpuDir.getPath+"/tasks")).!
      case _ =>
        logWarning(s"Failed to get executor pid for application $name. Executor is running in the root cgroup.")
    }
  }

  def destroy() = {
    logInfo(s"Attempting to delete cgroup for application $name")
    try {
      if (cpuDir.exists) {
        cpuDir.delete()
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to delete cgroup application $name.", e)
    }
  }

  def getProcessPid(process: java.lang.Process): Option[Int] = {
    var pid: Option[Int] = None
    try {
      val pClass = process.getClass()
      logInfo("Detected executor process class as "+pClass.getName())
      if (pClass.getName().equals("java.lang.UNIXProcess")) {
        val fPid = pClass.getDeclaredField("pid")
        fPid.setAccessible(true)
        pid = Some(fPid.getInt(process))
      }
    } catch {
      case e: Exception =>
      logError("Failed to get the pid with exception:", e)
    }
    pid
  }
}
