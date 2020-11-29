package parser.file

import java.nio.file.Path

import scala.io.Source

/**
  * An object that provides a method for reading a redo log file rom disk
  */
object FileReader {

  /**
    * Read a logfile from disk given by a path and properly close the file handle.
    *
    * @param path The file to the path
    */
  def readLogFile(path: Path): Seq[String] = {
    val bufferedSource = Source.fromFile(path.toFile)
    val fileContents = bufferedSource.getLines().toList
    bufferedSource.close
    fileContents
  }
}
