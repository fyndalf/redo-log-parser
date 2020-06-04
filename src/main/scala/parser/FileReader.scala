package parser

import java.nio.file.Path

import scala.io.Source

object FileReader {

  /**
   * Read a logfile from disk given by a path and properly close the file handle.
   *
   * @param path
   * @return
   */
  def readLogFile(path: Path): Seq[String] = {
    val bufferedSource = Source.fromFile(path.toFile)
    val fileContents = bufferedSource.getLines().toList
    bufferedSource.close
    fileContents
  }
}
