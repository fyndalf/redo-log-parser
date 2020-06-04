package cli

import cats.implicits._
import com.monovore.decline._

object LogExtractor extends CommandApp(
  name = "redo-log-extractor",
  header = "Extract Event logs from redo logs",
  main = {
    val userOpt =
      Opts.option[String]("target", help = "Person to greet.").withDefault("world")

    val quietOpt = Opts.flag("quiet", help = "Whether to be quiet.").orFalse

    (userOpt, quietOpt).mapN { (user, quiet) =>
      if (quiet) println("...")
      else println(s"Hello $user!")
    }
  }
)
