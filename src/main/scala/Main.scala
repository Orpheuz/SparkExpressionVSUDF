import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.StringType

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

object Main {

  private def compress(data: Array[Byte]): Array[Byte] = {

    val bos = new ByteArrayOutputStream()
    val gzip = new GZIPOutputStream(bos)
    gzip.write(data)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  // The performance of the expression is ~2 times greater than using a UDF (basic non rigorous test)
  def main(args: Array[String]): Unit = {

    val compressedString = compress("This is a compressed String".getBytes)
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    GZIPDecompressorExample.registerFunction(sparkSession)

    import sparkSession.implicits._

    val df = Seq(compressedString).toDF("value")

    val expressionDF = df.withColumn("value", GZIPDecompressorExample.decompressExpr(col("value")).cast(StringType))

    val sqlExpressionDF = df.select(expr(s"cast(${GZIPDecompressorExample.decompressExpr(col("value")).expr.sql} as STRING)"))

    val udfDF = df.withColumn("value", GZIPDecompressorExample.decompressUDF(col("value")).cast(StringType))
  }
}