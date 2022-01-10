import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{BinaryType, DataType}

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream

object GZIPDecompressorExample {

  /**
   * Decompress function that takes a GZipped byte array as input and outputs an unzipped byte array
   * @param compressed
   * @return
   */
  def decompress(compressed: Array[Byte]): Array[Byte] = {

    val inputStream = new GZIPInputStream(new ByteArrayInputStream(compressed))
    val decompressed = IOUtils.toByteArray(inputStream)
    IOUtils.closeQuietly(inputStream)
    decompressed
  }

  /**
   * UDF definition
   * @return UDF
   */
  def decompressUDF: UserDefinedFunction = {

    udf {
      (bytes: Array[Byte]) => decompress(bytes)
    }
  }

  /**
   * Expression as a Column
   * @param col
   * @return
   */
  def decompressExpr(col: Column): Column = {

    new Column(DecompressExpression(col.expr))
  }

  /**
   * Definition of the expression for Catalyst
   * @param child
   */
  case class DecompressExpression(child: Expression)
    extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

    override def dataType: DataType = BinaryType

    override def inputTypes: Seq[DataType] = Seq(BinaryType)

    protected override def nullSafeEval(bytes: Any): Any = {

      GZIPDecompressorExample.decompress(bytes.asInstanceOf[Array[Byte]])
    }

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

      nullSafeCodeGen(
        ctx,
        ev,
        child => {
          val c = GZIPDecompressorExample.getClass.getCanonicalName
          val dt = CodeGenerator.boxedType(dataType)

          s"""
             |$c obj = $c.MODULE$$;
             |$dt data = ($dt) $child;
             |${ev.value} = obj.findDecompressor(data).decompress(data);
             |""".stripMargin
        }
      )
    }
  }

  /**
   * Util method that is used to register the expression in the SparkSession functions
   * @param exprCls
   * @param name
   * @tparam T
   * @return
   */
  private def getExpressionInfo[T](exprCls: Class[T], name: String): ExpressionInfo = {

    val ed = exprCls.getAnnotation(classOf[ExpressionDescription])

    if(ed != null) {
      new ExpressionInfo(
        exprCls.getCanonicalName,
        "",
        name,
        ed.usage(),
        ed.arguments(),
        ed.examples(),
        ed.note(),
        ed.group(),
        ed.since(),
        ed.deprecated())
    } else {
      new ExpressionInfo(
        exprCls.getSimpleName,
        "",
        name)
    }
  }

  /**
   * Registers the function in SparkSession
   * @param ss
   */
  def registerFunction(ss: SparkSession): Unit = {
    val (name, info, builder) = (new FunctionIdentifier("decompressexpression"),
    getExpressionInfo(classOf[GZIPDecompressorExample.DecompressExpression], "decompressexpression"),
    (children: Seq[Expression]) => GZIPDecompressorExample.DecompressExpression(children.head))

    ss.sessionState.functionRegistry.registerFunction(name, info, builder)
  }
}
