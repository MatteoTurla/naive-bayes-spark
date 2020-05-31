import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object ReadFile {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("SparkProject").getOrCreate()
    //val df = spark.read.json("s3://myawsbucketuniboai/data/Pet_Supplies_5.json")
    val df = spark.read.json("data/Pet_Supplies_5.json")
    //df.show()

    //select target and data column
    val df2 = df.select("overall", "reviewText")
    //df2.show()

    //transfrom dataframe in a rdd
    val dataset = df2.na.drop().rdd.map{
      case Row(target: Double, data: String) => (target.toInt, data)
    }

    // classes to classify
    val classes = Array(1,2,3,4,5)
    
    // from here we expect a rdd[(Int, String)]
    val (dict, dictsize, pdata) = new bagOfWords().fit_transform(dataset, 3, 100)

    // divide  train and test
    val Array(test, train) = pdata.cache().randomSplit(weights=Array(0.3, 0.7), seed=1)

    //fit model
    val naive = new NaiveBayes()
    val model = naive.fit(train, classes)

    //compute accuracy
    val label_predictions = test.map(x => (x._1, naive.predict(model, x._2, 0.5, dictsize).maxBy(_._2)._1))
    val labelscount = label_predictions.map{case (y, y_) => y == y_}.aggregate((0,0))((zero: (Int, Int), l: Boolean) => {
          if (l) (zero._1, zero._2 + 1)
          else ((zero._1 + 1, zero._2))
        }, 
        (p1: (Int, Int), p2: (Int, Int)) => {
          (p1._1 + p2._1, p1._2 + p2._2)
        })

    val accuracy = labelscount._2.toDouble/(labelscount._1+labelscount._2).toDouble
    //println(labelscount)
    val example = dataset.first()
    val exampletransform = new bagOfWords().transform(example, 3, dict)
    val y_ = naive.predict(model, exampletransform._2, 0.5, dictsize).maxBy(_._2)._1

    println("dict size:")
    println(dictsize)
    println("-"*30)
    println("Accuracy over test set: ")
    println(accuracy)

    println("-"*30)
    println("Example of document: ")
    println(example)

    println("-"*30)
    println("Example of tr: ")
    println(exampletransform)

    println("-"*30)
    println("prediction: ")
    println(y_)


    spark.stop()
  }

}