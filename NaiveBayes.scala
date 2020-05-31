import scala.math
import org.apache.spark.rdd.RDD

class NaiveBayes extends Serializable {

  // generwate the zero element of the aggregate function based on the classes
  def generateZero(classes: Array[Int]): (Map[Int, Map[Int, Int]], Map[Int, Int])  =
    (classes.map(c => (c->Map(0->0))).toMap, classes.map(c => c->0).toMap)

  // merge 2 map, WARNING linear on map2 argument, || map 2 || < || map1 ||
  def mergeSimpleMap(map1: Map[Int, Int], map2: Map[Int, Int]): Map[Int, Int] = {
    map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0)) }
  }

  def seqOp(zero: (Map[Int, Map[Int, Int]], Map[Int, Int]), document: (Int, Map[Int, Int])) = {
    val labeldoc = document._1
    val featuresdoc = document._2

    val featureszero = zero._1(labeldoc)
    val exampleszero = zero._2(labeldoc)
    val merge = mergeSimpleMap(featureszero, featuresdoc)
    (zero._1 ++ Map(labeldoc -> merge), zero._2 ++ Map(labeldoc ->(exampleszero+1)))
  }

  def combOp(partion1: (Map[Int, Map[Int, Int]], Map[Int, Int]), partion2: (Map[Int, Map[Int, Int]], Map[Int, Int])) = {
    val partion11 = partion1._1
    val partion21 = partion2._1

    val part_1 = partion11.map{case (label, features) =>  label->mergeSimpleMap(features, partion21(label))}

    val partion12 = partion1._2
    val partion22 = partion2._2

    val part_2 = mergeSimpleMap(partion12, partion22)

    (part_1, part_2)
  }

  def fit(data: RDD[(Int, List[Int])], classes: Array[Int]) = {

    //count abs frequency of terms in each document
    val group = data.map(d => (d._1, d._2.groupBy(identity _).map({case (k,v) => k->v.size})))

    //generate zero
    val zero = generateZero(classes)

    val model = group.aggregate(zero)(seqOp, combOp)
    val totdoc = model._2.map(_._2).sum.toDouble
    //compute probability of a class
    val pclasses = model._2.map{case (label, examples) => label -> (examples.toDouble/totdoc)}
    //compute number of words in a given class
    val wordsclasses = model._1.map{case (label, token) => label -> (token.map(_._2).sum)}

    //return model
    (pclasses, model._1, wordsclasses)

  }

  def predict(naivebayes: (Map[Int, Double], Map[Int, Map[Int, Int]], Map[Int,Int]), document: List[Int], alpha:Double, dictsize: Int) = {
    val pclasses = naivebayes._1
    val maptokenprob = naivebayes._2
    val wordsclasses = naivebayes._3

    //GET FREQUENCY of a token given the label and the token
    def getProb(label: Int, token: Int) = {
      maptokenprob(label) getOrElse(token, 0)
    }

    pclasses.map{case (label, prob) => {
      label -> (prob :: document.map(token => (getProb(label, token) + alpha).toDouble / (wordsclasses(label)+alpha*dictsize))).map(x => math.log(x)).sum
    }}
  }

}


