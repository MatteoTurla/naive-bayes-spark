import org.apache.spark.rdd.RDD
import scala.util.hashing.MurmurHash3

class bagOfWords extends Serializable {
  // compute dictionary and return it
  def fit(df: RDD[(Int, String)], minLenght: Int, minOcc: Int) = {
    //remove unwanted charachters
    val dataset = df.map{
      case (target, document) => 
      (target, document.replace(".", "")
       .replace(",", "").replace("\n", " ")
       .replace("(", "").replace(")", ""))
    }
    //split doc in word and remove that with size < than min length
    val dataset2 = dataset.map{case (target,doc) => 
      (target, doc.split(" ").filter(word => word.size > minLenght).map(word => word.toLowerCase))}
    
    // count occurence of word in each document
    val dfrdd5 = dataset2.map{case (target, list) => list.groupBy(identity _).map{case (k,v) => k -> v.size}}
    
    def merge2(map1: Map[String, Int], map2: Map[String, Int]): Map[String, Int] = {
      map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0)) }
    }
    
    // count occurence of word in all the dataset and filter based on minimum number of occurences
    val dfrdd6 = dfrdd5.aggregate(Map[String, Int]())(merge2, merge2).filter(x => x._2 > minOcc)
    //make a dictionary
    val dict = dfrdd6.map{case (k,v) => k->MurmurHash3.stringHash(k)}
    val dictsize = dfrdd6.size
    (dict, dictsize)
    
  }
  // fit and trasform data based on dictionary
  def fit_transform(df: RDD[(Int, String)], minLenght: Int, minOcc: Int) = {
   //remove unwanted charachters
    val dataset = df.map{
      case (target, document) => 
      (target, document.replace(".", "")
       .replace(",", "").replace("\n", " ")
       .replace("(", "").replace(")", ""))
    }
    //split doc in word and remove that with size < than min length
    val dataset2 = dataset.map{case (target,doc) => 
      (target, doc.split(" ").filter(word => word.size > minLenght).map(word => word.toLowerCase))}
    
    // count occurence of word in each document
    val dfrdd5 = dataset2.map{case (target, list) => list.groupBy(identity _).map{case (k,v) => k -> v.size}}
    
    def merge2(map1: Map[String, Int], map2: Map[String, Int]): Map[String, Int] = {
      map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0)) }
    }
    
    // count occurence of word in all the dataset and filter based on minimum number of occurences
    val dfrdd6 = dfrdd5.aggregate(Map[String, Int]())(merge2, merge2).filter(x => x._2 > minOcc)
    //make a dictionary
    val dict = dfrdd6.map{case (k,v) => k->MurmurHash3.stringHash(k)}
    val dictsize = dfrdd6.size
    val pdata = dataset2.map{case (target, doc) => (target, doc.map(word => dict getOrElse(word, 0)).filter(token => token != 0).toList)}

    (dict, dictsize, pdata)
  }

  def transform(example: (Int, String), minLength: Int , dict: Map[String, Int]) = {
    val label = example._1
    val doc = example._2
    val newdoc = doc.replace(".", "")
       .replace(",", "").replace("\n", " ")
       .replace("(", "").replace(")", "")
       .split(" ").filter(word => word.size > minLength).map(word => word.toLowerCase)
       .map(word => dict getOrElse(word, 0))
       .filter(token => token != 0).toList
    
    (label, newdoc)

  }
}