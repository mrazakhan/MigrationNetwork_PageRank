import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import math.abs
// We need three different files for this analysis
// Raw Calls
// Monthly Modal Tower File
// Migration file : to check the status of Move to Kigali, Move From Kigali etc

class DSV (var line:String="", var delimiter:String=",",var parts:Array[String]=Array("")) extends Serializable {
	parts=line.split(delimiter,-1)
	def hasValidVal(index: Int):Boolean={
		return (parts(index)!=null)&&(parts.length>index)
	}
	def contains(text:String):Boolean={
		for(i <- 1 to (parts.length-1))
			if(parts(i).contains(text))
				return false
		true
	}
	override def toString():String={
		var rep:String=""
		for(i <- 0 to (parts.length-1)){
			rep=rep+parts(i)
			if (i!=(parts.length -1))
				rep=rep+","
		}
		rep=rep+"\n"
		rep
	}
}



object PageRankCalculator extends Serializable{

                val conf = new SparkConf().setMaster("yarn-client")
		//setMaster("spark://messi.ischool.uw.edu:7077")
                .setAppName("DailyModalTower")
                .set("spark.shuffle.consolidateFiles", "true")
		.set("spark.storage.blockManagerHeartBeatMs", "300000")
                //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.set("spark.kryo.registrator", "MyRegistrator")
                .set("spark.akka.frameSize","512")
                .set("spark.default.parallelism","200")
                //.set("spark.executor.memory", "40g")
                .set("spark.kryoserializer.buffer.max.mb","10024")
                .set("spark.kryoserializer.buffer.mb","1024")

                val sc = new SparkContext(conf)
	
def filterKigaliContacts(final_rawCalls_ABparty_w_Loc:RDD[(String, (Int,String, String, String, String, String))]):RDD[(String, (Int,String, String, String, String, String))]={
	var filteredRDD=final_rawCalls_ABparty_w_Loc.filter{case(k,v)=>(v._6=="Kigali")}
	filteredRDD
}

def filterNonKigaliContacts(final_rawCalls_ABparty_w_Loc:RDD[(String, (Int,String, String, String, String, String))]):RDD[(String, (Int,String, String, String, String, String))]={
	var filteredRDD=final_rawCalls_ABparty_w_Loc.filter{case(k,v)=>(v._6!="Kigali")}
	filteredRDD
}

def filterHomeDistContacts(final_rawCalls_ABparty_w_Loc:RDD[(String, (Int,String, String, String, String, String))]):RDD[(String, (Int,String, String, String, String, String))]={
	var filteredRDD=final_rawCalls_ABparty_w_Loc.filter{case(k,v)=>(v._2==v._5)}
	filteredRDD
}


def filterHomeProvContacts(final_rawCalls_ABparty_w_Loc:RDD[(String, (Int,String, String, String, String, String))]):RDD[(String, (Int,String, String, String, String, String))]={
	var filteredRDD=final_rawCalls_ABparty_w_Loc.filter{case(k,v)=>(v._3==v._6)}
	filteredRDD
}

//val r=scala.util.Random
//def calculatePageRank(final_rawCalls_ABparty_w_Loc:RDD[(String, (Int,String, String, String, String, String))], month:String, filename:String):{RDD[(String, (Double))],RDD[(String, (Double))],Graph[String,Int]}={
def calculatePageRank(final_rawCalls_ABparty_w_Loc:RDD[(String, (Int,String, String, String, String, String))], month:String, filename:String):(RDD[(String, (Double))],RDD[(String, (Double))],Graph[String,Int])={
	
	val triplets = final_rawCalls_ABparty_w_Loc.flatMap {
		
	   case(k,v)=>{
	    val rand = new scala.util.Random()
		  val t = new EdgeTriplet[String, Int]
		  t.srcId = k.hashCode
		  t.srcAttr = k
		  t.attr = v._1//lineArray(4)
		  t.dstId = v._4.hashCode
		  
		  t.dstAttr = v._4
		  Some(t)
		  }
		}
	val vertices = triplets.flatMap(t => Array((t.srcId, t.srcAttr), (t.dstId, t.dstAttr)))
	val edges = triplets.map(t => t: Edge[Int])
	var g=Graph(vertices, edges)
	val ranks = g.pageRank(0.0001).vertices
	var ego_pagerank=vertices.join(ranks).distinct().map{case(k,v)=>(v._1,v._2)}
	//ego_pagerank.saveAsTextFile("Rwanda_Out/PageRanks/EgoPageRank-"+month+""+filename)
	val averageRank: VertexRDD[Double] = g.mapReduceTriplets[(Int, Double)](
	  // map function returns a tuple of (1, Age)
	  edge => Iterator((edge.dstId, (1, edge.attr.toDouble))),
	  // reduce function combines (SumOfNeighbors, sumOfRank)
	  (a, b) => ((a._1 + b._1), (a._2 + b._2))
	  ).mapValues((id, p) => p._2 / p._1)

	var neighbors_avg_pagerank=vertices.join(averageRank).distinct().map{case(k,v)=>(v._1,v._2)}  
	(ego_pagerank,neighbors_avg_pagerank,g)
}

	def main(args:Array[String]){


var month=args(0)
//This is the raw calls file
var rawCallsFilePath="Rwanda_In/CallsFiles/"+month+"-Call.pai.sordate.txt"

var monthlyModalFilePath="Rwanda_In/CallsVolDegree/"+month+"-ModalCallVolDegree.csv"

var subsLocation=sc.textFile(monthlyModalFilePath).map(line=>(new DSV(line,"\\,"))).map(d=>(d.parts(0),(d.parts(2),d.parts(3)))).distinct()

subsLocation.take(10).foreach(println)

//var rawCalls_Aparty=sc.textFile(rawCallsFilePath).map(line=>(new DSV(line,"\\|"))).map(d=>((d.parts(0),d.parts(1)),(d.parts(2),d.parts(3)))).map{case(k,v)=>(k,1)}.reduceByKey(_ + _).map{case(k,v)=>(k._1,(k._2,v))}.distinct()

var rawCalls_Aparty=sc.textFile(rawCallsFilePath).map(line=>(new DSV(line,"\\|"))).map(d=>((d.parts(0),d.parts(1)),(d.parts(2),d.parts(3)))).map{case(k,v)=>(k,1)}.reduceByKey(_ + _).map{case(k,v)=>(k._1,(k._2,v))}.distinct()
rawCalls_Aparty.take(10).foreach(println)

var rawCalls_Bparty=sc.textFile(rawCallsFilePath).map(line=>(new DSV(line,"\\|"))).map(d=>((d.parts(1),d.parts(0)),(d.parts(2),d.parts(3)))).map{case(k,v)=>(k,1)}.reduceByKey(_ + _).map{case(k,v)=>(k._1,(k._2,v))}.distinct()
rawCalls_Bparty.take(10).foreach(println)

var rawCalls_union=sc.union(rawCalls_Aparty,rawCalls_Bparty)
rawCalls_union.take(10).foreach(println)

var rawCalls_Aparty_w_Loc=rawCalls_union.join(subsLocation).map{case(k,v)=>(k,(v._1._1,v._1._2,v._2._1,v._2._2))}.distinct()
rawCalls_Aparty_w_Loc.take(10).foreach(println)
/*Aparty, Bparty, callscount,District, Province
(L89908401,(L17325042,1,Rusizi,West))
(L59606498,(L90168133,7,Rusizi,West))
(L14089769,(L34804819,1,Rwamagana,East))
(L21916838,(L05233016,1,Bugesera,East))
*/

var rawCalls_ABparty_w_Loc=rawCalls_Aparty_w_Loc.map{case(k,v)=>(v._1,(k,v._2,v._3,v._4))}.join(subsLocation)

/*
(L26631007,((L22032047,Burera,North),(Burera,North)))
(L26631007,((L22032047,Burera,North),(Burera,North)))
(L26631007,((L22032047,Burera,North),(Burera,North)))
(L26631007,((L01678769,Rubavu,West),(Burera,North)))
(L26631007,((L01678769,Rubavu,West),(Burera,North)))
(L26631007,((L01678769,Rubavu,West),(Burera,North)))
(L26631007,((L01678769,Rubavu,West),(Burera,North)))
(L26631007,((L01678769,Rubavu,West),(Burera,North)))
(L26631007,((L01678769,Rubavu,West),(Burera,North)))
*/
rawCalls_ABparty_w_Loc.take(10).foreach(println)
var forward_rawCalls_ABparty_w_Loc= rawCalls_ABparty_w_Loc.map{case(k,v)=>(v._1._1,(v._1._2,v._1._3,v._1._4,k,v._2._1,v._2._2))}.distinct()
/*
(L99392707,(2,Nyarugenge,L93533845,Kigali,Kigali))
(L31210403,(2,Rusizi,L05284321,Rusizi,West))
(L83469369,(1,Kicukiro,L38716033,Kigali,Kigali))

*/
forward_rawCalls_ABparty_w_Loc.take(10).foreach(println)
/*
(L95013926,(2,Kigali,Kigali,L34805152,Kigali,Kigali))
(L38373629,(1,Kigali,Kigali,L31373811,Kigali,Kigali))
(L89382491,(1,Rusizi,West,L54968498,Kigali,Kigali))

*/
var final_rawCalls_ABparty_w_Loc_transpose=forward_rawCalls_ABparty_w_Loc.map{case(k,v)=>(v._4,(v._1,v._2,v._3,k,v._5,v._6))}
final_rawCalls_ABparty_w_Loc_transpose.take(10).foreach(println)

var final_rawCalls_ABparty_w_Loc=sc.union(forward_rawCalls_ABparty_w_Loc,final_rawCalls_ABparty_w_Loc_transpose)

var (egoRank, neighborRank,gr)=calculatePageRank(final_rawCalls_ABparty_w_Loc, month, "overall_pagerank.csv")

egoRank.saveAsTextFile("Rwanda_Out/PageRanks/EgoPageRank-"+month+"overall.csv")
neighborRank.saveAsTextFile("Rwanda_Out/PageRanks/AvgNeighborPageRank-"+month+"overall.csv")
var edgeListFile=final_rawCalls_ABparty_w_Loc.map{case(k,v)=>(k,v._4)}
edgeListFile.saveAsTextFile("Rwanda_Out/PageRanks/EdgeList-"+month+".csv")

var KigaliBPartyRDD=filterKigaliContacts(final_rawCalls_ABparty_w_Loc)
KigaliBPartyRDD.count()
var (egoRank_kg, neighborRank_kg,gr_kg)=calculatePageRank(KigaliBPartyRDD, month, "kigali_pagerank.csv")
egoRank_kg.saveAsTextFile("Rwanda_Out/PageRanks/EgoPageRank-kg-"+month+"overall.csv")
neighborRank_kg.saveAsTextFile("Rwanda_Out/PageRanks/AvgNeighborPageRank-kg-"+month+"overall.csv")

var edgeListFile_kg=KigaliBPartyRDD.map{case(k,v)=>(k,v._4)}

edgeListFile_kg.saveAsTextFile("Rwanda_Out/PageRanks/EdgeList_kg-"+month+".csv")


var NonKigaliBPartyRDD=filterNonKigaliContacts(final_rawCalls_ABparty_w_Loc)
NonKigaliBPartyRDD.count()
//calculatePageRank(NonKigaliBPartyRDD, month, "nonkigali_pagerank.csv")
var (egoRank_nkg, neighborRank_nkg,gr_nkg)=calculatePageRank(NonKigaliBPartyRDD, month, "nonkigali_pagerank.csv")
egoRank_nkg.saveAsTextFile("Rwanda_Out/PageRanks/EgoPageRank-nkg-"+month+"overall.csv")
neighborRank_nkg.saveAsTextFile("Rwanda_Out/PageRanks/AvgNeighborPageRank-nkg-"+month+"overall.csv")

var edgeListFile_nkg=NonKigaliBPartyRDD.map{case(k,v)=>(k,v._4)}
edgeListFile_nkg.saveAsTextFile("Rwanda_Out/PageRanks/EdgeList_nkg-"+month+".csv")

var HomeDistBPartyRDD=filterHomeDistContacts(final_rawCalls_ABparty_w_Loc)
HomeDistBPartyRDD.count()
var (egoRank_hd, neighborRank_hd,gr_hd)=calculatePageRank(HomeDistBPartyRDD, month, "homedist_pagerank.csv")
//var (egoRank_homedist, neighborRank_homedist,gr_homedist)=calculatePageRank(NonKigaliBPartyRDD, month, "nonkigali_pagerank.csv")
egoRank_hd.saveAsTextFile("Rwanda_Out/PageRanks/EgoPageRank-hd-"+month+"overall.csv")
neighborRank_hd.saveAsTextFile("Rwanda_Out/PageRanks/AvgNeighborPageRank-hd-"+month+"overall.csv")

var edgeListFile_homedist=HomeDistBPartyRDD.map{case(k,v)=>(k,v._4)}
edgeListFile_homedist.saveAsTextFile("Rwanda_Out/PageRanks/EdgeList_hd-"+month+".csv")


var HomeProvBPartyRDD=filterHomeProvContacts(final_rawCalls_ABparty_w_Loc)
HomeProvBPartyRDD.count()
var (egoRank_hp, neighborRank_hp,gr_hp)=calculatePageRank(HomeProvBPartyRDD, month, "homeprov_pagerank.csv")
//var (egoRank_homeprov, neighborRank_homeprov,gr_homeprov)=calculatePageRank(NonKigaliBPartyRDD, month, "nonkigali_pagerank.csv")
egoRank_hp.saveAsTextFile("Rwanda_Out/PageRanks/EgoPageRank-hp-"+month+"overall.csv")
neighborRank_hp.saveAsTextFile("Rwanda_Out/PageRanks/AvgNeighborPageRank-hp-"+month+"overall.csv")

var edgeListFile_homeprov=HomeProvBPartyRDD.map{case(k,v)=>(k,v._4)}
edgeListFile_homeprov.saveAsTextFile("Rwanda_Out/PageRanks/EdgeList_hp-"+month+".csv")
}
}
