
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuffer
import scala.math._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.routing.RoundRobinRouter
import scala.util.Random
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.concurrent.Future
import akka.pattern.ask
import akka.actor.ActorContext
import akka.actor.Cancellable
import scala.util.matching.Regex
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.immutable.TreeMap




case class startPastry(num_of_nodes:Int,num_of_requests:Int,mainnode:ActorRef)
case class routerequest(key:String,hop:Int)
case class destinationReached(hop:Int)
case class sendrequest()
case class finished(nodeId:String)
case class InitPastry(nodeId:String,leafSetSmaller:ArrayBuffer[String],leafSetLarger:ArrayBuffer[String],rt:Array[Array[String]],neighborset:ArrayBuffer[String],num_of_requests:Int,map:TreeMap[String,ActorRef],mainnode:ActorRef)
class Node extends Actor {
  var num:Int=0
  var numfin:Int=0
  var hops:Int=0
  val system = ActorSystem("Node")
  var done:Boolean=false
  var nodeId:String=""
  var leafSetSmaller:ArrayBuffer[String]=ArrayBuffer[String]()
  var leafSetLarger:ArrayBuffer[String]=ArrayBuffer[String]()
  var rt=Array.ofDim[String](8,4)
  var neighborset:ArrayBuffer[String]=ArrayBuffer[String]()
  var num_of_requests:Int=0
  var map=TreeMap.empty[String,ActorRef]
  var mainnode:ActorRef=null
  def sendreq()
  {
    var n=8
    var key:String=""
    var hop:Int=0
      while(n>0)
      {
        var rand=Random.nextInt(4)
        key+=rand.toString()
        n-=1
      }
    if(numfin<10)
     self ! routerequest(key,hop)
     numfin+=1
    
  }
  def shl(key:String,nodeId:String):Int =
  {
        var i:Int=0
        var flag=1
        while(i < 8 && flag==1)
        {
          if(key(i)!=nodeId(i))
            flag=0
          i+=1
        }
        i-=1
        i
  }
  def receive = {
    case InitPastry(nodeId,leafSetSmaller,leafSetLarger,rt,neighborset,num_of_requests,map,mainnode) => {
      this.nodeId=nodeId
      this.leafSetSmaller=leafSetSmaller
      this.leafSetLarger=leafSetLarger
      for(i <- 0 until 8)
        for(j<- 0 until 4)
          this.rt(i)(j)=rt(i)(j)
      this.neighborset=neighborset
      this.num_of_requests=num_of_requests
      this.map=map
      this.mainnode=mainnode
    }
    case sendrequest() => {
      system.scheduler.schedule(0 seconds,0.001 seconds)(sendreq())

    }
    case routerequest(key,hop) => { 
      hops=hop
      var combined:ArrayBuffer[String]=ArrayBuffer[String]()
      combined=leafSetSmaller
      combined++=leafSetLarger
      val newcombined=combined.sorted
      var condition:Boolean=false
      try
      {
        condition=(newcombined(0).toInt <=key.toInt && newcombined(newcombined.length-1).toInt>=key.toInt)
      }
      catch
      {
        case ex:Exception => {
          condition=false
        }
      }
      if(condition)
      {
        //find the numerically closest node
        hops+=1
        
        mainnode ! destinationReached(hops)
      }
       else 
      {
        var i=shl(key,nodeId)
        var dl=key(i)
        var routeNode:String=""
        try
        {
          routeNode=rt(i)(dl)
        }
        catch
        {
          case ex: Exception=> {
            routeNode="-1"
            
          }
        }
        if(!routeNode.equals("-1")) // route to node with nodeId as route Node
        {
          hops+=1
          map.apply(routeNode) ! routerequest(key,hops)
        }
        else
        {
          var T:ArrayBuffer[String]=ArrayBuffer[String]()
          var R:ArrayBuffer[String]=ArrayBuffer[String]()
          for(i <- 0 until 8)
            for(j <- 0 until 4)
              if(!rt(i)(j).equals("-1"))
                R+=rt(i)(j)
          T=leafSetSmaller
          T++=leafSetLarger
          T++=R
          T++=neighborset
          var shl_l = shl(key,nodeId)
          i=0
          var chk:Boolean=false
          while(i<T.length)
          {
            var shl_t=shl(key,T(i))
            if((shl_t >= shl_l) && (abs(T(i).toInt-key.toInt)< abs(nodeId.toInt-key.toInt)))
            {
              hops+=1 // route to that node with nodeId t(i)
              map.apply(T(i)) ! routerequest(key,hops)
              i=T.length
              chk=true
            }
            i+=1
          }
          if(chk==false)
            mainnode ! destinationReached(hops)
        }
      } 

     /* if(numfin>=num_of_requests && !done )
      {
        done=true
        mainnode ! finished(nodeId)
      } */   
    }
    
  }
}
class mainNode extends Actor {
  var node:List[ActorRef] = Nil
  var totalhops:Int=0
  var num_done=0
  var num_of_nodes=0
  var num_of_requests=0
  var startTime=System.currentTimeMillis
  val system = ActorSystem("Node")
  var dest:Int=0
  var done:Boolean=false
  def buildTopology(num_of_nodes : Int,num_of_requests :Int,mainnode:ActorRef)  = {
    var nodeIds:ArrayBuffer[String]=ArrayBuffer[String]()
    // no of digits in the node id is n
    var n=8
    // no of columns in routing table and no of values in each leafset is l
    var l=4
    var temp=""
    var flag=0
    var map=TreeMap.empty[String,ActorRef]
    for(i <- 0 until num_of_nodes)
    {
     while(flag==0)
     {
      while(n>0)
      {
        var rand=Random.nextInt(l)
        temp+=rand.toString()
        n-=1
      }
      if(!nodeIds.contains(temp))
      {
        nodeIds+=temp
        flag=1
      }
      else
      {
        n=8 
        temp=""
      }
     }
     flag=0
     n=8
     temp=""
     map=map.insert(nodeIds(i),node(i)) 
    } 

    val nodeSorted = nodeIds.sorted
    //println(nodeSorted)
    for(i <- 0 until num_of_nodes)
    {
      var leafnode_less:ArrayBuffer[String]=ArrayBuffer[String]()
      var leafnode_more:ArrayBuffer[String]=ArrayBuffer[String]()
      var j=0
      var flag=0
      while(j<num_of_nodes && flag==0)
      {
        if(nodeSorted(j).equals(nodeIds(i))) 
          flag=1
        j+=1
      }
      j-=1
       if(j<4)
       {
         for(k <- 0 until j )
           leafnode_less+=nodeSorted(k)
       }
       else
       {
         for(k<- (j-4) until j)
           leafnode_less+=nodeSorted(k)   
       }
       if((j+4)> (num_of_nodes-1))
       {
         for(k <- (j+1) until num_of_nodes )
           leafnode_more+=nodeSorted(k)
       }
       else
       {
         for(k<- (j+1) to j+4)
           leafnode_more+=nodeSorted(k)   
       }
       // to find routing table
       var rt=Array.ofDim[String](8,4)
       for(x <- 0 until 8)
       {
         var a = nodeIds(i).substring(0,x+1)
         var length=a.length()
         for(y <- 0 until 4)
         {
           if(y!=(a(length-1).toInt-48))
           {
             var reg= a.substring(0,x)+y
             var len=reg.length
             var repeat=8-len
             while(repeat > 0)
             {
               reg+="[0-9]"
               repeat-=1
             }
             val pattern=new Regex(reg)
             var tempbuff:ArrayBuffer[String]=ArrayBuffer[String]()
             var tempcounter=0
             for(tempcounter <- 0 until num_of_nodes)
                if(!pattern.findFirstIn(nodeIds(tempcounter)).equals(None))
                  tempbuff+=pattern.findFirstIn(nodeIds(tempcounter)).get
             if(tempbuff.length > 0){
             var rand=Random.nextInt(tempbuff.length)
             rt(x)(y)=tempbuff(rand).toString
             }
             else
               rt(x)(y)="-1"
           }
           else
             rt(x)(y)="-1"
           
         }
       }
       var neighborset:ArrayBuffer[String]=ArrayBuffer[String]()
       for(x <- 0 until 8)
       {
         var ran=Random.nextInt(8)
         while(neighborset.contains(nodeIds(ran)))
           ran=Random.nextInt(8)
           neighborset+=nodeIds(ran)
       }
       
       node(i) ! InitPastry(nodeIds(i),leafnode_less,leafnode_more,rt,neighborset,num_of_requests,map,mainnode)
    }
    startTime=System.currentTimeMillis
    for(i <- 0 until num_of_nodes)
      node(i) ! sendrequest()
  }
  def receive = {
    case startPastry(num_of_nodes,num_of_requests,mainnode) => {
      var i:Int = 0
      while(i<num_of_nodes){ 
        node ::= system.actorOf(Props[Node])
        i=i+1
      } 
      this.num_of_nodes=num_of_nodes
      this.num_of_requests=num_of_requests
      buildTopology(num_of_nodes,num_of_requests,mainnode)
    }
    case destinationReached(hops) => {
      dest+=1
      println(dest+" "+hops)
      totalhops+=hops
      if(dest==((num_of_nodes*num_of_requests)))
      {
      var avg:Double=totalhops.toDouble/(num_of_nodes*num_of_requests).toDouble
      println("Total number of hops "+totalhops)
      println("Average hops "+avg)
      println("Time taken "+ (System.currentTimeMillis-startTime))
      println("Destination reached "+dest)
      context.system.shutdown()
      }      
    }
    case finished(nodeId) => {
      num_done+=1
      println(num_done+" "+nodeId)
    }

  }
}

object project extends App {
  val system = ActorSystem("node")
  var no_of_nodes=args(0).toInt
  var no_of_requests=args(1).toInt
  val mainnode = system.actorOf(Props[mainNode],name="mainnode")
  mainnode ! startPastry(no_of_nodes,no_of_requests,mainnode)
}