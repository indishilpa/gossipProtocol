import akka.actor._
import scala.util.Random
import scala.collection._
import java.util.concurrent.atomic.AtomicReference

case class register_nodes(self_index: Int,  numNodes: Int)
case class rumor(numNodes: Int)
case class send(numNodes: Int, nodeMap: scala.collection.mutable.Map[Int, Int], num_done: Int)
case class push_sum(value: Double, weight: Double, numNodes: Int, nodeMap: scala.collection.mutable.Map[Int, Int], num_done: Int, change_count: Int, ratio:Double )
case class create_line_topo(numNodes: Int, done : Int, end1: Int, end2: Int)
case class addMeToo(numNodes: Int, index: Int, done : Int, end1: Int, end2: Int ,  toNode: Int)
case class create_dim(numNodes: Int, dimGroup: List[List[Int]], self_index: Int, new_num: Int, isImp: Boolean)
case class addMeDim(numNodes:Int, dimGroup: List[List[Int]], self_index: Int, toNode: Int, new_num: Int , isImp: Boolean)

class Goss() extends Actor{
	var gossipGroup: List[_] = List()
	var count:Int = 0
	var my_index:Int= 0
	var my_value: Double = 0.000
	var my_weight:Double = 1.000
	
	def req_size(args: Map[Int,Int]): Int = {	
		var c = 0
		args foreach (x => if (x._2 == 1 ){ c = c + 1})
		return c
	}

	def printList(args: List[_]): Unit = {
		args.foreach(print)
	}

	def printMap(args: Map[Int,Int]): Unit={
		args foreach ( (t2) => println (t2._1 + "-->" + t2._2))
	}

	def difference(arg0: String, arg1: String): Boolean = {
		var diff = 0
		for(i <- 0  to 3){
			diff = diff + math.abs(arg0(i).toInt - arg1(i).toInt)
		}
		if (diff == 1){
			return true
		}else{
			return false
		}
	}

	def receive = {
		case create_dim(numNodes: Int, dimGroup: List[List[Int]], self_index: Int, new_num: Int, isImp: Boolean) => {
			my_index = self_index
			my_value = my_index.toDouble
			var curr_node = dimGroup(my_index)
			my_weight = 1

			var x = curr_node(0) + 1
		
			if (x < new_num){
				var new_node1 = List(x, curr_node(1), curr_node(2))
				var rand = dimGroup.indexOf(new_node1)
				if (rand != -1 && !gossipGroup.contains(rand)){
					gossipGroup = rand :: gossipGroup
					var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand)
					peer ! addMeDim(numNodes, dimGroup, rand, my_index, new_num, isImp)	
				}
			}
			x = curr_node(1) + 1
			
			if (x < new_num){
				var new_node1 = List(curr_node(0), x , curr_node(2))
				var rand = dimGroup.indexOf(new_node1)
				if (rand != -1 && !gossipGroup.contains(rand)){
					gossipGroup = rand :: gossipGroup
					var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand)
					peer ! addMeDim(numNodes, dimGroup, rand, my_index, new_num, isImp)	
				}
			}
			
			x = curr_node(2) + 1
			
			if (x < new_num){
				var new_node1 = List(curr_node(0), curr_node(1), x)
				var rand = dimGroup.indexOf(new_node1)
				if (rand != -1 && !gossipGroup.contains(rand)){
					gossipGroup = rand :: gossipGroup
					var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand)
					peer ! addMeDim(numNodes, dimGroup, rand, my_index, new_num, isImp)	
				}
			}

			if (isImp == true && gossipGroup.length < 4){
				var r = new Random
				var rand = my_index;
				while ( rand == my_index || gossipGroup.contains(rand)){
					rand = r.nextInt(numNodes);
				}
				gossipGroup = rand :: gossipGroup
				var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand)
				peer ! addMeDim(numNodes, dimGroup, rand, my_index, new_num, isImp)	
			}
		}

		case addMeDim(numNodes:Int, dimGroup: List[List[Int]], self_index: Int, toNode: Int, new_num: Int, isImp: Boolean) => {
			if (!gossipGroup.contains(toNode)){
				gossipGroup = toNode :: gossipGroup
			}
			var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + self_index)
			peer ! create_dim(numNodes, dimGroup, self_index, new_num, isImp)			
		}

		case create_line_topo(numNodes: Int, done :Int, end1: Int, end2: Int) => {
			my_index = end1	
			my_value = end1.toDouble
			my_weight = 1

			var r = new Random
			var rand = my_index;
			
			while ( rand == my_index || (rand == end2 && numNodes > 2)){
					rand = r.nextInt(numNodes);
			}
			
			gossipGroup = rand :: gossipGroup
			var done2 = done + 1
			var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand)
			peer ! addMeToo(numNodes, my_index, done2, end1, end2, rand)				
		}

		case addMeToo(numNodes: Int, index: Int, done : Int, end1: Int, end2: Int, toNode: Int) => {
			gossipGroup = index :: gossipGroup  
			my_index = toNode
			my_value = toNode.toDouble
			my_weight = 1

			if ( my_index == end2){
				var done2 = done + 1
			}else if(gossipGroup.length == 2){
				var done2 = done + 1
			}else{
				var r = new Random
				var rand:Int = my_index;
				
				while ( rand == my_index || gossipGroup.contains(rand) || ( rand == end2 && done != (numNodes - 2)) ){
						rand = r.nextInt(numNodes);
				}
				
				gossipGroup = rand :: gossipGroup			
				var done2 = done + 1
				var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand)
				peer ! addMeToo(numNodes, my_index, done2, end1, end2, rand)
			}
		}

		case register_nodes(self_index: Int,  numNodes: Int ) => {
			my_index = self_index	
			my_value = self_index.toDouble
			for (index <- 0 to numNodes - 1) {
				if(index != self_index){
      				gossipGroup = index :: gossipGroup
				}
			}
		}

		case send(numNodes: Int, nodeMap: scala.collection.mutable.Map[Int, Int], num_done :Int) => {
			my_value = my_value/2
			my_weight = my_weight/2
			nodeMap(my_index) = 1

			var r = new Random
			var rand = my_index;
			while ( rand == my_index || ! gossipGroup.filter(_ == rand).isEmpty){
				rand = r.nextInt(numNodes);
			}	
		//	println(" SEND : \t " + my_index  + " ( " + my_value + ", " + my_weight + " ) ----> " + rand) 
			var num_done2 = num_done + 1			
			var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand)			
			peer ! push_sum(my_value, my_weight, numNodes,nodeMap, num_done2, 0, 0.000)
		}

		case push_sum(value: Double, weight: Double, numNodes: Int, nodeMap: scala.collection.mutable.Map[Int, Int], num_done: Int, change_count: Int, ratio: Double) => {

			if (nodeMap(my_index)== 0){
				var num_done2 = num_done + 1 
				my_value = my_value + value
				my_weight = my_weight + weight
				nodeMap(my_index) = 1
			
				var r1 = new Random
				var rand1 = my_index;
				while ( rand1 == my_index || ! gossipGroup.filter(_ == rand1).isEmpty){
					rand1 = r1.nextInt(numNodes);
				}	
				var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand1)
				peer ! push_sum(my_value/2, my_weight/2, numNodes, nodeMap, num_done2, change_count, ratio)							
			}else if (num_done == numNodes){
				my_value = my_value + value
				my_weight = my_weight + weight
				var num_done2 = 0
				var new_ratio:Double = my_value/my_weight
				println("ratios : " + new_ratio + " -- " + ratio)
				
				if ( (Math.abs(new_ratio - ratio) > scala.math.pow(10,-10) )  && change_count < 4) {
					var change_count2 = change_count + 1
					nodeMap.keys.foreach { (current_index) =>
							nodeMap(current_index) = 0
					}
					nodeMap(my_index) = 1
					
					var r1 = new Random
					var rand1 = my_index;
					while ( rand1 == my_index || ! gossipGroup.filter(_ == rand1).isEmpty){
						rand1 = r1.nextInt(numNodes);
					}	
					var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand1)
					peer ! push_sum(my_value/2, my_weight/2, numNodes, nodeMap, 1, change_count2, new_ratio)
				}else{
					println("calculated average = " + my_value/my_weight)
					context.system.shutdown
				}
			}else {
				var r1 = new Random
				var rand1 = my_index;
				while ( rand1 == my_index || ! gossipGroup.filter(_ == rand1).isEmpty){
					rand1 = r1.nextInt(numNodes);
				}		
				var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand1)
				peer ! push_sum(value, weight, numNodes, nodeMap, num_done, change_count, ratio)
			}	
		}

		case rumor(numNodes: Int)  => {
			count = count + 1
			println(" received : \t " + my_index + " \t" + count )

			if(numNodes == 1){
				println("Just one node.")
				context.system.shutdown
			}
			if (count > 10) {
				println("I have received more than the required messages .. shutting down the system.")
				context.system.shutdown
			}else{
				var r = new Random
				var rand = my_index;
				while ( rand == my_index || !gossipGroup.contains(rand)){
					rand = r.nextInt(numNodes);
				}
				var peer = context.system.actorFor("akka://Gossiper/user/nbr_" + rand)
				peer ! rumor(numNodes)
			}			
		}

		case _ => {
			println("received unknown message")
		}
	}
}

object project2 extends App{

	val system = ActorSystem("Gossiper")
	var numNodes: Int = args(0).toInt
	var topology: String = args(1).toString
	var algorithm: String = args(2).toString
	var nodeRefs =scala.collection.mutable.Map[Int, Int]()
	var doneMap =scala.collection.mutable.Map[Int, Int]()
	var num_done = 0
	var dimGroup: List[List[Int]] = List()
	var new_num = scala.math.cbrt(numNodes.toDouble)
	
	if (new_num - new_num.toInt.toDouble > 0.0){
		new_num = new_num.toInt + 1
	}
	
	var mnum = new_num.toInt * new_num.toInt * new_num.toInt

	var g = List(0,0,0)

	if(topology.toLowerCase.equals("3d") == true || topology.toLowerCase.equals("imp3d") == true){
		for(i <- 0 to new_num.toInt - 1){
			g = List(0,0,0)
			g = List(i,g(1), g(2))
			dimGroup = g :: dimGroup
			for(j <-0 to new_num.toInt - 1){
				g = List(g(0), j, g(2))
				dimGroup = g :: dimGroup
				for (k<-0 to new_num.toInt - 1){
					g = List(g(0), g(1), k)
					dimGroup = g :: dimGroup
				}
			}
		}
		dimGroup = dimGroup.distinct

		for (node <- dimGroup){
			var index = dimGroup.indexOf(node)
			doneMap(index) = 0
			nodeRefs += ( index -> 0)
			var peer = system.actorOf(Props(new Goss), name = "nbr_" + index)
		}
		var rand1 = dimGroup.indexOf(List(0,0,0))
		var peer = system.actorFor("akka://Gossiper/user/nbr_" + rand1)
		if(topology.toLowerCase.equals("imp3d") == true){
			peer ! create_dim(mnum, dimGroup, rand1, new_num.toInt, true)
		}else{
			peer ! create_dim(mnum, dimGroup, rand1, new_num.toInt, false)
		}
	}

	if (topology.toLowerCase.equals("line") == true){
		for (index <- 0 to numNodes) {
			doneMap(index) = 0
			nodeRefs += ( index -> 0)
			var peer = system.actorOf(Props(new Goss), name = "nbr_" + index)
		}
		var r1 = new Random
		var rand1 = r1.nextInt(numNodes)
		var rand2 = r1.nextInt(numNodes);
		while(rand2 == rand1){
			rand2 = r1.nextInt(numNodes);
		}		
		var peer = system.actorFor("akka://Gossiper/user/nbr_" + rand2)
		println("end nodes: " + rand1 + " , " +  rand2)
		peer ! create_line_topo(numNodes, 0, rand2, rand1)
	}
	
	if (topology.toLowerCase.equals("full") == true){
		for (index <- 0 to numNodes) {
			doneMap(index) = 0
			nodeRefs += ( index -> 0)
			var peer = system.actorOf(Props(new Goss), name = "nbr_" + index)
		}
		for (index <- 0 to numNodes) {
			var peer = system.actorFor("akka://Gossiper/user/nbr_" + index)
			peer ! register_nodes(index,numNodes)
		}
	}

	if (algorithm.toLowerCase.equals("push-sum") == true) {
		if (topology.toLowerCase.equals("full") == true || topology.toLowerCase.equals("line") == true) {
			var r1 = new Random
			var rand3 = r1.nextInt(numNodes);
			var peer1 = system.actorFor("akka://Gossiper/user/nbr_" + rand3)
			peer1 ! send(numNodes, nodeRefs, num_done)
		}
		
		if (topology.toLowerCase.equals("3d") == true || topology.toLowerCase.equals("imp3d") == true) {
			var rand1 = dimGroup.indexOf(List(0,0,0))
			var peer1 = system.actorFor("akka://Gossiper/user/nbr_" + rand1)	
			peer1 ! send(mnum, nodeRefs, num_done)
		}
	}			
	
	if (algorithm.toLowerCase.equals("gossip") == true) {
		if (topology.toLowerCase.equals("full") == true || topology.toLowerCase.equals("line") == true) {
			var r1 = new Random
			var rand1 = r1.nextInt(numNodes);
			var peer = system.actorFor("akka://Gossiper/user/nbr_" + rand1)
			peer ! rumor(numNodes)
		}
	
		if (topology.toLowerCase.equals("3d") == true || topology.toLowerCase.equals("imp3d") == true) {
			var rand1 = dimGroup.indexOf(List(0,0,0))
			var peer1 = system.actorFor("akka://Gossiper/user/nbr_" + rand1)	
			peer1 ! rumor(mnum)
		}
	}

}
