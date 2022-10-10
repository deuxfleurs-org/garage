
//! This module deals with graph algorithms.
//! It is used in layout.rs to build the partition to node assignation.

use rand::prelude::SliceRandom;
use std::cmp::{max, min};
use std::collections::VecDeque;
use std::collections::HashMap;

//Vertex data structures used in all the graphs used in layout.rs.
//usize parameters correspond to node/zone/partitions ids.
//To understand the vertex roles below, please refer to the formal description
//of the layout computation algorithm.
#[derive(Clone,Copy,Debug, PartialEq, Eq, Hash)]
pub enum Vertex{
    Source,
    Pup(usize),         //The vertex p+ of partition p
    Pdown(usize),       //The vertex p- of partition p
    PZ(usize,usize),    //The vertex corresponding to x_(partition p, zone z)
    N(usize),           //The vertex corresponding to node n
    Sink
}


//Edge data structure for the flow algorithm.
//The graph is stored as an adjacency list
#[derive(Clone, Copy, Debug)]
pub struct FlowEdge {
	cap: u32,     //flow maximal capacity of the edge
	flow: i32,  //flow value on the edge
	dest: usize,  //destination vertex id  
	rev: usize, //index of the reversed edge (v, self) in the edge list of vertex v
}

//Edge data structure for the detection of negative cycles.
//The graph is stored as a list of edges (u,v).
#[derive(Clone, Copy, Debug)]
pub struct WeightedEdge {
	w: i32,     //weight of the edge
	dest: usize,
}

pub trait Edge: Clone + Copy {}
impl Edge for FlowEdge {}
impl Edge for WeightedEdge {}

//Struct for the graph structure. We do encapsulation here to be able to both
//provide user friendly Vertex enum to address vertices, and to use usize indices
//and Vec instead of HashMap in the graph algorithm to optimize execution speed.
pub struct Graph<E : Edge>{
    vertextoid : HashMap<Vertex , usize>,
    idtovertex : Vec<Vertex>,
    
    graph : Vec< Vec<E> >
}

pub type CostFunction = HashMap<(Vertex,Vertex), i32>;

impl<E : Edge> Graph<E>{
    pub fn new(vertices : &[Vertex]) -> Self {
        let mut map = HashMap::<Vertex, usize>::new();
        for (i, vert) in vertices.iter().enumerate(){
            map.insert(*vert , i);
        }
        Graph::<E> {
                vertextoid : map,
                idtovertex: vertices.to_vec(),
                graph : vec![Vec::< E >::new(); vertices.len() ]
            }
    }
}

impl Graph<FlowEdge>{
    //This function adds a directed edge to the graph with capacity c, and the
    //corresponding reversed edge with capacity 0.
    pub fn add_edge(&mut self, u: Vertex, v:Vertex, c: u32) -> Result<(), String>{
        if !self.vertextoid.contains_key(&u) || !self.vertextoid.contains_key(&v) {
            return Err("The graph does not contain the provided vertex.".to_string());
        }
        let idu = self.vertextoid[&u];
        let idv = self.vertextoid[&v];
        let rev_u = self.graph[idu].len();
        let rev_v = self.graph[idv].len();
        self.graph[idu].push( FlowEdge{cap: c , dest: idv , flow: 0, rev : rev_v} );
        self.graph[idv].push( FlowEdge{cap: 0 , dest: idu , flow: 0, rev : rev_u} );
        Ok(())
    }
    
    //This function returns the list of vertices that receive a positive flow from
    //vertex v.
    pub fn get_positive_flow_from(&self , v:Vertex) -> Result< Vec<Vertex> , String>{
        if !self.vertextoid.contains_key(&v) {
            return Err("The graph does not contain the provided vertex.".to_string());
        }
        let idv = self.vertextoid[&v];
        let mut result = Vec::<Vertex>::new();
        for edge in self.graph[idv].iter() {
            if edge.flow > 0 {
                result.push(self.idtovertex[edge.dest]);
            }
        }
        Ok(result)
    }
    
    
    //This function returns the value of the flow incoming to v.
    pub fn get_inflow(&self , v:Vertex) -> Result< i32 , String>{
        if !self.vertextoid.contains_key(&v) {
            return Err("The graph does not contain the provided vertex.".to_string());
        }
        let idv = self.vertextoid[&v];
        let mut result = 0;
        for edge in self.graph[idv].iter() {
            result += max(0,self.graph[edge.dest][edge.rev].flow);
        }
        Ok(result)
    }

    //This function returns the value of the flow outgoing from v.
    pub fn get_outflow(&self , v:Vertex) -> Result< i32 , String>{
        if !self.vertextoid.contains_key(&v) {
            return Err("The graph does not contain the provided vertex.".to_string());
        }
        let idv = self.vertextoid[&v];
        let mut result = 0;
        for edge in self.graph[idv].iter() {
            result += max(0,edge.flow);
        }
        Ok(result)
    }

    //This function computes the flow total value by computing the outgoing flow
    //from the source.
    pub fn get_flow_value(&mut self) -> Result<i32, String> {
        self.get_outflow(Vertex::Source)
    }

    //This function shuffles the order of the edge lists. It keeps the ids of the
    //reversed edges consistent.
    fn shuffle_edges(&mut self) {
        let mut rng = rand::thread_rng();
        for i in 0..self.graph.len() {
            self.graph[i].shuffle(&mut rng);
            //We need to update the ids of the reverse edges.
            for j in 0..self.graph[i].len() {
                let target_v = self.graph[i][j].dest;
                let target_rev = self.graph[i][j].rev;
                self.graph[target_v][target_rev].rev = j;
            }
        }
    }

    //Computes an upper bound of the flow n the graph
    pub fn flow_upper_bound(&self) -> u32{
        let idsource = self.vertextoid[&Vertex::Source];
        let mut flow_upper_bound = 0;
        for edge in self.graph[idsource].iter(){
            flow_upper_bound += edge.cap;
        }
        flow_upper_bound
    }
    
    //This function computes the maximal flow using Dinic's algorithm. It starts with
    //the flow values already present in the graph. So it is possible to add some edge to
    //the graph, compute a flow, add other edges, update the flow.
    pub fn compute_maximal_flow(&mut self) -> Result<(), String> {
        if !self.vertextoid.contains_key(&Vertex::Source) { 
            return Err("The graph does not contain a source.".to_string());
        }
        if !self.vertextoid.contains_key(&Vertex::Sink) { 
            return Err("The graph does not contain a sink.".to_string());
        }

        let idsource = self.vertextoid[&Vertex::Source];
        let idsink = self.vertextoid[&Vertex::Sink];
       
        let nb_vertices = self.graph.len();

        let flow_upper_bound = self.flow_upper_bound();
        
        //To ensure the dispersion of the associations generated by the
        //assignation, we shuffle the neighbours of the nodes. Hence,
        //the vertices do not consider their neighbours in the same order.
        self.shuffle_edges();
        
        //We run Dinic's max flow algorithm
        loop {
            //We build the level array from Dinic's algorithm.
            let mut level = vec![None; nb_vertices];

            let mut fifo = VecDeque::new();
            fifo.push_back((idsource, 0));
            while !fifo.is_empty() {
                if let Some((id, lvl)) = fifo.pop_front() {
                    if level[id] == None {  //it means id has not yet been reached
                        level[id] = Some(lvl);
                        for edge in self.graph[id].iter() {
                            if edge.cap as i32 - edge.flow > 0 {
                                fifo.push_back((edge.dest, lvl + 1));
                            }
                        }
                    }
                }
            }
            if level[idsink] == None {
                //There is no residual flow
                break;
            }
            //Now we run DFS respecting the level array
            let mut next_nbd = vec![0; nb_vertices];
            let mut lifo = VecDeque::new();

            lifo.push_back((idsource, flow_upper_bound));

            while let Some((id_tmp, f_tmp)) = lifo.back() {
                let id = *id_tmp;
                let f = *f_tmp;
                if id == idsink {
                    //The DFS reached the sink, we can add a
                    //residual flow.
                    lifo.pop_back();
                    while let Some((id, _)) = lifo.pop_back() {
                        let nbd = next_nbd[id];
                        self.graph[id][nbd].flow += f as i32;
                        let id_rev = self.graph[id][nbd].dest;
                        let nbd_rev = self.graph[id][nbd].rev;
                        self.graph[id_rev][nbd_rev].flow -= f as i32;
                    }
                    lifo.push_back((idsource, flow_upper_bound));
                    continue;
                }
                //else we did not reach the sink
                let nbd = next_nbd[id];
                if nbd >= self.graph[id].len() {
                    //There is nothing to explore from id anymore
                    lifo.pop_back();
                    if let Some((parent, _)) = lifo.back() {
                        next_nbd[*parent] += 1;
                    }
                    continue;
                }
                //else we can try to send flow from id to its nbd
                let new_flow = min(f as i32, self.graph[id][nbd].cap as i32 - self.graph[id][nbd].flow) as u32;
                if new_flow == 0 {
                    next_nbd[id] += 1;
                    continue;
                }
                if let (Some(lvldest), Some(lvlid)) =
                                (level[self.graph[id][nbd].dest], level[id]){
                    if lvldest <= lvlid  {
                        //We cannot send flow to nbd.
                        next_nbd[id] += 1;
                        continue;
                    }
                }
                //otherwise, we send flow to nbd.
                lifo.push_back((self.graph[id][nbd].dest, new_flow));
            }
        }
        Ok(())
    }

    //This function takes a flow, and a cost function on the edges, and tries to find an
    // equivalent flow with a better cost, by finding improving overflow cycles. It uses 
    // as subroutine the Bellman Ford algorithm run up to path_length.
    // We assume that the cost of edge (u,v) is the opposite of the cost of (v,u), and only
    // one needs to be present in the cost function.
    pub fn optimize_flow_with_cost(&mut self , cost: &CostFunction, path_length: usize )
        -> Result<(),String>{
        //We build the weighted graph g where we will look for negative cycle
        let mut gf = self.build_cost_graph(cost)?;
        let mut cycles = gf.list_negative_cycles(path_length);
        while !cycles.is_empty() {
            //we enumerate negative cycles
            for c in cycles.iter(){
                for i in 0..c.len(){
                    //We add one flow unit to the edge (u,v) of cycle c
                    let idu = self.vertextoid[&c[i]];
                    let idv = self.vertextoid[&c[(i+1)%c.len()]];
                    for j in 0..self.graph[idu].len(){
                        //since idu appears at most once in the cycles, we enumerate every
                        //edge at most once.
                        let edge = self.graph[idu][j];
                        if edge.dest == idv {
                            self.graph[idu][j].flow += 1;
                            self.graph[idv][edge.rev].flow -=1;
                            break;
                        }
                    }
                }
            }

            gf = self.build_cost_graph(cost)?;
            cycles = gf.list_negative_cycles(path_length);
        }
        Ok(())
    }

    //Construct the weighted graph G_f from the flow and the cost function
    fn build_cost_graph(&self , cost: &CostFunction) -> Result<Graph<WeightedEdge>,String>{
        
        let mut g = Graph::<WeightedEdge>::new(&self.idtovertex);
        let nb_vertices = self.idtovertex.len();
        for i in 0..nb_vertices {
            for edge in self.graph[i].iter() {
                if edge.cap as  i32 -edge.flow > 0 {
                    //It is possible to send overflow through this edge
                    let u = self.idtovertex[i];
                    let v = self.idtovertex[edge.dest];
                    if cost.contains_key(&(u,v)) {
                        g.add_edge(u,v, cost[&(u,v)])?;
                    }
                    else if cost.contains_key(&(v,u)) {
                        g.add_edge(u,v, -cost[&(v,u)])?;
                    }
                    else{
                        g.add_edge(u,v, 0)?;
                    }
                }
            }
        }
        Ok(g)

    }


}

impl Graph<WeightedEdge>{
    //This function adds a single directed weighted edge to the graph.
    pub fn add_edge(&mut self, u: Vertex, v:Vertex, w: i32) -> Result<(), String>{
        if !self.vertextoid.contains_key(&u) || !self.vertextoid.contains_key(&v) {
            return Err("The graph does not contain the provided vertex.".to_string());
        }
        let idu = self.vertextoid[&u];
        let idv = self.vertextoid[&v];
        self.graph[idu].push( WeightedEdge{ w , dest: idv} );
        Ok(())
    }

    //This function lists the negative cycles it manages to find after path_length
    //iterations of the main loop of the Bellman-Ford algorithm. For the classical
    //algorithm, path_length needs to be equal to the number of vertices. However, 
    //for particular graph structures like our case, the algorithm is still correct
    //when path_length is the length of the longest possible simple path. 
    //See the formal description of the algorithm for more details. 
    fn list_negative_cycles(&self, path_length: usize) -> Vec< Vec<Vertex> > {
        
        let nb_vertices = self.graph.len();
        
        //We start with every vertex at distance 0 of some imaginary extra -1 vertex.
        let mut distance = vec![0 ; nb_vertices];
        //The prev vector collects for every vertex from where does the shortest path come
        let mut prev = vec![None; nb_vertices];

        for _ in 0..path_length +1 {
            for id in 0..nb_vertices{
                for e in self.graph[id].iter(){
                    if distance[id] + e.w < distance[e.dest] {
                        distance[e.dest] = distance[id] + e.w;
                        prev[e.dest] = Some(id);
                    }
                }
            }
        }


        //If self.graph contains a negative cycle, then at this point the graph described
        //by prev (which is a directed 1-forest/functional graph) 
        //must contain a cycle. We list the cycles of prev.
        let cycles_prev = cycles_of_1_forest(&prev);

        //Remark that the cycle in prev is in the reverse order compared to the cycle
        //in the graph. Thus the .rev().
        return cycles_prev.iter().map(|cycle| cycle.iter().rev().map(
                |id| self.idtovertex[*id]
                    ).collect() ).collect();
    }

}


//This function returns the list of cycles of a directed 1 forest. It does not
//check for the consistency of the input.
fn cycles_of_1_forest(forest: &[Option<usize>])  -> Vec<Vec<usize>> {
    let mut cycles = Vec::<Vec::<usize>>::new();
    let mut time_of_discovery = vec![None; forest.len()];

    for t in 0..forest.len(){
        let mut id = t;
        //while we are on a valid undiscovered node
        while time_of_discovery[id] == None {
            time_of_discovery[id] = Some(t);
            if let Some(i) = forest[id] {
                id = i;
            }
            else{
                break;
            }
        }
        if forest[id] != None && time_of_discovery[id] == Some(t) {
            //We discovered an id that we explored at this iteration t.
            //It means we are on a cycle
            let mut cy = vec![id; 1];
            let mut id2 = id;
            while let Some(id_next) = forest[id2] {
                id2 = id_next;
                if id2 != id {
                    cy.push(id2);
                }
                else {
                    break;
                }
            }
            cycles.push(cy);
        }
    }
    cycles
}


