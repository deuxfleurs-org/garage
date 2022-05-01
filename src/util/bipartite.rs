/*
 * This module deals with graph algorithm in complete bipartite 
 * graphs. It is used in layout.rs to build the partition to node
 * assignation.
 * */

use std::cmp::{min,max};
use std::collections::VecDeque;
use rand::prelude::SliceRandom;

//Graph data structure for the flow algorithm.
#[derive(Clone,Copy,Debug)]
struct EdgeFlow{
    c : i32,
    flow : i32,
    v : usize,
    rev : usize,
}

//Graph data structure for the detection of positive cycles.
#[derive(Clone,Copy,Debug)]
struct WeightedEdge{
    w : i32, 
    u : usize,
    v : usize,
}


/* This function takes two matchings (old_match and new_match) in a 
 * complete bipartite graph. It returns a matching that has the 
 * same degree as new_match at every vertex, and that is as close
 * as possible to old_match.
 * */
pub fn optimize_matching( old_match : &Vec<Vec<usize>> , 
                          new_match : &Vec<Vec<usize>> , 
                          nb_right : usize )
                                    -> Vec<Vec<usize>> {
    let nb_left = old_match.len();
    let ed = WeightedEdge{w:-1,u:0,v:0};
    let mut edge_vec = vec![ed ; nb_left*nb_right];
    
    //We build the complete bipartite graph structure, represented
    //by the list of all edges.
    for i in 0..nb_left {
        for j in 0..nb_right{
            edge_vec[i*nb_right + j].u = i;
            edge_vec[i*nb_right + j].v = nb_left+j;
        }
    }

    for i in 0..edge_vec.len() {
        //We add the old matchings
        if old_match[edge_vec[i].u].contains(&(edge_vec[i].v-nb_left)) {
            edge_vec[i].w *= -1;
        }
        //We add the new matchings
        if new_match[edge_vec[i].u].contains(&(edge_vec[i].v-nb_left)) {
            (edge_vec[i].u,edge_vec[i].v) = 
                            (edge_vec[i].v,edge_vec[i].u);
            edge_vec[i].w *= -1;
        }
    }
    //Now edge_vec is a graph where edges are oriented LR if we
    //can add them to new_match, and RL otherwise. If 
    //adding/removing them makes the matching closer to old_match
    //they have weight 1; and -1 otherwise.
    
    //We shuffle the edge list so that there is no bias depending in
    //partitions/zone label in the triplet dispersion 
    let mut rng = rand::thread_rng(); 
    edge_vec.shuffle(&mut rng);
    
    //Discovering and flipping a cycle with positive weight in this
    //graph will make the matching closer to old_match.
    //We use Bellman Ford algorithm to discover positive cycles 
    loop{
        if let Some(cycle) = positive_cycle(&edge_vec, nb_left, nb_right) {
            for i in cycle {
                //We flip the edges of the cycle.
                (edge_vec[i].u,edge_vec[i].v) = 
                            (edge_vec[i].v,edge_vec[i].u);
                edge_vec[i].w *= -1;        
            }
        }
        else {
            //If there is no cycle, we return the optimal matching.
            break;
        }
    }
    
    //The optimal matching is build from the graph structure.
    let mut matching = vec![Vec::<usize>::new() ; nb_left];
    for e in edge_vec {
        if e.u > e.v {
            matching[e.v].push(e.u-nb_left);
        }
    }
    matching
}

//This function finds a positive cycle in a bipartite wieghted graph.
fn positive_cycle( edge_vec : &Vec<WeightedEdge>, nb_left : usize,
                   nb_right : usize) -> Option<Vec<usize>> {
    let nb_side_min = min(nb_left, nb_right);
    let nb_vertices = nb_left+nb_right;
    let weight_lowerbound = -((nb_left +nb_right) as i32) -1; 
    let mut accessed = vec![false ; nb_left];
   
    //We try to find a positive cycle accessible from the left 
    //vertex i.
    for i in 0..nb_left{
        if accessed[i] {
            continue;
        }
        let mut weight =vec![weight_lowerbound ; nb_vertices];
        let mut prev =vec![ edge_vec.len() ; nb_vertices];
        weight[i] = 0;
        //We compute largest weighted paths from i.
        //Since the graph is bipartite, any simple cycle has length
        //at most 2*nb_side_min. In the general Bellman-Ford 
        //algorithm, the bound here is the number of vertices. Since
        //the number of partitions can be much larger than the 
        //number of nodes, we optimize that.
        for _ in 0..(2*nb_side_min) {
            for j in 0..edge_vec.len() {
                let e = edge_vec[j];
                if weight[e.v] < weight[e.u]+e.w {
                    weight[e.v] = weight[e.u]+e.w;
                    prev[e.v] = j;
                }
            }
        }
        //We update the accessed table
        for i in 0..nb_left {
            if weight[i] > weight_lowerbound {
                accessed[i] = true;
            }
        }
        //We detect positive cycle
        for e in edge_vec {
            if weight[e.v] < weight[e.u]+e.w {
        //it means e is on a path branching from a positive cycle
                let mut was_seen = vec![false ; nb_vertices];
                let mut curr = e.u;
                //We track back with prev until we reach the cycle.
                while !was_seen[curr]{
                    was_seen[curr] = true;
                    curr = edge_vec[prev[curr]].u;
                }
                //Now curr is on the cycle. We collect the edges ids.
                let mut cycle = Vec::<usize>::new();
                cycle.push(prev[curr]);
                let mut cycle_vert = edge_vec[prev[curr]].u;
                while cycle_vert != curr {
                    cycle.push(prev[cycle_vert]);
                    cycle_vert = edge_vec[prev[cycle_vert]].u;
                }

                return Some(cycle);
            }
        }
    }

    None
}


// This function takes two arrays of capacity and computes the 
// maximal matching in the complete bipartite graph such that the 
// left vertex i is matched to left_cap_vec[i] right vertices, and
// the right vertex j is matched to right_cap_vec[j] left vertices.
// To do so, we use Dinic's maximum flow algorithm.
pub fn dinic_compute_matching( left_cap_vec : Vec<u32>,
        right_cap_vec : Vec<u32>) -> Vec< Vec<usize> >
{
    let mut graph = Vec::<Vec::<EdgeFlow>  >::new();
    let ed = EdgeFlow{c:0,flow:0,v:0, rev:0};

    // 0 will be the source
    graph.push(vec![ed ; left_cap_vec.len()]);
    for i in 0..left_cap_vec.len()
    {
        graph[0][i].c = left_cap_vec[i] as i32;
        graph[0][i].v = i+2;
        graph[0][i].rev = 0;
    }

    //1 will be the sink
    graph.push(vec![ed ; right_cap_vec.len()]);
    for i in 0..right_cap_vec.len()
    {
        graph[1][i].c = right_cap_vec[i] as i32;
        graph[1][i].v = i+2+left_cap_vec.len();
        graph[1][i].rev = 0;
    }
    
    //we add left vertices
    for i in 0..left_cap_vec.len() {
        graph.push(vec![ed ; 1+right_cap_vec.len()]);
        graph[i+2][0].c = 0; //directed
        graph[i+2][0].v = 0;
        graph[i+2][0].rev = i;

        for j in 0..right_cap_vec.len() {
            graph[i+2][j+1].c = 1;
            graph[i+2][j+1].v = 2+left_cap_vec.len()+j;
            graph[i+2][j+1].rev = i+1;
        }
    }

    //we add right vertices
    for i in 0..right_cap_vec.len() {
        let lft_ln = left_cap_vec.len();
        graph.push(vec![ed ; 1+lft_ln]);
        graph[i+lft_ln+2][0].c = graph[1][i].c;
        graph[i+lft_ln+2][0].v = 1;
        graph[i+lft_ln+2][0].rev = i;

        for j in 0..left_cap_vec.len() {
            graph[i+2+lft_ln][j+1].c = 0; //directed
            graph[i+2+lft_ln][j+1].v = j+2;
            graph[i+2+lft_ln][j+1].rev = i+1;
        }
    }

    //To ensure the dispersion of the triplets generated by the 
    //assignation, we shuffle the neighbours of the nodes. Hence,
    //left vertices do not consider the right ones in the same order.
    let mut rng = rand::thread_rng(); 
    for i in 0..graph.len() {
        graph[i].shuffle(&mut rng);
        //We need to update the ids of the reverse edges.
        for j in 0..graph[i].len() {
            let target_v = graph[i][j].v;
            let target_rev = graph[i][j].rev;
            graph[target_v][target_rev].rev = j;
        }
    }

    let nb_vertices = graph.len();
    
    //We run Dinic's max flow algorithm
    loop{
        //We build the level array from Dinic's algorithm.
        let mut level = vec![-1; nb_vertices];

        let mut fifo = VecDeque::new();
        fifo.push_back((0,0));
        while !fifo.is_empty() {
            if let Some((id,lvl)) = fifo.pop_front(){
                if level[id] == -1 {
                    level[id] = lvl;
                    for e in graph[id].iter(){
                        if e.c-e.flow > 0{
                            fifo.push_back((e.v,lvl+1));
                        }
                    }
                }
            }
        }
        if level[1] == -1 {
            //There is no residual flow
            break;
        }

        //Now we run DFS respecting the level array
        let mut next_nbd = vec![0; nb_vertices];
        let mut lifo = VecDeque::new();
        
        let flow_upper_bound;
        if let Some(x) = left_cap_vec.iter().max() {
            flow_upper_bound=*x as i32;
        }
        else {
            flow_upper_bound = 0;
            assert!(false);
        }
        
        lifo.push_back((0,flow_upper_bound));
        
        loop
        {
            if let Some((id_tmp, f_tmp)) = lifo.back() {
                let id = *id_tmp;
                let f = *f_tmp;
                if id == 1  {
                    //The DFS reached the sink, we can add a 
                    //residual flow.
                    lifo.pop_back();
                    while !lifo.is_empty() {
                        if let Some((id,_)) = lifo.pop_back(){
                            let nbd=next_nbd[id];
                            graph[id][nbd].flow += f;
                            let id_v = graph[id][nbd].v;
                            let nbd_v = graph[id][nbd].rev;
                            graph[id_v][nbd_v].flow -= f;
                        }
                    }
                    lifo.push_back((0,flow_upper_bound));
                    continue;
                }
               //else we did not reach the sink
                let nbd = next_nbd[id];
                if nbd >= graph[id].len() {
                    //There is nothing to explore from id anymore
                    lifo.pop_back();
                    if let Some((parent, _)) = lifo.back(){
                        next_nbd[*parent] +=1;
                    }
                    continue;
                }
                //else we can try to send flow from id to its nbd
                let new_flow = min(f,graph[id][nbd].c
                                        - graph[id][nbd].flow);
                if level[graph[id][nbd].v] <= level[id] || 
                                                   new_flow == 0 {
                    //We cannot send flow to nbd.
                    next_nbd[id] += 1;
                    continue;
                }
                //otherwise, we send flow to nbd.
                lifo.push_back((graph[id][nbd].v, new_flow));
            }
            else {
                break;
            }
        }
    }
    
    //We return the association
    let assoc_table = (0..left_cap_vec.len()).map(
        |id| graph[id+2].iter()
                    .filter(|e| e.flow > 0)
                    .map( |e| e.v-2-left_cap_vec.len())
                    .collect()).collect();

    //consistency check
    
    //it is a flow
    for i in 3..graph.len(){
        assert!( graph[i].iter().map(|e| e.flow).sum::<i32>() == 0);
        for e in graph[i].iter(){
            assert!(e.flow + graph[e.v][e.rev].flow == 0);
        }
    }
    
    //it solves the matching problem
    for i in 0..left_cap_vec.len(){
        assert!(left_cap_vec[i] as i32 == 
            graph[i+2].iter().map(|e| max(0,e.flow)).sum::<i32>());
    }
    for i in 0..right_cap_vec.len(){
        assert!(right_cap_vec[i] as i32 == 
            graph[i+2+left_cap_vec.len()].iter()
                    .map(|e| max(0,e.flow)).sum::<i32>());
    }


    assoc_table
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_flow() {
        let left_vec = vec![3;8];
        let right_vec = vec![0,4,8,4,8];
        //There are asserts in the function that computes the flow
        let _ = dinic_compute_matching(left_vec, right_vec);
    }

    //maybe add tests relative to the matching optilization ?
}


