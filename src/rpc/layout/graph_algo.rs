//! This module deals with graph algorithms.
//! It is used in layout.rs to build the partition to node assignment.

use rand::prelude::{SeedableRng, SliceRandom};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::collections::VecDeque;

/// Vertex data structures used in all the graphs used in layout.rs.
/// usize parameters correspond to node/zone/partitions ids.
/// To understand the vertex roles below, please refer to the formal description
/// of the layout computation algorithm.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Vertex {
	Source,
	Pup(usize),       // The vertex p+ of partition p
	Pdown(usize),     // The vertex p- of partition p
	PZ(usize, usize), // The vertex corresponding to x_(partition p, zone z)
	N(usize),         // The vertex corresponding to node n
	Sink,
}

/// Edge data structure for the flow algorithm.
#[derive(Clone, Copy, Debug)]
pub struct FlowEdge {
	cap: u64,    // flow maximal capacity of the edge
	flow: i64,   // flow value on the edge
	dest: usize, // destination vertex id
	rev: usize,  // index of the reversed edge (v, self) in the edge list of vertex v
}

/// Edge data structure for the detection of negative cycles.
#[derive(Clone, Copy, Debug)]
pub struct WeightedEdge {
	w: i64, // weight of the edge
	dest: usize,
}

pub trait Edge: Clone + Copy {}
impl Edge for FlowEdge {}
impl Edge for WeightedEdge {}

/// Struct for the graph structure. We do encapsulation here to be able to both
/// provide user friendly Vertex enum to address vertices, and to use internally usize
/// indices and Vec instead of HashMap in the graph algorithm to optimize execution speed.
pub struct Graph<E: Edge> {
	vertex_to_id: HashMap<Vertex, usize>,
	id_to_vertex: Vec<Vertex>,

	// The graph is stored as an adjacency list
	graph: Vec<Vec<E>>,
}

pub type CostFunction = HashMap<(Vertex, Vertex), i64>;

impl<E: Edge> Graph<E> {
	pub fn new(vertices: &[Vertex]) -> Self {
		let mut map = HashMap::<Vertex, usize>::new();
		for (i, vert) in vertices.iter().enumerate() {
			map.insert(*vert, i);
		}
		Graph::<E> {
			vertex_to_id: map,
			id_to_vertex: vertices.to_vec(),
			graph: vec![Vec::<E>::new(); vertices.len()],
		}
	}

	fn get_vertex_id(&self, v: &Vertex) -> Result<usize, String> {
		self.vertex_to_id
			.get(v)
			.cloned()
			.ok_or_else(|| format!("The graph does not contain vertex {:?}", v))
	}
}

impl Graph<FlowEdge> {
	/// This function adds a directed edge to the graph with capacity c, and the
	/// corresponding reversed edge with capacity 0.
	pub fn add_edge(&mut self, u: Vertex, v: Vertex, c: u64) -> Result<(), String> {
		let idu = self.get_vertex_id(&u)?;
		let idv = self.get_vertex_id(&v)?;
		if idu == idv {
			return Err("Cannot add edge from vertex to itself in flow graph".into());
		}

		let rev_u = self.graph[idu].len();
		let rev_v = self.graph[idv].len();
		self.graph[idu].push(FlowEdge {
			cap: c,
			dest: idv,
			flow: 0,
			rev: rev_v,
		});
		self.graph[idv].push(FlowEdge {
			cap: 0,
			dest: idu,
			flow: 0,
			rev: rev_u,
		});
		Ok(())
	}

	/// This function returns the list of vertices that receive a positive flow from
	/// vertex v.
	pub fn get_positive_flow_from(&self, v: Vertex) -> Result<Vec<Vertex>, String> {
		let idv = self.get_vertex_id(&v)?;
		let mut result = Vec::<Vertex>::new();
		for edge in self.graph[idv].iter() {
			if edge.flow > 0 {
				result.push(self.id_to_vertex[edge.dest]);
			}
		}
		Ok(result)
	}

	/// This function returns the value of the flow outgoing from v.
	pub fn get_outflow(&self, v: Vertex) -> Result<i64, String> {
		let idv = self.get_vertex_id(&v)?;
		let mut result = 0;
		for edge in self.graph[idv].iter() {
			result += max(0, edge.flow);
		}
		Ok(result)
	}

	/// This function computes the flow total value by computing the outgoing flow
	/// from the source.
	pub fn get_flow_value(&mut self) -> Result<i64, String> {
		self.get_outflow(Vertex::Source)
	}

	/// This function shuffles the order of the edge lists. It keeps the ids of the
	/// reversed edges consistent.
	fn shuffle_edges(&mut self) {
		// We use deterministic randomness so that the layout calculation algorithm
		// will output the same thing every time it is run. This way, the results
		// pre-calculated in `garage layout show` will match exactly those used
		// in practice with `garage layout apply`
		let mut rng = rand::rngs::StdRng::from_seed([0x12u8; 32]);
		for i in 0..self.graph.len() {
			self.graph[i].shuffle(&mut rng);
			// We need to update the ids of the reverse edges.
			for j in 0..self.graph[i].len() {
				let target_v = self.graph[i][j].dest;
				let target_rev = self.graph[i][j].rev;
				self.graph[target_v][target_rev].rev = j;
			}
		}
	}

	/// Computes an upper bound of the flow on the graph
	pub fn flow_upper_bound(&self) -> Result<u64, String> {
		let idsource = self.get_vertex_id(&Vertex::Source)?;
		let mut flow_upper_bound = 0;
		for edge in self.graph[idsource].iter() {
			flow_upper_bound += edge.cap;
		}
		Ok(flow_upper_bound)
	}

	/// This function computes the maximal flow using Dinic's algorithm. It starts with
	/// the flow values already present in the graph. So it is possible to add some edge to
	/// the graph, compute a flow, add other edges, update the flow.
	pub fn compute_maximal_flow(&mut self) -> Result<(), String> {
		let idsource = self.get_vertex_id(&Vertex::Source)?;
		let idsink = self.get_vertex_id(&Vertex::Sink)?;

		let nb_vertices = self.graph.len();

		let flow_upper_bound = self.flow_upper_bound()?;

		// To ensure the dispersion of the associations generated by the
		// assignment, we shuffle the neighbours of the nodes. Hence,
		// the vertices do not consider their neighbours in the same order.
		self.shuffle_edges();

		// We run Dinic's max flow algorithm
		loop {
			// We build the level array from Dinic's algorithm.
			let mut level = vec![None; nb_vertices];

			let mut fifo = VecDeque::new();
			fifo.push_back((idsource, 0));
			while let Some((id, lvl)) = fifo.pop_front() {
				if level[id].is_none() {
					// it means id has not yet been reached
					level[id] = Some(lvl);
					for edge in self.graph[id].iter() {
						if edge.cap as i64 - edge.flow > 0 {
							fifo.push_back((edge.dest, lvl + 1));
						}
					}
				}
			}
			if level[idsink].is_none() {
				// There is no residual flow
				break;
			}
			// Now we run DFS respecting the level array
			let mut next_nbd = vec![0; nb_vertices];
			let mut lifo = Vec::new();

			lifo.push((idsource, flow_upper_bound));

			while let Some((id, f)) = lifo.last().cloned() {
				if id == idsink {
					// The DFS reached the sink, we can add a
					// residual flow.
					lifo.pop();
					while let Some((id, _)) = lifo.pop() {
						let nbd = next_nbd[id];
						self.graph[id][nbd].flow += f as i64;
						let id_rev = self.graph[id][nbd].dest;
						let nbd_rev = self.graph[id][nbd].rev;
						self.graph[id_rev][nbd_rev].flow -= f as i64;
					}
					lifo.push((idsource, flow_upper_bound));
					continue;
				}
				// else we did not reach the sink
				let nbd = next_nbd[id];
				if nbd >= self.graph[id].len() {
					// There is nothing to explore from id anymore
					lifo.pop();
					if let Some((parent, _)) = lifo.last() {
						next_nbd[*parent] += 1;
					}
					continue;
				}
				// else we can try to send flow from id to its nbd
				let new_flow = min(
					f as i64,
					self.graph[id][nbd].cap as i64 - self.graph[id][nbd].flow,
				) as u64;
				if new_flow == 0 {
					next_nbd[id] += 1;
					continue;
				}
				if let (Some(lvldest), Some(lvlid)) = (level[self.graph[id][nbd].dest], level[id]) {
					if lvldest <= lvlid {
						// We cannot send flow to nbd.
						next_nbd[id] += 1;
						continue;
					}
				}
				// otherwise, we send flow to nbd.
				lifo.push((self.graph[id][nbd].dest, new_flow));
			}
		}
		Ok(())
	}

	/// This function takes a flow, and a cost function on the edges, and tries to find an
	/// equivalent flow with a better cost, by finding improving overflow cycles. It uses
	/// as subroutine the Bellman Ford algorithm run up to path_length.
	/// We assume that the cost of edge (u,v) is the opposite of the cost of (v,u), and
	/// only one needs to be present in the cost function.
	pub fn optimize_flow_with_cost(
		&mut self,
		cost: &CostFunction,
		path_length: usize,
	) -> Result<(), String> {
		// We build the weighted graph g where we will look for negative cycle
		let mut gf = self.build_cost_graph(cost)?;
		let mut cycles = gf.list_negative_cycles(path_length);
		while !cycles.is_empty() {
			// we enumerate negative cycles
			for c in cycles.iter() {
				for i in 0..c.len() {
					// We add one flow unit to the edge (u,v) of cycle c
					let idu = self.vertex_to_id[&c[i]];
					let idv = self.vertex_to_id[&c[(i + 1) % c.len()]];
					for j in 0..self.graph[idu].len() {
						// since idu appears at most once in the cycles, we enumerate every
						// edge at most once.
						let edge = self.graph[idu][j];
						if edge.dest == idv {
							self.graph[idu][j].flow += 1;
							self.graph[idv][edge.rev].flow -= 1;
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

	/// Construct the weighted graph G_f from the flow and the cost function
	fn build_cost_graph(&self, cost: &CostFunction) -> Result<Graph<WeightedEdge>, String> {
		let mut g = Graph::<WeightedEdge>::new(&self.id_to_vertex);
		let nb_vertices = self.id_to_vertex.len();
		for i in 0..nb_vertices {
			for edge in self.graph[i].iter() {
				if edge.cap as i64 - edge.flow > 0 {
					// It is possible to send overflow through this edge
					let u = self.id_to_vertex[i];
					let v = self.id_to_vertex[edge.dest];
					if cost.contains_key(&(u, v)) {
						g.add_edge(u, v, cost[&(u, v)])?;
					} else if cost.contains_key(&(v, u)) {
						g.add_edge(u, v, -cost[&(v, u)])?;
					} else {
						g.add_edge(u, v, 0)?;
					}
				}
			}
		}
		Ok(g)
	}
}

impl Graph<WeightedEdge> {
	/// This function adds a single directed weighted edge to the graph.
	pub fn add_edge(&mut self, u: Vertex, v: Vertex, w: i64) -> Result<(), String> {
		let idu = self.get_vertex_id(&u)?;
		let idv = self.get_vertex_id(&v)?;
		self.graph[idu].push(WeightedEdge { w, dest: idv });
		Ok(())
	}

	/// This function lists the negative cycles it manages to find after path_length
	/// iterations of the main loop of the Bellman-Ford algorithm. For the classical
	/// algorithm, path_length needs to be equal to the number of vertices. However,
	/// for particular graph structures like in our case, the algorithm is still correct
	/// when path_length is the length of the longest possible simple path.
	/// See the formal description of the algorithm for more details.
	fn list_negative_cycles(&self, path_length: usize) -> Vec<Vec<Vertex>> {
		let nb_vertices = self.graph.len();

		// We start with every vertex at distance 0 of some imaginary extra -1 vertex.
		let mut distance = vec![0; nb_vertices];
		// The prev vector collects for every vertex from where does the shortest path come
		let mut prev = vec![None; nb_vertices];

		for _ in 0..path_length + 1 {
			for id in 0..nb_vertices {
				for e in self.graph[id].iter() {
					if distance[id] + e.w < distance[e.dest] {
						distance[e.dest] = distance[id] + e.w;
						prev[e.dest] = Some(id);
					}
				}
			}
		}

		// If self.graph contains a negative cycle, then at this point the graph described
		// by prev (which is a directed 1-forest/functional graph)
		// must contain a cycle. We list the cycles of prev.
		let cycles_prev = cycles_of_1_forest(&prev);

		// Remark that the cycle in prev is in the reverse order compared to the cycle
		// in the graph. Thus the .rev().
		return cycles_prev
			.iter()
			.map(|cycle| {
				cycle
					.iter()
					.rev()
					.map(|id| self.id_to_vertex[*id])
					.collect()
			})
			.collect();
	}
}

/// This function returns the list of cycles of a directed 1 forest. It does not
/// check for the consistency of the input.
fn cycles_of_1_forest(forest: &[Option<usize>]) -> Vec<Vec<usize>> {
	let mut cycles = Vec::<Vec<usize>>::new();
	let mut time_of_discovery = vec![None; forest.len()];

	for t in 0..forest.len() {
		let mut id = t;
		// while we are on a valid undiscovered node
		while time_of_discovery[id].is_none() {
			time_of_discovery[id] = Some(t);
			if let Some(i) = forest[id] {
				id = i;
			} else {
				break;
			}
		}
		if forest[id].is_some() && time_of_discovery[id] == Some(t) {
			// We discovered an id that we explored at this iteration t.
			// It means we are on a cycle
			let mut cy = vec![id; 1];
			let mut id2 = id;
			while let Some(id_next) = forest[id2] {
				id2 = id_next;
				if id2 != id {
					cy.push(id2);
				} else {
					break;
				}
			}
			cycles.push(cy);
		}
	}
	cycles
}
