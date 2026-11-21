use crate::{Checker, World};
use raft_core::{Action, Event, RPC, RaftNode, Role};
use std::{cmp::max, collections::HashSet};

#[derive(Debug)]
pub struct ElectionSafetyChecker {
    terms: Vec<u64>,
    voters: Vec<HashSet<u32>>,
    total_leaders: u64,
    highest_seen_term: u64,
}

impl ElectionSafetyChecker {
    pub fn new(world: &World<RaftNode>) -> Self {
        Self {
            terms: vec![0; world.nodes.len()],
            voters: vec![HashSet::new(); world.nodes.len()],
            total_leaders: 0,
            highest_seen_term: 0,
        }
    }
}
impl Checker<RaftNode> for ElectionSafetyChecker {
    fn check(&mut self, world: &World<RaftNode>) -> Result<(), String> {
        let mut new_terms = vec![];
        for node in &world.nodes {
            new_terms.push(node.current_term);
        }
        if new_terms.len() != self.terms.len() {
            return Err(format!(
                "Different term vector lengths! {} vs {}",
                self.terms.len(),
                new_terms.len(),
            ));
        }
        for (node_idx, (&new_value, &old_value)) in new_terms.iter().zip(&self.terms).enumerate() {
            if new_value != old_value {
                self.voters[node_idx].clear();
            }
            if new_value < old_value {
                return Err(format!(
                    "Node terms decrease! Node {}: {} -> {}",
                    node_idx, old_value, new_value
                ));
            }
        }
        self.terms = new_terms;
        // println!("{:?}", self.terms);

        if let Some((
            _,
            Event::MessageDelivered {
                from,
                to,
                msg:
                    RPC::RequestVoteReply {
                        term,
                        vote_granted: true,
                    },
            },
        )) = world.trace.last()
            && self.terms[*to as usize] == *term
        {
            self.voters[*to as usize].insert(*from);
        }

        // Leader count and the respective term set among those leaders. If there are more leaders
        // than their "shared" terms, then at least 2 leaders have the same term, which breaks the
        // election safty property
        let mut cnt_leaders = 0;
        let mut terms_set = HashSet::new();

        for (node_idx, node) in world.nodes.iter().enumerate() {
            if node.role == Role::Leader {
                if self.voters[node_idx].len() < world.nodes.len() / 2 {
                    return Err(format!(
                        "The leader {} didn't get sufficient votes! Voters: {:?}",
                        node_idx, self.voters[node_idx]
                    ));
                }
                cnt_leaders += 1;
                terms_set.insert(node.current_term);
            }
            self.highest_seen_term = max(self.highest_seen_term, node.current_term);
        }

        if terms_set.len() < cnt_leaders {
            return Err(format!(
                "There are {} leaders in the same term!",
                cnt_leaders
            ));
        }

        self.total_leaders += cnt_leaders as u64;
        Ok(())
    }
}

pub fn build_raft_nodes() -> Vec<RaftNode> {
    vec![
        RaftNode::new(vec![1, 2]),
        RaftNode::new(vec![0, 2]),
        RaftNode::new(vec![0, 1]),
    ]
}

pub fn start_raft_sim(world: &mut World<RaftNode>) {
    world.handle_action(
        0,
        Action::SetTimer {
            min_delay: 150,
            max_delay: 300,
        },
    );
    world.handle_action(
        1,
        Action::SetTimer {
            min_delay: 150,
            max_delay: 300,
        },
    );
    world.handle_action(
        2,
        Action::SetTimer {
            min_delay: 150,
            max_delay: 300,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ScenarioConfig, return_formatted_event};

    #[test]
    fn raft_election() {
        let config = ScenarioConfig {
            max_delay: 30,
            drop_percent: 0,
            preseed_size: 10000,
            until: 10000,
            max_steps: 1000,
            node_count: 3,
            seed_crashes: true,
        };

        let mut world = World::new(43, config, build_raft_nodes());
        let mut checker = ElectionSafetyChecker::new(&world);

        world.nodes[0].role = Role::Leader;
        world.nodes[0].current_term = 1;
        checker.terms[0] = 1;
        checker.voters[0] = HashSet::from([1]);

        world.nodes[1].role = Role::Leader;
        world.nodes[1].current_term = 1;
        checker.terms[1] = 1;
        checker.voters[1] = HashSet::from([2]);

        assert!(checker.check(&world).is_err());

        checker = ElectionSafetyChecker::new(&world);

        world.nodes[0].role = Role::Leader;
        world.nodes[0].current_term = 1;
        checker.terms[0] = 1;
        checker.voters[0] = HashSet::from([1]);

        world.nodes[1].role = Role::Leader;
        world.nodes[1].current_term = 2;
        checker.terms[1] = 2;
        checker.voters[1] = HashSet::from([2]);

        // if let Err(message) = checker.check(&world) {
        //     println!("{}", message);
        // }

        assert!(checker.check(&world).is_ok());
    }

    #[test]
    fn total_drop_timeout() {
        let config = ScenarioConfig {
            max_delay: 30,
            drop_percent: 100,
            preseed_size: 10000,
            until: 10000,
            max_steps: 1000,
            node_count: 3,
            seed_crashes: true,
        };

        let mut world = World::new(43, config, build_raft_nodes());
        let mut checker = ElectionSafetyChecker::new(&world);

        start_raft_sim(&mut world);

        let _ = world.run(&mut checker);

        // println!("{:?}", world.trace);
        // println!("{:?}", world.agenda);
        // println!("{:?}", world.now_ms);

        for node in &world.nodes {
            // println!("{:?}", node);
            assert!(node.current_term > 1);
        }
    }

    #[test]
    fn non_decreasing_terms() {
        let config = ScenarioConfig {
            max_delay: 30,
            drop_percent: 100,
            preseed_size: 10000,
            until: 10000,
            max_steps: 1000,
            node_count: 3,
            seed_crashes: true,
        };

        let mut world = World::new(43, config, build_raft_nodes());
        let mut checker = ElectionSafetyChecker::new(&world);

        // println!("{}", checker.terms.len());
        // println!("{:?}", world.nodes.len());

        // world.handle_action(
        //     0,
        //     Action::SetTimer {
        //         min_delay: 150,
        //         max_delay: 300,
        //     },
        // );

        // let _ = world.run(10000, 1000, &mut checker);
        world.nodes[0].current_term = 1;

        let _ = checker.check(&world);

        world.nodes[0].current_term = 0;

        // println!("{:?}", checker.check(&world));
        assert!(checker.check(&world).is_err());
    }

    #[test]
    fn seed_sweep_no_drop() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 0,
            preseed_size: 10000,
            until: 1000,
            max_steps: 1000,
            node_count: 3,
            seed_crashes: true,
        };
        for seed in 1..1000 {
            let mut world = World::new(seed, config, build_raft_nodes());
            start_raft_sim(&mut world);

            let mut checker = ElectionSafetyChecker::new(&world);

            let outcome = world.run(&mut checker);

            assert!(outcome.is_ok(), "{}", outcome.unwrap_err());
        }

        let config = ScenarioConfig {
            max_delay: 10,
            drop_percent: 0,
            preseed_size: 10000,
            until: 10000,
            max_steps: 1000,
            node_count: 3,
            seed_crashes: true,
        };
        for seed in 1..1000 {
            let mut world = World::new(seed, config, build_raft_nodes());
            start_raft_sim(&mut world);

            let mut checker = ElectionSafetyChecker::new(&world);

            let _ = world.run(&mut checker);

            assert!(checker.total_leaders > 0);

            // WARNING!: Could plausibly fail with another generator configuration
            // Fix: check the value without heartbeats and increase the upper bound with
            // heartbeats accordingly
            // assert!(checker.highest_seen_term < 40);
        }
    }

    #[test]
    fn seed_sweep_high_drop() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 10,
            preseed_size: 10000,
            until: 10000,
            max_steps: 10000,
            node_count: 3,
            seed_crashes: true,
        };
        for seed in 1..1000 {
            let mut world = World::new(seed, config, build_raft_nodes());
            start_raft_sim(&mut world);

            let mut checker = ElectionSafetyChecker::new(&world);

            let outcome = world.run(&mut checker);

            // if seed == 226 {
            //     world.trace.last_chunk::<20>());
            // }
            if outcome.is_err() {
                world.debug_trace();
            }
            assert!(outcome.is_ok(), "{}", outcome.unwrap_err());
        }
        let config = ScenarioConfig {
            max_delay: 10,
            drop_percent: 10,
            preseed_size: 10000,
            until: 10000,
            max_steps: 1000,
            node_count: 3,
            seed_crashes: true,
        };
        for seed in 1..1000 {
            let mut world = World::new(seed, config, build_raft_nodes());
            start_raft_sim(&mut world);

            let mut checker = ElectionSafetyChecker::new(&world);

            let _ = world.run(&mut checker);

            assert!(checker.total_leaders > 0);

            // WARNING!: Could plausibly fail with another generator configuration
            // Fix: check the value without heartbeats and increase the upper bound with
            // heartbeats accordingly
            // assert!(checker.highest_seen_term < 40);
        }
    }

    #[test]
    fn false_term_leader_grant() {
        // When a node (candidate or not) receives a reply (positive or negative), if the vote count
        // is stale (wasn't reset from one of previous terms), it shouldn't be granted leader

        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 0,
            preseed_size: 10000,
            until: 1000,
            max_steps: 100,
            node_count: 3,
            seed_crashes: true,
        };

        let mut world = World::new(1, config, build_raft_nodes());
        let mut checker = ElectionSafetyChecker::new(&world);

        world.nodes[0].current_term = 1;
        world.nodes[1].current_term = 2;
        world.nodes[2].current_term = 2;

        world.nodes[0].votes = 2;

        world.insert_into_agenda(
            100,
            Event::MessageDelivered {
                from: 1,
                to: 0,
                msg: RPC::RequestVoteReply {
                    term: 1,
                    vote_granted: false,
                },
            },
        );
        let _ = world.run(&mut checker);
        assert!(world.nodes[0].role != Role::Leader);
    }

    // To test:
    // - if a node became a follower and got a positive RequestVoteReply, it can't be promoted to
    // leader, as the leader for the term has already been elected
    // -
}
