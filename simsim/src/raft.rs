use crate::{Checker, World};
use raft_core::{RaftNode, Role};
use std::collections::HashSet;

#[derive(Debug)]
pub struct ElectionSafetyChecker {
    terms: Vec<u64>,
}

impl ElectionSafetyChecker {
    pub fn new(world: &World<RaftNode>) -> Self {
        Self {
            terms: vec![0; world.nodes.len()],
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
            if new_value < old_value {
                return Err(format!(
                    "Node terms decrease! Node {}: {} -> {}",
                    node_idx, old_value, new_value
                ));
            }
        }
        self.terms = new_terms;
        // println!("{:?}", self.terms);

        let mut cnt_leaders = 0;
        let mut terms_set = HashSet::new();
        for node in &world.nodes {
            if node.role == Role::Leader {
                cnt_leaders += 1;
                terms_set.insert(node.current_term);
            }
        }

        if terms_set.len() < cnt_leaders {
            return Err(format!(
                "There are {} leaders in the same term!",
                cnt_leaders
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ScenarioConfig;
    use raft_core::Action;

    #[test]
    fn raft_election() {
        let config = ScenarioConfig {
            max_delay: 30,
            drop_percent: 0,
            preseed_size: 10000,
        };

        let mut world = World::new(43, config, build_raft_nodes());
        let mut checker = ElectionSafetyChecker::new(&world);

        world.nodes[0].role = Role::Leader;
        world.nodes[0].current_term = 1;

        world.nodes[1].role = Role::Leader;
        world.nodes[1].current_term = 1;

        assert!(checker.check(&world).is_err());

        world.nodes[0].current_term = 2;

        assert!(checker.check(&world).is_ok());
    }

    #[test]
    fn total_drop_timeout() {
        let config = ScenarioConfig {
            max_delay: 30,
            drop_percent: 100,
            preseed_size: 10000,
        };

        let mut world = World::new(43, config, build_raft_nodes());
        let mut checker = ElectionSafetyChecker::new(&world);

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

        let _ = world.run(10000, 1000, &mut checker);

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
}
