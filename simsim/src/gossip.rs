use crate::{Action, Checker, Event, Protocol, World};

#[derive(Debug)]
pub struct Node {
    peers: Vec<u32>,
    value: u64,
}
impl Protocol for Node {
    type Msg = u64;

    fn handle(&mut self, event: &Event<u64>) -> Vec<Action<u64>> {
        let mut actions: Vec<Action<u64>> = Vec::new();
        match event {
            Event::MessageDelivered { from, to, msg } => {
                if self.value < *msg {
                    self.value = *msg;
                    for &peer in &self.peers {
                        actions.push(Action::Send {
                            to: peer,
                            msg: self.value,
                        });
                    }
                }
                actions
            }
            Event::TimerFired { node } => {
                for &peer in &self.peers {
                    actions.push(Action::Send {
                        to: peer,
                        msg: self.value,
                    });
                }
                actions
            }
        }
    }
}
#[derive(Debug)]
pub struct GossipChecker {
    values: Vec<u64>,
}

impl GossipChecker {
    pub fn new(world: &World<Node>) -> Self {
        let mut values = vec![];
        for node in &world.nodes {
            values.push(node.value);
        }
        Self { values }
    }
}
impl Checker<Node> for GossipChecker {
    fn check(&mut self, world: &World<Node>) -> Result<(), String> {
        let mut new_values = vec![];
        for node in &world.nodes {
            new_values.push(node.value);
        }
        if new_values.len() != self.values.len() {
            return Err(format!(
                "Different value vector lengths! {} vs {}",
                self.values.len(),
                new_values.len(),
            ));
        }
        println!("{:?}", world.drops);
        for (node_idx, (&new_value, &old_value)) in new_values.iter().zip(&self.values).enumerate()
        {
            if new_value < old_value {
                return Err(format!(
                    "Node values decreas! Node {}: {} -> {}",
                    node_idx, old_value, new_value
                ));
            }
            if world.agenda.is_empty() && new_value != new_values[0] {
                return Err(format!(
                    "Agenda is empty and values are not equal! Node values: {:?}",
                    new_values
                ));
            }
        }
        self.values = new_values;
        Ok(())
    }
}
pub fn build_nodes() -> Vec<Node> {
    vec![
        Node {
            peers: vec![1, 2],
            value: 0,
        },
        Node {
            peers: vec![0, 2],
            value: 0,
        },
        Node {
            peers: vec![0, 1],
            value: 5,
        },
    ]
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ScenarioConfig, print_event_list};

    #[test]
    fn world_step() {
        let config = ScenarioConfig {
            max_delay: 10,
            drop_percent: 0,
            preseed_size: 1000,
        };
        let mut world = World::new(43, config, build_nodes());

        world.insert_into_agenda(
            100,
            Event::MessageDelivered {
                from: 1,
                to: 2,
                msg: 10,
            },
        );

        world.insert_into_agenda(
            300,
            Event::MessageDelivered {
                from: 1,
                to: 2,
                msg: 5,
            },
        );

        world.insert_into_agenda(200, Event::TimerFired { node: 2 });

        let _ = world.step();

        assert_eq!(world.now_ms, 100);
        assert_eq!(
            world.trace[0],
            (
                100,
                Event::MessageDelivered {
                    from: 1,
                    to: 2,
                    msg: 10
                }
            )
        );

        _ = world.step();
        assert!(world.now_ms > 100 && world.now_ms <= 110);
    }

    #[test]
    fn world_run_determinism() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 20,
            preseed_size: 1000,
        };
        let mut worlds: Vec<World<Node>> = Vec::new();

        worlds.push(World::new(22, config, build_nodes()));
        worlds.push(World::new(22, config, build_nodes()));
        worlds.push(World::new(25, config, build_nodes()));

        for world in &mut worlds {
            let mut checker = GossipChecker::new(world);
            world.insert_into_agenda(100, Event::TimerFired { node: 2 });
            let _ = world.run(10000, 1000, &mut checker);
        }

        assert_eq!(worlds[0].trace, worlds[1].trace);
        assert_ne!(worlds[1].trace, worlds[2].trace);
        assert_ne!(worlds[0].trace, worlds[2].trace);
        assert!(worlds[0].agenda.is_empty());
    }

    #[test]
    fn world_run_convergence() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 0,
            preseed_size: 1000,
        };
        let mut world1 = World::new(42, config, build_nodes());
        world1.insert_into_agenda(100, Event::TimerFired { node: 0 });
        world1.insert_into_agenda(100, Event::TimerFired { node: 1 });
        world1.insert_into_agenda(100, Event::TimerFired { node: 2 });

        let mut checker = GossipChecker::new(&world1);

        let outcome = world1.run(10000, 1000, &mut checker);
        assert!(outcome.is_ok(), "{}", outcome.unwrap_err());

        assert_eq!(world1.nodes[1].value, 5);
        assert_eq!(world1.nodes[2].value, 5);
        assert_eq!(world1.nodes[0].value, 5);
    }

    #[test]
    fn seed_sweep_no_drop() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 0,
            preseed_size: 10000,
        };
        for seed in 1..1000 {
            let mut world = World::new(seed, config, build_nodes());
            world.insert_into_agenda(100, Event::TimerFired { node: 2 });

            let mut checker = GossipChecker::new(&world);

            let outcome = world.run(10000, 1000, &mut checker);
            assert!(outcome.is_ok(), "{}", outcome.unwrap_err());
        }
    }

    #[test]
    fn seed_sweep_high_drop() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 10,
            preseed_size: 1000,
        };

        let mut count_error_seeds = 0;

        for seed in 1..1000 {
            let mut world = World::new(seed, config, build_nodes());
            world.insert_into_agenda(100, Event::TimerFired { node: 2 });

            let mut checker = GossipChecker::new(&world);

            let outcome = world.run(10000, 1000, &mut checker);
            if outcome.is_err() {
                count_error_seeds += 1;
            }
        }
        println!(
            "Percentage of seeds that produce an error on a {}% drop seed sweep with {} ms delay: {} / 1000 = {}%!",
            config.drop_percent,
            config.max_delay,
            count_error_seeds,
            (count_error_seeds as f64) / 1000.0
        );
        assert!(count_error_seeds > 0);
    }

    #[test]
    fn baseline_seed() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 20,
            preseed_size: 1000,
        };

        let mut world = World::new(22, config, build_nodes());
        world.insert_into_agenda(100, Event::TimerFired { node: 2 });
        let mut checker = GossipChecker::new(&world);

        let _ = world.run(1000, 10, &mut checker);

        print_event_list(&world.trace);
    }

    #[test]
    fn latest_timer() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 20,
            preseed_size: 1000,
        };

        let mut world = World::new(22, config, build_nodes());
        let mut checker = GossipChecker::new(&world);

        world.insert_into_agenda(100, Event::TimerFired { node: 2 });
        world.insert_into_agenda(
            10,
            Event::MessageDelivered {
                from: 0,
                to: 1,
                msg: 10,
            },
        );
        let _ = world.step();
        world.insert_into_agenda(200, Event::TimerFired { node: 2 });
        let _ = world.run(1000, 10, &mut checker);

        println!("{:#?}", world.trace);

        assert_eq!(world.ghost_timer_cnt[2], 1);

        assert!(world.trace.contains(&(200, Event::TimerFired { node: 2 })));
        assert!(!world.trace.contains(&(100, Event::TimerFired { node: 2 })));

        // What's supposed to happen:
        // 0 ms; node 2: wake me up at 100
        // 10 ms; something happens -> node 2: wake me up at 200
        // 100 ms; node 2 doesn't get pinged
        // trace: (10, something happens) -> ... -> NOTHING fires at 100 for 2 -> ... -> (200, TimerFired for 2)
        // node 2: 1 ghost
        // Note: "something happens" is only to skip time at the start
    }
}
