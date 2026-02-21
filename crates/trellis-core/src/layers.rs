use crate::errors::DagValidationError;
use crate::model::Task;
use std::collections::{HashMap, VecDeque};

pub fn assign_layers(tasks: &mut HashMap<String, Task>) -> Result<u32, DagValidationError> {
    if tasks.is_empty() {
        return Ok(0);
    }

    let mut in_degree: HashMap<String, usize> = tasks
        .iter()
        .map(|(id, task)| (id.clone(), task.depends_on.len()))
        .collect();

    let mut layer: HashMap<String, u32> = tasks.keys().map(|id| (id.clone(), 0)).collect();

    let mut roots: Vec<String> = in_degree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(id.clone()))
        .collect();
    roots.sort();

    let mut queue: VecDeque<String> = roots.into();
    let mut visited = 0usize;

    while let Some(task_id) = queue.pop_front() {
        visited += 1;
        let parent_layer = *layer
            .get(&task_id)
            .ok_or_else(|| DagValidationError::MissingDependency(task_id.clone()))?;
        let downstream = tasks
            .get(&task_id)
            .map(|t| t.downstream.clone())
            .ok_or_else(|| DagValidationError::MissingDependency(task_id.clone()))?;

        for child_id in downstream {
            let child_layer = layer
                .get_mut(&child_id)
                .ok_or_else(|| DagValidationError::MissingDependency(child_id.clone()))?;
            *child_layer = (*child_layer).max(parent_layer + 1);

            let child_in_degree = in_degree
                .get_mut(&child_id)
                .ok_or_else(|| DagValidationError::MissingDependency(child_id.clone()))?;
            *child_in_degree -= 1;
            if *child_in_degree == 0 {
                queue.push_back(child_id);
            }
        }
    }

    if visited != tasks.len() {
        return Err(DagValidationError::CycleDetected);
    }

    let mut max_layer = 0u32;
    for (task_id, task) in tasks.iter_mut() {
        let assigned = *layer
            .get(task_id)
            .ok_or_else(|| DagValidationError::MissingDependency(task_id.clone()))?;
        task.layer = assigned;
        max_layer = max_layer.max(assigned);
    }

    Ok(max_layer + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Task;

    fn build_tasks(defs: Vec<(&str, Vec<&str>)>) -> HashMap<String, Task> {
        let mut tasks = HashMap::new();
        for (id, deps) in &defs {
            let task = Task {
                task_id: (*id).to_string(),
                task_type: format!("type_{id}"),
                depends_on: deps.iter().map(|d| (*d).to_string()).collect(),
                deps_remaining: deps.len(),
                ..Task::default()
            };
            tasks.insert((*id).to_string(), task);
        }

        let ids: Vec<String> = tasks.keys().cloned().collect();
        for id in ids {
            let deps = tasks[&id].depends_on.clone();
            for dep in deps {
                tasks
                    .get_mut(&dep)
                    .expect("dependency exists")
                    .downstream
                    .push(id.clone());
            }
        }

        tasks
    }

    #[test]
    fn assigns_layers_for_linear_chain() {
        let mut tasks = build_tasks(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let total_layers = assign_layers(&mut tasks).expect("assign layers");
        assert_eq!(total_layers, 3);
        assert_eq!(tasks["a"].layer, 0);
        assert_eq!(tasks["b"].layer, 1);
        assert_eq!(tasks["c"].layer, 2);
    }

    #[test]
    fn assigns_layers_for_diamond() {
        let mut tasks = build_tasks(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);
        let total_layers = assign_layers(&mut tasks).expect("assign layers");
        assert_eq!(total_layers, 3);
        assert_eq!(tasks["a"].layer, 0);
        assert_eq!(tasks["b"].layer, 1);
        assert_eq!(tasks["c"].layer, 1);
        assert_eq!(tasks["d"].layer, 2);
    }

    #[test]
    fn assigns_layers_for_wide_fan_out() {
        let mut tasks = build_tasks(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["a"]),
            ("e", vec!["a"]),
        ]);
        let total_layers = assign_layers(&mut tasks).expect("assign layers");
        assert_eq!(total_layers, 2);
        assert_eq!(tasks["a"].layer, 0);
        assert_eq!(tasks["b"].layer, 1);
        assert_eq!(tasks["c"].layer, 1);
        assert_eq!(tasks["d"].layer, 1);
        assert_eq!(tasks["e"].layer, 1);
    }

    #[test]
    fn max_parent_rule_applies() {
        let mut tasks = build_tasks(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b"]),
            ("e", vec!["c", "d"]),
        ]);
        let total_layers = assign_layers(&mut tasks).expect("assign layers");
        assert_eq!(total_layers, 4);
        assert_eq!(tasks["e"].layer, 3);
    }

    #[test]
    fn single_task_has_single_layer() {
        let mut tasks = build_tasks(vec![("a", vec![])]);
        let total_layers = assign_layers(&mut tasks).expect("assign layers");
        assert_eq!(total_layers, 1);
        assert_eq!(tasks["a"].layer, 0);
    }

    #[test]
    fn cycle_is_rejected() {
        let mut tasks = build_tasks(vec![("a", vec!["c"]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let err = assign_layers(&mut tasks).expect_err("cycle should fail");
        assert_eq!(err, DagValidationError::CycleDetected);
    }
}
