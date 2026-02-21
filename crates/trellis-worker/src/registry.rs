use std::collections::HashMap;
use std::sync::Arc;
use trellis_core::TaskError;

pub type TaskHandler = Arc<dyn Fn(InputMap) -> Result<serde_json::Value, TaskError> + Send + Sync>;
pub type InputMap = HashMap<String, serde_json::Value>;
pub type LifecycleHook = Arc<dyn Fn() + Send + Sync>;

#[derive(Default)]
pub struct HandlerRegistry {
    handlers: HashMap<String, TaskHandler>,
    fallback: Option<TaskHandler>,
    on_worker_start: Option<LifecycleHook>,
    on_worker_stop: Option<LifecycleHook>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, task_type: &str, handler: TaskHandler) {
        self.handlers.insert(task_type.to_string(), handler);
    }

    pub fn register_fallback(&mut self, handler: TaskHandler) {
        self.fallback = Some(handler);
    }

    pub fn task_types(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    pub fn get(&self, task_type: &str) -> Option<&TaskHandler> {
        self.handlers.get(task_type).or(self.fallback.as_ref())
    }

    pub fn set_on_worker_start(&mut self, hook: LifecycleHook) {
        self.on_worker_start = Some(hook);
    }

    pub fn set_on_worker_stop(&mut self, hook: LifecycleHook) {
        self.on_worker_stop = Some(hook);
    }

    pub fn on_worker_start(&self) {
        if let Some(hook) = &self.on_worker_start {
            hook();
        }
    }

    pub fn on_worker_stop(&self) {
        if let Some(hook) = &self.on_worker_stop {
            hook();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn registry_registers_and_looks_up_handlers() {
        let mut registry = HandlerRegistry::new();
        let handler: TaskHandler = Arc::new(|_| Ok(serde_json::json!({"ok": true})));
        registry.register("extract", Arc::clone(&handler));

        assert!(registry.get("extract").is_some());
        assert!(registry.get("missing").is_none());
    }

    #[test]
    fn task_types_include_only_explicit_handlers() {
        let mut registry = HandlerRegistry::new();
        registry.register("a", Arc::new(|_| Ok(serde_json::json!({}))));
        registry.register("b", Arc::new(|_| Ok(serde_json::json!({}))));
        registry.register_fallback(Arc::new(|_| Ok(serde_json::json!({"fallback": true}))));

        let mut types = registry.task_types();
        types.sort();
        assert_eq!(types, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn specific_handler_takes_precedence_over_fallback() {
        let mut registry = HandlerRegistry::new();
        registry.register_fallback(Arc::new(|_| {
            Ok(serde_json::json!({"selected": "fallback"}))
        }));
        registry.register(
            "extract",
            Arc::new(|_| Ok(serde_json::json!({"selected": "specific"}))),
        );

        let handler = registry.get("extract").expect("specific handler");
        let output = handler(HashMap::new()).expect("invoke handler");
        assert_eq!(output["selected"], serde_json::json!("specific"));
    }

    #[test]
    fn lifecycle_hooks_are_called_once() {
        let mut registry = HandlerRegistry::new();
        let start_count = Arc::new(AtomicUsize::new(0));
        let stop_count = Arc::new(AtomicUsize::new(0));

        {
            let start_count = Arc::clone(&start_count);
            registry.set_on_worker_start(Arc::new(move || {
                start_count.fetch_add(1, Ordering::SeqCst);
            }));
        }

        {
            let stop_count = Arc::clone(&stop_count);
            registry.set_on_worker_stop(Arc::new(move || {
                stop_count.fetch_add(1, Ordering::SeqCst);
            }));
        }

        registry.on_worker_start();
        registry.on_worker_stop();

        assert_eq!(start_count.load(Ordering::SeqCst), 1);
        assert_eq!(stop_count.load(Ordering::SeqCst), 1);
    }
}
