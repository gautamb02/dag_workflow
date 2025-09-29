# Async Workflow Engine

A simple, asynchronous workflow engine built in Python using **asyncio**, **Pydantic** for models, and **Motor** for non-blocking MongoDB access.

This engine is designed to execute directed acyclic graphs (DAGs) of tasks, manage dependencies, support dynamic task generation at runtime, and automatically resume failed or interrupted workflows upon restart.

## Features

-   **Asynchronous Execution:** Leverages `asyncio` for high concurrency and non-blocking I/O.
    
-   **Dependency Management:** Tasks are only executed after all their dependencies have successfully completed.
    
-   **State Persistence:** Uses MongoDB to store workflow and task states, ensuring durability.
    
-   **Auto-Resume:** A built-in monitor automatically detects and resumes incomplete workflows upon engine startup or during operation.
    
-   **Dynamic Tasks:** Supports tasks that can introduce new, dependent tasks into the running workflow at completion.
    
-   **Model Validation:** Uses Pydantic for clean, type-hinted data models for workflows and tasks.

## Models

| Model          | Description              | Key Fields                                                     |
|----------------|--------------------------|----------------------------------------------------------------|
| **TaskState**  | Enum for task lifecycle. | `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`                     |
| **TaskModel**  | Defines a single unit of work. | `id`, `dependencies` (list of task IDs), `runtime` (seconds), `dynamic` (bool), `state` |
| **WorkflowModel** | A collection of tasks and metadata. | `version`, `name`, `tasks` (List of `TaskModel`)              |


## Architecture Overview

### 1. `MongoDB` (Database Layer)

-   Manages all interactions with the MongoDB database using `motor.motor_asyncio` for asynchronous operations.
    
-   Key methods include `save_workflow`, `get_workflow_by_name`, `update_task_state`, and `add_runtime_tasks`.
    
-   Stores workflow definitions and their current task states (e.g., `PENDING`, `RUNNING`).
    

### 2. `WorkflowEngine` (Core Logic)

The heart of the system, responsible for scheduling and execution.

-   **`register_workflow(workflow: dict)`:** Stores a new workflow definition in the database and initializes local in-memory graphs:
    
    -   `task_graph`: Maps `version -> task_id -> task_data`.
        
    -   `dependency_graph`: Maps `version -> dependency_id -> set_of_dependent_task_ids`.
        
-   **`execute_workflow(version: str)`:** The main loop that drives the workflow execution:
    
    1.  Identifies **ready** tasks (all dependencies are `SUCCESS`).
        
    2.  Runs ready tasks concurrently using `asyncio.gather`.
        
    3.  Sleeps briefly when no tasks are ready but the workflow is not complete.
        
-   **`_run_task(...)`:** Simulates task execution, updating its state to `RUNNING` then `SUCCESS` in both the local graph and the database. This method is also responsible for triggering **dynamic task** creation.
    
-   **`add_runtime_tasks(...)`:** Atomically updates the database and the in-memory graphs to incorporate new tasks generated during execution.
    
-   **`monitor_and_resume(interval: int)`:** A long-running background task that periodically queries the database for workflows that have incomplete tasks (state is _not_ `SUCCESS`) and resumes their execution if they are not already running.

## Getting Started

### Prerequisites

1.  **Python 3.10+**
    
2.  **MongoDB Instance** (Local or remote URI)
    
3.  Install dependencies:
    
    Bash
    
    ```
    uv add pydantic motor asyncio
    
    ```
    
    _(Note: You'll also need a configuration setup to load `config.yml`.)_
    

### Example Usage

The provided `main` function demonstrates how to register and start workflows:



```python
# from main.py

async def main():
    # ... setup and configuration loading ...
    engine = WorkflowEngine(cfg.db.mongodb.dag_workflow) 

    # Start the auto-resume monitor
    asyncio.create_task(engine.monitor_and_resume(interval=5))

    # Define a workflow with a dynamic task
    workflow_v1 = WorkflowModel(
        version= "v1",
        name= "data_pipeline",
        tasks= [
            TaskModel(id="fetch_data", runtime=2),
            TaskModel(id="clean_data", dependencies=["fetch_data"], runtime=5, dynamic=True), # Dynamic Task
            TaskModel(id="log_result", dependencies=["clean_data"], runtime=1),
        ]
    )

    # Register and start the execution (kicks off execute_workflow_by_name)
    await engine.add_runtime_workflow(workflow_v1.model_dump()) 
    
    # ... other workflows and runtime task additions ...
    
    # Keep the main loop running
    while True:
        await asyncio.sleep(1)


```

### Dynamic Task Execution

When a task with `dynamic=True` completes, it adds new tasks to the workflow. In the provided code, the `clean_data` task in `v1` will add two new tasks upon successful completion:



```python
# Inside _run_task() when task.get("dynamic") is True

new_tasks = [
    {"id": f"{task_id}_validate_1", "dependencies": [task_id], ...},
    {"id": f"{task_id}_validate_2", "dependencies": [task_id], ...},
]
await self.add_runtime_tasks(version, new_tasks)

# The WorkflowEngine will then detect these new PENDING tasks and schedule them 
# once their dependency (the dynamic task itself) is SUCCESS.
```
