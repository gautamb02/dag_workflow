import asyncio
from collections import defaultdict
from typing import List
from .db import MongoDB
from .models import TaskModel, TaskState
from config.models import MongoDBConfig

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkflowEngine:
    def __init__(self, conn: MongoDBConfig):
        self.db = MongoDB(conn.uri, conn.db)
        self.task_graph = defaultdict(dict)  
        self.dependency_graph = defaultdict(lambda: defaultdict(set)) 
        self.lock = asyncio.Lock()
        self.running_versions = set()

    async def register_workflow(self, workflow: dict):
        tasks_as_dicts = []
        for t in workflow["tasks"]:
            if isinstance(t, TaskModel):
                tasks_as_dicts.append(t.model_dump())
            else:
                tasks_as_dicts.append(t)
        
        workflow["tasks"] = tasks_as_dicts
        
        await self.db.save_workflow(workflow)
        
        version = workflow["version"]
        
        self.task_graph[version] = {t["id"]: t for t in workflow["tasks"]}
        
        for t in workflow["tasks"]:
            for dep in t["dependencies"]:
                self.dependency_graph[version][dep].add(t["id"])

    async def add_runtime_workflow(self, workflow):
        workflow_dict = workflow if isinstance(workflow, dict) else workflow.model_dump()
        await self.register_workflow(workflow_dict)
        logger.info(f"[{workflow_dict['version']}] Workflow added: {workflow_dict['name']}")
        
        asyncio.create_task(self.execute_workflow_by_name(workflow_dict["name"]))

    async def execute_workflow(self, version: str):
        if version in self.running_versions:
            logger.info(f"[{version}] Already running")
            return
            
        self.running_versions.add(version)
        workflow = await self.db.get_workflow_by_version(version)
        if not workflow:
            logger.error(f"Workflow {version} not found")
            self.running_versions.remove(version)
            return

        self.task_graph[version] = {t["id"]: t for t in workflow["tasks"]}
        
        logger.info(f"Executing workflow {workflow['name']} ({version})")
        
        pending = {t["id"] for t in self.task_graph[version].values() if t["state"] == TaskState.PENDING.value}
        running = {t["id"] for t in self.task_graph[version].values() if t["state"] == TaskState.RUNNING.value}

        while pending or running:
            
            ready = [
                self.task_graph[version][tid]
                for tid in list(pending)
                if all(self.task_graph[version].get(dep, {}).get("state") == TaskState.SUCCESS.value
                       for dep in self.task_graph[version][tid].get("dependencies", []))
            ]

            tasks = []
            for task in ready:
                pending.remove(task["id"])
                running.add(task["id"])
                tasks.append(self._run_task(version, task, running))

            if tasks:
                await asyncio.gather(*tasks)
            else:
                if not running:
                    await asyncio.sleep(0.5)
                else:
                    await asyncio.sleep(0.1)

            pending = {t["id"] for t in self.task_graph[version].values() if t["state"] == TaskState.PENDING.value}
            running = {t["id"] for t in self.task_graph[version].values() if t["state"] == TaskState.RUNNING.value}


        logger.info(f"Workflow {version} finished")
        self.running_versions.remove(version)

    async def execute_workflow_by_name(self, name: str):
        wf_data = await self.db.get_workflow_by_name(name)
        if not wf_data:
            logger.error(f"Workflow {name} not found")
            return
        await self.execute_workflow(wf_data["version"])

    async def _run_task(self, version: str, task: dict, running: set):
        task_id = task["id"]
        logger.info(f"[{version}] Starting task: {task_id}")
        
        # 1. Update state locally and in DB to RUNNING
        task["state"] = TaskState.RUNNING.value
        await self.db.update_task_state(version, task_id, task["state"]) 

        # Simulate task execution
        await asyncio.sleep(task["runtime"])

        # 2. Update state locally and in DB to SUCCESS
        task["state"] = TaskState.SUCCESS.value
        await self.db.update_task_state(version, task_id, task["state"])
        logger.info(f"[{version}] Completed task: {task_id}")
        
        # 3. Remove from running set
        running.discard(task_id) # Use discard in case it was removed by another process/function

        if task.get("dynamic"):
            new_tasks = [
                {"id": f"{task_id}_validate_1", "dependencies": [task_id], "runtime": 1, "dynamic": False, "state": TaskState.PENDING.value},
                {"id": f"{task_id}_validate_2", "dependencies": [task_id], "runtime": 1, "dynamic": False, "state": TaskState.PENDING.value},
            ]
            await self.add_runtime_tasks(version, new_tasks)

    async def add_runtime_tasks(self, version: str, tasks: list[dict]):
        async with self.lock:
            # tasks are already simple dicts with string states
            await self.db.add_runtime_tasks(version, tasks) 
            for t in tasks:
                self.task_graph[version][t["id"]] = t
                for dep in t["dependencies"]:
                    self.dependency_graph[version][dep].add(t["id"])
            logger.info(f"[{version}] Added runtime tasks: {[t['id'] for t in tasks]}")

    async def monitor_and_resume(self, interval: int = 5):
        while True:
            workflows = await self.db.get_all_workflows()
            for wf_data in workflows:
                version = wf_data["version"]
                # CRITICAL: Check for any incomplete task state (PENDING, RUNNING, FAILED)
                incomplete = [t for t in wf_data["tasks"] if t["state"] != TaskState.SUCCESS.value]
                
                if incomplete and version not in self.running_versions:
                    self.task_graph[version] = {t["id"]: t for t in wf_data["tasks"]}
                    for t in wf_data["tasks"]:
                        for dep in t.get("dependencies", []): # Use .get for robustness
                            self.dependency_graph[version][dep].add(t["id"])
                    # self.register_workflow(wf_data)
                    logger.info(f"[AUTO-RESUME] Starting workflow {wf_data['name']} ({version})")
                    asyncio.create_task(self.execute_workflow(version))
            await asyncio.sleep(interval)