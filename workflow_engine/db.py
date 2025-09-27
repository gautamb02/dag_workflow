import motor.motor_asyncio
from pymongo import ReturnDocument
from workflow_engine.models import TaskModel, TaskState

class MongoDB:
    def __init__(self, MONGO_URI: str, DB_NAME: str):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.collection = self.db["workflows"]

    async def save_workflow(self, workflow: dict):
        # Ensure that if TaskModels are passed in, they are converted to simple dicts
        workflow_to_save = {
            "name": workflow["name"],
            "version": workflow["version"],
            "tasks": [
                # Use simple dict access if available, otherwise check if it's a TaskModel
                # This logic is mainly to handle the initial registration where tasks are TaskModel objects
                {
                    "id": t.id if isinstance(t, TaskModel) else t["id"],
                    "dependencies": t.dependencies if isinstance(t, TaskModel) else t.get("dependencies", []),
                    "runtime": t.runtime if isinstance(t, TaskModel) else t.get("runtime", 1),
                    "dynamic": t.dynamic if isinstance(t, TaskModel) else t.get("dynamic", False),
                    # Ensure state is saved as a string (PENDING, RUNNING, etc.)
                    "state": t.state.value if isinstance(t, TaskModel) else t.get("state", TaskState.PENDING.value),
                }
                for t in workflow["tasks"]
            ]
        }
        await self.collection.update_one(
            {"name": workflow_to_save["name"], "version": workflow_to_save["version"]},
            {"$set": workflow_to_save},
            upsert=True
        )

    async def get_workflow_by_name(self, name: str) -> dict | None:
        cursor = self.collection.find({"name": name}).sort("version", -1).limit(1)
        async for wf in cursor:
            return wf
        return None

    async def get_workflow_by_version(self, version: str) -> dict | None:
        return await self.collection.find_one({"version": version})

    async def update_task_state(self, version: str, task_id: str, state: str):
        """Updates a specific task's state in the database."""
        # state is already expected to be a string here
        await self.collection.update_one(
            {"version": version, "tasks.id": task_id},
            {"$set": {"tasks.$.state": state}}
        )

    async def add_runtime_tasks(self, version: str, tasks: list[dict]) -> dict:
        """Adds a list of tasks (expected to be simple dicts) to the workflow."""
        # Tasks are already expected to be simple dicts with string states here
        return await self.collection.find_one_and_update(
            {"version": version},
            {"$push": {"tasks": {"$each": tasks}}},
            return_document=ReturnDocument.AFTER,
        )

    async def get_all_workflows(self) -> list[dict]:
        cursor = self.collection.find({})
        return [wf async for wf in cursor]