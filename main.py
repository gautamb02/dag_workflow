from config.reader import load_config
from config.models import Error 

import asyncio
from workflow_engine.engine import WorkflowEngine
from workflow_engine.models import WorkflowModel, TaskModel, TaskState
from config.models import MongoDBConfig, Config 


cfg : Config | None = None


async def main():
    global cfg
    engine = WorkflowEngine(cfg.db.mongodb.dag_workflow) 

    asyncio.create_task(engine.monitor_and_resume(interval=5))

    workflow_v1 = WorkflowModel(
        version= "v1",
        name= "data_pipeline",
        tasks= [
            TaskModel(id="fetch_data", runtime=2),
            TaskModel(id="clean_data", dependencies=["fetch_data"], runtime=5, dynamic=True),
            TaskModel(id="log_result", dependencies=["clean_data"], runtime=1),
        ]
    )

    workflow_v2 = WorkflowModel(
        version= "v2",
        name = "etl_pipeline",
        tasks= [
            TaskModel(id="extract", runtime=2),
            TaskModel(id="transform", dependencies=["extract"], runtime=2),
            TaskModel(id="load", dependencies=["transform"], runtime=1),
        ]
    )

    await engine.add_runtime_workflow(workflow_v1.model_dump()) 

    await engine.add_runtime_workflow(workflow_v2.model_dump())

    async def add_runtime():
        await asyncio.sleep(5)
        
        workflow_v3 = {
            "version": "v3",
            "name": "runtime_pipeline",
            "tasks": [
                TaskModel(id="start", runtime=2).model_dump(),
                TaskModel(id="process", dependencies=["start"], runtime=3).model_dump(),
                TaskModel(id="finish", dependencies=["process"], runtime=1).model_dump(),
            ]
        }
        await engine.add_runtime_workflow(workflow_v3)

    asyncio.create_task(add_runtime())

    while True:
        await asyncio.sleep(1)

def loadConfig():
    global cfg
    cfg = load_config("./config.yml")
    if isinstance(cfg, Error):
        return False  
    return True  

if __name__ == "__main__":
    if loadConfig():
        asyncio.run(main())
    else:
        print("Couldn't read config")