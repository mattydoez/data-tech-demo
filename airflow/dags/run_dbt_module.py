import json
from airflow.operators.bash import BashOperator

DBT_PATH = '/opt/airflow/dbt'
DBT_BIN = '/home/airflow/.local/bin/dbt'

def load_manifest():
    local_filepath = f"{DBT_PATH}/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data

def make_dbt_task(dag, node, dbt_verb, schema):
    """Returns an Airflow operator to run and test an individual model"""
    DBT_DIR = DBT_PATH
    GLOBAL_CLI_FLAGS = "--no-write-json"
    model = node.split(".")[-1]

    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node,
            bash_command=f"""
            export DBT_SCHEMA={schema} &&
            cd {DBT_DIR} &&
            {DBT_BIN} {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
            """,
            dag=dag,
        )
    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_test,
            bash_command=f"""
            export DBT_SCHEMA={schema} &&
            cd {DBT_DIR} &&
            {DBT_BIN} {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
            """,
            dag=dag,
        )

    return dbt_task

def create_dbt_tasks(dag, models_to_run, schema):
    data = load_manifest()
    dbt_tasks = {}
    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            model_name = node.split(".")[-1]
            if not models_to_run or model_name in models_to_run:
                node_test = node.replace("model", "test")

                dbt_tasks[node] = make_dbt_task(dag, node, "run", schema)
                dbt_tasks[node_test] = make_dbt_task(dag, node, "test", schema)

    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            model_name = node.split(".")[-1]
            if not models_to_run or model_name in models_to_run:
                # Set dependency to run tests on a model after model runs finishes
                node_test = node.replace("model", "test")
                dbt_tasks[node] >> dbt_tasks[node_test]

                # Set all model -> model dependencies
                for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
                    upstream_node_type = upstream_node.split(".")[0]
                    if upstream_node_type == "model" and (not models_to_run or upstream_node.split(".")[-1] in models_to_run):
                        dbt_tasks[upstream_node] >> dbt_tasks[node]

    return dbt_tasks