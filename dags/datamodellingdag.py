from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from pathlib import Path
from datetime import datetime






DAG_FILE_PATH = (Path(__file__).resolve()).parent
AIRFLOW_PATH=DAG_FILE_PATH.parent
DBT_PATH="/zeustra_realestate_dbt"
SPLINK_PATH="/splink_scripts"

zeustra_dag=DAG( dag_id='automated_ELT_dag',
                start_date=datetime(2025,1,1),
                schedule=None,
                catchup=False,

    
                )

prepare_tables=BashOperator(task_id="extract_raw_reonomy_and_dbusa_tables_and_prepare_splink_serving_models",
                                  bash_command=f"""
        echo "Running dbt raw models..."
        cd {DBT_PATH}
        dbt run --select models/raw
        echo "Running dbt  intermediate_models..."
        dbt run --select models/intermediate
        echo "Running dbt  splink_serving_models..."
        dbt run --select models/splink_serving



        """,
        dag=zeustra_dag
                                )






linkage_jobs=BashOperator(task_id="perform_linking_and_deduplication",
                                  bash_command=f"""
        
        cd {AIRFLOW_PATH}
        cd {SPLINK_PATH}
        echo "Running properties linkage_and_deduplication..."
        python properties_matching_deduplication.py        
        echo "Running tenants linkage_and_deduplication..."
        python tenants_matching_deduplication.py


        """,
        dag=zeustra_dag
                                )






building_mart_models=BashOperator(task_id="building_final_dims_and_facts_tables",
                                  bash_command=f"""
        cd {AIRFLOW_PATH}
        cd {DBT_PATH}
        dbt seed
        echo "creating dim models..."
        dbt run --select models/marts/dims
        echo "creating fact models..."
        dbt run --select models/marts/facts
        echo "Done"
        


        """,
        dag=zeustra_dag
                                )

prepare_tables>>linkage_jobs>>building_mart_models

