from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime

# Définition du YAML (votre code)
spark_app_yaml = """
On met ici, le contenu de son fichier yaml pour spark-operator
"""

with DAG(
    dag_id='spark_operator_pi_test',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    # LanceMENT l'application Spark
    submit_spark = SparkKubernetesOperator(
        task_id='submit_spark_pi',
        namespace="on met le meme namespace que celui du contenu yaml ci-dessus",
        application_file=spark_app_yaml,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )

    # Surveiller l'exécution (Sensor)
    # Le Sensor permet au DAG d'attendre que le job Spark soit fini (Success ou Fail)
    monitor_spark = SparkKubernetesSensor(
        task_id='monitor_spark_pi',
        namespace="on met le meme namespace que celui du contenu yaml ci-dessus",
        application_name="{{ task_instance.xcom_pull(task_ids='submit_spark_pi')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    submit_spark >> monitor_spark
