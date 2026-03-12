from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Hata durumunda ne yapılacağı gibi varsayılan ayarlar
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG (İş Akışı) Tanımlaması
with DAG(
    'daily_ecommerce_etl_pipeline',
    default_args=default_args,
    description='PySpark ETL sürecini yöneten günlük Airflow akışı',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Adım 1: Pipeline Başlangıcı
    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # Adım 2: Gerekli verilerin hazır olup olmadığını kontrol et (Temsili)
    check_data_ready = BashOperator(
        task_id='check_raw_data',
        bash_command='echo "Ham veriler kontrol ediliyor..." && sleep 2'
    )

    # Adım 3: PySpark ETL scriptini çalıştır
    run_spark_etl = BashOperator(
        task_id='run_pyspark_etl',
        bash_command='python /opt/airflow/dags/scripts/spark_etl_processor.py'
    )

    # Adım 4: İşlem bittiğinde log yazdır
    log_completion = BashOperator(
        task_id='log_completion',
        bash_command='echo "ETL süreci başarıyla tamamlandı ve Parquet dosyası oluşturuldu."'
    )

    # Adım 5: Pipeline Bitişi
    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # Görevlerin çalışma sırasını (Dependency) belirleme
    start_pipeline >> check_data_ready >> run_spark_etl >> log_completion >> end_pipeline
