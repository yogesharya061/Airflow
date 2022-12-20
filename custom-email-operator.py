"""
Dag info : custom-email-operator
--------
This dag is to show how to use custom operator created by extending Airflow EmailOperator.
"""
from datetime import datetime
from airflow import DAG
from util.email_send_operator.py import EmailOperatorRefined

# DAG declaration
default_args = {
    "depends_on_past": False,
    "catchup": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
    "on_failure_callback": dag_failure_alert,
    "execution_timeout": timedelta(hours=24),
}

dag = DAG(
    'custom-email-operator',
    default_args=default_args,
    start_date=datetime(2022, 12, 20),
    schedule_interval='0 2 * * *',
    catchup=False,
    concurrency=1,
    doc_md=__doc__,
    tags=['python']
)

# Task to send email
send_email = EmailOperatorRefined(
    task_id='send-email',
    dag=dag,
    to=['yogesh.ku061@gmail.com', 'yogesh.arya89@gmail.com'],
    subject='Test Email',
    html_content="""
        <p>Hi Team,</p>
        <p>This is just a test email. Please ignore.</p>
        <p>Thanks,
        <br>Yogesh Kumar</p>
        <p style="color:red; style="font-size:8px">This is an automated e-mail alert. Please do not reply.</p>
        """,
    dont_send_empty_email=True
)
