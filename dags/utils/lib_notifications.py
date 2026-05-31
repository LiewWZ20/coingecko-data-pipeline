from airflow.utils.email import send_email


def notify_failure(context):
    """Send email alert on task failure."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    run_id = context["run_id"]

    subject = f"❌ Airflow Alert: {dag_id}.{task_id} failed"
    body = f"""
    <h3>Pipeline Failure Alert</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Run ID:</b> {run_id}</p>
    <p><b>Logs:</b> <a href="{log_url}">View logs</a></p>
    """
    send_email(
        to="wzliew20@gmail.com",
        subject=subject,
        html_content=body,
        conn_id="smtp_gmail",
    )