import functions_framework
from google.cloud import tasks_v2
import time
import json
import os

@functions_framework.http
def windower_sub_1_trigger(request):
    project_id = os.getenv("GCP_PROJECT")
    project_region = os.getenv("GCP_REGION")
    queue_id = os.getenv("TASKS_QUEUE")
    windower_url = os.getenv("WINDOWER_URL")

    client = tasks_v2.CloudTasksClient()
    queue_path = client.queue_path(project_id, project_region, queue_id)

    current_time = int(time.time())

    # Schedule 12 tasks, each delayed by 5 seconds from the previous one
    # Since  service is called every 60 seconds, this will cover the entire lifecycle
    for i in range(12):
        delay = i * 5
        execution_time = current_time + delay

        # Create the task dictionary exactly like the basic GCP documentation examples
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": windower_url,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"step": i, "delay": delay}).encode("utf-8")
            },
            "schedule_time": {
                "seconds": execution_time
            }
        }
        try:
            client.create_task(request={"parent": queue_path, "task": task})
            human_readable_time = time.ctime(execution_time)
            print(f"[Pinger] Sent task {i} scheduled for {human_readable_time}")
        except Exception as e:
            print(f"[Pinger] Failed to create task {i}: {e}")

    return "All tasks succesfully triggered", 200