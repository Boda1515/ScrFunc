# HttpStarter
import logging

from azure.functions import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient


async def main(req: HttpRequest, starter: str) -> HttpResponse:
    # Create a DurableOrchestrationClient
    client = DurableOrchestrationClient(starter)
    try:
        req_data = req.get_json()
    except:
        req_data = dict(req.params)

    start_url = req_data.get("start_url")
    region = req_data.get("region")
    max_pages = req_data.get("max_pages")
    if not start_url or not region:
        return HttpResponse(
            "Please pass both start_url and region in the request body",
            status_code=400
        )

    # Start the orchestration
    instance_id = await client.start_new("Orchest", None, {
        "start_url": start_url,
        "region": region,
        "max_pages": max_pages
    })

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)
