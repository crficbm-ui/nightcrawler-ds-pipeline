import azure.functions as func
import azure.durable_functions as df

import json
import logging
import os
import gc
import nightcrawler.cli.main
import nightcrawler.cli.full_pipeline
from libnightcrawler.context import Context

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# Triggers
## HTTP
@app.route(route="orchestrators/pipeline_orchestrator")
@app.durable_client_input(client_name="client")
async def pipeline_start(req: func.HttpRequest, client):
    try:
        req_data = {x: req.params.get(x) for x in req.params.keys()}
        instance_id = await client.start_new("pipeline_orchestrator", None, req_data)
        logging.info(f"Trigger pipeline with instance id {instance_id}")
        response = client.create_check_status_response(req, instance_id)
    except Exception as e:
        logging.error(e, exc_info=True)
    return response


## Message Queue
@app.function_name(name="ServiceBusQueueKeyword")
@app.service_bus_queue_trigger(
    arg_name="msg", queue_name="run-keyword", connection="ServiceBus"
)
def sb_pipeline_start(msg: func.ServiceBusMessage):
    logging.info("Python ServiceBus queue trigger processed message")
    try:
        req_data = json.loads(msg.get_body().decode("utf-8"))
        pipeline_wrapper(req_data)
    except Exception as e:
        logging.error(e, exc_info=True)


# Orchestrator
@app.orchestration_trigger(context_name="context")
def pipeline_orchestrator(context: df.DurableOrchestrationContext):
    params = context.get_input()
    req_data = {x: params.get(x) for x in params.keys()}
    status = yield context.call_activity("pipeline_work", req_data)

    return [status]


# Activity
@app.activity_trigger(input_name="query")
def pipeline_work(query: dict):
    try:
        pipeline_wrapper(query)
    except Exception as e:
        logging.error(e, exc_info=True)
        return f"Failed: {e}"

    return "Success"


def pipeline_wrapper(query):
    os.environ["NIGHTCRAWLER_USE_FILE_STORAGE"] = "false"
    context = Context()

    requests = context.get_crawl_requests(case_id=int(query.get("case_id", 0)))

    if not requests:
        logging.warning("No requests found in database")
        return
    for request in requests:
        logging.info(f'Running pipeline for keyword `{request.keyword_value}\' for organization {request.organization.name}')
        # TODO: remove next line when fixed
        request.number_of_results = 5
        try:
            nightcrawler.cli.full_pipeline.handle_request(context, request)
        except Exception as e:
            logging.critical(e, exc_info=True)
            context.set_crawl_error(request.case_id, request.keyword_id, str(e))
        gc.collect()
    logging.info('Pipeline terminated successfully')
