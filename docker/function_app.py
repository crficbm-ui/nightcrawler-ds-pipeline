import azure.functions as func
import azure.durable_functions as df

import json
import logging
import os
import nightcrawler.cli.main

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# Triggers
## HTTP
@app.route(route="orchestrators/pipeline_orchestrator")
@app.durable_client_input(client_name="client")
async def pipeline_start(req: func.HttpRequest, client):
    try:
        req_data = {
            "keyword": req.params.get("keyword"),
            "country": req.params.get("country", "CH"),
            "step": req.params.get("step", "fullrun"),
        }

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
    input_context = context.get_input()
    req_data = {
        "keyword": input_context.get("keyword"),
        "country": input_context.get("country", "CH"),
        "step": input_context.get("step", "fullrun"),
    }

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
    keyword = query.get("keyword")
    country = query.get("country", "CH")
    step = query.get("step", "fullrun")

    if keyword == "ALL":
        os.environ["NIGHTCRAWLER_USE_FILE_STORAGE"] = "false"
        os.environ["NIGHTCRAWLER_STORE_INTERMEDIATE"] = "false"
        from libnightcrawler.context import Context
        context = Context()
        requests = context.get_crawl_requests()
        if not requests:
            logging.warning("No requests found in database")
            return
        for request in requests:
            logging.info(f'Running pipeline for keyword `{request.keyword_value}\' for organization {request.organization.name}')
            nightcrawler.cli.main.run(["fullrun", request.keyword_value, '--org', request.organization.name, "--keyword-id", str(request.keyword_id), "--case-id", str(request.case_id), "-n", "5"])
    else:
        logging.info(f'Running pipeline for keyword `{keyword}\' for country {country}')
        nightcrawler.cli.main.run( [step, keyword, f'--country={country}'] )

    logging.info('Pipeline terminated successfully')
