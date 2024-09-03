import azure.functions as func
import azure.durable_functions as df

import datetime
import json
import logging
import nightcrawler.cli.main

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def pipeline_start(req: func.HttpRequest, client):
    function_name = req.route_params.get('functionName')
    instance_id = await client.start_new(function_name)
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrator
@app.orchestration_trigger(context_name="context")
def hello_orchestrator(context):
    logging.info('Python HTTP trigger function processed a request.')
    logging.info(f'context: {context}')

    status = yield context.call_activity("hello", "In progress")

    keyword = req.params.get('keyword')
    country = req.params.get('country')
    step = req.params.get('step')

    if not country:
      country = 'CH'

    if not step:
      step = 'fullrun'

    if not keyword:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            keyword = req_body.get('keyword')

    logging.info(f'Keyword is {keyword} for country {country}')

    try:
      nightcrawler.cli.main.run( [step, keyword, f'--country={country}'] )
      status = yield context.call_activity("hello", "Finished")
    except Exception as e:
      import traceback
      logging.error(traceback.format_exc())
      status = yield context.call_activity("hello", "Failed")

    return [status]

# Activity
@app.activity_trigger(input_name="status")
def hello(status: str):
  return f'status: {status}'
