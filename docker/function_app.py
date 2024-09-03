import azure.functions as func
import azure.durable_functions as df

import datetime
import json
import logging
import nightcrawler.cli.main

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="orchestrators/pipeline_orchestrator")
@app.durable_client_input(client_name="client")
async def pipeline_start(req: func.HttpRequest, client):
    try:
      logging.info(f'Trigger pipeline')
      function_name = "pipeline_orchestrator"

      req_data = { 
        'keyword': req.params.get('keyword'),
        'country': req.params.get('country','CH'),
        'step':  req.params.get('step','fullrun')
      }

      instance_id = await client.start_new(function_name, None, req_data)
      response = client.create_check_status_response(req, instance_id)
    except Exception as e:
      logging.error(e, exc_info=True) 
    return response

# Orchestrator
@app.orchestration_trigger(context_name="context")
def pipeline_orchestrator(context):
    logging.info('Pipeline started')

    status = yield context.call_activity("pipeline_status", "Initialize")

    input_context = context.get_input()
    keyword = input_context.get('keyword')
    country = input_context.get('country','CH')
    step = input_context.get('step','fullrun')

    logging.info(f'Running pipeline for keyword `{keyword}\' for country {country}')
    status = yield context.call_activity("pipeline_status", "In progress")

    try:
      nightcrawler.cli.main.run( [step, keyword, f'--country={country}'] )
      status = yield context.call_activity("pipeline_status", "Completed")
    except Exception as e:
      logging.error(e, exc_info=True) 
      status = yield context.call_activity("pipeline_status", f'Failed: {e}')

    return [status]

# Activity
@app.activity_trigger(input_name="status")
def pipeline_status(status: str):
  return f'status: {status}'
