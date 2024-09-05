import azure.functions as func
import azure.durable_functions as df

import logging
import nightcrawler.cli.main

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="orchestrators/pipeline_orchestrator")
@app.durable_client_input(client_name="client")
async def pipeline_start(req: func.HttpRequest, client):
    try:
      req_data = {
        'keyword': req.params.get('keyword'),
        'country': req.params.get('country','CH'),
        'step':  req.params.get('step','fullrun')
      }

      instance_id = await client.start_new("pipeline_orchestrator", None, req_data)
      logging.info(f'Trigger pipeline with instance id {instance_id}')
      response = client.create_check_status_response(req, instance_id)
    except Exception as e:
      logging.error(e, exc_info=True)
    return response

# Orchestrator
@app.orchestration_trigger(context_name="context")
def pipeline_orchestrator(context: df.DurableOrchestrationContext):
    logging.info('Pipeline started')

    input_context = context.get_input()
    req_data = {
      'keyword': input_context.get('keyword'),
      'country': input_context.get('country','CH'),
      'step': input_context.get('step','fullrun')
    }

    logging.info(f'Running pipeline for keyword `{req_data["keyword"]}\' for country {req_data["country"]}')
    status = yield context.call_activity("pipeline_work", req_data)

    return [status]

# Activity
@app.activity_trigger(input_name="query")
def pipeline_work(query: dict):
    keyword = query.get('keyword')
    country = query.get('country','CH')
    step = query.get('step','fullrun')

    try:
      nightcrawler.cli.main.run( [step, keyword, f'--country={country}'] )
    except Exception as e:
      logging.error(e, exc_info=True)

    return 'OK'
