import azure.functions as func
import datetime
import json
import logging
import nightcrawler.cli.main

app = func.FunctionApp()

@app.route(route="NcPipeline", auth_level=func.AuthLevel.FUNCTION)
def NcPipeline(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

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
    except Exception as e:
      import traceback
      logging.error(traceback.format_exc())
      return func.HttpResponse(
         e,
         status_code=500
      )

    return func.HttpResponse(
       "OK",
       status_code=200
    )
