## [Code] decision log (temp and too be deleted or documented elsewhere)

1. argparse vs click -> we go with argparse
2. how do we bring code that was writen outside of the 'nightcralwer' dir (i.e. helpers) into nc?
    - we will use the 'helpers' dir and make sure that whenever a change is done in that dir, it does not affect the prod. code (PR to Nico / Alex)
3. Reusability of MediCrawl code
    - "steal with pride"
4. What to do with with context and settings?
    - we will have a settings / context in the helpers AND in the pipeline. The one in the pipeline will add new variables that will only be used in the pipeline. The .env file in the pipeline, however, will keep all env-variables, also those from the helpers.
