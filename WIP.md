## [Code] decision log (temp and too be deleted or documented elsewhere)


| Issue | Options Considered | Decision Made | Author |
|-------|--------------------|---------------|--------|
| argparse vs click | Considered using either an external package (click) or a built-in package (argparse). | Decided to use argparse. | Alho |
| Helpers Code | Discussed how to integrate code written outside of the 'nightcrawler' directory (i.e., helpers) into the nightcrawler project. | We will create a 'helpers' directory and ensure that any changes made there do not impact the production code. A pull request will be submitted to Nico/Alex for review. | Alho |
| Reusability of MediCrawl Code | Considered the potential reuse of code from the MediCrawl project. | Decided to "steal with pride" and reuse applicable code where beneficial. | Alho |
| Handling Context and Settings | Considered how to manage context and settings across different parts of the project. | Decided to have separate settings/context files in both the helpers and pipeline directories. The pipeline's context will include additional variables specific to it, while the pipeline's .env file will contain all environment variables, including those from helpers. | Alho |
