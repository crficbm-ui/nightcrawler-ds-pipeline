import logging
import json
from pathlib import Path
import zipfile
import patoolib
import rarfile
from typing import List, Dict, Any
import torch

from transformers import pipeline
from nightcrawler.context import Context
from nightcrawler.base import (
    DomainLabels,
    PipelineResult,
    ExtractZyteData,
    BaseStep,
    Organization,
    ContentDomainData,
)
from nightcrawler.helpers.api import endpoint_api
from nightcrawler.helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class ContentDomainDetector(BaseStep):
    """
    Implementation of the content domain detection (step 8)
    
    This step concerns itself with models which determine if a page is likely to be related to medicines, medical devices, or any other category 
    relevant to the organization.
    """

    def __init__(
        self,
        context: Context,
        organization: Organization,
    ) -> None:
        """
        Initialize the ContentDomainDetector class.

        Args:
            context (Context):
                The context object.
            organization (Organization):
                TODO: We could use different models for different organizations.

        Returns:
            None
        """
        super().__init__(context)
        self.context = context
        self.organization = organization

        self.api = self._api_initialization()

    def _api_initialization(self):
        return endpoint_api.EndpointAPI(
            context=self.context,
            endpoint_url=self.context.settings.content_domain.endpoint,
            endpoint_auth_creds=(
                self.context.settings.content_domain.username,
                self.context.settings.content_domain.password,
            ),
            cache_name="content_domain_detection",
        )

    def _process_one(self, item: ExtractZyteData) -> Dict[str, Any]:
        """
        Predict the content domain of the given item.

        Args:
            item (ExtractZyteData):
                The item to predict the content domain for.

        Returns:
            Dict[str, Any]:
                A dictionary containing the domain and the probability.
                Check ContentDomainData class for more details.
        """
        logger.debug(f"Calling domain detection API for url: `{item.url}`")
        title = item.title if item.title else ""
        full_description = item.fullDescription if item.fullDescription else ""

        if not title and not full_description:
            logger.warning(f"Item does not contain any text content. url: `{item.url}`")
            return {
                "content_domain_label": DomainLabels.UNKNOWN.value,
                "content_domain_probability": 1.0,
            }

        #---------------------------------------------------------------------
        #
        # THIS HAS BEEN DEACTIVATED BY NICO FOR CHILE's DEPLOYMENT
        # THIS HAS BEEN DEACTIVATED BY NICO FOR CHILE's DEPLOYMENT
        # THIS HAS BEEN DEACTIVATED BY NICO FOR CHILE's DEPLOYMENT
        #
        #---------------------------------------------------------------------
        # Load and extract file
        ROOT_DIR = Path(__file__).parent.parent
        #model_path = ROOT_DIR / 'model' / 'classification_hf_pipeline.tar.gz'
        model_path = ROOT_DIR / 'model' / 'classification_hf_pipeline.rar'

        encoder_path = ROOT_DIR / 'model'
        
        print("Is file?", model_path.is_file())
        #patoolib.extract_archive(str(model_path), verbosity=-1, outdir=str(encoder_path))
        #with rarfile.RarFile(str(model_path), 'r') as rar_ref:
       #     rar_ref.extractall(str(encoder_path))
        

        logger.info(f"Loading preprocessor model path={model_path}")

        pipe = pipeline(
            model=str(encoder_path),
            tokenizer=str(encoder_path),
            task="text-classification",
            top_k=2,
            truncation="only_first",
        )

        logger.info("Classifier pipeline loaded successfully")

        params_path = f"{str(encoder_path)}/params.json"
        
        with open(params_path, "r") as f:
            params = json.load(f)
        threshold = params["threshold"]
        logger.info("Classifier params loaded successfully")

        text = title + " " + full_description
        outputs = pipe(text)[0]
        preds = {label_meta["label"]: label_meta["score"] for label_meta in outputs}
        y_pred_proba = preds["LABEL_1"]
        print('probability')
        print(y_pred_proba)
        
        print('CONTENT DOMAIN DETECTION FILTER DEACTIVATED FOR NOW')
        #api_response = self.api.call_api(playload={"text": title + " " + full_description})
        #prediction = api_response["response"]["prediction"]
        prediction = {"score":y_pred_proba,"label":"LABEL_1"}

        # TODO: this logic should be moved into corresponding API service
        probability = (
            prediction["score"]
            if prediction["label"] == "LABEL_1"
            else 1 - prediction["score"]
        )

        domain = DomainLabels.MEDICAL if probability > 0.5 else DomainLabels.OTHER

        logger.debug(
            f"Predicted domain: `{domain}` with probability: `{probability:.2f}`"
        )

        return {
            "content_domain_label": domain.value,
            "content_domain_probability": probability,
        }

    def _process_prev_results(
        self, previous_step_result: PipelineResult
    ) -> List[Dict[str, Any]]:
        """
        Process data using Method 1.

        Args:
            previous_step_result (PipelineResult):
                The result from the previous pipeline step.

        Returns:
            List[Dict[str, Any]]:
                A list of processed results.
        """
        results: List[Dict[str, Any]] = []
        irrelevant_results: List[Dict[str, Any]] = []

        prev_results: List[ExtractZyteData] = previous_step_result.relevant_results
        for item in prev_results:
            result = self._process_one(item)

            processed_data = ContentDomainData(**item.to_dict(), **result)

            if result["content_domain_label"] == DomainLabels.MEDICAL.value:
                results.append(processed_data)  # TODO: Wrap into ContentDomainData
            else:
                irrelevant_results.append(
                    processed_data
                )  # TODO: Wrap into ContentDomainData

        return results, irrelevant_results

    def apply_step(
        self,
        previous_step_results: PipelineResult,
    ) -> PipelineResult:
        """
        Apply the processing step to the previous results.

        Args:
            previous_step_results (PipelineResult):
                The result from the previous pipeline step.

        Returns:
            PipelineResult:
                Updated PipelineResult after processing.
        """
        results, irrelevant_results = self._process_prev_results(previous_step_results)

        # Update the PipelineResults Object
        pipeline_results = self.add_pipeline_steps_to_results(
            currentStepResults=results,
            pipelineResults=previous_step_results,
            currentStepIrrelevantResults=irrelevant_results,
        )

        return pipeline_results
