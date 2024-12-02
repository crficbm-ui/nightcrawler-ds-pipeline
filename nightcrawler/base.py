import copy
import json
import logging
import enum
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, Iterator, List, Union
from collections.abc import Mapping
from datetime import datetime, timezone
from abc import ABC, abstractmethod
import importlib.resources

from nightcrawler.helpers.utils import _get_uuid, write_json
from nightcrawler.context import Context
from nightcrawler.helpers import LOGGER_NAME
import libnightcrawler.objects as lo

logger = logging.getLogger(LOGGER_NAME)


# ---------------------------------------------------
# Data Model - Abstract Classes used to either enforce specific class initialization or to provide common functionalities to its children classes.
# ---------------------------------------------------
@dataclass
class ObjectUtilitiesContainer(ABC, Mapping):
    """Abstract base class that allows for list-like object handling."""

    def to_dict(self) -> Dict[str, Any]:
        """
        Recursively converts the dataclass to a dictionary, excluding any fields that are None, -1, or empty strings.
        Handles nested dataclass instances as well.
        Returns:
            Dict[str, Optional[str]]: A dictionary representation of the instance with None fields removed.
        """

        def _filter(value):
            # Exclude None, -1, and empty strings
            return value is not None and value != -1 and value != ""

        def _recursive_asdict(obj):
            if isinstance(obj, list):
                return [_recursive_asdict(item) for item in obj if _filter(item)]
            elif isinstance(obj, dict):
                return {k: _recursive_asdict(v) for k, v in obj.items() if _filter(v)}
            elif hasattr(obj, "__dataclass_fields__"):
                return {
                    k: _recursive_asdict(v)
                    for k, v in asdict(obj).items()
                    if _filter(v)
                }
            else:
                return obj

        return _recursive_asdict(self)

    def get(self, attr: str, default: Any = None) -> Any:
        """
        Retrieves the value of the specified attribute.

        Args:
            attr (str): The name of the attribute to retrieve.
            default (Any): The value to return if the attribute is not found. Defaults to None.

        Returns:
            Any: The value of the attribute if it exists, otherwise the default value.
        """
        return getattr(self, attr, default)

    def __getitem__(self, key: str) -> Any:
        """
        Allows dict-like access to the attributes of the instance.
        The function will raise an error if the key is not found.

        Args:
            key (str): The attribute name to access.

        Returns:
            Any: The value of the attribute corresponding to the key.
        """
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(f"{key} not found in {self.__class__.__name__}")

    def __iter__(self) -> Iterator[str]:
        """
        Allows iteration over the attribute names of the instance.

        Returns:
            str: The attribute names.
        """
        return iter(self.to_dict())

    def __len__(self) -> int:
        """
        Returns:
            int: The number of non-None attributes.
        """
        return len(self.to_dict())

    def keys(self):
        """
        Returns:
            dict_keys: A view object that displays the keys of the dictionary
        representation of the instance.
        """
        return self.to_dict().keys()


# ---------------------------------------------------
# Data Model - Stage Classes
# ---------------------------------------------------


@dataclass
class Organization(lo.Organization):
    """Add specific elments to organization only used in pipeline"""

    language_codes: list[str]
    languages: list[str]
    country_codes: list[str]
    countries: list[str]
    currencies: list[str]
    settings: dict

    @staticmethod
    def get_all():
        with importlib.resources.as_file(
            importlib.resources.files("nightcrawler").joinpath("organizations.json")
        ) as path:
            with open(path, "r") as ifile:
                data = json.load(ifile)
                return {
                    org_name: Organization(name=org_name, **org_data)
                    for org_name, org_data in data.items()
                }


@dataclass
class MetaData(ObjectUtilitiesContainer):
    """Metadata class for storing information about the full pipeline run valid for all crawlresults"""

    keyword: str = field(default_factory=str)
    numberOfResults: int = field(default_factory=int)
    numberOfResultsAfterStage: int = field(default_factory=int)
    resultDate: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    )
    uuid: str = field(init=False)

    def __post_init__(self):
        self.uuid = _get_uuid(self.keyword, self.resultDate)


@dataclass
class ExtractSerpapiData(ObjectUtilitiesContainer):
    """Data class for step 1, 2 and 3 (all steps serpapi related): Extract URLs using Serpapi"""

    offerRoot: str
    url: str
    original_url: Optional[str] = None
    resolved_url: Optional[str] = None
    keywordEnriched: Optional[str] = (
        None  # this is only used for keyword enrichement and holds enriched keyword (i.e. keyword is 'viagra' and enriched is 'viagra kaufen'). This is only used when '-e' is set
    )
    keywordVolume: Optional[
        float
    ] = -1  # this is only used for keyword enrichement and indicated how often this enriched keyword is used based on dataforseo estimates. This is only used when '-e' is set
    keywordLanguage: Optional[str] = (
        None  # this is only used for keyword enrichement and indicated the language of the enriched keyword (i.e. 'viagra kaufen' would be 'DE'). This is only used when '-e' is set
    )
    keywordLocation: Optional[str] = (
        None  # this is only used for keyword enrichement and indicated the dataforseo localization option i.e. 'CH'. This is only used when '-e' is set
    )
    imageUrl: Optional[str] = (
        None  # this is only used for the reverse image search and indicates the direct url to the image
    )


@dataclass
class ExtractZyteData(ExtractSerpapiData):
    """Data class for step 4: Use Zyte to retrieve structured information from each URL collected by serpapi"""

    price: Optional[str] = None
    title: Optional[str] = None
    fullDescription: Optional[str] = None
    zyteExecutionTime: Optional[float] = 0.0
    html: Optional[str] = None
    zyteProbability: Optional[float] = 0.0
    images: list[str] = field(default_factory=list)


@dataclass
class ProcessData(ExtractZyteData):
    """Data class for step 5: Apply some (for the time-being) manual filtering logic: filter based on URL, currency and blacklists. All these depend on the --country input of the pipeline call.
    TODO replace the manual filtering logic with Mistral call by Nicolas W.


    TODO make this more abstract (not for CH but for any country) i.e.:
    country: Optional[str]
    countryInUrl: Optional[bool]
    webextensionInUrl: Optional[bool]
    currencyInUrl: Optional[bool]
    soldToCountry: Optional[bool]

    -> change the processor accordingly

    """

    ch_de_in_url: Optional[bool] = False
    swisscompany_in_url: Optional[bool] = False
    web_extension_in_url: Optional[bool] = False
    francs_in_url: Optional[bool] = False
    result_sold_CH: Optional[bool] = False


@dataclass
class DeliveryPolicyData(ProcessData):
    """Data class for step 6: delivery policy filtering based on offline analysis of domains public delivery information"""

    # TODO add fields relevant to only this step
    pass


@dataclass
class PageTypeData(DeliveryPolicyData):
    """Data class for step 7: page type filtering based on either a probability of Zyte (=default) or a custom BERT model deployed on the mutualized GPU. The pageType can be either 'ecommerce_product' or 'other'."""

    pageType: Optional[str] = None


@dataclass
class CorruptedContentData(PageTypeData):
    """Data class for step 8: blocked / corrupted content detection based the prediction with a BERT model."""

    is_corrupted_content: Optional[bool] = None
    corrupted_content_probability: Optional[float] = None


class DomainLabels(enum.Enum):
    MEDICAL = "medical"
    OTHER = "other"
    UNKNOWN = "unknown"


@dataclass
class ContentDomainData(CorruptedContentData):
    """Data class for step 9: classification of the product type is relvant to the target organization domain (i.e. pharmaceutical for Swissmedic AM or medical device for Swissmedic MD)"""

    content_domain_label: Optional[DomainLabels] = None
    content_domain_probability: Optional[float] = None


@dataclass
class ProcessSuspiciousnessData(ContentDomainData):
    """Data class for step 10: binary classifier per organisation, whether a product is classified as suspicious or not.
    TODO: maybe this class can be deleted and the Suspiciousness step could return directly CrawlResultData as most likely no new variables will come used after this step
    TODO: add fields relevant to only this step
    """

    pass


@dataclass
class CrawlResultData(ProcessSuspiciousnessData):
    """Data class for step 11: Apply any kinf of (rule-based?) ranking or filtering of results. If this last step is really needed needs be be confirmed, maybe this step will fall away."""

    # TODO add fields relevant to only this step
    pass


# ---------------------------------------------------
# Data Model - Class that will hold the MetaData and the Data per step. This is the main object that is passed from one step to the next and always append the new fields to the data objects and after the step will modify the MetaData.
# ---------------------------------------------------


@dataclass
class PipelineResult(ObjectUtilitiesContainer):
    """Class for storing a comprehensive report, including Zyte data."""

    meta: MetaData
    results: List[CrawlResultData]
    usage: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------
# Data Model - Abstract Classes providing shared functionalities and / or enforcing method implementation at children classes.
# ---------------------------------------------------


class BaseStep(ABC):
    """
    Class that provides core functionality for all steps in the pipeline:
        - enforces an apply function that is used uniformly as the entry point to a new step. We do not care what happens inside apply, just that it exists.
        - provides a method to store the results on the filesystem the pipeline is running on. There should be a CLI option similar to --local-results in
          TODO where the current behaviour is fine, but the default should be to store to S3.
    """

    _step_counter = 0

    def __init__(self, context: Context) -> None:
        self._entity_name = (
            self.__class__.__qualname__
        )  # Automatically set name for children
        self.context = context
        BaseStep._step_counter += 1

    def store_results(
        self, structured_results: PipelineResult, output_dir: str, filename: str
    ) -> None:
        """
        Stores the structured results into a JSON file.

        Args:
            structured_results (PipelineResult): The structured data to be stored.
            output_dir (str): The directory where the JSON file will be saved.
        """
        if not self.context.settings.store_intermediate:
            return

        structured_results_dict = structured_results.to_dict()
        path = f"{BaseStep._step_counter}_{filename}"
        if not self.context.settings.use_file_storage:
            blob_path = (output_dir + path).replace("/", "_")
            self.context.blob_client.put_processing(blob_path, structured_results_dict)
            return

        write_json(
            output_dir,
            path,
            structured_results_dict,
        )

    def add_pipeline_steps_to_results(
        self,
        currentStepResults: List[Any],
        pipelineResults: PipelineResult,
        currentStepResultsIsPipelineResultsObject=True,
        usage: dict[str, int] | None = None,
    ) -> PipelineResult:
        # Depending on the class implementation the currentStepResults is either a List of DataObjects (default) or already a PipelineResult Object.
        if currentStepResultsIsPipelineResultsObject:
            # If it is a PipelineResult object, it does contain the results of all previous steps and the current results.
            results = currentStepResults
            # Update the number of results after stage
            pipelineResults.meta.numberOfResultsAfterStage = len(currentStepResults)
        else:
            # If not, we have to append the list of DataObjects generated during the current step to the results of the last step.
            results = pipelineResults.results + currentStepResults
            pipelineResults.meta.numberOfResultsAfterStage = len(results)

        # Merge usages
        if usage is None:
            usage = dict()
        new_usage = copy.deepcopy(pipelineResults.usage)
        for k, v in usage.items():
            new_usage[k] = new_usage.get(k, 0) + v

        updatedResults = PipelineResult(
            meta=pipelineResults.meta, results=results, usage=new_usage
        )
        return updatedResults

    @abstractmethod
    def apply_step(self, *args: Any, **kwargs: Any) -> Any:
        """Enforces the apply_step method, leaves the implementation up for the children classes"""
        pass

    def apply(self, *args: Any, **kwargs: Any) -> PipelineResult:
        logger.info(f"Executing step {BaseStep._step_counter}: {self._entity_name}")
        results = self.apply_step(*args, **kwargs)
        self.context.crawlStatus = self._entity_name + " successfull"
        return results


class Extract(BaseStep):
    """
    Abstract base class that enforces methods to interact with an API, process its results, and store the data.

    Subclasses should implement the following methods:
        - initiate_client: Initiate the API client.
        - retrieve_response: Make the API calls and retrieve data.
        - structure_results: Postprocess the API results into the desired format.
        - store_results: Write the results to the file system.
        - apply: Run the entire data collection process.

    Attributes:
        context (Any): The context object containing configuration and settings.
    """

    @abstractmethod
    def initiate_client(self) -> Any:
        """
        Initializes the API client necessary for making requests.
        Should set up any authentication or connection requirements.

        Returns:
            Any: An initialized API client instance.
        """
        pass

    @abstractmethod
    def retrieve_response(
        self, *args: Any, **kwargs: Any
    ) -> Union[List[PipelineResult], List[PipelineResult]]:
        """
        Makes the API calls to retrieve data.

        Args:
            *args (Any): Positional arguments required by the API call.
            **kwargs (Any): Keyword arguments required by the API call.

        Returns:
            Union[List[PipelineResult], List[PipelineResult]]: The raw response data from the API, either as a list of `PipelineResult` or `PipelineResult` instances.
        """
        pass

    @abstractmethod
    def structure_results(
        self,
        data: Union[List[PipelineResult], List[PipelineResult]],
        *args: Any,
        **kwargs: Any,
    ) -> Union[List[str], List[Dict[str, Any]]]:
        """
        Processes and structures the raw API response data into the desired format.

        Args:
            data (Union[List[PipelineResult], List[PipelineResult]]): The raw data returned from the API.
            *args (Any): Additional positional arguments for structuring the data.
            **kwargs (Any): Additional keyword arguments for structuring the data.

        Returns:
            Union[List[str], List[Dict[str, Any]]]: The structured and processed data, either as a list of URLs (strings) or a list of dictionaries.
        """
        pass


# ---------------------------------------------------
# Process - Page types used in step 7: page type detection
# ---------------------------------------------------
class PageTypes:
    ECOMMERCE_PRODUCT = "ecommerce_product"
    ECOMMERCE_OTHER = "ecommerce_other"
    WEB_PRODUCT_ARTICLE = "web_product_article"
    WEB_ARTICLE = "web_article"
    BLOGPOST = "blogpost"
    OTHER = "other"


# ---------------------------------------------------
# Callback for counting
# ---------------------------------------------------
class CounterCallback:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self, count):
        self.value += count
