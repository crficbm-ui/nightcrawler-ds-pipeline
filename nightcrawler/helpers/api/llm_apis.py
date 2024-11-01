import os
import time
import base64
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict

from openai import OpenAI
from mistralai.client import MistralClient
from mistralai.models.chat_completion import ChatMessage

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.api.api_caller import APICaller


logger = logging.getLogger(LOGGER_NAME)


class MistralAPI(APICaller):
    def __init__(self, cache_name="llms", max_retries=3, retry_delay=2):
        super().__init__(
            cache_name=cache_name, max_retries=max_retries, retry_delay=retry_delay
        )
        self.client = MistralClient(api_key=os.environ["MISTRAL_API_KEY"])

    def call_api(self, prompt, config, force_refresh=False):
        data_hash = self._generate_hash((prompt, str(config)))

        if not force_refresh and self._is_cached(data_hash):
            logger.warning("Using cached response")
            return self._read_cache(data_hash)
        else:
            attempts = 0
            while attempts < self.max_retries:
                try:
                    start_time = time.time()
                    chat_response = self.client.chat(
                        messages=[ChatMessage(role="user", content=prompt)],
                        model=config.get("model", "open-mixtral-8x7b"),
                        temperature=config.get("temperature", 0.7),
                        top_p=config.get("top_p", 1),
                        max_tokens=config.get("max_tokens", None),
                        response_format=config.get("response_format", None),
                    )

                    end_time = time.time()
                    response = {
                        "content": chat_response.choices[0].message.content,
                        "prompt_tokens": chat_response.usage.prompt_tokens,
                        "completion_tokens": chat_response.usage.completion_tokens,
                        "model": chat_response.model,
                        "seconds_taken": end_time - start_time,
                        "created": chat_response.created,
                    }
                    if not response["content"]:
                        raise Exception("Empty response received")
                    self._write_cache(data_hash, response)
                    return response
                except Exception as e:
                    logger.warning(
                        f"API call failed with error: {e}. Retrying in {self.retry_delay} seconds..."
                    )
                    attempts += 1
                    time.sleep(self.retry_delay)
            raise Exception("All API call attempts failed.")


@dataclass
class OpanaiConfig:
    model: str = field(
        metadata={
            "help": "ID of the model to use. Supported models: gpt-4o, gpt-4-turbo, gpt-4, gpt-3.5-turbo."
        }
    )
    response_format: Optional[Dict[str, str]] = field(
        default=None,
        metadata={
            "help": """Setting to { "type": "json_object" } enables JSON mode, which guarantees the message the model generates is valid JSON."""
        },
    )
    temperature: Optional[float] = field(
        default=1.0, metadata={"help": "Sampling temperature to use, between 0 and 2."}
    )
    top_p: Optional[float] = field(
        default=1.0,
        metadata={
            "help": "Nucleus sampling parameter, where the model considers the results of the tokens with top_p probability mass."
        },
    )
    frequency_penalty: Optional[float] = field(
        default=0.0,
        metadata={
            "help": "Number between -2.0 and 2.0 to penalize new tokens based on their existing frequency in the text so far."
        },
    )
    max_tokens: Optional[int] = field(
        default=256,
        metadata={
            "help": "The maximum number of tokens that can be generated in the chat completion."
        },
    )
    presence_penalty: Optional[float] = field(
        default=0.0,
        metadata={
            "help": "Number between -2.0 and 2.0 to penalize new tokens based on whether they appear in the text so far."
        },
    )

    seed: Optional[int] = field(
        default=None,
        metadata={
            "help": "If specified, the system will make a best effort to sample deterministically."
        },
    )

    def to_dict(self):
        return asdict(self)


class OpenaiAPI(APICaller):
    def __init__(self, cache_name="llms", max_retries=3, retry_delay=2):
        super().__init__(
            cache_name=cache_name, max_retries=max_retries, retry_delay=retry_delay
        )
        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    def call_api(self, prompt, config: Dict, force_refresh=False):
        data_hash = self._generate_hash((str(prompt), str(config)))

        if not force_refresh and self._is_cached(data_hash):
            logger.warning("Using cached response")
            return self._read_cache(data_hash)
        else:
            attempts = 0
            while attempts < self.max_retries:
                try:
                    start_time = time.time()
                    chat_response = self.client.chat.completions.create(
                        messages=[
                            {"role": "user", "content": prompt},
                        ],
                        **config,
                    )
                    end_time = time.time()
                    response = {
                        "content": chat_response.choices[0].message.content,
                        "finish_reason": chat_response.choices[0].finish_reason,
                        "prompt_tokens": chat_response.usage.prompt_tokens,
                        "completion_tokens": chat_response.usage.completion_tokens,
                        "model": chat_response.model,
                        "seconds_taken": end_time - start_time,
                        "created": chat_response.created,
                        "created_date": time.strftime(
                            "%Y-%m-%d %H:%M:%S", time.localtime(chat_response.created)
                        ),
                    }
                    self._write_cache(data_hash, response)
                    return response
                except Exception as e:
                    logger.info(
                        f"API call failed: {e}. Retrying in {self.retry_delay} seconds..."
                    )
                    attempts += 1
                    time.sleep(self.retry_delay)
            raise Exception("All retries failed")


def local_image_to_base64_url(image_path):
    with open(image_path, "rb") as image_file:
        image_bytes = image_file.read()
    base64_image = base64.b64encode(image_bytes).decode("utf-8")
    return f"data:image/jpeg;base64,{base64_image}"


def make_gpt_image_content_from_image(prompt, image_path, detail="auto"):
    """
    Creates a list of content for GPT image generation from an image file.

    Args:
        prompt (str): The prompt text for the GPT model.
        image_path (str): The path to the image file.
        detail (str, optional): The level of detail for the image. "low", "high", or "auto". Defaults to "auto".

    Returns:
        list: A list of dictionaries representing the content for GPT image generation.
    """
    return [
        {"type": "text", "text": prompt},
        {
            "type": "image_url",
            "image_url": {
                "url": local_image_to_base64_url(image_path),
                "detail": detail,
            },
        },
    ]
