from abc import ABC, abstractmethod
from typing import Dict, List

from tqdm import tqdm

class BaseLogic(ABC):
    @abstractmethod
    def apply_one(self, item: Dict) -> Dict:
        """Abstract method that needs to be implemented in a subclass."""
        pass

    def apply_batch(self, items: List[Dict[str, Dict]]) -> List[Dict]:
        results = []
        for item in tqdm(items):
            stage_result = self.apply_one(item)
            results.append(stage_result)
        return results
