"""Package aggregator for the OOP modules."""

from .DatabaseManager import DatabaseManager
from .DataFetcher import DataFetcher
from .DataProcessor import DataProcessor
from .AISummarizer import AISummarizer
from .APISender import APISender
from .WorkFlowManager import WorkFlowManager
from .notifier import Notifier

__all__ = [
    "DatabaseManager",
    "DataFetcher",
    "DataProcessor",
    "AISummarizer",
    "APISender",
    "WorkFlowManager",
    "Notifier",
]
