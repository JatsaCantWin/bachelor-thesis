from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Union
import csv
import itertools
from datetime import datetime

class DatabaseTestingInterface (ABC):
    @abstractmethod
    def create(self, rows_created: int = 1, transactions: int = 1):
        pass

    @abstractmethod
    def read(self, rows_read: int = 1, transactions: int = 1):
        pass

    @abstractmethod
    def update(self, rows_updated: int = 1, transactions: int = 1):
        pass

    @abstractmethod
    def delete(self, rows_deleted: int = 1, transactions: int = 1):
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def getName(self) -> str:
        pass