from abc import ABC, abstractmethod
from lasair_consumer import msgConsumer

class lasair_zooniverse_base_class(ABC):

    @abstractmethod
    def query_lasair_topic(self): pass

    @abstractmethod
    def gather_metadata(self): pass

    @abstractmethod
    def parse_object_data(self): pass
    
    @abstractmethod
    def build_plots(self): pass

    @abstractmethod
    def create_subjects_and_link_to_project(self): pass


