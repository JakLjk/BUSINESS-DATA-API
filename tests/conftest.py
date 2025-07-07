import warnings
from bs4 import XMLParsedAsHTMLWarning

def pytest_configure(config):
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)