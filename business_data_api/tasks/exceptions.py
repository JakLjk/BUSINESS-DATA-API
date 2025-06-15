class EntityNotFoundException(Exception):
    """Exception raised when an entity is not found."""
    pass

class InvalidParameterException(Exception):
    """Exception raised for invalid parameters."""
    pass

class ScrapingFunctionFailed(Exception):
    """Exception raised when defined scraping function failed.
    Failure is almost always triggered by changed website structure and references"""
    pass

class WebpageThrottlingException(Exception):
    """Error raised when scraped webpage raises throttling error"""
    pass