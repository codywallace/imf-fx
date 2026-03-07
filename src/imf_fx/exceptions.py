class ImfFxError(Exception):
    """Base error for imf-fx."""


class StructureNotFound(ImfFxError):
    """Raised when expected IMF structure/codelist content is missing."""


class SdmxParseError(ImfFxError):
    """Raised when SDMX payload cannot be parsed as expected."""


class InvalidIndicatorError(ImfFxError):
    """Raised when an indicator/base/quote combination is not supported by the ER dataflow."""


class DataFetchError(ImfFxError):
    """Raised when data could not be fetched (after retries / splits)."""
