class ImfFxError(Exception):
    """Base error for imf-fx."""


class StructureNotFound(ImfFxError):
    """Raised when expected IMF structure/codelist content is missing."""


class SdmxParseError(ImfFxError):
    """Raised when SDMX payload cannot be parsed as expected."""
