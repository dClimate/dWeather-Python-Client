class IPFSError(Exception):
    """Base class for exceptions in this module."""
    pass

class DatasetError(IPFSError):
    """Excpetion raised when a dataset cannot be found on ipfs"""
    pass

class InputOutOfRangeError(IPFSError):
    """Exception raised for lat/lon requested outside the valid range for a dataset"""
    pass

class CoordinateNotFoundError(IPFSError):
    """Exception raised when a lat/lon coordinate pair does not have a file on the server"""
    pass

class DataMalformedError(IPFSError):
    """Raised when a grid cell text file is unable to be parsed according to metadata"""
    pass