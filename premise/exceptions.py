class PremiseErrors(Exception):
    """A base class for premise exceptions."""


class NoCandidateInDatabase(PremiseErrors):
    """A specific class."""
