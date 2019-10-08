class Ecoinvent_Database():
    """Hosts ecoinvent database name and version.

    :ivar name: name of the ecoinvent database
    :vartype destination_db: str
    :ivar version: version of the ecoinvent database
    :vartype destination_db: float

    """
    def __init__(self, name, version):
        self.name = name
        self.version = version
