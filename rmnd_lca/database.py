class EcoinventDatabase():
    """Hosts ecoinvent database name and version.

    :ivar name: name of the ecoinvent database
    :vartype name: str
    :ivar version: version of the ecoinvent database
    :vartype version: float

    """
    def __init__(self, name, version):
        self.name = name
        self.version = version
