from marimba.platforms.instruments.base import Instrument


class NoopInstrument(Instrument):
    """
    No-op instrument. Does nothing; useful for testing.
    """

    def __init__(self, root: str, collection_config: dict, instrument_config: dict):
        super().__init__(root, collection_config, instrument_config)
        self.logger.info("NoopInstrument initialized")
