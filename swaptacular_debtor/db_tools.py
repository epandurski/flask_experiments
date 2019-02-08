import os
import struct
from sqlalchemy.exc import IntegrityError
from .extensions import db


class ShardingKeyGenerationMixin:
    """Adds sharding key generation functionality to a model.

    The model should be defined as follows::

      class SomeModelName(ShardingKeyGenerationMixin, db.Model):
          sharding_key_value = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    """

    def __init__(self, sharding_key_value=None):
        modulo = 1 << 63
        if sharding_key_value is None:
            sharding_key_value = struct.unpack('>q', os.urandom(8))[0] % modulo or 1
        assert 0 < sharding_key_value < modulo
        self.sharding_key_value = sharding_key_value

    @classmethod
    def generate(cls, *, sharding_key_value=None, tries=50):
        """Create a unique instance and return its `sharding_key_value`."""

        for _ in range(tries):
            instance = cls(sharding_key_value=sharding_key_value)
            db.session.begin_nested()
            db.session.add(instance)
            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                continue
            return instance.sharding_key_value
        raise RuntimeError('Can not generate a unique sharding key.')
