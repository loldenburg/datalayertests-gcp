from typing import Optional

from firebase_admin import firestore, initialize_app
from google.cloud.firestore_v1 import Client


class FireRef:
    """Provides helper accessors to the commonly-used Firestore collection references."""

    __CLIENT: Optional[Client] = None

    @classmethod
    def client(cls) -> Client:
        """Returns a cached Firestore client."""

        if cls.__CLIENT is None:
            initialize_app()
            cls.__CLIENT = firestore.client()
        return cls.__CLIENT


    @classmethod
    def collectionDynamic(cls, collection):
        """Returns a dynamic reference to the collection of choice"""
        return cls.client().collection(collection)
