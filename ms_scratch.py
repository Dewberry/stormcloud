### Test Building index for Upper Green V01 ###
import os
from meilisearch import Client

index_name = "upper-green"
ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])
ms_client.create_index(index_name, {"primaryKey": "start.timestamp"})
ms_client.index(index_name).delete()
