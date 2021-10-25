from apache_ranger.model.ranger_service     import RangerService
from apache_ranger.client.ranger_client     import RangerClient

ranger_client = RangerClient('http://ranger:6080', ('admin', 'rangerR0cks!'))

service = RangerService({'name': 'dev_knox', 'type': 'knox', 'configs': {'username':'knox', 'password':'knox', 'knox.url': 'http://ranger-hadoop:8088'}})

ranger_client.create_service(service)
