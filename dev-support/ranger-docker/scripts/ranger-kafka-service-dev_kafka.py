from apache_ranger.model.ranger_service     import RangerService
from apache_ranger.client.ranger_client     import RangerClient

ranger_client = RangerClient('http://ranger:6080', 'admin', 'rangerR0cks!')

service = RangerService(name='dev_kafka', type='kafka', configs={'username':'kafka', 'password':'kafka', 'zookeeper.connect': 'ranger-kafka:2181'})

ranger_client.create_service(service)
