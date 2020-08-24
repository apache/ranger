from apache_ranger.model.ranger_service     import RangerService
from apache_ranger.client.ranger_client     import RangerClient

ranger_client = RangerClient('http://ranger:6080', 'admin', 'rangerR0cks!')

service = RangerService(name='dev_hdfs', type='hdfs', configs={'username':'hdfs', 'password':'hdfs', 'fs.default.name': 'hdfs://ranger-hadoop:9000', 'hadoop.security.authentication': 'simple', 'hadoop.security.authorization': 'true'})

ranger_client.create_service(service)
