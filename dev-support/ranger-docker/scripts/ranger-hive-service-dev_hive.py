from apache_ranger.model.ranger_service     import RangerService
from apache_ranger.client.ranger_client     import RangerClient

ranger_client = RangerClient('http://ranger:6080', 'admin', 'rangerR0cks!')

service = RangerService(name='dev_hive', type='hive', configs={'username':'hive', 'password':'hive', 'jdbc.driverClassName': 'org.apache.hive.jdbc.HiveDriver', 'jdbc.url': 'jdfb:hive2://ranger-hadoop:10000', 'hadoop.security.authorization': 'true'})

ranger_client.create_service(service)
