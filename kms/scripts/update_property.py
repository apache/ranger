import sys
import os
from xml.etree import ElementTree as ET

def write_properties_to_xml(xml_path, property_name='', property_value=''):
	if(os.path.isfile(xml_path)):
		xml = ET.parse(xml_path)
		root = xml.getroot()
		for child in root.findall('property'):
			name = child.find("name").text.strip()
			if name == property_name:
				child.find("value").text = property_value
		xml.write(xml_path)
		return 0
	else:
		return -1


if __name__ == '__main__':
	if(len(sys.argv) > 1):
		parameter_name = sys.argv[1] if len(sys.argv) > 1  else None
		parameter_value = sys.argv[2] if len(sys.argv) > 2  else None
		file_path = sys.argv[3] if len(sys.argv) > 3  else None
		write_properties_to_xml(file_path,parameter_name,parameter_value)
