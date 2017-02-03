#!/usr/bin/env python2
# Copyright 2016, RadiantBlue Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import requests
from requests.auth import HTTPBasicAuth
import json
import re
import sys

def getBadLayer(geoserverUri):
	# WMS Request to Root GeoServer Layer
	uri = geoserverUri + '/geoserver/wms?request=GetCapabilities&service=wms&version=1.1.1'
	response = requests.get(uri)
	if 'Error occurred trying to write out metadata for layer:' in response.text:
		# A Bad Layer is found. Return the name.
		guid = re.findall("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", response.text)
		return guid[0]
	return None

def deleteLayer(geoserverUri, username, password, layer):
	# Deletes a bad GeoServer Layer/Data Store
	uri = geoserverUri + '/geoserver/rest/layers/' + layer
	response = requests.delete(uri, auth=HTTPBasicAuth(username, password))
	print 'Culling ' + layer + ', response was ' + str(response.status_code)
	if response.status_code == 500:
		if 'Unable to delete layer referenced by layer group' in response.text:
			# Delete the Layer Group
			guid = re.findall("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", response.text)
			deleteUri = geoserverUri + '/geoserver/rest/workspaces/piazza/layergroups/' + guid[0] + '.json'
			response = requests.delete(deleteUri, auth=HTTPBasicAuth(username, password))
			print 'Culled Layer Group ' + guid[0] + ', response was ' + str(response.status_code)
			if response.status_code == 500:
				print response.text	
			# Try to Delete the Layer again
			response = requests.delete(uri, auth=HTTPBasicAuth(username, password))
			print 'Retry culling ' + layer + ', response was ' + str(response.status_code)
			if response.status_code == 500:
				print response.text
				print 'Could not delete layer. Exiting.'
				sys.exit(1)
		else:
			print 'Could not delete layer. Exiting.'
			sys.exit(1)

def main():
	# Pull in required variables from command line
    parser = argparse.ArgumentParser(
        description='Cull corrupted layers from GeoServer.')
    parser.add_argument('-g', help='GeoServer URI')
    parser.add_argument('-u', help='GeoServer UserName')
    parser.add_argument('-p', help='GeoServer Password')
    args = parser.parse_args()
    geoserverUri = args.g
    username = args.u
    password = args.p

    # Check for Bad Layers
    print 'Begin culling of Bad Layers'
    badLayer = getBadLayer(geoserverUri)
    while (badLayer is not None):
    	# Delete the Layer
    	deleteLayer(geoserverUri, username, password, badLayer)
    	# Check Again
    	badLayer = getBadLayer(geoserverUri)

    # No more Bad Layers, nothing to do
    print 'Done culling Bad Layers'

if __name__ == "__main__":
    main()
