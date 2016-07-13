/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package access.deploy.geoserver;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the JSON Model that GeoServer uses to transfer information
 * regarding Layer Groups. This is used with the GeoServer REST API to create or
 * update Layer Groups.
 * 
 * @author Patrick.Doody
 *
 */
public class LayerGroupModel {
	public LayerGroup layerGroup = new LayerGroup();

	public class LayerGroup {
		public String name;
		public String mode = "SINGLE";
		public Publishable publishables = new Publishable();
		public Styles styles = new Styles();
	}

	public class Publishable {
		public List<GroupLayer> published = new ArrayList<GroupLayer>();
	}

	public static class GroupLayer {
		@JsonProperty(value = "@type")
		public String type = "layer";
		public String name;
	}

	public class Styles {
		public List<String> style = new ArrayList<String>();
	}
}
